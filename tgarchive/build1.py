import logging
import math
import os
import pkg_resources
import shutil
from collections import OrderedDict, deque

import magic
import commonmark
from feedgen.feed import FeedGenerator
from jinja2 import Template

from .db import User, Message
from .telegram_format import TelegramFormatter


class Build:
    def __init__(self, config, db, symlink: bool):
        self.config = config
        self.db = db
        self.symlink = symlink

        self.template: Template | None = None
        self.rss_template: Template | None = None

        # message_id -> filename
        self.page_ids: dict[int, str] = {}

        # year -> [month]
        self.timeline: OrderedDict[int, list] = OrderedDict()

        self._mime_cache: dict[str, str] = {}

        # Telegram + Markdown
        self._tg_formatter = TelegramFormatter()
        self._md_parser = commonmark.Parser()
        self._md_renderer = commonmark.HtmlRenderer()

    # ======================================================
    # Public API
    # ======================================================

    def build(self):
        self._create_publish_dir()

        timeline = list(self.db.get_timeline())
        if not timeline:
            logging.info("no data found to publish site")
            return

        self._build_timeline_index(timeline)
        self._collect_page_ids(timeline)

        rss_entries = deque([], self.config["rss_feed_entries"])
        last_rendered = None

        for month in timeline:
            dayline = self._get_dayline(month)
            total = self.db.get_message_count(month.date.year, month.date.month)
            total_pages = math.ceil(total / self.config["per_page"])

            last_id = 0
            page = 0

            while True:
                messages = list(
                    self.db.get_messages(
                        month.date.year,
                        month.date.month,
                        last_id,
                        self.config["per_page"],
                    )
                )
                if not messages:
                    break

                page += 1
                fname = self.make_filename(month, page)
                last_rendered = fname
                last_id = messages[-1].id

                if self.config["publish_rss_feed"]:
                    rss_entries.extend(messages)

                self._render_page(
                    messages=messages,
                    month=month,
                    dayline=dayline,
                    fname=fname,
                    page=page,
                    total_pages=total_pages,
                )

        self._build_index(last_rendered)

        if self.config["publish_rss_feed"]:
            self._build_rss(rss_entries)

    # ======================================================
    # Template loading
    # ======================================================

    def load_template(self, fname: str):
        with open(fname, "r", encoding="utf-8") as f:
            self.template = Template(f.read(), autoescape=True)

    def load_rss_template(self, fname: str):
        with open(fname, "r", encoding="utf-8") as f:
            self.rss_template = Template(f.read(), autoescape=True)

    # ======================================================
    # Timeline & pagination
    # ======================================================

    def _build_timeline_index(self, timeline):
        for month in timeline:
            self.timeline.setdefault(month.date.year, []).append(month)

    def _collect_page_ids(self, timeline):
        for month in timeline:
            last_id = 0
            page = 0

            while True:
                messages = list(
                    self.db.get_messages(
                        month.date.year,
                        month.date.month,
                        last_id,
                        self.config["per_page"],
                    )
                )
                if not messages:
                    break

                page += 1
                fname = self.make_filename(month, page)

                for m in messages:
                    self.page_ids[m.id] = fname

                last_id = messages[-1].id

    def _get_dayline(self, month):
        dayline = OrderedDict()
        for d in self.db.get_dayline(
            month.date.year, month.date.month, self.config["per_page"]
        ):
            dayline[d.slug] = d
        return dayline

    def make_filename(self, month, page: int) -> str:
        return f"{month.slug}{'_' + str(page) if page > 1 else ''}.html"

    # ======================================================
    # HTML rendering
    # ======================================================

    def _render_page(self, *, messages, month, dayline, fname, page, total_pages):
        if not self.template:
            raise RuntimeError("HTML template not loaded")

        html = self.template.render(
            config=self.config,
            timeline=self.timeline,
            dayline=dayline,
            month=month,
            messages=messages,
            page_ids=self.page_ids,
            pagination={"current": page, "total": total_pages},
            make_filename=self.make_filename,
            nl2br=self._markdown,  # backward compatible name
        )

        with open(
            os.path.join(self.config["publish_dir"], fname),
            "w",
            encoding="utf-8",
        ) as f:
            f.write(html)

    def _build_index(self, fname):
        if not fname:
            return

        dst = os.path.join(self.config["publish_dir"], "index.html")
        src = os.path.join(self.config["publish_dir"], fname)

        if self.symlink:
            os.symlink(fname, dst)
        else:
            shutil.copy(src, dst)

    # ======================================================
    # RSS
    # ======================================================

    def _build_rss(self, messages):
        f = FeedGenerator()
        f.id(self.config["site_url"])
        f.generator(
            f"tg-archive {pkg_resources.get_distribution('tg-archive').version}"
        )
        f.link(href=self.config["site_url"], rel="alternate")
        f.title(self.config["site_name"].format(group=self.config["group"]))
        f.subtitle(self.config["site_description"])

        for m in messages:
            self._add_rss_entry(f, m)

        pubdir = self.config["publish_dir"]
        f.rss_file(os.path.join(pubdir, "index.xml"), pretty=True)
        f.atom_file(os.path.join(pubdir, "index.atom"), pretty=True)

    def _add_rss_entry(self, feed, m: Message):
        url = f"{self.config['site_url']}/{self.page_ids[m.id]}#{m.id}"

        e = feed.add_entry()
        e.id(url)
        e.title(f"@{m.user.username} on {m.date} (#{m.id})")
        e.link({"href": url})
        e.published(m.date)

        media_mime = ""
        if m.media and m.media.url:
            media_mime = self._attach_media(e, m)

        e.content(self._make_abstract(m), type="html")

    def _attach_media(self, entry, m: Message):
        murl = (
            f"{self.config['site_url']}/"
            f"{os.path.basename(self.config['media_dir'])}/{m.media.url}"
        )
        media_path = os.path.join(self.config["media_dir"], m.media.url)

        size = "0"
        mime = "application/octet-stream"

        if os.path.exists(media_path):
            try:
                size = str(os.path.getsize(media_path))
                mime = self._detect_mime(media_path)
            except OSError:
                pass
        elif "://" in media_path:
            mime = "text/html"

        entry.enclosure(murl, size, mime)
        return mime

    def _detect_mime(self, path):
        if path in self._mime_cache:
            return self._mime_cache[path]

        try:
            mime = magic.from_file(path, mime=True)
        except Exception:
            mime = "application/octet-stream"

        self._mime_cache[path] = mime
        return mime

    def _make_abstract(self, m: Message):
        text = m.content or (m.media.title if m.media else "") or ""
        return self._markdown(text)

    # ======================================================
    # Telegram → CommonMark → HTML
    # ======================================================

    def _markdown(self, text: str) -> str:
        if not text:
            return ""

        # Telegram → CommonMark
        text = self._tg_formatter.convert(text)

        # CommonMark → HTML
        ast = self._md_parser.parse(text)
        return self._md_renderer.render(ast)

    # ======================================================
    # Filesystem
    # ======================================================

    def _create_publish_dir(self):
        pubdir = os.path.abspath(self.config["publish_dir"])

        if pubdir in ("/", os.path.expanduser("~")):
            raise RuntimeError(f"Refusing to delete dangerous directory: {pubdir}")

        if os.path.exists(pubdir):
            shutil.rmtree(pubdir)

        os.mkdir(pubdir)
        self._copy_static(pubdir)
        self._copy_media(pubdir)

    def _copy_static(self, pubdir):
        static = self.config["static_dir"]
        target = os.path.join(pubdir, static)

        if self.symlink:
            self._relative_symlink(os.path.abspath(static), target)
        elif os.path.isfile(static):
            shutil.copyfile(static, target)
        else:
            shutil.copytree(static, target)

    def _copy_media(self, pubdir):
        mediadir = self.config["media_dir"]
        if not os.path.exists(mediadir):
            return

        target = os.path.join(pubdir, os.path.basename(mediadir))
        if self.symlink:
            self._relative_symlink(os.path.abspath(mediadir), target)
        else:
            shutil.copytree(mediadir, target)

    def _relative_symlink(self, src, dst):
        dst_dir = os.path.dirname(dst)
        rel_src = os.path.relpath(src, dst_dir)
        os.symlink(rel_src, dst)
