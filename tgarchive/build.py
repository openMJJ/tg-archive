import logging
import math
import os
import shutil
import json
from collections import OrderedDict, deque
from pathlib import Path
from typing import Dict

from importlib.metadata import version as pkg_version

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

        self.page_ids: Dict[int, str] = {}
        self.timeline: OrderedDict[int, list] = OrderedDict()
        self._mime_cache: Dict[str, str] = {}

        self._tg_formatter = TelegramFormatter()
        self._md_parser = commonmark.Parser()
        self._md_renderer = commonmark.HtmlRenderer()

    # ======================================================
    # Build
    # ======================================================

    def build(self):
        self._prepare_publish_dir()

        timeline = list(self.db.get_timeline())
        if not timeline:
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
        self._build_search_index(timeline)

        if self.config["publish_rss_feed"]:
            self._build_rss(rss_entries)

    # ======================================================
    # Template
    # ======================================================

    def load_template(self, fname: str):
        with open(fname, "r", encoding="utf-8") as f:
            self.template = Template(f.read(), autoescape=True)

    # ======================================================
    # Timeline helpers
    # ======================================================

    def _build_timeline_index(self, timeline):
        for m in timeline:
            self.timeline.setdefault(m.date.year, []).append(m)

    def _collect_page_ids(self, timeline):
        for m in timeline:
            last_id = 0
            page = 0
            while True:
                msgs = list(
                    self.db.get_messages(
                        m.date.year, m.date.month, last_id, self.config["per_page"]
                    )
                )
                if not msgs:
                    break
                page += 1
                fname = self.make_filename(m, page)
                for msg in msgs:
                    self.page_ids[msg.id] = fname
                last_id = msgs[-1].id

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
    # Rendering
    # ======================================================

    def _render_page(self, *, messages, month, dayline, fname, page, total_pages):
        html = self.template.render(
            config=self.config,
            timeline=self.timeline,
            dayline=dayline,
            month=month,
            messages=messages,
            page_ids=self.page_ids,
            pagination={"current": page, "total": total_pages},
            make_filename=self.make_filename,
            nl2br=self._markdown,
            markdown=self._markdown,
        )

        html = self._inject_search_ui(html)
        (Path(self.config["publish_dir"]) / fname).write_text(html, encoding="utf-8")

    def _build_index(self, fname):
        if not fname:
            return
        pub = Path(self.config["publish_dir"])
        dst = pub / "index.html"
        if dst.exists():
            dst.unlink()
        if self.symlink:
            dst.symlink_to(fname)
        else:
            shutil.copyfile(pub / fname, dst)

    # ======================================================
    # Search index
    # ======================================================

    def _build_search_index(self, timeline):
        records = []
        for m in timeline:
            last_id = 0
            while True:
                msgs = list(
                    self.db.get_messages(
                        m.date.year, m.date.month, last_id, self.config["per_page"]
                    )
                )
                if not msgs:
                    break
                for msg in msgs:
                    raw = (msg.content or "").strip()
                    if not raw:
                        continue
                    records.append(
                        {
                            "id": msg.id,
                            "user": msg.user.username if msg.user else "",
                            "date": msg.date.isoformat(),
                            "text": raw,
                            "html": self._markdown(raw),
                            "url": f"{self.page_ids[msg.id]}#{msg.id}",
                        }
                    )
                last_id = msgs[-1].id

        (Path(self.config["publish_dir"]) / "search.json").write_text(
            json.dumps(records, ensure_ascii=False),
            encoding="utf-8",
        )

    # ======================================================
    # Search UI（完全保持你给的版本）
    # ======================================================

    def _inject_search_ui(self, html: str) -> str:
        if "</body>" not in html:
            return html
        return html.replace("</body>", self._search_ui_block() + "\n</body>")

    def _search_ui_block(self) -> str:
        return r"""
<style>
#search-btn{position:fixed;right:24px;bottom:24px;width:52px;height:52px;
border-radius:50%;background:#38bdf8;color:#020617;display:flex;
align-items:center;justify-content:center;font-size:24px;cursor:pointer;
z-index:9998}
#search-overlay{position:fixed;inset:0;background:rgba(0,0,0,.65);
backdrop-filter:blur(6px);z-index:9999;display:none;
align-items:flex-start;justify-content:center;padding-top:8vh}
#search-overlay.active{display:flex}
#search-dialog{width:min(920px,96vw);background:#020617;
border:1px solid #1e293b;border-radius:14px;overflow:hidden}
#search-input{width:100%;padding:18px;font-size:17px;border:none;
outline:none;background:#020617;color:#f8fafc;
border-bottom:1px solid #1e293b}
#search-results{max-height:70vh;overflow-y:auto;padding:8px}
.search-item{background:#020617;border:1px solid #1e293b;
border-radius:10px;padding:18px 20px;margin-bottom:12px}
.search-user{font-size:13px;color:#7dd3fc;margin-bottom:10px}
.search-text{font-size:16px;line-height:1.75;color:#f8fafc}
.search-text p{margin:0 0 1em 0}
mark.search-hit{background:#fde047;color:#020617;
padding:0 3px;border-radius:4px}
</style>

<div id="search-btn">🔍</div>

<div id="search-overlay">
  <div id="search-dialog">
    <input id="search-input" type="search" placeholder="搜索消息…" />
    <div id="search-results"></div>
  </div>
</div>

<script>
let DATA=null;

function highlight(html,q){
  if(!q) return html;
  const re=new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g,"\\$&"),"gi");
  return html.replace(re,m=>`<mark class="search-hit">${m}</mark>`);
}

async function loadData(){
  if(DATA) return DATA;
  const r=await fetch("/search.json");
  DATA=await r.json();
  return DATA;
}

function openSearch(){
  document.getElementById("search-overlay").classList.add("active");
  const i=document.getElementById("search-input");
  i.value="";i.focus();
}

function closeSearch(){
  document.getElementById("search-overlay").classList.remove("active");
}

document.getElementById("search-btn").onclick=openSearch;

document.addEventListener("keydown",e=>{
  if(e.key==="/"&&!e.target.matches("input,textarea")){
    e.preventDefault();openSearch();
  }
  if(e.key==="Escape")closeSearch();
});

document.getElementById("search-input").addEventListener("input",async e=>{
  const q=e.target.value.trim();
  const box=document.getElementById("search-results");
  box.innerHTML="";
  if(!q) return;
  const data=await loadData();
  let n=0;
  for(const m of data){
    if(m.text.toLowerCase().includes(q.toLowerCase())){
      const d=document.createElement("div");
      d.className="search-item";
      d.innerHTML=`<div class="search-user">@${m.user} · ${m.date}</div>
                   <div class="search-text">${highlight(m.html,q)}</div>`;
      d.onclick=()=>location.href=m.url;
      box.appendChild(d);
      if(++n>=50) break;
    }
  }
});
</script>
"""

    # ======================================================
    # RSS
    # ======================================================

    def _build_rss(self, messages):
        f = FeedGenerator()
        f.id(self.config["site_url"])
        f.generator(f"tg-archive {pkg_version('tg-archive')}")
        f.link(href=self.config["site_url"], rel="alternate")
        f.title(self.config["site_name"].format(group=self.config["group"]))
        f.subtitle(self.config["site_description"])
        for m in messages:
            self._add_rss_entry(f, m)
        pubdir = self.config["publish_dir"]
        f.rss_file(os.path.join(pubdir, "index.xml"), pretty=True)
        f.atom_file(os.path.join(pubdir, "index.atom"), pretty=True)

    def _add_rss_entry(self, feed, m: Message):
    # 使用 m 代替 msg
        url = f"{self.config['site_url']}/{self.page_ids[m.id]}#{m.id}"  # 修改这里的 'msg' 为 'm'
        e = feed.add_entry()
        e.id(url)
        e.title(f"@{m.user.username} · {m.date}")
        e.link({"href": url})
        e.published(m.date)
        e.content(self._markdown(m.content or ""), type="html")

    # ======================================================
    # Markdown
    # ======================================================

    def _markdown(self, text: str) -> str:
        if not text:
            return ""
        text = self._tg_formatter.convert(text)
        ast = self._md_parser.parse(text)
        return self._md_renderer.render(ast)

    # ======================================================
    # Filesystem（关键修复）
    # ======================================================

    def _prepare_publish_dir(self):
        pubdir = Path(self.config["publish_dir"]).resolve()
        if pubdir.exists():
            shutil.rmtree(pubdir)
        pubdir.mkdir()
        self._copy_static(pubdir)
        self._copy_media(pubdir)

    def _copy_static(self, pubdir: Path):
        static = Path(self.config["static_dir"])
        target = pubdir / static.name
        if self.symlink:
            target.symlink_to(os.path.relpath(static.resolve(), pubdir))
        elif static.is_file():
            shutil.copyfile(static, target)
        else:
            shutil.copytree(static, target)

    def _copy_media(self, pubdir: Path):
        mediadir = Path(self.config["media_dir"])
        if not mediadir.exists():
            return
        target = pubdir / mediadir.name
        if self.symlink:
            target.symlink_to(os.path.relpath(mediadir.resolve(), pubdir))
        else:
            shutil.copytree(mediadir, target)
