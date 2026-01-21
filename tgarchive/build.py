import logging
import math
import os
import shutil
import json
import re
from collections import OrderedDict, deque
from pathlib import Path
from typing import Dict

from importlib.metadata import version as pkg_version

# 保持原有导入
import magic
import commonmark
from feedgen.feed import FeedGenerator
from jinja2 import Template

from .db import User, Message
from .telegram_format import TelegramFormatter

# 预编译正则
_NL2BR = re.compile(r"\n\n+")

class Build:
    def __init__(self, config, db, symlink: bool):
        self.config = config
        self.db = db
        self.symlink = symlink

        self.template: Template | None = None
        self.rss_template: Template | None = None

        # [索引数据]
        self.page_ids: Dict[int, str] = {}
        self.day_to_page: Dict[str, str] = {}
        self.timeline: OrderedDict[int, list] = OrderedDict()
        
        # 渲染器
        self._tg_formatter = TelegramFormatter()
        self._md_parser = commonmark.Parser()
        self._md_renderer = commonmark.HtmlRenderer()

    # ======================================================
    # Build 主流程
    # ======================================================

    def build(self):
        # 1. 初始化目录
        self._create_publish_dir()

        timeline = list(self.db.get_timeline())
        if not timeline:
            logging.info("no data found to publish site")
            return

        # 构建 timeline 年份结构
        for month in timeline:
            if month.date.year not in self.timeline:
                self.timeline[month.date.year] = []
            self.timeline[month.date.year].append(month)

        # 2. [关键步骤] 第一遍扫描 (Pre-scan)
        self._scan_timeline(timeline)

        # 3. [关键步骤] 第二遍遍历 (Render)
        rss_entries = deque([], self.config["rss_feed_entries"])
        last_rendered_fname = None

        for month in timeline:
            dayline = OrderedDict()
            for d in self.db.get_dayline(month.date.year, month.date.month, self.config["per_page"]):
                dayline[d.slug] = d

            page = 0
            last_id = 0
            total = self.db.get_message_count(month.date.year, month.date.month)
            total_pages = math.ceil(total / self.config["per_page"])

            while True:
                messages = list(self.db.get_messages(
                    month.date.year, month.date.month, last_id, self.config["per_page"]
                ))

                if not messages:
                    break

                # [强制排序] 保证分页稳定性
                messages.sort(key=lambda x: x.id)

                last_id = messages[-1].id
                page += 1
                fname = self.make_filename(month, page)
                last_rendered_fname = fname

                if self.config["publish_rss_feed"]:
                    rss_entries.extend(messages)

                self._render_page(
                    messages=messages,
                    month=month,
                    dayline=dayline,
                    fname=fname,
                    page=page,
                    total_pages=total_pages
                )

        # 4. 生成首页
        if last_rendered_fname:
            self._build_index(last_rendered_fname)

        # 5. 生成搜索索引
        self._build_search_index(timeline)

        # 6. 生成 RSS
        if self.config["publish_rss_feed"]:
            self._build_rss(rss_entries)

    # ======================================================
    # 逻辑：扫描与映射
    # ======================================================

    def _scan_timeline(self, timeline):
        """
        第一遍扫描：不生成文件，只记录位置。
        """
        for month in timeline:
            last_id = 0
            page = 0
            while True:
                messages = list(self.db.get_messages(
                    month.date.year, month.date.month, last_id, self.config["per_page"]
                ))
                if not messages:
                    break
                
                # [强制排序]
                messages.sort(key=lambda x: x.id)

                last_id = messages[-1].id
                page += 1
                fname = self.make_filename(month, page)

                for m in messages:
                    self.page_ids[m.id] = fname
                    
                    try:
                        date_slug = m.date.strftime("%Y-%m-%d")
                    except AttributeError:
                        date_slug = str(m.date)[:10]

                    if date_slug not in self.day_to_page:
                        self.day_to_page[date_slug] = fname

    def make_filename(self, month, page: int) -> str:
        return "{}{}.html".format(
            month.slug, "_" + str(page) if page > 1 else "")

    # ======================================================
    # 渲染页面
    # ======================================================

    def load_template(self, fname: str):
        with open(fname, "r", encoding="utf-8") as f:
            self.template = Template(f.read(), autoescape=True)

    def _render_page(self, messages, month, dayline, fname, page, total_pages):
        html = self.template.render(
            config=self.config,
            timeline=self.timeline,
            dayline=dayline,
            month=month,
            messages=messages,
            page_ids=self.page_ids,
            day_to_page=self.day_to_page,
            pagination={"current": page, "total": total_pages},
            make_filename=self.make_filename,
            nl2br=self._markdown,
            markdown=self._markdown
        )

        # 注入 搜索UI 和 CSS布局修复
        html = self._inject_extras(html)

        with open(os.path.join(self.config["publish_dir"], fname), "w", encoding="utf-8") as f:
            f.write(html)

    def _build_index(self, fname):
        pubdir = self.config["publish_dir"]
        dst = os.path.join(pubdir, "index.html")
        src = os.path.join(pubdir, fname)
        
        if os.path.exists(dst):
            os.remove(dst)
            
        if self.symlink:
            self._relative_symlink(os.path.abspath(src), dst)
        else:
            shutil.copy(src, dst)

    # ======================================================
    # 搜索功能 & 样式修复
    # ======================================================

    def _build_search_index(self, timeline):
        records = []
        for m in timeline:
            last_id = 0
            while True:
                msgs = list(self.db.get_messages(
                    m.date.year, m.date.month, last_id, self.config["per_page"]
                ))
                if not msgs:
                    break
                
                msgs.sort(key=lambda x: x.id)

                for msg in msgs:
                    raw = (msg.content or "").strip()
                    if not raw:
                        continue
                    
                    file_loc = self.page_ids.get(msg.id, "")
                    msg_url = f"{file_loc}#{msg.id}"
                    
                    records.append({
                        "id": msg.id,
                        "user": msg.user.username if msg.user else "",
                        "date": msg.date.isoformat(),
                        "text": raw,
                        "html": self._markdown(raw),
                        "url": msg_url,
                    })
                last_id = msgs[-1].id

        with open(os.path.join(self.config["publish_dir"], "search.json"), "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False)

    def _inject_extras(self, html: str) -> str:
        """同时注入搜索代码和CSS布局修复"""
        if "</body>" not in html:
            return html
        
        # 拼接 Search UI 和 CSS Fix
        extras = self._css_fix_block() + "\n" + self._search_ui_block()
        return html.replace("</body>", extras + "\n</body>")

    def _css_fix_block(self) -> str:
        """
        [CSS修复] 
        强制限制内容宽度，解决长字符串/大图片撑破页面的问题。
        """
        return r"""
<style>
/* 全局防止横向溢出 */
html, body {
    max-width: 100%;
    overflow-x: hidden; 
}

/* 强制长单词/长URL换行 */
* {
    overflow-wrap: break-word;
    word-wrap: break-word;
    word-break: break-word; 
}

/* 限制图片和视频最大宽度为容器宽度 */
img, video {
    max-width: 100% !important;
    height: auto !important;
}

/* 代码块自动换行或滚动 */
pre, code {
    white-space: pre-wrap; /* 强制换行 */
    word-wrap: break-word;
    max-width: 100%;
    overflow-x: auto;
}
</style>
"""

    def _search_ui_block(self) -> str:
        return r"""
<style>
#search-btn{position:fixed;right:24px;bottom:24px;width:52px;height:52px;
border-radius:50%;background:#38bdf8;color:#020617;display:flex;
align-items:center;justify-content:center;font-size:24px;cursor:pointer;
z-index:9998;box-shadow:0 4px 12px rgba(0,0,0,0.3);}
#search-overlay{position:fixed;inset:0;background:rgba(0,0,0,.65);
backdrop-filter:blur(6px);z-index:9999;display:none;
align-items:flex-start;justify-content:center;padding-top:8vh}
#search-overlay.active{display:flex}
#search-dialog{width:min(920px,96vw);background:#020617;
border:1px solid #1e293b;border-radius:14px;overflow:hidden;box-shadow:0 10px 25px rgba(0,0,0,0.5);}
#search-input{width:100%;padding:18px;font-size:17px;border:none;
outline:none;background:#020617;color:#f8fafc;
border-bottom:1px solid #1e293b}
#search-results{max-height:70vh;overflow-y:auto;padding:8px}
.search-item{background:#020617;border:1px solid #1e293b;
border-radius:10px;padding:18px 20px;margin-bottom:12px;cursor:pointer;transition:background 0.2s;}
.search-item:hover{background:#0f172a;}
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
  const r=await fetch("search.json");
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
    # RSS / Atom
    # ======================================================

    def _build_rss(self, messages, rss_file="index.xml", atom_file="index.atom"):
        f = FeedGenerator()
        f.id(self.config["site_url"])
        f.generator(f"tg-archive {pkg_version('tg-archive')}")
        f.link(href=self.config["site_url"], rel="alternate")
        f.title(self.config["site_name"].format(group=self.config["group"]))
        f.subtitle(self.config["site_description"])

        for m in messages:
            fname = self.page_ids.get(m.id, "")
            url = "{}/{}#{}".format(self.config["site_url"], fname, m.id)
            
            e = f.add_entry()
            e.id(url)
            e.title(f"@{m.user.username} on {m.date} (#{m.id})")
            e.link({"href": url})
            e.published(m.date)

            media_mime = ""
            if m.media and m.media.url:
                murl = "{}/{}/{}".format(self.config["site_url"],
                                         os.path.basename(self.config["media_dir"]), m.media.url)
                media_mime = "application/octet-stream"
                e.enclosure(murl, 0, media_mime)

            e.content(self._markdown(m.content or ""), type="html")

        pubdir = self.config["publish_dir"]
        f.rss_file(os.path.join(pubdir, "index.xml"), pretty=True)
        f.atom_file(os.path.join(pubdir, "index.atom"), pretty=True)

    # ======================================================
    # Markdown Helper
    # ======================================================

    def _markdown(self, text: str) -> str:
        if not text:
            return ""
        try:
            # 1. 转换 Telegram 格式
            text = self._tg_formatter.convert(text)
            # 2. 解析 MD 并渲染 HTML
            ast = self._md_parser.parse(text)
            return self._md_renderer.render(ast)
        except Exception as e:
            logging.error(f"Markdown render error: {e}")
            return text.replace("\n", "<br>")

    def _nl2br(self, s) -> str:
        return _NL2BR.sub("\n\n", s).replace("\n", "\n<br />")

    # ======================================================
    # 文件系统操作
    # ======================================================

    def _create_publish_dir(self):
        pubdir = self.config["publish_dir"]
        if os.path.exists(pubdir):
            shutil.rmtree(pubdir)
        os.mkdir(pubdir)

        # 复制 Static
        static_dir = self.config["static_dir"]
        if os.path.exists(static_dir):
            target = os.path.join(pubdir, os.path.basename(static_dir))
            if self.symlink:
                self._relative_symlink(os.path.abspath(static_dir), target)
            elif os.path.isfile(static_dir):
                shutil.copyfile(static_dir, target)
            else:
                shutil.copytree(static_dir, target)

        # 复制 Media
        mediadir = self.config["media_dir"]
        if os.path.exists(mediadir):
            target = os.path.join(pubdir, os.path.basename(mediadir))
            if self.symlink:
                self._relative_symlink(os.path.abspath(mediadir), target)
            else:
                shutil.copytree(mediadir, target)

    def _relative_symlink(self, src, dst):
        dir_path = os.path.dirname(dst)
        src = os.path.relpath(src, dir_path)
        return os.symlink(src, dst)
