import logging
import os
import re
import requests
from bs4 import BeautifulSoup as bso
from pathvalidate import sanitize_filename

from qobuz_dl import downloader, qopy
from qobuz_dl.bundle import Bundle
from qobuz_dl.color import RED, YELLOW, OFF
from qobuz_dl.exceptions import NonStreamable
from qobuz_dl.db import create_db, handle_download_id
from qobuz_dl.utils import (
    get_url_info, make_m3u, smart_discography_filter, create_and_return_dir
)
from rich.console import Console
console = Console()
logger = logging.getLogger(__name__)

QUALITIES = {5: "5 - MP3", 6: "6 - 16 bit, 44.1kHz", 7: "7 - 24 bit, <96kHz", 27: "27 - 24 bit, >96kHz"}

class QobuzDL:
    def __init__(self, directory="Qobuz Downloads", quality=6, embed_art=False, ignore_singles_eps=False, no_m3u_for_playlists=False, quality_fallback=True, cover_og_quality=False, no_cover=False, downloads_db=None, folder_format="{artist} - {album} ({year})", track_format="{tracknumber}. {tracktitle}", smart_discography=False):
        self.directory = create_and_return_dir(directory)
        self.quality = quality
        self.embed_art = embed_art
        self.ignore_singles_eps = ignore_singles_eps
        self.no_m3u_for_playlists = no_m3u_for_playlists
        self.quality_fallback = quality_fallback
        self.cover_og_quality = cover_og_quality
        self.no_cover = no_cover
        self.downloads_db = create_db(downloads_db) if downloads_db else None
        self.folder_format = folder_format
        self.track_format = track_format
        self.smart_discography = smart_discography

    def initialize_client(self, email, pwd, app_id, secrets, use_token, user_id, user_auth_token):
        self.client = qopy.Client(email, pwd, app_id, secrets, use_token, user_id, user_auth_token)
        console.print(f"[dim]设定最高画质: {QUALITIES[int(self.quality)]}[/dim]\n")

    def download_from_id(self, item_id, album=True, alt_path=None):
        # 移除数据库存在即返回的逻辑，仅在下载成功后记录
        if handle_download_id(self.downloads_db, item_id, add_id=False):
            pass # 继续尝试下载，让 downloader 检测本地文件

        try:
            dloader = downloader.Download(
                self.client, item_id, alt_path or self.directory, int(self.quality),
                self.embed_art, self.ignore_singles_eps, self.quality_fallback,
                self.cover_og_quality, self.no_cover, self.folder_format, self.track_format
            )
            dloader.download_id_by_type(not album)
            handle_download_id(self.downloads_db, item_id, add_id=True)
        except (requests.exceptions.RequestException, NonStreamable) as e:
            console.print(f"[red]获取资源出错: {e}[/red]")

    def handle_url(self, url):
        possibles = {
            "playlist": {"func": self.client.get_plist_meta, "iterable_key": "tracks"},
            "artist": {"func": self.client.get_artist_meta, "iterable_key": "albums"},
            "label": {"func": self.client.get_label_meta, "iterable_key": "albums"},
            "album": {"album": True, "func": None},
            "track": {"album": False, "func": None},
        }
        try:
            url_type, item_id = get_url_info(url)
            type_dict = possibles[url_type]
        except (KeyError, IndexError, TypeError): return

        if type_dict.get("func"):
            try:
                content = [item for item in type_dict["func"](item_id)]
                if not content:
                    console.print("[red]未找到内容[/red]")
                    return
                content_name = content[0]["name"]
                console.print(f"[bold yellow]正在获取 {url_type}: {content_name}[/]")
                new_path = create_and_return_dir(os.path.join(self.directory, sanitize_filename(content_name)))

                if self.smart_discography and url_type == "artist":
                    items = smart_discography_filter(content, save_space=True, skip_extras=True)
                else:
                    items = [item[type_dict["iterable_key"]]["items"] for item in content][0]

                console.print(f"[yellow]包含 {len(items)} 个项目，准备并发下载...[/]")
                dloader = downloader.Download(
                    self.client, item_id, new_path, int(self.quality), self.embed_art,
                    self.ignore_singles_eps, self.quality_fallback, self.cover_og_quality,
                    self.no_cover, self.folder_format, self.track_format
                )
                dloader.download_batch(items, content_name=content_name)
                if url_type == "playlist" and not self.no_m3u_for_playlists:
                    console.print("[dim]正在生成 .m3u 播放列表文件...[/dim]")
                    make_m3u(new_path)
            except Exception as e: console.print(f"[red]处理批量内容出错: {e}[/red]")
        else:
            self.download_from_id(item_id, type_dict["album"])

    def download_list_of_urls(self, raw_args):
        if not raw_args: return
        valid_urls = []
        full_text = " ".join(raw_args)
        qobuz_pattern = r"(https?://(?:open|play|www)\.qobuz\.com(?:/[a-zA-Z0-9_-]+)*/(?:album|artist|track|playlist|label)/[a-zA-Z0-9]+)"
        extracted = re.findall(qobuz_pattern, full_text)
        if extracted: valid_urls.extend(extracted)
        
        if not valid_urls:
            for u in raw_args:
                if os.path.isfile(u): self.download_from_txt_file(u)
                elif "last.fm" in u: self.download_lastfm_pl(u)

        if not valid_urls and not any(os.path.isfile(x) or "last.fm" in x for x in raw_args):
            console.print(f"[bold red]未检测到有效的 Qobuz 链接！[/]")
            return

        unique_urls = list(set(valid_urls))
        console.print(f"[green]识别到 {len(unique_urls)} 个链接，开始处理...[/]")
        for url in unique_urls: self.handle_url(url)

    def download_from_txt_file(self, txt_file):
        with open(txt_file, "r") as txt:
            urls = [l.strip() for l in txt.readlines() if not l.strip().startswith("#")]
            self.download_list_of_urls(urls)

    def download_lastfm_pl(self, playlist_url):
        try:
            r = requests.get(playlist_url, timeout=10)
        except: pass