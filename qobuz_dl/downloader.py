import logging
import os
import time
from typing import Tuple
from collections import Counter # 新增：用于统计歌手频率
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from pathvalidate import sanitize_filename, sanitize_filepath
from rich.progress import (
    Progress,
    BarColumn,
    TextColumn,
    TransferSpeedColumn,
    TimeRemainingColumn,
    DownloadColumn,
    TaskID
)
from rich.console import Console

import qobuz_dl.metadata as metadata
from qobuz_dl.exceptions import NonStreamable

# --- 补回 cli.py 需要的变量 ---
DEFAULT_FOLDER = "{artist} - {album} ({year})"
DEFAULT_TRACK = "{artist} - {tracktitle} [{bit_depth}B-{sampling_rate}kHz]"
# ----------------------------

QL_DOWNGRADE = "FormatRestrictedByFormatAvailability"
MAX_WORKERS = 10 

console = Console()

DEFAULT_FORMATS = {
    "MP3": ["{artist} - {album} ({year}) [MP3]", "{tracknumber}. {tracktitle}"],
    "Unknown": ["{artist} - {album}", "{tracknumber}. {tracktitle}"],
}

logger = logging.getLogger(__name__)

class Download:
    def __init__(
        self,
        client,
        item_id: str,
        path: str,
        quality: int,
        embed_art: bool = False,
        albums_only: bool = False,
        downgrade_quality: bool = False,
        cover_og_quality: bool = False,
        no_cover: bool = False,
        folder_format=None,
        track_format=None,
    ):
        self.client = client
        self.item_id = item_id
        self.path = path
        self.quality = quality
        self.albums_only = albums_only
        self.embed_art = embed_art
        self.downgrade_quality = downgrade_quality
        self.cover_og_quality = cover_og_quality
        self.no_cover = no_cover
        
        self.fmt_album = "{tracknumber} {artist} - {tracktitle} [{bit_depth}B-{sampling_rate}kHz]"
        self.fmt_single = "{artist} - {tracktitle} [{bit_depth}B-{sampling_rate}kHz]"
        self.folder_format = folder_format or DEFAULT_FOLDER

    def download_id_by_type(self, track=True):
        if not track:
            self.download_release()
        else:
            self.download_track()

    def _process_single_track(self, i, count, total_items, meta, dirn, is_multiple, progress, overall_task_id, failed_list, ind_cover, track_fmt):
        display_name = f"({count}/{total_items}) {i.get('title', 'Unknown')}"[:25]
        
        task_id = progress.add_task(f"[cyan]等待中...[/] {display_name}", filename=display_name, start=False, visible=True)
        
        try:
            # 判断是否为专辑 (Artist模式下 i 是专辑信息)
            if "tracks_count" in i and "track_number" not in i:
                self._process_album_batch(i, count, total_items, dirn, progress, task_id, failed_list)
            else:
                self._process_real_track(i, count, total_items, meta, dirn, is_multiple, progress, task_id, ind_cover, track_fmt, failed_list)

        except Exception as e:
            error_msg = f"{display_name} - {str(e)}"
            failed_list.append(error_msg)
            progress.console.print(f"[red]出错 {display_name}: {e}[/red]")
        finally:
            progress.update(overall_task_id, advance=1)
            progress.remove_task(task_id)

    # 专门处理 Artist 下载时的单个专辑逻辑
    def _process_album_batch(self, album_simple_meta, album_idx, total_albums, base_dir, progress, task_id, failed_list):
        album_id = album_simple_meta['id']
        album_title_raw = album_simple_meta.get('title', 'Unknown')
        
        progress.update(task_id, description=f"({album_idx}/{total_albums}) {album_title_raw[:15]} [获取元数据...]")
        
        try:
            # 1. 获取完整元数据
            meta = self.client.get_album_meta(album_id)
            if not meta.get("streamable"):
                raise Exception("专辑不可串流")

            # 2. 计算专辑路径
            album_title = _get_title(meta)
            format_info = self._get_format(meta)
            file_format, _, bit_depth, sampling_rate = format_info
            album_attr = self._get_album_attr(meta, album_title, file_format, bit_depth, sampling_rate)
            sanitized_title = sanitize_filepath(self.folder_format.format(**album_attr))
            album_dir = os.path.join(base_dir, sanitized_title)
            os.makedirs(album_dir, exist_ok=True)

            # 3. 下载封面
            if not self.no_cover:
                _get_extra(meta["image"]["large"], album_dir, "cover.jpg", og_quality=self.cover_og_quality)

            # 4. 遍历下载曲目
            tracks = meta["tracks"]["items"]
            total_tracks = len(tracks)
            is_multiple = len({t.get("media_number", 1) for t in tracks}) > 1
            
            for idx, track in enumerate(tracks):
                track_title = track.get('title', 'Unknown')
                
                # UI: (专辑序号/总专数) 专辑名 (曲目序号/总曲数)
                short_album_name = album_title[:15].strip() 
                display_desc = f"({album_idx}/{total_albums}) {short_album_name} ({idx+1}/{total_tracks})"
                progress.update(task_id, description=display_desc)
                
                try:
                    parse = self.client.get_track_url(track["id"], fmt_id=self.quality)
                    if "sample" not in parse and parse["sampling_rate"]:
                        is_mp3 = True if int(self.quality) == 5 else False
                        self._download_and_tag(
                            album_dir, idx + 1, parse, track, meta,
                            False, is_mp3, track.get("media_number") if is_multiple else None,
                            progress, task_id, ind_cover=False, track_fmt=self.fmt_album
                        )
                except Exception as e:
                    failed_list.append(f"专辑 [{album_title_raw}] - {track_title}: {e}")

        except Exception as e:
            raise Exception(f"专辑处理失败: {e}")

    # 原有的单曲处理逻辑
    def _process_real_track(self, i, count, total_items, meta, dirn, is_multiple, progress, task_id, ind_cover, track_fmt, failed_list):
        track_meta = i
        album_meta = meta if meta else i.get('album', i)
        title = i.get('title', 'Unknown')
        
        display_desc = f"({count}/{total_items}) {title[:20]}"
        progress.update(task_id, description=display_desc)
        
        try:
            parse = self.client.get_track_url(i["id"], fmt_id=self.quality)
        except Exception as e:
            raise Exception(f"获取链接失败: {e}")

        if "sample" not in parse and parse["sampling_rate"]:
            is_mp3 = True if int(self.quality) == 5 else False
            self._download_and_tag(
                dirn, count, parse, track_meta, album_meta,
                False, is_mp3, i.get("media_number") if is_multiple else None,
                progress, task_id, ind_cover=ind_cover, track_fmt=track_fmt
            )
        else:
            progress.console.print(f"[yellow]跳过试听片段: {title}[/]")

    def download_release(self):
        with console.status("[bold green]正在获取元数据...", spinner="dots"):
            meta = self.client.get_album_meta(self.item_id)
        if not meta.get("streamable"): raise NonStreamable("无法串流")

        album_title = _get_title(meta)
        format_info = self._get_format(meta)
        file_format, quality_met, bit_depth, sampling_rate = format_info

        album_attr = self._get_album_attr(meta, album_title, file_format, bit_depth, sampling_rate)
        sanitized_title = sanitize_filepath(self.folder_format.format(**album_attr))
        dirn = os.path.join(self.path, sanitized_title)
        os.makedirs(dirn, exist_ok=True)

        if not self.no_cover:
            _get_extra(meta["image"]["large"], dirn, "cover.jpg", og_quality=self.cover_og_quality)

        tracks = meta["tracks"]["items"]
        is_multiple = len({t.get("media_number", 1) for t in tracks}) > 1
        self._run_multithreaded_download(tracks, dirn, meta, is_multiple, ind_cover=False, track_fmt=self.fmt_album)
        console.print(f"[bold green]✔ 专辑流程结束: {album_title}[/]")

    # 修改：增加了智能艺人过滤器
    def download_batch(self, track_list, content_name="歌单"):
        final_list = track_list

        # --- 智能过滤核心逻辑 ---
        # 1. 检查这是否是一个包含专辑的列表 (Artist/Label)，而不是歌单 (Playlist)
        if track_list and "tracks_count" in track_list[0] and "artist" in track_list[0]:
            # 2. 统计出现频率最高的艺人名
            artists = [item['artist']['name'] for item in track_list if 'artist' in item]
            if artists:
                most_common = Counter(artists).most_common(1)
                if most_common:
                    main_artist, count = most_common[0]
                    # 3. 如果某个艺人占比超过 40%，我们假设这是由于下载该艺人触发的
                    if count / len(track_list) > 0.4:
                        console.print(f"[bold yellow]检测到主艺人: {main_artist}，正在过滤无关专辑...[/]")
                        
                        filtered_items = []
                        for item in track_list:
                            item_artist = item.get('artist', {}).get('name', '')
                            # 保留条件：
                            # 1. 专辑艺人包含主艺人名 (例如 "Billie Eilish", "Billie Eilish & Khalid")
                            # 2. 或者是 Various Artists (精选集/原声带)
                            if (main_artist.lower() in item_artist.lower()) or \
                               (item_artist.lower() in main_artist.lower()) or \
                               "various" in item_artist.lower():
                                filtered_items.append(item)
                            else:
                                console.print(f"[dim]已剔除无关专辑: {item_artist} - {item['title']}[/dim]")
                        
                        final_list = filtered_items
                        console.print(f"[green]过滤完成: {len(track_list)} -> {len(final_list)} 张专辑[/]")

        self._run_multithreaded_download(final_list, self.path, None, False, ind_cover=True, track_fmt=self.fmt_single)
        console.print(f"[bold green]✔ {content_name} 流程结束[/]")

    def _run_multithreaded_download(self, tracks, dirn, meta, is_multiple, ind_cover, track_fmt):
        failed_list = []
        total_items = len(tracks)
        
        progress = Progress(
            TextColumn("{task.description}", justify="left"), 
            BarColumn(bar_width=15), 
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            DownloadColumn(),
            TransferSpeedColumn(),
            TimeRemainingColumn(),
            console=console, 
            transient=True
        )
        
        with progress:
            overall_task_id = progress.add_task(f"[green]总进度 ({total_items} 项)[/]", filename="Batch", total=total_items)
            
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = []
                for idx, item in enumerate(tracks):
                    futures.append(executor.submit(
                        self._process_single_track, 
                        item, 
                        idx + 1, 
                        total_items,
                        meta, dirn, is_multiple, 
                        progress, overall_task_id, failed_list, 
                        ind_cover, track_fmt
                    ))
                for future in as_completed(futures): 
                    future.result()

        if failed_list:
            console.rule("[bold red]下载完成，但存在错误[/]")
            for fail in failed_list:
                console.print(f"[red]❌ {fail}[/]")
            console.print("\n[bold yellow]建议检查：\n1. 您的 Qobuz 订阅是否包含这些曲目\n2. 您的网络环境是否稳定 (已自动重试3次)[/]")
        else:
            console.print("[bold green]✨ 所有内容下载成功！[/]")

    def download_track(self):
        try:
            meta = self.client.get_track_meta(self.item_id)
            parse = self.client.get_track_url(self.item_id, self.quality)
            
            progress = Progress(
                TextColumn("{task.description}", justify="left"),
                BarColumn(bar_width=15),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                DownloadColumn(),
                TransferSpeedColumn(),
                TimeRemainingColumn(),
                console=console, transient=True
            )
            
            with progress:
                track_num = meta.get('track_number', 0)
                disp_name = f"{track_num:02d}. {meta.get('title', 'Unknown')}"[:30]
                task_id = progress.add_task(description=disp_name, filename=disp_name, start=False)
                
                is_mp3 = True if int(self.quality) == 5 else False
                try:
                    self._download_and_tag(self.path, 1, parse, meta, meta, True, is_mp3, None, progress, task_id, ind_cover=True, track_fmt=self.fmt_single)
                except Exception as e:
                     console.print(f"[red]下载失败: {e}[/red]")
        except Exception as e: console.print(f"[red]获取元数据失败: {e}[/red]")

    def _download_and_tag(self, root_dir, tmp_count, track_url_dict, track_metadata, album_or_track_metadata, is_track, is_mp3, multiple, progress, task_id, ind_cover, track_fmt):
        extension = ".mp3" if is_mp3 else ".flac"
        try: url = track_url_dict["url"]
        except: return

        if multiple:
            root_dir = os.path.join(root_dir, f"Disc {multiple}")
            os.makedirs(root_dir, exist_ok=True)
            
        filename = os.path.join(root_dir, f".{tmp_count:02}.tmp")
        artist = _safe_get(track_metadata, "performer", "name")
        filename_attr = self._get_filename_attr(artist, track_metadata, track_metadata.get("title", "Unknown"), track_url_dict)
        formatted_name = sanitize_filename(track_fmt.format(**filename_attr))
        final_file = os.path.join(root_dir, formatted_name)[:240] + extension
        
        if os.path.isfile(final_file):
            progress.update(task_id, visible=False)
            return

        max_retries = 3
        success = False
        last_error = None

        for attempt in range(max_retries):
            try:
                response = requests.get(url, stream=True, timeout=30)
                response.raise_for_status()
                total_length = int(response.headers.get("content-length", 0))
                
                progress.update(task_id, completed=0, total=total_length)
                progress.start_task(task_id)
                
                with open(filename, "wb") as file:
                    for chunk in response.iter_content(chunk_size=32768):
                        if chunk:
                            file.write(chunk)
                            progress.advance(task_id, len(chunk))
                success = True
                break 
            except (requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout) as e:
                last_error = e
                if ind_cover: 
                    console.print(f"[yellow]重试 ({attempt + 1}/{max_retries})... {formatted_name}[/]")
                time.sleep(3)
                if os.path.exists(filename):
                    try: os.remove(filename)
                    except: pass
                progress.update(task_id, completed=0)
            except Exception as e:
                raise e

        if not success:
            raise Exception(f"重试3次后失败: {last_error}")

        try:
            metadata.tag_mp3(filename, root_dir, final_file, track_metadata, album_or_track_metadata, is_track, self.embed_art) if is_mp3 else \
            metadata.tag_flac(filename, root_dir, final_file, track_metadata, album_or_track_metadata, is_track, self.embed_art)
        except: pass
        
        if os.path.exists(filename):
            try: os.rename(filename, final_file)
            except: pass
            
        if ind_cover and not self.no_cover:
            img_url = album_or_track_metadata.get("image", {}).get("large")
            if not img_url and track_metadata.get("album"): img_url = track_metadata["album"].get("image", {}).get("large")
            if img_url:
                img_path = os.path.join(root_dir, formatted_name)[:240] + ".jpg"
                if not os.path.exists(img_path):
                    try:
                        r_img = requests.get(img_url.replace("_600.", "_org.") if self.cover_og_quality else img_url)
                        with open(img_path, "wb") as f_img: f_img.write(r_img.content)
                    except: pass

    @staticmethod
    def _get_filename_attr(artist, track_metadata, track_title, url_dict=None):
        sr = track_metadata.get("maximum_sampling_rate", 44.1)
        if url_dict and url_dict.get("sampling_rate"): sr = url_dict["sampling_rate"]
        if sr > 1000: sr = sr / 1000
        sr_str = f"{sr:g}"
        bd = track_metadata.get("maximum_bit_depth", 16)
        if url_dict and url_dict.get("bit_depth"): bd = url_dict["bit_depth"]
        return {"artist": artist, "bit_depth": bd, "sampling_rate": sr_str, "tracktitle": track_title, "tracknumber": f"{track_metadata.get('track_number', 0):02}"}

    @staticmethod
    def _get_album_attr(meta, album_title, file_format, bit_depth, sampling_rate):
        return {"artist": meta["artist"]["name"], "album": album_title, "year": meta["release_date_original"].split("-")[0], "format": file_format, "bit_depth": bit_depth, "sampling_rate": sampling_rate}

    def _get_format(self, item_dict, is_track_id=False, track_url_dict=None):
        quality_met = True
        if int(self.quality) == 5: return ("MP3", quality_met, None, None)
        track_dict = item_dict if is_track_id else item_dict["tracks"]["items"][0]
        try:
            new_track_dict = self.client.get_track_url(track_dict["id"], fmt_id=self.quality) if not track_url_dict else track_url_dict
            if int(self.quality) > 6 and new_track_dict.get("bit_depth") == 16: quality_met = False
            return ("FLAC", quality_met, new_track_dict["bit_depth"], new_track_dict["sampling_rate"])
        except: return ("Unknown", quality_met, None, None)

def _get_title(item_dict):
    album_title = item_dict["title"]
    version = item_dict.get("version")
    if version: album_title = f"{album_title} ({version})" if version.lower() not in album_title.lower() else album_title
    return album_title

def _get_extra(item, dirn, extra="cover.jpg", og_quality=False):
    extra_file = os.path.join(dirn, extra)
    if os.path.isfile(extra_file): return
    try:
        r = requests.get(item.replace("_600.", "_org.") if og_quality else item)
        with open(extra_file, "wb") as f: f.write(r.content)
    except: pass

def _safe_get(d: dict, *keys, default=None):
    curr = d
    for key in keys:
        try: curr = curr[key]
        except: return default
    return curr