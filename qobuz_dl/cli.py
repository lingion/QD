import configparser
import hashlib
import logging
import glob
import os
import sys

from qobuz_dl.bundle import Bundle
from qobuz_dl.color import RED, YELLOW, GREEN, OFF
from qobuz_dl.commands import qobuz_dl_args
from qobuz_dl.core import QobuzDL
try:
    from qobuz_dl.downloader import DEFAULT_FOLDER, DEFAULT_TRACK
except ImportError:
    DEFAULT_FOLDER = "{artist} - {album} ({year})"
    DEFAULT_TRACK = "{artist} - {tracktitle} [{bit_depth}B-{sampling_rate}kHz]"
# 使用 Rich 的 Console 进行更干净的输出
from rich.console import Console
console = Console()

logging.basicConfig(level=logging.INFO, format="%(message)s")

if os.name == "nt":
    OS_CONFIG = os.environ.get("APPDATA")
else:
    OS_CONFIG = os.path.join(os.environ["HOME"], ".config")

CONFIG_PATH = os.path.join(OS_CONFIG, "qobuz-dl")
CONFIG_FILE = os.path.join(CONFIG_PATH, "config.ini")
QOBUZ_DB = os.path.join(CONFIG_PATH, "qobuz_dl.db")


def _reset_config(config_file):
    console.rule("[bold cyan]初始化配置[/]")
    console.print(f"[yellow]正在创建配置文件: {config_file}[/]")
    config = configparser.ConfigParser()

    # 强制使用 Token 登录，不询问邮箱
    config["DEFAULT"]["use_token"] = "true"
    config["DEFAULT"]["email"] = ""
    config["DEFAULT"]["password"] = ""
    
    console.print("\n[bold]请在下方输入您的 Qobuz 凭证 (Token):[/]")
    config["DEFAULT"]["user_id"] = console.input("[green]请输入 User ID[/]: ").strip()
    config["DEFAULT"]["user_auth_token"] = console.input("[green]请输入 User Auth Token[/]: ").strip()
    
    console.print("\n[bold]下载设置:[/]")
    folder_input = console.input(f"下载目录 [默认: 'Qobuz Downloads']: ").strip()
    config["DEFAULT"]["default_folder"] = folder_input if folder_input else "Qobuz Downloads"
    
    quality_input = console.input("默认画质 (5=MP3, 6=无损, 7=24bit, 27=最高) [默认: 27]: ").strip()
    config["DEFAULT"]["default_quality"] = quality_input if quality_input else "27"
    
    config["DEFAULT"]["default_limit"] = "20"
    
    # 默认设置
    defaults = {
        "no_m3u": "false", "albums_only": "false", "no_fallback": "false",
        "og_cover": "false", "embed_art": "false", "no_cover": "false",
        "no_database": "false", "smart_discography": "false",
        "folder_format": DEFAULT_FOLDER, "track_format": DEFAULT_TRACK
    }
    for k, v in defaults.items():
        config["DEFAULT"][k] = v

    console.print("\n[yellow]正在获取 App ID 和密钥 (Bundle)...[/]")
    try:
        bundle = Bundle()
        config["DEFAULT"]["app_id"] = str(bundle.get_app_id())
        config["DEFAULT"]["secrets"] = ",".join(bundle.get_secrets().values())
        console.print("[green]密钥获取成功！[/]")
    except Exception as e:
        console.print(f"[bold red]获取密钥失败: {e}[/]")
        sys.exit(1)

    with open(config_file, "w") as configfile:
        config.write(configfile)
    console.print(f"[bold green]配置已保存！请重新运行命令开始下载。[/]")


def _remove_leftovers(directory):
    directory = os.path.join(directory, "**", ".*.tmp")
    for i in glob.glob(directory, recursive=True):
        try:
            os.remove(i)
        except: pass


def _initial_checks():
    if not os.path.isdir(CONFIG_PATH) or not os.path.isfile(CONFIG_FILE):
        os.makedirs(CONFIG_PATH, exist_ok=True)
        _reset_config(CONFIG_FILE)


def main():
    _initial_checks()

    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    try:
        # 读取配置
        d = config["DEFAULT"]
        secrets = [s for s in d["secrets"].split(",") if s]
        
        args = qobuz_dl_args(
            d["default_quality"], d["default_limit"], d["default_folder"]
        ).parse_args()
        
    except Exception:
        console.print("[bold red]配置文件损坏或版本过旧，正在重置...[/]")
        _reset_config(CONFIG_FILE)
        return

    if args.reset:
        _reset_config(CONFIG_FILE)
        return

    if args.purge:
        try:
            os.remove(QOBUZ_DB)
            console.print("[green]已清空下载记录数据库。[/]")
        except: pass
        return

    if not args.urls:
        console.print("[bold red]错误: 未提供 URL。[/]")
        console.print("用法示例: [cyan]qd https://play.qobuz.com/album/xxxx[/]")
        return

    # 初始化核心
    qobuz = QobuzDL(
        args.directory,
        args.quality,
        args.embed_art or config.getboolean("DEFAULT", "embed_art"),
        ignore_singles_eps=args.albums_only or config.getboolean("DEFAULT", "albums_only"),
        no_m3u_for_playlists=args.no_m3u or config.getboolean("DEFAULT", "no_m3u"),
        quality_fallback=not args.no_fallback, 
        cover_og_quality=args.og_cover or config.getboolean("DEFAULT", "og_cover"),
        no_cover=args.no_cover or config.getboolean("DEFAULT", "no_cover"),
        downloads_db=None if args.no_db else QOBUZ_DB,
        folder_format=args.folder_format or d["folder_format"],
        track_format=args.track_format or d["track_format"],
        smart_discography=args.smart_discography or config.getboolean("DEFAULT", "smart_discography"),
    )

    try:
        qobuz.initialize_client(
            d["email"], d["password"], d["app_id"], secrets, 
            d["use_token"], d["user_id"], d["user_auth_token"]
        )
        # 直接下载
        qobuz.download_list_of_urls(args.urls)
    except KeyboardInterrupt:
        console.print("\n[red]用户强制停止。[/]")
    finally:
        _remove_leftovers(qobuz.directory)


if __name__ == "__main__":
    sys.exit(main())
