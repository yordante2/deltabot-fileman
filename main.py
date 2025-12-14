from deltabot_cli import BotCli
from deltachat2 import events, NewMsgEvent, MsgData
import requests
import time
import shutil
import re
from pathlib import Path
import py7zr
import schedule
import threading
from typing import Dict, List, Tuple
import os

cli = BotCli("download-bot")

BASE_DIR = Path(os.getenv("BASE_DIR"))
MAX_FILE_SIZE = 15728640

stats = {
    "total_downloads": 0,
    "total_files_sent": 0,
    "total_size_downloaded": 0,
    "active_downloads": 0
}
user_file_lists = {}
user_last_list_time = {}

HELP_TEXT = """📁 File Manager - Ayuda

Comandos:
📥 Descarga automática:
- Envía cualquier enlace HTTP/HTTPS directo
- Archivos >15MB se dividen en partes
- Los archivos se borran cada 6 horas

📋 Gestión de archivos:
- /ls - Lista archivos con números
- /rm <número> - Elimina archivo por número
- /clear - Borra todos los archivos
- /send <número> - Reenvía archivo por número
- /help - Muestra ayuda

📊 Estadísticas:
- /stats - Estadísticas de uso"""

def ensure_user_dir(user_id: int) -> Path:
    user_dir = BASE_DIR / str(user_id)
    user_dir.mkdir(parents=True, exist_ok=True)
    return user_dir

def cleanup_old_files():
    if not BASE_DIR.exists():
        return
    cutoff_time = time.time() - CLEANUP_INTERVAL
    for user_dir in BASE_DIR.iterdir():
        if user_dir.is_dir():
            for file_path in user_dir.glob("*"):
                if file_path.is_file() and os.path.getmtime(file_path) < cutoff_time:
                    file_path.unlink()

def split_large_file(filepath: Path, part_size: int = PART_SIZE) -> List[Path]:
    parts = []
    zip_path = filepath.with_suffix('.7z')
    with py7zr.SevenZipFile(zip_path, 'w') as archive:
        archive.write(filepath, filepath.name)
    part_num = 1
    with open(zip_path, 'rb') as f:
        while True:
            chunk = f.read(part_size)
            if not chunk:
                break
            part_name = f"{zip_path.name}.{part_num:03d}"
            part_path = filepath.parent / part_name
            with open(part_path, 'wb') as part_file:
                part_file.write(chunk)
            parts.append(part_path)
            part_num += 1
    zip_path.unlink()
    filepath.unlink()
    return parts

def download_file(url: str, user_id: int) -> Tuple[bool, str, Path]:
    try:
        response = requests.get(url, stream=True, timeout=30)
        if response.status_code != 200:
            return False, f"Error HTTP {response.status_code}", None
        filename = url.split('/')[-1]
        if '?' in filename:
            filename = filename.split('?')[0]
        if not filename or '.' not in filename:
            filename = f"file_{int(time.time())}"
        user_dir = ensure_user_dir(user_id)
        filepath = user_dir / filename
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        file_size = filepath.stat().st_size
        stats["total_downloads"] += 1
        stats["total_size_downloaded"] += file_size
        return True, f"✅ {filename} ({file_size/1024/1024:.2f} MB)", filepath
    except Exception as e:
        return False, f"Error: {str(e)}", None

def get_user_file_list(user_id: int) -> Tuple[str, List[str]]:
    user_dir = ensure_user_dir(user_id)
    files = []
    for file_path in sorted(user_dir.glob("*")):
        if file_path.is_file():
            size = file_path.stat().st_size
            size_str = f"{size/1024/1024:.2f}MB" if size > 1048576 else f"{size/1024:.2f}KB"
            files.append(file_path.name)
    if not files:
        return "📂 Carpeta vacía", []
    text_lines = ["📁 **Tus archivos (usa el número):**"]
    for i, filename in enumerate(files):
        text_lines.append(f"{i}. {filename}")
    return "\n".join(text_lines), files

def send_file_parts(bot, accid: int, chatid: int, parts: List[Path]):
    for i, part_path in enumerate(parts):
        try:
            msg = MsgData(
                file=str(part_path),
                text=f"📦 Parte {i+1}/{len(parts)}: {part_path.name}"
            )
            bot.rpc.send_msg(accid, chatid, msg)
            stats["total_files_sent"] += 1
            if i < len(parts) - 1:
                time.sleep(SEND_INTERVAL)
        except Exception as e:
            error_msg = f"❌ Error parte {i+1}: {str(e)}"
            bot.rpc.send_msg(accid, chatid, MsgData(text=error_msg))
    complete_msg = f"✅ {len(parts)} partes enviadas."
    bot.rpc.send_msg(accid, chatid, MsgData(text=complete_msg))

def get_stats_text() -> str:
    return f"""📊 Estadísticas

📥 Descargas: {stats['total_downloads']}
📤 Archivos: {stats['total_files_sent']}
💾 Tamaño: {stats['total_size_downloaded']/1024/1024:.2f} MB
🔄 Activas: {stats['active_downloads']}"""

@cli.on(events.NewMessage(is_info=False))
def handle_message(bot, accid: int, event: NewMsgEvent):
    if bot.has_command(event.command):
        return
    msg = event.msg
    text = msg.text.strip()
    url_pattern = re.compile(r'https?://[^\s]+')
    urls = url_pattern.findall(text)
    if not urls:
        return
    chatid = msg.chat_id
    user_id = msg.sender.id
    for url in urls[:3]:
        bot.rpc.send_msg(accid, chatid, MsgData(
            text="⏬ Descargando...",
            quoted_message_id=msg.id
        ))
        stats["active_downloads"] += 1
        success, message, filepath = download_file(url, user_id)
        stats["active_downloads"] -= 1
        if not success:
            bot.rpc.send_msg(accid, chatid, MsgData(text=message))
            continue
        file_size = filepath.stat().st_size
        if file_size > MAX_FILE_SIZE:
            bot.rpc.send_msg(accid, chatid, MsgData(
                text=f"📦 Dividiendo ({file_size/1024/1024:.2f} MB)..."
            ))
            parts = split_large_file(filepath)
            threading.Thread(target=send_file_parts, args=(bot, accid, chatid, parts), daemon=True).start()
            bot.rpc.send_msg(accid, chatid, MsgData(
                text=f"✅ Dividido en {len(parts)} partes."
            ))
        else:
            try:
                bot.rpc.send_msg(accid, chatid, MsgData(
                    file=str(filepath),
                    text=f"✅ {filepath.name}"
                ))
                stats["total_files_sent"] += 1
            except Exception as e:
                bot.rpc.send_msg(accid, chatid, MsgData(
                    text=f"❌ Error: {str(e)}"
                ))

@cli.on(events.NewMessage(command="/help", is_info=False))
def handle_help(bot, accid: int, event: NewMsgEvent):
    bot.rpc.send_msg(accid, event.msg.chat_id, MsgData(text=HELP_TEXT))

@cli.on(events.NewMessage(command="/ls", is_info=False))
def handle_ls(bot, accid: int, event: NewMsgEvent):
    user_id = event.msg.sender.id
    text, files = get_user_file_list(user_id)
    user_file_lists[user_id] = files
    user_last_list_time[user_id] = time.time()
    bot.rpc.send_msg(accid, event.msg.chat_id, MsgData(text=text))

@cli.on(events.NewMessage(command="/clear", is_info=False))
def handle_clear(bot, accid: int, event: NewMsgEvent):
    user_id = event.msg.sender.id
    user_dir = ensure_user_dir(user_id)
    if any(user_dir.iterdir()):
        shutil.rmtree(user_dir)
        user_dir.mkdir()
        user_file_lists.pop(user_id, None)
        bot.rpc.send_msg(accid, event.msg.chat_id, MsgData(text="✅ Carpeta limpiada"))
    else:
        bot.rpc.send_msg(accid, event.msg.chat_id, MsgData(text="📂 Carpeta vacía"))

@cli.on(events.NewMessage(command="/rm", is_info=False))
def handle_rm(bot, accid: int, event: NewMsgEvent):
    user_id = event.msg.sender.id
    arg = event.payload.strip()
    if not arg:
        bot.rpc.send_msg(accid, event.msg.chat_id, MsgData(text="⚠️ Uso: /rm <número>"))
        return
    user_dir = ensure_user_dir(user_id)
    if arg.isdigit():
        idx = int(arg)
        if user_id in user_file_lists:
            files = user_file_lists[user_id]
            if 0 <= idx < len(files):
                filename = files[idx]
                filepath = user_dir / filename
                if filepath.exists():
                    filepath.unlink()
                    bot.rpc.send_msg(accid, event.msg.chat_id, MsgData(text=f"✅ Eliminado: {filename}"))
                    user_file_lists[user_id] = [f for f in files if f != filename]
                else:
                    bot.rpc.send_msg(accid, event.msg.chat_id, MsgData(text="❌ Archivo no existe"))
            else:
                bot.rpc.send_msg(accid, event.msg.chat_id, MsgData(text="❌ Número inválido"))
        else:
            bot.rpc.send_msg(accid, event.msg.chat_id, MsgData(text="❌ Usa /ls primero"))
    else:
        bot.rpc.send_msg(accid, event.msg.chat_id, MsgData(text="⚠️ Usa un número: /rm 0"))

@cli.on(events.NewMessage(command="/send", is_info=False))
def handle_send(bot, accid: int, event: NewMsgEvent):
    user_id = event.msg.sender.id
    chatid = event.msg.chat_id
    arg = event.payload.strip()
    if not arg:
        bot.rpc.send_msg(accid, chatid, MsgData(text="⚠️ Uso: /send <número>"))
        return
    user_dir = ensure_user_dir(user_id)
    if arg.isdigit():
        idx = int(arg)
        if user_id in user_file_lists:
            files = user_file_lists[user_id]
            if 0 <= idx < len(files):
                filename = files[idx]
                filepath = user_dir / filename
                if filepath.exists():
                    file_size = filepath.stat().st_size
                    if file_size > MAX_FILE_SIZE:
                        parts = split_large_file(filepath)
                        threading.Thread(target=send_file_parts, args=(bot, accid, chatid, parts), daemon=True).start()
                        bot.rpc.send_msg(accid, chatid, MsgData(
                            text=f"📦 Enviando {len(parts)} partes..."
                        ))
                    else:
                        try:
                            bot.rpc.send_msg(accid, chatid, MsgData(
                                file=str(filepath),
                                text=f"📤 {filename}"
                            ))
                            stats["total_files_sent"] += 1
                        except Exception as e:
                            bot.rpc.send_msg(accid, chatid, MsgData(
                                text=f"❌ Error: {str(e)}"
                            ))
                else:
                    bot.rpc.send_msg(accid, chatid, MsgData(text="❌ Archivo no existe"))
            else:
                bot.rpc.send_msg(accid, chatid, MsgData(text="❌ Número inválido"))
        else:
            bot.rpc.send_msg(accid, chatid, MsgData(text="❌ Usa /ls primero"))
    else:
        bot.rpc.send_msg(accid, chatid, MsgData(text="⚠️ Usa un número: /send 0"))

@cli.on(events.NewMessage(command="/stats", is_info=False))
def handle_stats(bot, accid: int, event: NewMsgEvent):
    stats_text = get_stats_text()
    bot.rpc.send_msg(accid, event.msg.chat_id, MsgData(text=stats_text))

def schedule_cleanup():
    schedule.every(6).hours.do(cleanup_old_files)
    while True:
        schedule.run_pending()
        time.sleep(60)

@cli.on_init
def on_init(bot, args):
    BASE_DIR.mkdir(exist_ok=True)
    cleanup_thread = threading.Thread(target=schedule_cleanup, daemon=True)
    cleanup_thread.start()
    print(f"✅ Bot iniciado - Carpeta: {BASE_DIR}")

@cli.after(events.NewMessage)
def delete_msgs(bot, accid, event):
    bot.rpc.delete_messages(accid, [event.msg.id])

if __name__ == "__main__":
    try:
        cli.start()
    except KeyboardInterrupt:
        cleanup_old_files()
        print("\n👋 Bot detenido")
