import sys
import asyncio
import gc
import random
import os
import tempfile
import glob
import html
from urllib.parse import urlparse
from io import BytesIO

# Библиотека для автоматического поиска/установки FFmpeg
import imageio_ffmpeg 

import uvloop
import ujson
import asyncpg
from aiohttp import web, ClientSession, TCPConnector, ClientTimeout
from aiogram import Bot
from aiogram.types import FSInputFile, BufferedInputFile
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from loguru import logger
from cachetools import TTLCache

# ---------------- [ КОНФИГУРАЦИЯ ] ---------------- #

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")
ADMIN_ID = os.getenv("ADMIN_ID") # Для алертов
DB_DSN = os.getenv("DB_DSN")

E621_USER_AGENT = os.getenv("E621_USER_AGENT", "TelegramVideoBot/11.0 (by Dexz)")
HEADERS = {"User-Agent": E621_USER_AGENT}

# Теги: Исключаем безопасное, рандом, без людей
BASE_TAGS = "-rating:safe order:random -human"
MIN_SCORE = 130
MAX_ORIGINAL_SIZE_MB = 49.9

# Таймауты
CONVERT_TIMEOUT = 300 # 5 минут на конвертацию
NETWORK_TIMEOUT = ClientTimeout(total=600, connect=10) # 10 сек на коннект, 10 мин на скачивание

VIDEOS_PER_BATCH = 2
SLEEP_INTERVAL = int(os.getenv("SLEEP_INTERVAL", 3600))

ALLOWED_EXTS = {"webm", "mp4", "gif"}
BLACKLIST_WORDS = {"scat", "guro", "bestiality", "cub", "gore", "watersports", "hyper"}
BLACKLIST_SET = set(BLACKLIST_WORDS)

# Игнорируем системные имена в поле Artist
IGNORED_ARTISTS = {
    "conditional_dnp", "sound_warning", "unknown", "anonymous", 
    "ai_generated", "ai_assisted", "stable_diffusion", "img2img", "midjourney"
}

ARTIST_CACHE = TTLCache(maxsize=2000, ttl=86400)

# Максимальная оптимизация Event Loop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
logger.remove()
logger.add(sys.stdout, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{message}</level>")

if not BOT_TOKEN or not CHANNEL_ID or not DB_DSN:
    logger.critical("❌ Variables BOT_TOKEN, CHANNEL_ID, DB_DSN are missing!")
    sys.exit(1)


# ---------------- [ СИСТЕМНЫЕ УТИЛИТЫ ] ---------------- #

async def send_alert(bot, message):
    """Шлет уведомление админу в ЛС при критических сбоях."""
    if not ADMIN_ID: return
    try:
        await bot.send_message(chat_id=ADMIN_ID, text=f"⚠️ <b>Bot Alert:</b>\n{html.escape(str(message))}")
    except Exception as e:
        logger.error(f"Failed to send alert: {e}")

def clean_temp_garbage():
    """Чистит папку tmp от 'зомби-файлов' при старте."""
    try:
        tmp_dir = tempfile.gettempdir()
        files = glob.glob(os.path.join(tmp_dir, "video_*")) + glob.glob(os.path.join(tmp_dir, "thumb_*")) + glob.glob(os.path.join(tmp_dir, "in.*"))
        count = 0
        for f in files:
            try:
                if os.path.isfile(f):
                    os.remove(f)
                    count += 1
            except: pass
        if count > 0:
            logger.info(f"🧹 Startup Cleanup: Removed {count} old temp files.")
    except Exception as e:
        logger.warning(f"Cleanup warning: {e}")

def smart_domain_name(url):
    """
    (v11.0 Feature) Умное извлечение названия сайта.
    fanbox.cc -> Fanbox, twitter.com -> Twitter, boosty.to -> Boosty
    """
    try:
        domain = urlparse(url).netloc
        # Убираем www.
        if domain.startswith("www."): domain = domain[4:]
        # Берем первую часть домена (twitter.com -> twitter)
        name = domain.split('.')[0]
        
        # Словарь красивых исключений
        overrides = {
            "furaffinity": "FA", "inkbunny": "Inkbunny", "deviantart": "DeviantArt",
            "sofurry": "SoFurry", "newgrounds": "Newgrounds", "subscribestar": "SubStar",
            "bsky": "Bluesky", "t": "Telegram", "vk": "VK"
        }
        return overrides.get(name.lower(), name.capitalize())
    except:
        return "Link"


# ---------------- [ БАЗА ДАННЫХ ] ---------------- #

async def init_db(pool):
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS posted_videos (
                id SERIAL PRIMARY KEY,
                e621_id INT UNIQUE NOT NULL,
                md5_hash TEXT, 
                posted_at TIMESTAMP DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_e621_id ON posted_videos(e621_id);
            CREATE INDEX IF NOT EXISTS idx_md5 ON posted_videos(md5_hash);
        """)
        # Миграция для старых баз
        try: await conn.execute("ALTER TABLE posted_videos ADD COLUMN IF NOT EXISTS md5_hash TEXT;")
        except: pass

async def check_db_health(pool):
    """Keepalive: Пингуем базу, если уснула - пересоздаем."""
    try:
        async with pool.acquire() as conn:
            await conn.execute("SELECT 1")
        return pool
    except Exception as e:
        logger.warning(f"🔌 DB Connection lost ({e}). Reconnecting...")
        try: await pool.close()
        except: pass
        return await asyncpg.create_pool(dsn=DB_DSN, min_size=1, max_size=2)

async def filter_new_posts(pool, posts_data):
    if not posts_data: return []
    
    candidate_ids = [p['id'] for p in posts_data]
    candidate_md5s = [p.get('file', {}).get('md5') for p in posts_data if p.get('file', {}).get('md5')]

    async with pool.acquire() as conn:
        rows_id = await conn.fetch("SELECT e621_id FROM posted_videos WHERE e621_id = ANY($1::int[])", candidate_ids)
        existing_ids = {r['e621_id'] for r in rows_id}
        
        existing_md5s = set()
        if candidate_md5s:
            rows_md5 = await conn.fetch("SELECT md5_hash FROM posted_videos WHERE md5_hash = ANY($1::text[])", candidate_md5s)
            existing_md5s = {r['md5_hash'] for r in rows_md5 if r['md5_hash']}

    final_posts = []
    for p in posts_data:
        if p['id'] in existing_ids: continue
        if p.get('file', {}).get('md5') in existing_md5s: continue
        final_posts.append(p)
        
    return final_posts

async def mark_as_posted(pool, post_id, md5):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO posted_videos (e621_id, md5_hash) VALUES ($1, $2) ON CONFLICT DO NOTHING", 
            post_id, md5
        )


# ---------------- [ КОНВЕРТАЦИЯ ] ---------------- #

async def convert_to_mp4(input_path, output_path):
    ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
    
    # nice -n 15: Низкий приоритет CPU
    # scale='min(640,iw)': Сжатие разрешения для скорости
    cmd = [
        "nice", "-n", "15", 
        ffmpeg_exe, "-y", "-v", "error",
        "-i", input_path,
        "-vf", "scale='min(640,iw)':-2", 
        "-c:v", "libx264", "-preset", "ultrafast", "-crf", "28",
        "-c:a", "aac", "-b:a", "128k",
        "-movflags", "+faststart",
        output_path
    ]
    
    try:
        process = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE
        )
        await asyncio.wait_for(process.wait(), timeout=CONVERT_TIMEOUT)
        
        if process.returncode == 0: return True
        else:
            _, stderr = await process.communicate()
            logger.error(f"FFmpeg failed: {stderr.decode()}")
            return False
            
    except asyncio.TimeoutError:
        logger.warning("⏱️ Conversion timed out.")
        try: process.kill()
        except: pass
        return False
    except Exception as e:
        logger.error(f"Convert error: {e}"); return False


# ---------------- [ ПАРСИНГ ] ---------------- #

def get_query_tags():
    tags = f"{BASE_TAGS} -type:png -type:jpg -type:swf"
    roll = random.random()
    if roll < 0.15: tags += " date:<6months"; mode = "Fresh"
    elif roll < 0.35: tags += " date:<1year"; mode = "Modern"
    else: mode = "Legacy"
    logger.info(f"🎲 {mode} | Query: {tags}")
    return tags

async def fetch_posts(session):
    try:
        url = "https://e621.net/posts.json"
        params = {"tags": f"{get_query_tags()} score:>={MIN_SCORE}", "limit": 50}
        async with session.get(url, params=params, headers=HEADERS) as resp:
            if resp.status != 200: return []
            data = await resp.json(loads=ujson.loads)
            return data.get("posts", [])
    except: return []

async def get_artist_links(session, artist_name):
    if artist_name in ARTIST_CACHE: return ARTIST_CACHE[artist_name]
    if artist_name.lower() in IGNORED_ARTISTS: return []

    try:
        url = "https://e621.net/artists.json"
        params = {"search[name]": artist_name, "limit": 1}
        async with session.get(url, params=params, headers=HEADERS, timeout=5) as resp:
            if resp.status == 200:
                data = await resp.json(loads=ujson.loads)
                if data:
                    urls = data[0].get("urls", [])
                    links = []
                    for u in urls:
                        addr = u.get("url", "")
                        if not addr: continue
                        # (v11.0) Умное определение названия сайта
                        name = smart_domain_name(addr)
                        links.append(f'<a href="{addr}">{name}</a>')
                    
                    seen = set(); unique = []
                    for l in links:
                        if l not in seen:
                            unique.append(l); seen.add(l)
                            if len(unique) >= 3: break 
                    ARTIST_CACHE[artist_name] = unique
                    return unique
    except: pass 
    ARTIST_CACHE[artist_name] = [] 
    return []

async def parse_post_async(session, post):
    f = post.get("file")
    if not f or not f.get("url"): return None
    ext = f["ext"]
    if ext not in ALLOWED_EXTS: return None
    
    ptags = post["tags"]
    all_tags = set(ptags["general"] + ptags["character"] + ptags["species"] + ptags["copyright"])
    if not all_tags.isdisjoint(BLACKLIST_SET): return None

    # Quality check
    orig_mb = f["size"] / 1_048_576
    target_url = f["url"]; target_size = f["size"]; is_compressed = False
    
    if orig_mb > MAX_ORIGINAL_SIZE_MB:
        sample = post.get("sample")
        if sample and sample.get("has") and sample.get("url"):
            target_url = sample["url"]; target_size = 0; is_compressed = True
        else: return None 

    # --- TEXT GENERATION ---
    artists_names = [a for a in ptags["artist"] if a.lower() not in IGNORED_ARTISTS]
    artist_lines = []
    
    for name in artists_names[:3]:
        # (v11.0) Escaping: Защита от спецсимволов в никах (<, &, >)
        safe_name = html.escape(name.replace("_", " ").title())
        e621_link = f'<a href="https://e621.net/posts?tags={name}">{safe_name}</a>'
        
        ext_links = await get_artist_links(session, name)
        line = f"{e621_link} ({' | '.join(ext_links)})" if ext_links else e621_link
        artist_lines.append(line)

    if not artist_lines: artist_block = "<b>Artist:</b> Unknown"
    elif len(artist_lines) > 1: artist_block = f"<b>Artists:</b> \n          " + "\n          ".join(artist_lines)
    else: artist_block = f"<b>Artist:</b> {artist_lines[0]}"
    if len(artists_names) > 3: artist_block += f" <i>(+{len(artists_names)-3} others)</i>"

    source_link = f"https://e621.net/posts/{post['id']}"
    source_block = f"<b>Source:</b> <a href='{source_link}'>e621</a>"
    quality_tag = " <i>(Compressed)</i>" if is_compressed else ""
    caption = f"{artist_block}\n{source_block}{quality_tag}"
    
    return {
        "id": post["id"], "md5": f.get("md5"), "url": target_url, "size": target_size, "ext": ext, 
        "caption": caption, "is_compressed": is_compressed,
        "width": f.get("width"), "height": f.get("height"), 
        "duration": int(float(post.get("duration") or 0)),
        "preview_url": post.get("sample", {}).get("url") or post.get("preview", {}).get("url")
    }


# ---------------- [ SENDING LOGIC ] ---------------- #

async def send_media(bot, session, meta):
    size_mb = meta["size"] / 1_048_576
    if meta["is_compressed"] or size_mb == 0:
        try:
            async with session.head(meta["url"], headers=HEADERS) as r:
                if r.status==200: size_mb = int(r.headers.get("Content-Length",0))/1048576
        except: size_mb = 25 

    if size_mb >= MAX_ORIGINAL_SIZE_MB: return False
    logger.info(f"⬇️ Processing {meta['id']} ({size_mb:.2f} MB)")

    with tempfile.TemporaryDirectory() as temp_dir:
        input_file = os.path.join(temp_dir, f"in.{meta['ext']}")
        output_file = os.path.join(temp_dir, f"vid_{meta['id']}.mp4")
        thumb_path = os.path.join(temp_dir, "thumb.jpg")
        
        # 1. Download Video (3 Retries)
        dl_ok = False
        for _ in range(3):
            try:
                async with session.get(meta["url"], headers=HEADERS) as resp:
                    if resp.status == 200:
                        with open(input_file, 'wb') as f:
                            while True:
                                chunk = await resp.content.read(65536)
                                if not chunk: break
                                f.write(chunk)
                        dl_ok = True; break
            except: await asyncio.sleep(1)
        
        if not dl_ok: return False

        # 2. Download Thumb
        has_thumb = False
        if meta["preview_url"] and meta["ext"] != "gif":
            try:
                async with session.get(meta["preview_url"], headers=HEADERS) as r:
                    if r.status==200:
                        with open(thumb_path, 'wb') as f: f.write(await r.read())
                        has_thumb = True
            except: pass

        final_path = input_file
        
        # 3. Convert WebM
        if meta["ext"] == "webm":
            logger.info("⚙️ Converting WebM -> MP4...")
            if await convert_to_mp4(input_file, output_file):
                if os.path.getsize(output_file)/1048576 < MAX_ORIGINAL_SIZE_MB:
                    final_path = output_file
                    logger.info("✅ Converted OK")
                else: logger.warning("⚠️ Converted > 50MB, reverting")

        # 4. Upload (3 Retries)
        for attempt in range(1, 4):
            try:
                media = FSInputFile(final_path, filename=f"video_{meta['id']}.mp4" if final_path.endswith("mp4") else f"v.{meta['ext']}")
                thumb = FSInputFile(thumb_path) if has_thumb else None
                
                kwargs = {
                    "chat_id": CHANNEL_ID, "caption": meta["caption"], "parse_mode": ParseMode.HTML
                }

                if meta["ext"] == "gif":
                    await bot.send_animation(animation=media, **kwargs)
                else:
                    await bot.send_video(
                        video=media, supports_streaming=True,
                        width=meta["width"], height=meta["height"], duration=meta["duration"],
                        thumbnail=thumb, **kwargs
                    )
                
                gc.collect()
                return True
            except Exception as e:
                logger.warning(f"⚠️ Upload retry {attempt}: {e}")
                await asyncio.sleep(2)
        
        return False


async def processing_cycle(bot, session, pool):
    logger.info("--- 🔄 Cycle Start ---")
    pool = await check_db_health(pool)
    posts = await fetch_posts(session)
    new_posts = await filter_new_posts(pool, posts)
    
    if not new_posts: logger.info("💤 No new content"); return

    sent = 0
    for post in new_posts:
        if sent >= VIDEOS_PER_BATCH: break
        meta = await parse_post_async(session, post)
        if not meta: continue
        
        if await send_media(bot, session, meta):
            await mark_as_posted(pool, meta["id"], meta["md5"])
            sent += 1
            await asyncio.sleep(5)
    
    logger.info(f"--- ✅ Cycle End. Sent: {sent} ---")


# ---------------- [ MAIN ] ---------------- #

async def health(r): return web.Response(text="Alive")

async def scheduler(bot, session, pool):
    while True:
        try: await processing_cycle(bot, session, pool)
        except Exception as e:
            logger.critical(f"🔥 Crash: {e}")
            await send_alert(bot, f"Critical Error:\n{e}")
        logger.info(f"⏳ Sleeping {SLEEP_INTERVAL}s...")
        await asyncio.sleep(SLEEP_INTERVAL)

async def main():
    clean_temp_garbage()
    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=1, max_size=2)
    await init_db(pool)
    
    app = web.Application(); app.add_routes([web.get('/', health)])
    runner = web.AppRunner(app); await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', int(os.getenv("PORT",8080))).start()
    
    # (v11.0) Network Timeout settings
    async with ClientSession(connector=TCPConnector(limit=10, ssl=False), 
                             json_serialize=ujson.dumps, headers=HEADERS,
                             timeout=NETWORK_TIMEOUT) as session:
        bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        if ADMIN_ID: asyncio.create_task(send_alert(bot, "🟢 Bot started."))
        await scheduler(bot, session, pool)

if __name__ == "__main__":
    try: asyncio.run(main())
    except: pass