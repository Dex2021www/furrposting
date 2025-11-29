import sys
import asyncio
import gc
import random
import os
import tempfile
from io import BytesIO

import uvloop
import ujson
import asyncpg
from aiohttp import web, ClientSession, TCPConnector
from aiogram import Bot
from aiogram.types import FSInputFile, BufferedInputFile
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from loguru import logger
from cachetools import TTLCache

# ---------------- [ КОНФИГУРАЦИЯ ] ---------------- #

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")
DB_DSN = os.getenv("DB_DSN")
E621_USER_AGENT = os.getenv("E621_USER_AGENT", "TelegramVideoBot/8.0 (by Dexz)")
HEADERS = {"User-Agent": E621_USER_AGENT}

BASE_TAGS = "-rating:safe order:random -human"
MIN_SCORE = 130
MAX_ORIGINAL_SIZE_MB = 49.9

# Сколько времени даем на конвертацию (секунд). 
# На 0.1 CPU лучше не ставить слишком много, чтобы не вешать бота.
CONVERT_TIMEOUT = 90 

VIDEOS_PER_BATCH = 2
SLEEP_INTERVAL = int(os.getenv("SLEEP_INTERVAL", 3600))

ALLOWED_EXTS = {"webm", "mp4", "gif"}
BLACKLIST_WORDS = {"scat", "guro", "bestiality", "cub", "gore", "watersports", "hyper"}
BLACKLIST_SET = set(BLACKLIST_WORDS)

IGNORED_ARTISTS = {
    "conditional_dnp", "sound_warning", "unknown", "anonymous", 
    "ai_generated", "ai_assisted", "stable_diffusion", "img2img", "midjourney"
}

ARTIST_CACHE = TTLCache(maxsize=2000, ttl=86400)

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
logger.remove()
logger.add(sys.stdout, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{message}</level>")

if not BOT_TOKEN or not CHANNEL_ID or not DB_DSN:
    logger.critical("❌ Variables BOT_TOKEN, CHANNEL_ID, DB_DSN are missing!")
    sys.exit(1)


# ---------------- [ БАЗА ДАННЫХ ] ---------------- #

async def init_db(pool):
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS posted_videos (
                id SERIAL PRIMARY KEY,
                e621_id INT UNIQUE NOT NULL,
                posted_at TIMESTAMP DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_e621_id ON posted_videos(e621_id);
        """)

async def filter_new_posts(pool, posts_data):
    if not posts_data: return []
    candidate_ids = [p['id'] for p in posts_data]
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT e621_id FROM posted_videos WHERE e621_id = ANY($1::int[])", candidate_ids)
    existing_ids = {r['e621_id'] for r in rows}
    return [p for p in posts_data if p['id'] not in existing_ids]

async def mark_as_posted(pool, e621_id):
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO posted_videos (e621_id) VALUES ($1) ON CONFLICT DO NOTHING", e621_id)


# ---------------- [ КОНВЕРТАЦИЯ (FFMPEG) ] ---------------- #

async def convert_to_mp4(input_path, output_path):
    """
    Конвертирует WebM -> MP4 с жесткой оптимизацией под слабый CPU.
    Возвращает True, если успешно.
    """
    # Аргументы FFmpeg:
    # -y : перезаписать файл
    # -vf scale='min(640,iw)':-2 : Если видео шире 640px, уменьшаем до 640. Это спасает CPU!
    # -preset ultrafast : Максимальная скорость кодирования (ценой размера файла)
    # -crf 28 : Немного снижаем качество для скорости
    # -c:a aac : Аудио кодек
    cmd = [
        "ffmpeg", "-y", "-v", "error",
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
        # Ждем с таймаутом
        await asyncio.wait_for(process.wait(), timeout=CONVERT_TIMEOUT)
        
        if process.returncode == 0:
            return True
        else:
            _, stderr = await process.communicate()
            logger.error(f"FFmpeg failed: {stderr.decode()}")
            return False
            
    except asyncio.TimeoutError:
        logger.warning("⏱️ Conversion timed out! CPU is too slow.")
        try: process.kill()
        except: pass
        return False
    except FileNotFoundError:
        logger.error("❌ FFmpeg not installed on server!")
        return False
    except Exception as e:
        logger.error(f"Convert error: {e}")
        return False


# ---------------- [ ПАРСИНГ ] ---------------- #

def get_query_tags():
    tags = f"{BASE_TAGS} -type:png -type:jpg -type:swf"
    roll = random.random()
    if roll < 0.15:
        tags += " date:<6months"
        mode = "Fresh (<6mo)"
    elif roll < 0.35:
        tags += " date:<1year"
        mode = "Modern (<1yr)"
    else:
        mode = "Legacy (Any)"
    logger.info(f"🎲 Mode: {mode} | Query: {tags}")
    return tags

async def fetch_posts(session):
    url = "https://e621.net/posts.json"
    params = {"tags": f"{get_query_tags()} score:>={MIN_SCORE}", "limit": 50}
    try:
        async with session.get(url, params=params, headers=HEADERS) as response:
            if response.status != 200: return []
            data = await response.json(loads=ujson.loads)
            return data.get("posts", [])
    except Exception: return []

async def get_artist_links(session, artist_name):
    if artist_name in ARTIST_CACHE: return ARTIST_CACHE[artist_name]
    if artist_name.lower() in IGNORED_ARTISTS: return []

    url = "https://e621.net/artists.json"
    params = {"search[name]": artist_name, "limit": 1}
    try:
        async with session.get(url, params=params, headers=HEADERS, timeout=5) as resp:
            if resp.status == 200:
                data = await resp.json(loads=ujson.loads)
                if data:
                    urls = data[0].get("urls", [])
                    links = []
                    sites = {
                        "twitter": "Twitter", "x.com": "Twitter", "furaffinity": "FA", 
                        "patreon": "Patreon", "inkbunny": "Inkbunny", "pixiv": "Pixiv", 
                        "bluesky": "Bluesky", "bsky.app": "Bluesky", "newgrounds": "Newgrounds",
                        "weasyl": "Weasyl", "kobold": "Kobold", "sofurry": "SoFurry",
                        "deviantart": "DA", "tumblr": "Tumblr", "ko-fi": "Ko-fi",
                        "gumroad": "Gumroad", "subscribestar": "SubStar", "itaku": "Itaku"
                    }
                    for u in urls:
                        addr = u.get("url", "")
                        if not addr: continue
                        name = "Link"
                        for key, val in sites.items():
                            if key in addr: name = val; break
                        links.append(f'<a href="{addr}">{name}</a>')
                    
                    seen = set(); unique_links = []
                    for l in links:
                        if l not in seen:
                            unique_links.append(l); seen.add(l)
                            if len(unique_links) >= 3: break
                    ARTIST_CACHE[artist_name] = unique_links
                    return unique_links
    except Exception: pass 
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

    # Качество
    original_size_mb = f["size"] / 1_048_576
    target_url = f["url"]
    target_size = f["size"]
    is_compressed = False

    if original_size_mb > MAX_ORIGINAL_SIZE_MB:
        sample = post.get("sample")
        if sample and sample.get("has") and sample.get("url"):
            target_url = sample["url"]
            target_size = 0 
            is_compressed = True
        else:
            return None 

    # Метаданные
    width = f.get("width")
    height = f.get("height")
    duration = post.get("duration") 
    if duration: duration = int(float(duration))
    preview_url = post.get("sample", {}).get("url") or post.get("preview", {}).get("url")

    # Авторы
    artists_names = [a for a in ptags["artist"] if a.lower() not in IGNORED_ARTISTS]
    artist_lines = []
    for name in artists_names[:3]:
        e621_link = f'<a href="https://e621.net/posts?tags={name}">{name.replace("_", " ").title()}</a>'
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
        "id": post["id"], "url": target_url, "size": target_size, "ext": ext, 
        "caption": caption, "is_compressed": is_compressed,
        "width": width, "height": height, "duration": duration,
        "preview_url": preview_url
    }


# ---------------- [ ОТПРАВКА С КОНВЕРТАЦИЕЙ ] ---------------- #

async def send_media(bot, session, meta):
    size_mb = meta["size"] / 1_048_576
    
    # Узнаем размер, если сэмпл
    if meta["is_compressed"] or size_mb == 0:
        try:
            async with session.head(meta["url"], headers=HEADERS) as resp:
                if resp.status == 200:
                    cl = int(resp.headers.get("Content-Length", 0))
                    if cl > 0: size_mb = cl / 1_048_576
        except Exception: size_mb = 25 

    if size_mb >= MAX_ORIGINAL_SIZE_MB:
        logger.warning(f"⚠️ Skip: too big ({size_mb:.2f} MB)")
        return False

    logger.info(f"⬇️ Processing [{meta['ext']}]: {meta['id']} ({size_mb:.2f} MB)")

    # Создаем временные файлы для конвертации
    # Используем tempfile, чтобы не забивать RAM
    with tempfile.TemporaryDirectory() as temp_dir:
        input_file = os.path.join(temp_dir, f"input.{meta['ext']}")
        output_file = os.path.join(temp_dir, f"video_{meta['id']}.mp4")
        thumb_path = os.path.join(temp_dir, "thumb.jpg")
        
        # 1. Скачиваем файл на диск
        try:
            async with session.get(meta["url"], headers=HEADERS) as resp:
                if resp.status != 200: return False
                with open(input_file, 'wb') as f:
                    while True:
                        chunk = await resp.content.read(65536) # 64KB chunks
                        if not chunk: break
                        f.write(chunk)
        except Exception as e:
            logger.error(f"DL Error: {e}")
            return False

        # 2. Скачиваем обложку (нужна в любом случае)
        has_thumb = False
        if meta["preview_url"] and meta["ext"] != "gif":
            try:
                async with session.get(meta["preview_url"], headers=HEADERS) as resp:
                    if resp.status == 200:
                        data = await resp.read()
                        with open(thumb_path, 'wb') as f: f.write(data)
                        has_thumb = True
            except: pass

        final_file_path = input_file
        final_ext = meta["ext"]

        # 3. КОНВЕРТАЦИЯ (Только если это WebM и не GIF)
        # Если GIF - шлем как есть. Если WebM - пытаемся сделать MP4.
        if meta["ext"] == "webm":
            logger.info("⚙️ Converting WebM -> MP4 (CPU intensive)...")
            success = await convert_to_mp4(input_file, output_file)
            
            if success:
                # Проверяем размер после конвертации
                out_size_mb = os.path.getsize(output_file) / 1_048_576
                if out_size_mb < MAX_ORIGINAL_SIZE_MB:
                    logger.info(f"✅ Converted! Size: {out_size_mb:.2f} MB")
                    final_file_path = output_file
                    final_ext = "mp4"
                else:
                    logger.warning(f"⚠️ MP4 too big ({out_size_mb:.2f} MB), reverting to WebM")
            else:
                logger.warning("⚠️ Conversion failed, sending original WebM")

        # 4. Отправка в Telegram
        try:
            # FSInputFile читает с диска, экономит RAM
            video_file = FSInputFile(final_file_path, filename=f"video_{meta['id']}.{final_ext}")
            thumb_file = FSInputFile(thumb_path) if has_thumb else None
            
            kwargs = {
                "chat_id": CHANNEL_ID,
                "caption": meta["caption"],
                "parse_mode": ParseMode.HTML
            }

            logger.info(f"⬆️ Uploading...")
            if meta["ext"] == "gif":
                await bot.send_animation(animation=video_file, **kwargs)
            else:
                await bot.send_video(
                    video=video_file, 
                    supports_streaming=True,
                    width=meta["width"],
                    height=meta["height"],
                    duration=meta["duration"],
                    thumbnail=thumb_file,
                    **kwargs
                )
            
            # Файлы удалятся автоматически при выходе из with tempfile
            gc.collect()
            return True

        except Exception as e:
            logger.error(f"❌ Upload Error: {e}")
            return False


async def processing_cycle(bot, session, pool):
    logger.info("--- 🔄 Cycle Start ---")
    posts = await fetch_posts(session)
    new_posts = await filter_new_posts(pool, posts)
    
    if not new_posts:
        logger.info("💤 No content.")
        return

    sent_count = 0
    for post in new_posts:
        if sent_count >= VIDEOS_PER_BATCH: break
        
        meta = await parse_post_async(session, post)
        if not meta: continue
        
        if await send_media(bot, session, meta):
            await mark_as_posted(pool, meta["id"])
            sent_count += 1
            await asyncio.sleep(5)
    
    logger.info(f"--- ✅ Done. Sent: {sent_count} ---")


# ---------------- [ MAIN ] ---------------- #

async def health_check(request): return web.Response(text="Alive")

async def start_web_server():
    app = web.Application()
    app.add_routes([web.get('/', health_check)])
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", 8080))
    await web.TCPSite(runner, '0.0.0.0', port).start()
    logger.info(f"🌍 Web server: {port}")

async def scheduler(bot, session, pool):
    while True:
        try:
            await processing_cycle(bot, session, pool)
        except Exception as e:
            logger.critical(f"🔥 Crash: {e}")
        logger.info(f"⏳ Sleeping {SLEEP_INTERVAL}s...")
        await asyncio.sleep(SLEEP_INTERVAL)

async def main():
    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=1, max_size=2)
    await init_db(pool)
    async with ClientSession(connector=TCPConnector(limit=10, ssl=False), 
                             json_serialize=ujson.dumps,
                             headers=HEADERS) as session:
        bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        await asyncio.gather(start_web_server(), scheduler(bot, session, pool))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): pass