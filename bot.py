import sys
import asyncio
import gc
import random
import os
import tempfile
import glob
import html
import shutil
from urllib.parse import urlparse

# Сторонние библиотеки
import imageio_ffmpeg
import uvloop
import ujson
import asyncpg
from aiohttp import ClientSession, TCPConnector, ClientTimeout, web
from aiogram import Bot
from aiogram.types import FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from loguru import logger
from cachetools import TTLCache

# ===========================
# ⚙️ CONFIGURATION
# ===========================

CONFIG = {
    "BOT_TOKEN": os.getenv("BOT_TOKEN"),
    "CHANNEL_ID": os.getenv("CHANNEL_ID"),
    "ADMIN_ID": os.getenv("ADMIN_ID"),
    "DB_DSN": os.getenv("DB_DSN"),
    "USER_AGENT": os.getenv("E621_USER_AGENT", "TgBot/13.0 (Optimized for 0.1CPU)"),
    "SLEEP_INTERVAL": int(os.getenv("SLEEP_INTERVAL", 3600)),
    "VIDEOS_PER_BATCH": 2,
    "MIN_SCORE": 200,
    # Максимальный размер для скачивания и попытки сжатия
    "MAX_DOWNLOAD_MB": 80.0, 
    # Максимальный размер итогового файла для Telegram
    "MAX_TG_MB": 49.9,
    # Таймаут конвертации (10 минут)
    "CONVERT_TIMEOUT": 600
}

# Теги: Исключаем безопасное, людей, картинки, флеш
BASE_TAGS = "-rating:safe order:random -human -type:png -type:jpg -type:swf"

# Игнорируемые "художники"
IGNORED_ARTISTS = {
    "conditional_dnp", "sound_warning", "unknown", "anonymous",
    "ai_generated", "ai_assisted", "stable_diffusion", "img2img", "midjourney"
}

# Кэш для ссылок авторов
ARTIST_CACHE = TTLCache(maxsize=2000, ttl=86400)

# Проверка переменных
if not all([CONFIG["BOT_TOKEN"], CONFIG["CHANNEL_ID"], CONFIG["DB_DSN"]]):
    logger.critical("❌ Missing Environment Variables!")
    sys.exit(1)

# Установка быстрого лупа
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# ===========================
# 🛠️ UTILITIES
# ===========================

def clean_temp_dir():
    """Чистит временные файлы при старте."""
    try:
        tmp = tempfile.gettempdir()
        count = 0
        for p in glob.glob(os.path.join(tmp, "bot_temp_*")):
            try:
                os.remove(p)
                count += 1
            except: pass
        if count: logger.info(f"🧹 Cleaned {count} old temp files.")
    except Exception as e:
        logger.warning(f"Cleanup error: {e}")

def get_site_name(url):
    """Превращает ссылку в красивое название сайта."""
    try:
        domain = urlparse(url).netloc.replace("www.", "").split('.')[0].lower()
        mapping = {
            "twitter": "Twitter", "x": "Twitter", "furaffinity": "FA",
            "patreon": "Patreon", "inkbunny": "Inkbunny", "pixiv": "Pixiv",
            "bluesky": "Bluesky", "bsky": "Bluesky", "gumroad": "Gumroad",
            "ko-fi": "Ko-fi", "subscribestar": "SubStar", "newgrounds": "Newgrounds",
            "t": "Telegram", "vk": "VK", "sofurry": "SoFurry"
        }
        return mapping.get(domain, domain.capitalize())
    except: return "Link"

# ===========================
# 🗄️ DATABASE CLASS
# ===========================

class Database:
    def __init__(self, dsn):
        self.dsn = dsn
        self.pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(dsn=self.dsn, min_size=1, max_size=2)
        await self._init_schema()

    async def _init_schema(self):
        async with self.pool.acquire() as conn:
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
            # Миграция
            try: await conn.execute("ALTER TABLE posted_videos ADD COLUMN IF NOT EXISTS md5_hash TEXT;")
            except: pass

    async def check_health(self):
        """Keepalive для базы."""
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("SELECT 1")
        except:
            logger.warning("🔌 DB Reconnecting...")
            try: await self.pool.close()
            except: pass
            self.pool = await asyncpg.create_pool(dsn=self.dsn, min_size=1, max_size=2)

    async def filter_posts(self, posts):
        if not posts: return []
        
        ids = [p['id'] for p in posts]
        md5s = [p['file']['md5'] for p in posts if p.get('file', {}).get('md5')]

        async with self.pool.acquire() as conn:
            # Проверка ID
            rows_id = await conn.fetch("SELECT e621_id FROM posted_videos WHERE e621_id = ANY($1::int[])", ids)
            existing_ids = {r['e621_id'] for r in rows_id}
            
            # Проверка MD5
            existing_md5s = set()
            if md5s:
                rows_md5 = await conn.fetch("SELECT md5_hash FROM posted_videos WHERE md5_hash = ANY($1::text[])", md5s)
                existing_md5s = {r['md5_hash'] for r in rows_md5}

        filtered = []
        for p in posts:
            if p['id'] not in existing_ids and p.get('file', {}).get('md5') not in existing_md5s:
                filtered.append(p)
        return filtered

    async def add_post(self, post_id, md5):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO posted_videos (e621_id, md5_hash) VALUES ($1, $2) ON CONFLICT DO NOTHING", 
                post_id, md5
            )

# ===========================
# 🎬 CONVERTER CLASS (THE FIX)
# ===========================

class VideoConverter:
    @staticmethod
    async def process(input_path, output_path):
        """
        Экстремальная оптимизация для 0.1 CPU.
        """
        ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
        
        # 1. nice -n 19: Минимальный приоритет процесса (не мешает веб-серверу)
        # 2. scale='min(540,iw)':-2 : Уменьшаем до 540p (мобильное качество)
        # 3. fps=24 : Снижаем FPS до кинематографичных 24 (экономит 60% CPU по сравнению с 60fps)
        # 4. crf 32 : Низкое качество кодирования (быстрее)
        # 5. preset ultrafast : Самый быстрый пресет
        cmd = [
            "nice", "-n", "19", 
            ffmpeg_exe, "-y", "-v", "error",
            "-i", input_path,
            "-vf", "scale='min(540,iw)':-2,fps=24", 
            "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency", "-crf", "32",
            "-pix_fmt", "yuv420p", # Для iPhone
            "-c:a", "aac", "-b:a", "64k", "-ac", "2", # Легкое аудио
            "-movflags", "+faststart",
            output_path
        ]
        
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE
            )
            # Ждем 10 минут
            await asyncio.wait_for(process.wait(), timeout=CONFIG["CONVERT_TIMEOUT"])
            
            if process.returncode == 0:
                return True
            else:
                _, stderr = await process.communicate()
                logger.error(f"FFmpeg Error: {stderr.decode()}")
                return False
        except asyncio.TimeoutError:
            logger.error("⏱️ FFmpeg Timeout! Killing process.")
            try: process.kill()
            except: pass
            return False
        except Exception as e:
            logger.error(f"Converter Exception: {e}")
            return False

# ===========================
# 🌐 E621 CLIENT CLASS
# ===========================

class E621Client:
    def __init__(self, session):
        self.session = session
        self.headers = {"User-Agent": CONFIG["USER_AGENT"]}

    def _get_tags(self):
        tags = BASE_TAGS
        roll = random.random()
        if roll < 0.15: tags += " date:<6months"; mode = "Fresh"
        elif roll < 0.35: tags += " date:<1year"; mode = "Modern"
        else: mode = "Legacy"
        logger.info(f"🎲 {mode} | Query: {tags}")
        return tags

    async def fetch_posts(self):
        try:
            params = {"tags": f"{self._get_tags()} score:>={CONFIG['MIN_SCORE']}", "limit": 50}
            async with self.session.get("https://e621.net/posts.json", params=params, headers=self.headers) as resp:
                if resp.status != 200: return []
                data = await resp.json(loads=ujson.loads)
                return data.get("posts", [])
        except: return []

    async def get_artist_links(self, name):
        if name in ARTIST_CACHE: return ARTIST_CACHE[name]
        if name.lower() in IGNORED_ARTISTS: return []
        
        try:
            params = {"search[name]": name, "limit": 1}
            async with self.session.get("https://e621.net/artists.json", params=params, headers=self.headers) as resp:
                if resp.status == 200:
                    data = await resp.json(loads=ujson.loads)
                    if data:
                        urls = data[0].get("urls", [])
                        links = []
                        seen = set()
                        for u in urls:
                            url = u.get("url", "")
                            if url and url not in seen:
                                links.append((get_site_name(url), url))
                                seen.add(url)
                                if len(links) >= 3: break
                        ARTIST_CACHE[name] = links
                        return links
        except: pass
        ARTIST_CACHE[name] = []
        return []

    async def parse_post(self, post):
        f = post.get("file")
        if not f or not f.get("url"): return None
        ext = f["ext"]
        if ext not in {"webm", "mp4", "gif"}: return None
        
        # Blacklist check
        tags_flat = set(t for cat in post["tags"].values() for t in cat)
        blacklist = {"scat", "guro", "bestiality", "cub", "gore", "watersports", "hyper"}
        if not tags_flat.isdisjoint(blacklist): return None

        # Size check
        size_mb = f["size"] / 1_048_576
        target_url = f["url"]
        is_compressed = False
        
        # Если большой оригинал, ищем сэмпл
        if size_mb > CONFIG["MAX_TG_MB"]:
            sample = post.get("sample")
            if sample and sample.get("has") and sample.get("url"):
                target_url = sample["url"]
                is_compressed = True
            elif size_mb > CONFIG["MAX_DOWNLOAD_MB"]:
                return None # Даже сжимать слишком тяжело

        # Metadata
        preview_url = post.get("sample", {}).get("url") or post.get("preview", {}).get("url")
        
        # Artists & Buttons
        artists = [a for a in post["tags"]["artist"] if a.lower() not in IGNORED_ARTISTS][:3]
        keyboard = []
        keyboard.append([InlineKeyboardButton(text="🔗 e621 Source", url=f"https://e621.net/posts/{post['id']}")])
        
        artist_texts = []
        for name in artists:
            safe_name = html.escape(name.replace("_", " ").title())
            links = await self.get_artist_links(name)
            
            row = [InlineKeyboardButton(text=f"🎨 {safe_name}", url=f"https://e621.net/posts?tags={name}")]
            for site_name, site_url in links:
                row.append(InlineKeyboardButton(text=site_name, url=site_url))
            keyboard.append(row)
            artist_texts.append(f'<a href="https://e621.net/posts?tags={name}">{safe_name}</a>')

        markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
        artist_str = ", ".join(artist_texts) if artist_texts else "Unknown"
        qual_tag = " <i>(Compressed)</i>" if is_compressed else ""
        caption = f"<b>Artist:</b> {artist_str}{qual_tag}"

        return {
            "id": post["id"], "md5": f.get("md5"), "url": target_url, "ext": ext,
            "caption": caption, "markup": markup, "is_compressed": is_compressed,
            "width": f.get("width"), "height": f.get("height"), 
            "duration": int(float(post.get("duration") or 0)),
            "preview_url": preview_url, "has_spoiler": post["rating"] == "e"
        }

# ===========================
# 🤖 BOT WORKER
# ===========================

async def processing_cycle(bot, e621, db):
    logger.info("--- 🔄 Cycle Start ---")
    await db.check_health()
    
    posts = await e621.fetch_posts()
    new_posts = await db.filter_posts(posts)
    
    if not new_posts:
        logger.info("💤 No new content.")
        return

    sent = 0
    # Создаем временную папку для этого цикла
    with tempfile.TemporaryDirectory(prefix="bot_temp_") as temp_dir:
        for post in new_posts:
            if sent >= CONFIG["VIDEOS_PER_BATCH"]: break
            
            meta = await e621.parse_post(post)
            if not meta: continue

            # --- DOWNLOADING & PROCESSING ---
            logger.info(f"⬇️ Processing {meta['id']}...")
            
            input_file = os.path.join(temp_dir, f"in_{meta['id']}.{meta['ext']}")
            thumb_file = os.path.join(temp_dir, f"thumb_{meta['id']}.jpg")
            final_file = input_file
            
            # 1. Скачивание видео
            dl_success = False
            for _ in range(3):
                try:
                    async with e621.session.get(meta["url"], timeout=300) as resp:
                        if resp.status == 200:
                            with open(input_file, 'wb') as f:
                                while True:
                                    chunk = await resp.content.read(65536)
                                    if not chunk: break
                                    f.write(chunk)
                            dl_success = True; break
                except: await asyncio.sleep(1)
            
            if not dl_success: continue

            # 2. Скачивание превью
            has_thumb = False
            if meta["preview_url"] and meta["ext"] != "gif":
                try:
                    async with e621.session.get(meta["preview_url"], timeout=30) as r:
                        if r.status == 200:
                            with open(thumb_file, 'wb') as f: f.write(await r.read())
                            has_thumb = True
                except: pass

            # 3. Конвертация (WebM или Heavy MP4)
            file_size = os.path.getsize(input_file) / 1_048_576
            needs_convert = (meta["ext"] == "webm") or (file_size > CONFIG["MAX_TG_MB"] and meta["ext"] == "mp4")
            
            if needs_convert:
                logger.info(f"⚙️ Converting ({meta['ext']} -> mp4)...")
                output_mp4 = os.path.join(temp_dir, f"out_{meta['id']}.mp4")
                
                if await VideoConverter.process(input_file, output_mp4):
                    new_size = os.path.getsize(output_mp4) / 1_048_576
                    if new_size < CONFIG["MAX_TG_MB"]:
                        final_file = output_mp4
                        logger.info(f"✅ Success! {file_size:.1f}MB -> {new_size:.1f}MB")
                    else:
                        logger.warning("⚠️ Compressed result too big.")
                else:
                    logger.warning("⚠️ Conversion failed/timed out.")
                    # Если WebM не сконвертировался, но он маленький - пробуем слать как есть
                    if meta["ext"] == "webm" and file_size < CONFIG["MAX_TG_MB"]:
                        pass 
                    else:
                        continue # Не можем отправить

            # 4. Отправка
            for attempt in range(1, 4):
                try:
                    is_mp4 = final_file.endswith(".mp4")
                    video_input = FSInputFile(final_file, filename=f"video_{meta['id']}.mp4" if is_mp4 else f"v.{meta['ext']}")
                    thumb_input = FSInputFile(thumb_file) if has_thumb else None
                    
                    common = {
                        "chat_id": CONFIG["CHANNEL_ID"],
                        "caption": meta["caption"],
                        "parse_mode": ParseMode.HTML,
                        "reply_markup": meta["markup"]
                    }

                    if meta["ext"] == "gif":
                        await bot.send_animation(animation=video_input, has_spoiler=meta["has_spoiler"], **common)
                    else:
                        await bot.send_video(
                            video=video_input, thumbnail=thumb_input,
                            width=meta["width"], height=meta["height"], duration=meta["duration"],
                            supports_streaming=True, has_spoiler=meta["has_spoiler"], **common
                        )
                    
                    await db.add_post(meta["id"], meta["md5"])
                    sent += 1
                    logger.info("✅ Sent.")
                    break
                except Exception as e:
                    logger.error(f"Upload fail {attempt}: {e}")
                    await asyncio.sleep(2)
            
            # Чистка RAM после каждого файла
            gc.collect()
            await asyncio.sleep(5)

    logger.info(f"--- Cycle End. Sent: {sent} ---")

# ===========================
# 🚀 MAIN ENTRY POINT
# ===========================

async def health_check(r): return web.Response(text="Alive")

async def main():
    # Настройка логгера
    logger.remove()
    logger.add(sys.stdout, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{message}</level>")
    
    clean_temp_dir()
    
    # Инициализация
    db = Database(CONFIG["DB_DSN"])
    await db.connect()
    
    # Web Server
    app = web.Application()
    app.add_routes([web.get('/', health_check)])
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', int(os.getenv("PORT", 8080))).start()
    
    # Bot Session
    async with ClientSession(
        connector=TCPConnector(limit=10, ssl=False),
        json_serialize=ujson.dumps,
        timeout=ClientTimeout(total=600, connect=10)
    ) as session:
        
        bot = Bot(token=CONFIG["BOT_TOKEN"], default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        e621 = E621Client(session)
        
        if CONFIG["ADMIN_ID"]:
            try: await bot.send_message(CONFIG["ADMIN_ID"], "🟢 Bot Started (v13.0 Clean Rewrite)")
            except: pass

        while True:
            try:
                await processing_cycle(bot, e621, db)
            except Exception as e:
                logger.critical(f"🔥 Critical: {e}")
                if CONFIG["ADMIN_ID"]:
                    try: await bot.send_message(CONFIG["ADMIN_ID"], f"⚠️ Crash: {e}")
                    except: pass
            
            logger.info(f"⏳ Sleeping {CONFIG['SLEEP_INTERVAL']}s...")
            await asyncio.sleep(CONFIG["SLEEP_INTERVAL"])

if __name__ == "__main__":
    try: asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): pass