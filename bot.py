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
from aiogram.types import FSInputFile
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
    # Исправленный User-Agent с твоим ником
    "USER_AGENT": os.getenv("E621_USER_AGENT", "TelegramVideoBot/14.0 (by Dexz)"),
    "SLEEP_INTERVAL": int(os.getenv("SLEEP_INTERVAL", 3600)),
    "VIDEOS_PER_BATCH": 2,
    "MIN_SCORE": 200,
    "MAX_DOWNLOAD_MB": 80.0, 
    "MAX_TG_MB": 49.9,
    "CONVERT_TIMEOUT": 600
}

BASE_TAGS = "-rating:safe order:random -human -type:png -type:jpg -type:swf"

IGNORED_ARTISTS = {
    "conditional_dnp", "sound_warning", "unknown", "anonymous",
    "ai_generated", "ai_assisted", "stable_diffusion", "img2img", "midjourney"
}

ARTIST_CACHE = TTLCache(maxsize=2000, ttl=86400)

if not all([CONFIG["BOT_TOKEN"], CONFIG["CHANNEL_ID"], CONFIG["DB_DSN"]]):
    logger.critical("❌ Missing Environment Variables!")
    sys.exit(1)

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# ===========================
# 🛠️ UTILITIES
# ===========================

def clean_temp_dir():
    try:
        tmp = tempfile.gettempdir()
        count = 0
        for p in glob.glob(os.path.join(tmp, "bot_temp_*")):
            try: os.remove(p); count += 1
            except: pass
        if count: logger.info(f"🧹 Cleaned {count} old temp files.")
    except Exception as e: logger.warning(f"Cleanup error: {e}")

def get_site_name(url):
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
                    posted_at TIMESTAMP DEFAULT NOW()
                );
            """)
            try: await conn.execute("ALTER TABLE posted_videos ADD COLUMN IF NOT EXISTS md5_hash TEXT;")
            except: pass
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_e621_id ON posted_videos(e621_id);
                CREATE INDEX IF NOT EXISTS idx_md5 ON posted_videos(md5_hash);
            """)

    async def check_health(self):
        try:
            async with self.pool.acquire() as conn: await conn.execute("SELECT 1")
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
            rows_id = await conn.fetch("SELECT e621_id FROM posted_videos WHERE e621_id = ANY($1::int[])", ids)
            existing_ids = {r['e621_id'] for r in rows_id}
            
            existing_md5s = set()
            if md5s:
                rows_md5 = await conn.fetch("SELECT md5_hash FROM posted_videos WHERE md5_hash = ANY($1::text[])", md5s)
                existing_md5s = {r['md5_hash'] for r in rows_md5}

        return [p for p in posts if p['id'] not in existing_ids and p.get('file', {}).get('md5') not in existing_md5s]

    async def add_post(self, post_id, md5):
        async with self.pool.acquire() as conn:
            await conn.execute("INSERT INTO posted_videos (e621_id, md5_hash) VALUES ($1, $2) ON CONFLICT DO NOTHING", post_id, md5)

# ===========================
# 🎬 CONVERTER CLASS
# ===========================

class VideoConverter:
    @staticmethod
    async def process(input_path, output_path):
        ffmpeg_exe = imageio_ffmpeg.get_ffmpeg_exe()
        
        # Настройки баланса (720p, 30fps, crf 26)
        cmd = [
            "nice", "-n", "19", 
            ffmpeg_exe, "-y", "-v", "error",
            "-i", input_path,
            "-vf", "scale='min(720,iw)':-2,fps=30", 
            "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency", "-crf", "26",
            "-pix_fmt", "yuv420p", 
            "-c:a", "aac", "-b:a", "128k", "-ac", "2",
            "-movflags", "+faststart",
            output_path
        ]
        
        try:
            process = await asyncio.create_subprocess_exec(
                *cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE
            )
            await asyncio.wait_for(process.wait(), timeout=CONFIG["CONVERT_TIMEOUT"])
            
            if process.returncode == 0: return True
            else:
                _, stderr = await process.communicate()
                logger.error(f"FFmpeg Error: {stderr.decode()}")
                return False
        except asyncio.TimeoutError:
            logger.error("⏱️ FFmpeg Timeout!")
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
        
        tags_flat = set(t for cat in post["tags"].values() for t in cat)
        blacklist = {"scat", "guro", "bestiality", "cub", "gore", "watersports", "hyper"}
        if not tags_flat.isdisjoint(blacklist): return None

        size_mb = f["size"] / 1_048_576
        target_url = f["url"]
        is_compressed = False
        
        if size_mb > CONFIG["MAX_TG_MB"]:
            sample = post.get("sample")
            if sample and sample.get("has") and sample.get("url"):
                target_url = sample["url"]
                is_compressed = True
            elif size_mb > CONFIG["MAX_DOWNLOAD_MB"]: return None 

        preview_url = post.get("sample", {}).get("url") or post.get("preview", {}).get("url")
        artists = [a for a in post["tags"]["artist"] if a.lower() not in IGNORED_ARTISTS][:3]
        
        # Генерация текста (без кнопок)
        artist_texts = []
        for name in artists:
            safe_name = html.escape(name.replace("_", " ").title())
            links = await self.get_artist_links(name)
            
            # Ссылка на e621 + (Ссылки в скобках)
            e621_link = f'<a href="https://e621.net/posts?tags={name}">{safe_name}</a>'
            
            if links:
                # Формируем строку ссылок: Twitter | Patreon ...
                external_links_str = " | ".join([f'<a href="{url}">{site}</a>' for site, url in links])
                line = f"{e621_link} ({external_links_str})"
            else:
                line = e621_link
            
            artist_texts.append(line)

        # Собираем красивый блок авторов
        if not artist_texts:
            artist_block = "<b>Artist:</b> Unknown"
        elif len(artist_texts) == 1:
            artist_block = f"<b>Artist:</b> {artist_texts[0]}"
        else:
            # Если несколько авторов - каждый с новой строки для читаемости
            artist_block = "<b>Artists:</b>\n" + "\n".join(artist_texts)

        source_link = f"https://e621.net/posts/{post['id']}"
        qual_tag = " <i>(Compressed)</i>" if is_compressed else ""
        
        # Итоговое описание: Автор, Источник, Пометка сжатия
        caption = f"{artist_block}\n<b>Source:</b> <a href='{source_link}'>e621</a>{qual_tag}"

        return {
            "id": post["id"], "md5": f.get("md5"), "url": target_url, "ext": ext,
            "caption": caption, "is_compressed": is_compressed,
            "width": f.get("width"), "height": f.get("height"), 
            "duration": int(float(post.get("duration") or 0)),
            "preview_url": preview_url
        }

# ===========================
# 🤖 BOT WORKER
# ===========================

async def processing_cycle(bot, e621, db):
    logger.info("--- 🔄 Cycle Start ---")
    await db.check_health()
    
    posts = await e621.fetch_posts()
    new_posts = await db.filter_posts(posts)
    
    if not new_posts: logger.info("💤 No new content."); return

    sent = 0
    with tempfile.TemporaryDirectory(prefix="bot_temp_") as temp_dir:
        for post in new_posts:
            if sent >= CONFIG["VIDEOS_PER_BATCH"]: break
            meta = await e621.parse_post(post)
            if not meta: continue

            logger.info(f"⬇️ Processing {meta['id']}...")
            
            input_file = os.path.join(temp_dir, f"in_{meta['id']}.{meta['ext']}")
            thumb_file = os.path.join(temp_dir, f"thumb_{meta['id']}.jpg")
            final_file = input_file
            
            # Download
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

            # Thumb
            has_thumb = False
            if meta["preview_url"] and meta["ext"] != "gif":
                try:
                    async with e621.session.get(meta["preview_url"], timeout=30) as r:
                        if r.status == 200:
                            with open(thumb_file, 'wb') as f: f.write(await r.read())
                            has_thumb = True
                except: pass

            # Convert
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
                    else: logger.warning("⚠️ Compressed result too big.")
                else:
                    logger.warning("⚠️ Conversion failed.")
                    if meta["ext"] == "webm" and file_size < CONFIG["MAX_TG_MB"]: pass 
                    else: continue

            # Upload
            for attempt in range(1, 4):
                try:
                    is_mp4 = final_file.endswith(".mp4")
                    video_input = FSInputFile(final_file, filename=f"video_{meta['id']}.mp4" if is_mp4 else f"v.{meta['ext']}")
                    thumb_input = FSInputFile(thumb_file) if has_thumb else None
                    
                    common = {
                        "chat_id": CONFIG["CHANNEL_ID"],
                        "caption": meta["caption"],
                        "parse_mode": ParseMode.HTML
                    }

                    if meta["ext"] == "gif":
                        await bot.send_animation(animation=video_input, **common)
                    else:
                        await bot.send_video(
                            video=video_input, thumbnail=thumb_input,
                            width=meta["width"], height=meta["height"], duration=meta["duration"],
                            supports_streaming=True, **common
                        )
                    
                    await db.add_post(meta["id"], meta["md5"])
                    sent += 1
                    logger.info("✅ Sent.")
                    break
                except Exception as e:
                    logger.error(f"Upload fail {attempt}: {e}")
                    await asyncio.sleep(2)
            
            gc.collect()
            await asyncio.sleep(5)

    logger.info(f"--- Cycle End. Sent: {sent} ---")

# ===========================
# 🚀 MAIN
# ===========================

async def health_check(r): return web.Response(text="Alive")

async def main():
    logger.remove()
    logger.add(sys.stdout, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{message}</level>")
    clean_temp_dir()
    
    db = Database(CONFIG["DB_DSN"])
    await db.connect()
    
    app = web.Application()
    app.add_routes([web.get('/', health_check)])
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', int(os.getenv("PORT", 8080))).start()
    
    async with ClientSession(
        connector=TCPConnector(limit=10, ssl=False),
        json_serialize=ujson.dumps,
        timeout=ClientTimeout(total=600, connect=10)
    ) as session:
        
        bot = Bot(token=CONFIG["BOT_TOKEN"], default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        e621 = E621Client(session)
        
        if CONFIG["ADMIN_ID"]:
            try: await bot.send_message(CONFIG["ADMIN_ID"], "🟢 Bot Started (v14.0 Clean & Simple)")
            except: pass

        while True:
            try: await processing_cycle(bot, e621, db)
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