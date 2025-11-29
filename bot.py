import sys
import asyncio
import gc
import random
import os
from io import BytesIO

# Высокопроизводительный Event Loop
import uvloop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

import ujson
import asyncpg
from aiohttp import web, ClientSession, TCPConnector
from aiogram import Bot
from aiogram.types import BufferedInputFile, URLInputFile
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from loguru import logger

# ---------------- [ КОНФИГУРАЦИЯ ] ---------------- #

# Telegram (Читаем из Render Environment)
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")

# Database (Neon PostgreSQL)
DB_DSN = os.getenv("DB_DSN")

# e621 Settings
E621_USER_AGENT = os.getenv("E621_USER_AGENT", "TelegramVideoBot/2.0 (by Dexz)")

# Базовый запрос
BASE_TAGS = "-rating:safe order:random -human"
MIN_SCORE = 130

# Настройки планировщика
VIDEOS_PER_BATCH = 2
SLEEP_INTERVAL = int(os.getenv("SLEEP_INTERVAL", 3600))

# Блеклист
BLACKLIST_WORDS = {"scat", "guro", "bestiality", "cub", "gore", "watersports"}
BLACKLIST_SET = set(BLACKLIST_WORDS)

# Разрешенные расширения
ALLOWED_EXTS = {"webm", "mp4", "gif"}

# Логгер
logger.remove()
logger.add(sys.stdout, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{message}</level>")

if not BOT_TOKEN or not CHANNEL_ID or not DB_DSN:
    logger.critical("❌ Переменные окружения не заданы!")
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
    if not posts_data:
        return []
    candidate_ids = [p['id'] for p in posts_data]
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT e621_id FROM posted_videos WHERE e621_id = ANY($1::int[])", candidate_ids)
    existing_ids = {r['e621_id'] for r in rows}
    new_posts = [p for p in posts_data if p['id'] not in existing_ids]
    logger.info(f"🔍 DB Filter: {len(posts_data)} fetched -> {len(new_posts)} new.")
    return new_posts

async def mark_as_posted(pool, e621_id):
    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO posted_videos (e621_id) VALUES ($1) ON CONFLICT DO NOTHING", e621_id)


# ---------------- [ E621 API ] ---------------- #

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
    headers = {"User-Agent": E621_USER_AGENT}
    try:
        async with session.get(url, params=params, headers=headers) as response:
            if response.status != 200:
                logger.error(f"❌ API Error: {response.status}")
                return []
            data = await response.json(loads=ujson.loads)
            return data.get("posts", [])
    except Exception as e:
        logger.error(f"❌ Fetch Error: {e}")
        return []

def parse_post(post):
    f = post.get("file")
    if not f or not f.get("url"): return None
    ext = f["ext"]
    if ext not in ALLOWED_EXTS: return None
    
    ptags = post["tags"]
    all_tags = set(ptags["general"] + ptags["character"] + ptags["species"] + ptags["copyright"])
    if not all_tags.isdisjoint(BLACKLIST_SET): return None

    artists = ptags["artist"]
    artist_links = [f'<a href="https://e621.net/posts?tags={a}">{a.replace("_", " ").title()}</a>' for a in artists]
    caption = f"<b>Artist:</b> {', '.join(artist_links) if artist_links else 'Unknown'}\n<b>Source:</b> <a href='https://e621.net/posts/{post['id']}'>e621</a>"
    
    return {"id": post["id"], "url": f["url"], "size": f["size"], "ext": ext, "caption": caption}


# ---------------- [ ОТПРАВКА (FIXED) ] ---------------- #

async def send_media(bot, session, meta):
    """
    Отправляет медиа. 
    ИСПРАВЛЕНО: Строгое разделение send_video и send_animation.
    """
    size_mb = meta["size"] / 1_048_576 
    is_gif = meta["ext"] == "gif"
    
    try:
        # 1. URL Sending (< 20 MB)
        if size_mb < 20:
            logger.info(f"📤 Sending via URL [{meta['ext']}]: {meta['id']} ({size_mb:.2f} MB)")
            media_file = URLInputFile(meta["url"])
            
            if is_gif:
                await bot.send_animation(
                    chat_id=CHANNEL_ID,
                    animation=media_file,
                    caption=meta["caption"],
                    parse_mode=ParseMode.HTML
                )
            else:
                await bot.send_video(
                    chat_id=CHANNEL_ID,
                    video=media_file,
                    caption=meta["caption"],
                    parse_mode=ParseMode.HTML,
                    supports_streaming=True
                )
            return True

        # 2. RAM Upload (20-50 MB)
        elif size_mb < 50:
            logger.info(f"⬇️ RAM Download [{meta['ext']}]: {meta['id']} ({size_mb:.2f} MB)")
            async with session.get(meta["url"]) as resp:
                if resp.status != 200: return False
                content = await resp.read()
                
            file_obj = BytesIO(content)
            file_obj.name = f"{meta['id']}.{meta['ext']}"
            del content # Free RAM
            
            file_input = BufferedInputFile(file_obj.getvalue(), filename=file_obj.name)
            logger.info(f"⬆️ RAM Uploading...")
            
            if is_gif:
                await bot.send_animation(
                    chat_id=CHANNEL_ID,
                    animation=file_input,
                    caption=meta["caption"],
                    parse_mode=ParseMode.HTML
                )
            else:
                await bot.send_video(
                    chat_id=CHANNEL_ID,
                    video=file_input,
                    caption=meta["caption"],
                    parse_mode=ParseMode.HTML,
                    supports_streaming=True
                )
            
            file_obj.close()
            del file_obj
            del file_input
            gc.collect()
            return True
            
        else:
            logger.warning(f"⚠️ Skip: File too big ({size_mb:.2f} MB)")
            return False

    except Exception as e:
        logger.error(f"❌ Send Error {meta['id']}: {e}")
        return False


async def processing_cycle(bot, session, pool):
    logger.info("--- 🔄 Cycle Start ---")
    posts = await fetch_posts(session)
    new_posts = await filter_new_posts(pool, posts)
    
    if not new_posts:
        logger.info("💤 No content to process.")
        return

    sent_count = 0
    for post in new_posts:
        if sent_count >= VIDEOS_PER_BATCH: break
        
        meta = parse_post(post)
        if not meta: continue
        
        if await send_media(bot, session, meta):
            await mark_as_posted(pool, meta["id"])
            sent_count += 1
            await asyncio.sleep(5)
    
    logger.info(f"--- ✅ Cycle End. Sent: {sent_count} ---")


# ---------------- [ RUNNER ] ---------------- #

async def health_check(request): return web.Response(text="Alive")

async def start_web_server():
    app = web.Application()
    app.add_routes([web.get('/', health_check)])
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", 8080))
    await web.TCPSite(runner, '0.0.0.0', port).start()
    logger.info(f"🌍 Web server running on port {port}")

async def scheduler(bot, session, pool):
    while True:
        try:
            await processing_cycle(bot, session, pool)
        except Exception as e:
            logger.critical(f"🔥 Scheduler Crash: {e}")
        logger.info(f"⏳ Sleeping for {SLEEP_INTERVAL} seconds...")
        await asyncio.sleep(SLEEP_INTERVAL)

async def main():
    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=1, max_size=2)
    await init_db(pool)
    async with ClientSession(connector=TCPConnector(limit=10, ssl=False), json_serialize=ujson.dumps) as session:
        bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        await asyncio.gather(start_web_server(), scheduler(bot, session, pool))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped.")