import sys
import asyncio
import gc
import random
import os
from io import BytesIO

import uvloop
import ujson
import asyncpg
from aiohttp import web, ClientSession, TCPConnector
from aiogram import Bot
from aiogram.types import BufferedInputFile
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from loguru import logger
from cachetools import TTLCache

# ---------------- [ КОНФИГУРАЦИЯ ] ---------------- #

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")
DB_DSN = os.getenv("DB_DSN")
E621_USER_AGENT = os.getenv("E621_USER_AGENT", "TelegramVideoBot/6.0 (by Dexz)")
HEADERS = {"User-Agent": E621_USER_AGENT}

# Теги
BASE_TAGS = "-rating:safe order:random -human"
MIN_SCORE = 130

# Лимиты
MAX_ORIGINAL_SIZE_MB = 49.9

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


# ---------------- [ ПАРСИНГ И API ] ---------------- #

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
            if response.status != 200:
                logger.error(f"❌ API Error: {response.status}")
                return []
            data = await response.json(loads=ujson.loads)
            return data.get("posts", [])
    except Exception as e:
        logger.error(f"❌ Fetch Error: {e}")
        return []

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
                            if key in addr:
                                name = val
                                break
                        links.append(f'<a href="{addr}">{name}</a>')
                    
                    seen = set()
                    unique_links = []
                    for l in links:
                        if l not in seen:
                            unique_links.append(l)
                            seen.add(l)
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
    
    # Блеклист
    ptags = post["tags"]
    all_tags = set(ptags["general"] + ptags["character"] + ptags["species"] + ptags["copyright"])
    if not all_tags.isdisjoint(BLACKLIST_SET): return None

    # --- КАЧЕСТВО ---
    original_size_mb = f["size"] / 1_048_576
    target_url = f["url"]
    target_size = f["size"]
    is_compressed = False

    # Если оригинал больше лимита Telegram, ищем сэмпл
    if original_size_mb > MAX_ORIGINAL_SIZE_MB:
        sample = post.get("sample")
        if sample and sample.get("has") and sample.get("url"):
            target_url = sample["url"]
            target_size = 0 
            is_compressed = True
        else:
            return None # Слишком большой и нет сэмпла

    # --- МЕТАДАННЫЕ ВИДЕО (ВАЖНО ДЛЯ TELEGRAM) ---
    # Мы явно достаем ширину, высоту и длительность.
    # Это гарантирует, что Telegram отобразит файл как видео, а не документ.
    width = f.get("width")
    height = f.get("height")
    # Длительность иногда бывает null
    duration = post.get("duration") 
    if duration: 
        duration = int(float(duration))

    # --- АВТОРЫ ---
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

    # --- ИСТОЧНИК ---
    source_link = f"https://e621.net/posts/{post['id']}"
    source_block = f"<b>Source:</b> <a href='{source_link}'>e621</a>"

    quality_tag = " <i>(Compressed)</i>" if is_compressed else ""
    caption = f"{artist_block}\n{source_block}{quality_tag}"
    
    return {
        "id": post["id"], 
        "url": target_url, 
        "size": target_size, 
        "ext": ext, 
        "caption": caption,
        "is_compressed": is_compressed,
        "width": width,
        "height": height,
        "duration": duration
    }


# ---------------- [ ОТПРАВКА ] ---------------- #

async def send_media(bot, session, meta):
    size_mb = meta["size"] / 1_048_576
    filename = f"video_{meta['id']}.{meta['ext']}"
    
    # Если размер неизвестен (сэмпл), узнаем его
    if meta["is_compressed"] or size_mb == 0:
        try:
            async with session.head(meta["url"], headers=HEADERS) as resp:
                if resp.status == 200:
                    content_length = int(resp.headers.get("Content-Length", 0))
                    if content_length > 0:
                        size_mb = content_length / 1_048_576
                        logger.info(f"📏 Sample size: {size_mb:.2f} MB")
        except Exception:
            size_mb = 25 # Fallback

    # ЕДИНАЯ ЛОГИКА: Всегда качаем в RAM, если влезает в лимит.
    # Это решает проблему "Документ вместо видео" для мелких файлов по URL.
    if size_mb < MAX_ORIGINAL_SIZE_MB:
        logger.info(f"⬇️ RAM DL [{meta['ext']}]: {meta['id']} ({size_mb:.2f} MB)")
        
        try:
            async with session.get(meta["url"], headers=HEADERS) as resp:
                if resp.status != 200:
                    logger.error(f"DL Fail: {resp.status}")
                    return False
                content = await resp.read()
            
            file_obj = BytesIO(content)
            file_obj.name = filename
            del content
            
            logger.info(f"⬆️ RAM Upload...")
            media = BufferedInputFile(file_obj.getvalue(), filename=file_obj.name)
            
            kwargs = {
                "chat_id": CHANNEL_ID,
                "caption": meta["caption"],
                "parse_mode": ParseMode.HTML
            }

            if meta["ext"] == "gif":
                # Для GIF параметры width/height не так важны, но можно передать
                await bot.send_animation(animation=media, **kwargs)
            else:
                # ВАЖНО: Передаем метаданные видео!
                await bot.send_video(
                    video=media, 
                    supports_streaming=True,
                    width=meta["width"],
                    height=meta["height"],
                    duration=meta["duration"],
                    **kwargs
                )
            
            file_obj.close()
            del file_obj
            del media
            gc.collect()
            return True
            
        except Exception as e:
            logger.error(f"❌ RAM Send Error {meta['id']}: {e}")
            return False
            
    else:
        logger.warning(f"⚠️ Skip: File too big ({size_mb:.2f} MB)")
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