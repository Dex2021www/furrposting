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
from aiogram.types import BufferedInputFile, URLInputFile
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from loguru import logger
from cachetools import TTLCache

# ---------------- [ КОНФИГУРАЦИЯ ] ---------------- #

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHANNEL_ID = os.getenv("CHANNEL_ID")
DB_DSN = os.getenv("DB_DSN")

# User-Agent обязателен для всех запросов (API и файлы)
E621_USER_AGENT = os.getenv("E621_USER_AGENT", "TelegramVideoBot/3.0 (by Dexz)")
HEADERS = {"User-Agent": E621_USER_AGENT}

# Теги поиска
BASE_TAGS = "-rating:safe order:random -human"
MIN_SCORE = 130

# Настройки
VIDEOS_PER_BATCH = 2
SLEEP_INTERVAL = int(os.getenv("SLEEP_INTERVAL", 3600))
ALLOWED_EXTS = {"webm", "mp4", "gif"}

# Блеклист
BLACKLIST_WORDS = {"scat", "guro", "bestiality", "cub", "gore", "watersports", "hyper"}
BLACKLIST_SET = set(BLACKLIST_WORDS)

# Кэш для данных об авторах (храним 1000 авторов 24 часа)
# Это критично для оптимизации API запросов
ARTIST_CACHE = TTLCache(maxsize=1000, ttl=86400)

# Инициализация
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


# ---------------- [ E621 API & PARSING ] ---------------- #

def get_query_tags():
    # Исключаем картинки и флеш. Оставляем видео и гифки.
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
    """
    Получает ссылки на ресурсы автора (Twitter, FA и т.д.).
    Использует кэш для экономии запросов.
    """
    if artist_name in ARTIST_CACHE:
        return ARTIST_CACHE[artist_name]
    
    if artist_name.lower() in {"unknown", "anonymous", "conditional_dnp"}:
        return []

    url = "https://e621.net/artists.json"
    params = {"search[name]": artist_name, "limit": 1}
    
    try:
        # Небольшой таймаут, чтобы не тормозить весь процесс
        async with session.get(url, params=params, headers=HEADERS, timeout=5) as resp:
            if resp.status == 200:
                data = await resp.json(loads=ujson.loads)
                if data:
                    artist_data = data[0]
                    # Извлекаем ссылки из поля 'urls'
                    urls = artist_data.get("urls", [])
                    # Фильтруем пустые и берем нужные поля
                    links = []
                    for u in urls:
                        addr = u.get("url", "")
                        if not addr: continue
                        
                        # Красивое название для ссылки
                        name = "Link"
                        if "twitter" in addr or "x.com" in addr: name = "Twitter"
                        elif "furaffinity" in addr: name = "FA"
                        elif "patreon" in addr: name = "Patreon"
                        elif "inkbunny" in addr: name = "Inkbunny"
                        elif "pixiv" in addr: name = "Pixiv"
                        elif "bluesky" in addr or "bsky.app" in addr: name = "Bluesky"
                        
                        links.append(f'<a href="{addr}">{name}</a>')
                    
                    # Сохраняем топ-3 ссылки, чтобы не спамить
                    result = links[:4]
                    ARTIST_CACHE[artist_name] = result
                    return result
    except Exception:
        pass # Если ошибка API, просто возвращаем пустоту
    
    ARTIST_CACHE[artist_name] = [] # Кэшируем пустоту, чтобы не долбить API снова
    return []

async def parse_post_async(session, post):
    """Асинхронный парсинг поста с подгрузкой инфо об авторе."""
    f = post.get("file")
    if not f or not f.get("url"): return None
    ext = f["ext"]
    if ext not in ALLOWED_EXTS: return None
    
    # Блеклист
    ptags = post["tags"]
    all_tags = set(ptags["general"] + ptags["character"] + ptags["species"] + ptags["copyright"])
    if not all_tags.isdisjoint(BLACKLIST_SET): return None

    # Обработка автора
    artists_names = ptags["artist"]
    # Исключаем служебные теги
    valid_artists = [a for a in artists_names if a not in ["conditional_dnp", "sound_warning"]]
    
    artist_block = ""
    if valid_artists:
        # Берем первого основного автора для поиска ссылок
        main_artist = valid_artists[0]
        links = await get_artist_links(session, main_artist)
        
        # Ссылка на тег e621
        e621_artist_link = f'<a href="https://e621.net/posts?tags={main_artist}">{main_artist.replace("_", " ").title()}</a>'
        
        if links:
            # Формат: ArtistName (Twitter | Patreon)
            links_str = " | ".join(links)
            artist_block = f"<b>Artist:</b> {e621_artist_link} ({links_str})"
        else:
            artist_block = f"<b>Artist:</b> {e621_artist_link}"
    else:
        artist_block = "<b>Artist:</b> Unknown"

    # Источник (Source) из самого поста
    sources = post.get("sources", [])
    source_link_e621 = f"https://e621.net/posts/{post['id']}"
    
    # Если есть внешний источник (Twitter, etc), указываем его
    if sources and sources[0]:
        # Обрезаем длинные ссылки для красоты (опционально)
        direct_source = f"<a href='{sources[0]}'>Original</a>"
        source_block = f"<b>Source:</b> {direct_source} | <a href='{source_link_e621}'>e621</a>"
    else:
        source_block = f"<b>Source:</b> <a href='{source_link_e621}'>e621</a>"

    caption = f"{artist_block}\n{source_block}"
    
    return {"id": post["id"], "url": f["url"], "size": f["size"], "ext": ext, "caption": caption}


# ---------------- [ SENDING LOGIC ] ---------------- #

async def send_media(bot, session, meta):
    size_mb = meta["size"] / 1_048_576 
    is_gif = meta["ext"] == "gif"
    filename = f"video_{meta['id']}.{meta['ext']}" # Явное имя файла!
    
    try:
        # 1. URL Sending (< 20 MB)
        if size_mb < 20:
            logger.info(f"📤 URL Send [{meta['ext']}]: {meta['id']} ({size_mb:.2f} MB)")
            media_file = URLInputFile(meta["url"], filename=filename)
            
            func = bot.send_animation if is_gif else bot.send_video
            kwargs = {
                "chat_id": CHANNEL_ID,
                "caption": meta["caption"],
                "parse_mode": ParseMode.HTML
            }
            if is_gif: kwargs["animation"] = media_file
            else: 
                kwargs["video"] = media_file
                kwargs["supports_streaming"] = True
            
            await func(**kwargs)
            return True

        # 2. RAM Upload (20-50 MB)
        elif size_mb < 50:
            logger.info(f"⬇️ RAM DL [{meta['ext']}]: {meta['id']} ({size_mb:.2f} MB)")
            
            # ВАЖНО: Передаем HEADERS при скачивании!
            async with session.get(meta["url"], headers=HEADERS) as resp:
                if resp.status != 200:
                    logger.error(f"DL Fail: {resp.status}")
                    return False
                content = await resp.read()
                
            file_obj = BytesIO(content)
            file_obj.name = filename # Имя файла критично для Telegram
            del content
            
            logger.info(f"⬆️ RAM Upload...")
            file_input = BufferedInputFile(file_obj.getvalue(), filename=file_obj.name)
            
            func = bot.send_animation if is_gif else bot.send_video
            kwargs = {
                "chat_id": CHANNEL_ID,
                "caption": meta["caption"],
                "parse_mode": ParseMode.HTML
            }
            if is_gif: kwargs["animation"] = file_input
            else:
                kwargs["video"] = file_input
                kwargs["supports_streaming"] = True

            await func(**kwargs)
            
            file_obj.close()
            del file_obj
            del file_input
            gc.collect()
            return True
        else:
            logger.warning(f"⚠️ Too big: {size_mb:.2f} MB")
            return False

    except Exception as e:
        logger.error(f"❌ Send Error {meta['id']}: {e}")
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
        
        # Теперь парсинг асинхронный (запрашивает ссылки автора)
        meta = await parse_post_async(session, post)
        if not meta: continue
        
        if await send_media(bot, session, meta):
            await mark_as_posted(pool, meta["id"])
            sent_count += 1
            await asyncio.sleep(5)
    
    logger.info(f"--- ✅ Done. Sent: {sent_count} ---")


# ---------------- [ SERVER & MAIN ] ---------------- #

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
    # Используем HEADERS по умолчанию для всей сессии
    async with ClientSession(connector=TCPConnector(limit=10, ssl=False), 
                             json_serialize=ujson.dumps,
                             headers=HEADERS) as session:
        bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        await asyncio.gather(start_web_server(), scheduler(bot, session, pool))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit): pass