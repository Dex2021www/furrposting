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
# ВАЖНО: User-Agent должен быть уникальным.
E621_USER_AGENT = os.getenv("E621_USER_AGENT", "TelegramVideoBot/2.0 (by Dexz)")

# Базовый запрос:
# -rating:safe  -> Исключить безопасный контент (остается Q и E)
# order:random  -> Случайный порядок (чтобы не застревать на топах)
# -human        -> Без людей
BASE_TAGS = "-rating:safe order:random -human"
MIN_SCORE = 130  # Высокое качество

# Настройки планировщика
VIDEOS_PER_BATCH = 2
# Если переменная SLEEP_INTERVAL не задана, спим 3600 сек (1 час)
SLEEP_INTERVAL = int(os.getenv("SLEEP_INTERVAL", 3600))

# Блеклист (Множество для скорости O(1))
BLACKLIST_WORDS = {"scat", "guro", "bestiality", "cub", "gore", "watersports"}
BLACKLIST_SET = set(BLACKLIST_WORDS)

# Разрешенные расширения
ALLOWED_EXTS = {"webm", "mp4", "gif"}

# Логгер
logger.remove()
logger.add(sys.stdout, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{message}</level>")

# Проверка обязательных переменных
if not BOT_TOKEN or not CHANNEL_ID or not DB_DSN:
    logger.critical("❌ Переменные окружения (BOT_TOKEN, CHANNEL_ID, DB_DSN) не заданы!")
    sys.exit(1)


# ---------------- [ БАЗА ДАННЫХ ] ---------------- #

async def init_db(pool):
    """Инициализация таблицы."""
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
    """
    Фильтрация дубликатов. 
    Отправляем список ID в базу, получаем те, что уже есть, и вычитаем их.
    Экономит CPU и сеть (1 запрос вместо N).
    """
    if not posts_data:
        return []
    
    # Собираем ID из пришедших данных
    candidate_ids = [p['id'] for p in posts_data]
    
    async with pool.acquire() as conn:
        # ANY($1::int[]) - очень быстрая проверка вхождения в массив в Postgres
        rows = await conn.fetch(
            "SELECT e621_id FROM posted_videos WHERE e621_id = ANY($1::int[])",
            candidate_ids
        )
    
    existing_ids = {r['e621_id'] for r in rows}
    
    # Возвращаем только те посты, ID которых НЕТ в базе
    new_posts = [p for p in posts_data if p['id'] not in existing_ids]
    logger.info(f"🔍 DB Filter: {len(posts_data)} fetched -> {len(new_posts)} new.")
    return new_posts

async def mark_as_posted(pool, e621_id):
    """Добавляем ID в базу (игнорируя, если вдруг уже есть)."""
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO posted_videos (e621_id) VALUES ($1) ON CONFLICT DO NOTHING", 
            e621_id
        )


# ---------------- [ E621 API ] ---------------- #

def get_query_tags():
    """
    Формирует строку запроса.
    ОПТИМИЗАЦИЯ: Вместо скобок (A ~ B) используем исключение (-C -D).
    Это намного стабильнее работает в API.
    """
    # Исключаем статику (jpg, png) и флеш (swf). 
    # Остаются только gif и webm (видео).
    # order:random меняем посты при каждом запросе.
    tags = f"{BASE_TAGS} -type:png -type:jpg -type:swf"
    
    roll = random.random()
    
    # Логика: date:<Time означает "Младше чем Time"
    if roll < 0.15:
        # 15% шанс: Свежее (младше 6 месяцев)
        tags += " date:<6months"
        mode = "Fresh (<6mo)"
    elif roll < 0.35:
        # 20% шанс: Современное (младше 1 года)
        tags += " date:<1year"
        mode = "Modern (<1yr)"
    else:
        # 65% шанс: Любое время
        mode = "Legacy (Any)"
        
    logger.info(f"🎲 Mode: {mode} | Query: {tags} | Score: >={MIN_SCORE}")
    return tags

async def fetch_posts(session):
    """Запрос к API e621."""
    url = "https://e621.net/posts.json"
    
    params = {
        "tags": f"{get_query_tags()} score:>={MIN_SCORE}",
        "limit": 50  # Берем с запасом, чтобы после фильтрации БД что-то осталось
    }
    headers = {"User-Agent": E621_USER_AGENT}
    
    try:
        async with session.get(url, params=params, headers=headers) as response:
            if response.status != 200:
                logger.error(f"❌ API Error: {response.status}")
                return []
            
            # ujson быстрее стандартного json
            data = await response.json(loads=ujson.loads)
            posts = data.get("posts", [])
            
            if not posts:
                logger.warning("⚠️ API returned 0 posts. Check tags/score.")
                
            return posts
    except Exception as e:
        logger.error(f"❌ Fetch Error: {e}")
        return []

def parse_post(post):
    """Извлекает и валидирует данные поста."""
    f = post.get("file")
    if not f or not f.get("url"):
        return None
    
    ext = f["ext"]
    if ext not in ALLOWED_EXTS:
        return None
    
    # 1. Проверка Блеклиста (Set optimized)
    ptags = post["tags"]
    # Объединяем все категории тегов
    all_tags_list = (ptags["general"] + ptags["character"] + 
                     ptags["species"] + ptags["copyright"])
    
    # Преобразуем в set для быстрой проверки пересечения
    post_tags_set = set(all_tags_list)
    
    # isdisjoint = True, если нет общих элементов. 
    # Если False (есть общие) -> сработал блеклист.
    if not post_tags_set.isdisjoint(BLACKLIST_SET):
        return None

    # 2. Форматирование текста
    artists = ptags["artist"]
    # Генератор списка + f-string (быстро)
    artist_links = [
        f'<a href="https://e621.net/posts?tags={a}">{a.replace("_", " ").title()}</a>' 
        for a in artists
    ]
    artist_str = ", ".join(artist_links) if artist_links else "Unknown"
    source_link = f"https://e621.net/posts/{post['id']}"
    
    caption = f"<b>Artist:</b> {artist_str}\n<b>Source:</b> <a href='{source_link}'>e621</a>"
    
    return {
        "id": post["id"],
        "url": f["url"],
        "size": f["size"],
        "ext": ext,
        "caption": caption
    }


# ---------------- [ БОТ & ОТПРАВКА ] ---------------- #

async def send_media(bot, session, meta):
    """Умная отправка: URL или RAM Upload."""
    
    # Константа байт в МБ
    size_mb = meta["size"] / 1_048_576 
    
    # Определяем метод (GIF -> Animation, Video -> Video)
    is_gif = meta["ext"] == "gif"
    send_func = bot.send_animation if is_gif else bot.send_video
    
    try:
        # --- СЦЕНАРИЙ 1: Легкий файл (< 20 МБ) ---
        # Отправляем прямую ссылку. Сервер Telegram сам скачает.
        # RAM usage: ~0 MB.
        if size_mb < 20:
            logger.info(f"📤 Sending via URL [{meta['ext']}]: {meta['id']} ({size_mb:.2f} MB)")
            file_input = URLInputFile(meta["url"])
            
            await send_func(
                chat_id=CHANNEL_ID,
                animation=file_input if is_gif else None,
                video=file_input if not is_gif else None,
                caption=meta["caption"],
                parse_mode=ParseMode.HTML
            )
            return True

        # --- СЦЕНАРИЙ 2: Средний файл (20-50 МБ) ---
        # Лимит URL upload - 20MB. Лимит Bot API upload - 50MB.
        # Качаем в RAM, отправляем, чистим.
        elif size_mb < 50:
            logger.info(f"⬇️ RAM Download [{meta['ext']}]: {meta['id']} ({size_mb:.2f} MB)")
            
            async with session.get(meta["url"]) as resp:
                if resp.status != 200:
                    logger.error(f"Download failed: {resp.status}")
                    return False
                
                # Читаем в память
                content = await resp.read()
                
            # Оборачиваем в BytesIO
            file_obj = BytesIO(content)
            file_obj.name = f"{meta['id']}.{meta['ext']}"
            
            # ВАЖНО: Удаляем исходную переменную content, чтобы освободить память 
            # еще до начала отправки (BytesIO уже держит копию данных)
            del content
            
            # Отправляем
            file_input = BufferedInputFile(file_obj.getvalue(), filename=file_obj.name)
            logger.info(f"⬆️ RAM Uploading...")
            
            await send_func(
                chat_id=CHANNEL_ID,
                animation=file_input if is_gif else None,
                video=file_input if not is_gif else None,
                caption=meta["caption"],
                parse_mode=ParseMode.HTML,
                supports_streaming=not is_gif
            )
            
            # ЯВНАЯ ОЧИСТКА ПАМЯТИ
            file_obj.close()
            del file_obj
            del file_input
            gc.collect() # Принудительный вызов сборщика мусора
            
            return True
            
        # --- СЦЕНАРИЙ 3: Тяжелый файл (> 50 МБ) ---
        else:
            logger.warning(f"⚠️ Skip: File too big ({size_mb:.2f} MB)")
            return False

    except Exception as e:
        logger.error(f"❌ Send Error {meta['id']}: {e}")
        return False


async def processing_cycle(bot, session, pool):
    """Один цикл работы бота."""
    logger.info("--- 🔄 Cycle Start ---")
    
    # 1. Получение
    posts = await fetch_posts(session)
    
    # 2. Фильтрация БД
    new_posts = await filter_new_posts(pool, posts)
    
    if not new_posts:
        logger.info("💤 No content to process.")
        return

    sent_count = 0
    
    # 3. Обработка списка
    for post in new_posts:
        if sent_count >= VIDEOS_PER_BATCH:
            break
            
        meta = parse_post(post)
        if not meta:
            continue
            
        # Попытка отправки
        success = await send_media(bot, session, meta)
        
        if success:
            # Если отправили - пишем в базу
            await mark_as_posted(pool, meta["id"])
            sent_count += 1
            # Пауза между сообщениями (Anti-flood)
            await asyncio.sleep(5)
    
    logger.info(f"--- ✅ Cycle End. Sent: {sent_count} ---")


# ---------------- [ WEB & RUNNER ] ---------------- #

async def health_check(request):
    """Пинг для Cloudflare Workers."""
    return web.Response(text="Alive")

async def start_web_server():
    """Запуск aiohttp сервера."""
    app = web.Application()
    app.add_routes([web.get('/', health_check)])
    runner = web.AppRunner(app)
    await runner.setup()
    
    # Порт от Render или 8080
    port = int(os.getenv("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"🌍 Web server running on port {port}")

async def scheduler(bot, session, pool):
    """Бесконечный цикл."""
    while True:
        try:
            await processing_cycle(bot, session, pool)
        except Exception as e:
            logger.critical(f"🔥 Scheduler Crash: {e}")
            
        logger.info(f"⏳ Sleeping for {SLEEP_INTERVAL} seconds...")
        await asyncio.sleep(SLEEP_INTERVAL)

async def main():
    # Настройка БД (ограничиваем пул до 2 соединений для экономии RAM)
    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=1, max_size=2)
    await init_db(pool)
    
    # Настройка HTTP сессии (ujson + лимит соединений)
    connector = TCPConnector(limit=10, ssl=False) 
    # ssl=False немного разгружает CPU, если Render/CF берет SSL на себя, 
    # но e621 требует https, поэтому aiohttp сам поднимет ssl для внешних запросов.
    
    async with ClientSession(connector=connector, json_serialize=ujson.dumps) as session:
        
        bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        
        # Запускаем параллельно сервер и бота
        await asyncio.gather(
            start_web_server(),
            scheduler(bot, session, pool)
        )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped.")