import sys
import asyncio
import gc
import random
import os
from io import BytesIO

# Сторонние библиотеки
import uvloop
import ujson
import asyncpg
from aiohttp import web, ClientSession, TCPConnector
from aiogram import Bot
from aiogram.types import BufferedInputFile, URLInputFile
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from loguru import logger

# КОНФИГУРАЦИЯ

# Читаем переменные окружения
# Если переменной нет, бот упадет с ошибкой
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    logger.error("BOT_TOKEN is not set")
    sys.exit(1)

CHANNEL_ID = os.getenv("CHANNEL_ID")
DB_DSN = os.getenv("DB_DSN")

# Идентификация
# Можно оставить жестко в коде, либо тоже вынести в переменные
E621_USER_AGENT = os.getenv("E621_USER_AGENT", "TelegramVideoBot/1.0 (by Dexz)")

# Настройки поиска
BASE_TAGS = "rating:q,e order:score -human" 
MIN_SCORE = 120
ALLOWED_EXTS = {"webm", "mp4", "gif"}
BLACKLIST_WORDS = {"scat", "guro", "loli", "blood", "lolikon", "shota", "cub", "gore", "poop", "shit", "vore"} 
BLACKLIST_SET = set(BLACKLIST_WORDS)
VIDEOS_PER_HOUR = 2

# ИНИЦИАЛИЗАЦИЯ

# Используем быстрый Event Loop
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

# Настройка красивых логов
logger.remove()
logger.add(sys.stdout, level="INFO", format="<green>{time:HH:mm:ss}</green> | <level>{message}</level>")

# БАЗА ДАННЫХ (NEON)

async def init_db(pool):
    """Создает таблицу для учета отправленных видео."""
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS posted_videos (
                id SERIAL PRIMARY KEY,
                e621_id INT UNIQUE NOT NULL,
                posted_at TIMESTAMP DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_e621_id ON posted_videos(e621_id);
        """)

async def filter_existing_posts(pool, posts_data):
    """
    Принимает список постов, проверяет их ID в базе одним запросом.
    Возвращает список только тех, которых еще нет в БД.
    """
    if not posts_data:
        return []
        
    candidate_ids = [p['id'] for p in posts_data]
    
    async with pool.acquire() as conn:
        # Эффективная проверка массива ID через ANY
        rows = await conn.fetch(
            "SELECT e621_id FROM posted_videos WHERE e621_id = ANY($1::int[])",
            candidate_ids
        )
        
    existing_ids = {r['e621_id'] for r in rows}
    
    # Оставляем только новые
    unique_posts = [p for p in posts_data if p['id'] not in existing_ids]
    logger.info(f"DB Check: {len(posts_data)} fetched -> {len(unique_posts)} new.")
    return unique_posts

async def mark_as_posted(pool, e621_id):
    """Записывает ID в базу, чтобы не постить повторно."""
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO posted_videos (e621_id) VALUES ($1) ON CONFLICT DO NOTHING", 
            e621_id
        )

# E621 ЛОГИКА

def get_dynamic_tags():
    """
    Генерирует строку тегов с учетом вероятности 'свежести' контента.
    """
    # Обязательно добавляем фильтр типов, чтобы API не присылало картинки
    tags = f"{BASE_TAGS} (type:webm ~ type:mp4 ~ type:gif)"
    
    # Бросаем кубик (0.0 - 1.0)
    roll = random.random()
    
    if roll < 0.15:
        # 15% шанс: Видео младше 6 месяцев
        tags += " date:>6months"
        logger.info(f"🎲 Mode: Fresh (< 6 months) | Min Score: {MIN_SCORE}")
    elif roll < 0.35: 
        # 20% шанс: Видео младше 1 года
        tags += " date:>1year"
        logger.info(f"🎲 Mode: Modern (< 1 year) | Min Score: {MIN_SCORE}")
    else:
        # 65% шанс: Любая дата
        logger.info(f"🎲 Mode: Legacy (Any date) | Min Score: {MIN_SCORE}")
        
    return tags

async def fetch_e621_posts(session, limit=50):
    url = "https://e621.net/posts.json"
    
    # Получаем теги для текущего запроса
    current_tags = get_dynamic_tags()
    
    params = {
        "tags": f"{current_tags} score:>={MIN_SCORE}",
        "limit": limit
    }
    headers = {"User-Agent": E621_USER_AGENT}
    
    try:
        async with session.get(url, params=params, headers=headers) as response:
            if response.status != 200:
                logger.error(f"E621 API Error: {response.status}")
                return []
            data = await response.json(loads=ujson.loads)
            return data.get("posts", [])
    except Exception as e:
        logger.error(f"Fetch error: {e}")
        return []

def extract_metadata(post):
    """Парсит JSON поста и возвращает чистые данные или None."""
    f = post.get("file")
    if not f or not f.get("url"):
        return None
    
    ext = f["ext"]
    if ext not in ALLOWED_EXTS:
        return None

    # Оптимизированная проверка черного списка
    ptags = post["tags"]
    all_tags = set(ptags["general"] + ptags["character"] + ptags["species"] + ptags["copyright"])
    
    # Если есть пересечение с черным списком - пропускаем
    if not all_tags.isdisjoint(BLACKLIST_SET):
        return None

    # Формируем ссылки на художников
    artists = ptags["artist"]
    artist_links = [
        f'<a href="https://e621.net/posts?tags={a}">{a.replace("_", " ").title()}</a>' 
        for a in artists
    ]
    
    artist_str = ", ".join(artist_links) if artist_links else "Unknown"
    source_link = f"https://e621.net/posts/{post['id']}"
    
    return {
        "id": post["id"],
        "url": f["url"],
        "size": f["size"], # Байты
        "ext": ext,
        "caption": f"<b>Artist:</b> {artist_str}\n<b>Source:</b> <a href='{source_link}'>e621</a>"
    }

# TELEGRAM ЛОГИКА

async def process_and_send(bot, session, pool):
    logger.info("Starting processing cycle")
    
    # 1. Скачиваем список постов
    posts = await fetch_e621_posts(session)
    
    # 2. Фильтруем дубликаты через БД
    new_posts = await filter_existing_posts(pool, posts)
    
    if not new_posts:
        logger.warning("No new posts found")
        return

    sent_count = 0
    
    # 3. Обрабатываем и отправляем
    for post in new_posts:
        if sent_count >= VIDEOS_PER_HOUR:
            break
            
        meta = extract_metadata(post)
        if not meta:
            continue
            
        try:
            # Конвертация байт в МБ
            file_size_mb = meta["size"] * 0.00000095367432 
            
            # Выбираем метод: GIF как анимацию, остальное как видео
            is_gif = meta["ext"] == "gif"
            send_method = bot.send_animation if is_gif else bot.send_video
            
            # ВАРИАНТ 1: Отправка по URL (< 20 MB)
            if file_size_mb < 20:
                logger.info(f"Sending URL [{meta['ext']}]: {meta['id']} ({file_size_mb:.2f} MB)")
                
                media_file = URLInputFile(meta["url"])
                await send_method(
                    chat_id=CHANNEL_ID,
                    animation=media_file if is_gif else None,
                    video=media_file if not is_gif else None,
                    caption=meta["caption"],
                    parse_mode=ParseMode.HTML
                )
                
            # ВАРИАНТ 2: Скачивание в RAM (20-50 MB)
            elif file_size_mb < 50:
                logger.info(f"RAM Upload [{meta['ext']}]: {meta['id']} ({file_size_mb:.2f} MB)")
                
                async with session.get(meta["url"]) as resp:
                    if resp.status == 200:
                        content = await resp.read()
                        file_obj = BytesIO(content)
                        del content # Освобождаем сырые байты
                        
                        file_input = BufferedInputFile(file_obj.getvalue(), filename=f"{meta['id']}.{meta['ext']}")
                        
                        if is_gif:
                            await bot.send_animation(CHANNEL_ID, animation=file_input, caption=meta["caption"])
                        else:
                            await bot.send_video(CHANNEL_ID, video=file_input, caption=meta["caption"], supports_streaming=True)
                        
                        # Очистка ресурсов
                        file_obj.close()
                        del file_obj
                        del file_input
                        gc.collect() # Принудительный GC
                    else:
                        logger.error(f"Download failed: {resp.status}")
                        continue
            else:
                logger.warning(f"File too big ({file_size_mb:.2f} MB), skipping.")
                continue

            # Успешно отправлено -> пишем в БД
            await mark_as_posted(pool, meta["id"])
            sent_count += 1
            
            # Пауза перед следующим видео
            await asyncio.sleep(5)
            
        except Exception as e:
            logger.error(f"Error processing {meta['id']}: {e}")
            await asyncio.sleep(2)

    logger.info(f"Cycle finished. Sent {sent_count}/{VIDEOS_PER_HOUR}.")

# SERVER & SCHEDULER

async def health_check(request):
    """Простой эндпоинт для пинга."""
    return web.Response(text="Alive")

async def scheduler(bot, session, pool):
    """Основной цикл: работа -> сон 1 час."""
    while True:
        try:
            await process_and_send(bot, session, pool)
        except Exception as e:
            logger.critical(f"Scheduler error: {e}")
        
        logger.info("Sleeping for 60 minutes...")
        await asyncio.sleep(3600)

async def start_web_server():
    """Запуск веб-сервера для Health Check."""
    app = web.Application()
    app.add_routes([web.get('/', health_check)])
    runner = web.AppRunner(app)
    await runner.setup()
    
    # Порт берется из аргументов (нужно для облаков) или 8080 по дефолту
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8080
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"Web server running on port {port}")

async def main():
    # Создаем пул соединений к БД (минимальный размер для экономии RAM)
    pool = await asyncpg.create_pool(dsn=DB_DSN, min_size=1, max_size=2)
    await init_db(pool)
    
    # Настраиваем HTTP сессию
    connector = TCPConnector(limit=10, ssl=False) # ssl=False немного быстрее, если Cloudflare обрабатывает SSL
    async with ClientSession(connector=connector, json_serialize=ujson.dumps) as session:
        
        bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        
        # Запускаем всё вместе
        await asyncio.gather(
            start_web_server(),
            scheduler(bot, session, pool)
        )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped")