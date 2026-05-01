import sys
import asyncio
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.webhook.aiohttp_server import SimpleRequestHandler
from aiohttp import web

from config import (
    BOT_TOKEN, YANDEX_DISK_TOKEN, logger, PORT,
    USE_WEBHOOK, WEBHOOK_PATH, WEBHOOK_URL
)
from services.yandex_disk import YandexDiskClient
from services.village_db import VillageDatabase
from services.photos_db import PhotosDatabase
from services.kml_processor import KMLProcessor
from services.afs_catalog import AFSCatalog
from services.kml_catalog import KMLCatalog
from handlers import (
    register_start_handlers,
    register_search_handlers,
    register_kml_handlers,
    register_settings_handlers,
    register_callbacks,
    register_coord_calculator_handlers
)


async def on_startup(bot: Bot, base_url: str, webhook_path: str):
    """Настройка webhook при запуске"""
    webhook_url = f"{base_url}{webhook_path}"
    await bot.set_webhook(webhook_url)
    logger.info(f"✅ Webhook установлен: {webhook_url}")


async def on_shutdown(bot: Bot):
    """Очистка при остановке"""
    await bot.delete_webhook()
    await bot.session.close()
    logger.info("👋 Webhook удален, бот остановлен")


async def main():
    if not BOT_TOKEN:
        logger.critical("❌ ОШИБКА: BOT_TOKEN не найден!")
        sys.exit(1)

    if not YANDEX_DISK_TOKEN:
        logger.critical("❌ ОШИБКА: YANDEX_DISK_TOKEN не найден!")
        sys.exit(1)

    storage = MemoryStorage()
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=storage)

    # Инициализация сервисов
    yd_client = YandexDiskClient(YANDEX_DISK_TOKEN)
    village_db = VillageDatabase()
    afs_catalog = AFSCatalog()
    kml_catalog = KMLCatalog()
    photos_db = PhotosDatabase(yd_client, village_db, afs_catalog)
    kml_processor = KMLProcessor(village_db, photos_db)

    # Регистрация обработчиков
    register_start_handlers(dp)
    register_search_handlers(dp, photos_db, village_db)
    register_kml_handlers(dp, kml_processor, village_db, photos_db, afs_catalog)
    register_settings_handlers(dp, village_db, photos_db, afs_catalog)
    register_callbacks(dp, village_db, photos_db)
    register_coord_calculator_handlers(dp)

    logger.info("🚀 Запуск бота...")
    logger.info(f"📊 Статистика:")
    logger.info(f"   • Населенных пунктов: {village_db.stats['total']}")
    logger.info(f"   • Снимков в АФС: {len(afs_catalog.catalog)}")
    logger.info(f"   • Файлов KML: {len(kml_catalog.catalog)}")

    if USE_WEBHOOK and WEBHOOK_URL:
        # Режим webhook для Amvera
        logger.info(f"🔄 Запуск в режиме WEBHOOK на порту {PORT}")

        webhook_path = WEBHOOK_PATH
        base_url = WEBHOOK_URL.rstrip('/')

        # Создаем aiohttp приложение
        app = web.Application()

        # Обработчик webhook
        webhook_handler = SimpleRequestHandler(
            dispatcher=dp,
            bot=bot,
            secret_token=None,
        )
        webhook_handler.register(app, path=webhook_path)

        # Настройка жизненного цикла
        app.on_startup.append(lambda _: on_startup(bot, base_url, webhook_path))
        app.on_shutdown.append(lambda _: on_shutdown(bot))

        # Запуск сервера
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
        await site.start()

        logger.info(f"✅ Сервер запущен на 0.0.0.0:{PORT}")

        # Бесконечное ожидание
        try:
            await asyncio.Event().wait()
        finally:
            await runner.cleanup()
            await on_shutdown(bot)
    else:
        # Режим polling (для локальной разработки)
        logger.info("🔄 Запуск в режиме POLLING...")

        try:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("✅ Webhook удален")
        except Exception as e:
            logger.error(f"Ошибка удаления webhook: {e}")

        await dp.start_polling(bot, skip_updates=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")