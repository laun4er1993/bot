# bot.py
import sys
import asyncio
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from config import BOT_TOKEN, YANDEX_DISK_TOKEN, logger
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
    register_callbacks
)


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
    
    yd_client = YandexDiskClient(YANDEX_DISK_TOKEN)
    village_db = VillageDatabase()
    afs_catalog = AFSCatalog()
    kml_catalog = KMLCatalog()
    photos_db = PhotosDatabase(yd_client, village_db, afs_catalog)
    kml_processor = KMLProcessor(village_db, photos_db)
    
    register_start_handlers(dp)
    register_search_handlers(dp, photos_db, village_db)
    register_kml_handlers(dp, kml_processor, village_db, photos_db)
    register_settings_handlers(dp, village_db, photos_db)
    register_callbacks(dp, village_db, photos_db)
    
    try:
        info = await bot.get_webhook_info()
        if info.url:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("✅ Webhook удален")
    except Exception as e:
        logger.error(f"Ошибка удаления webhook: {e}")
    
    logger.info("🚀 Запуск бота...")
    logger.info(f"📊 Статистика:")
    logger.info(f"   • Населенных пунктов: {village_db.stats['total']}")
    logger.info(f"   • Снимков в АФС: {len(afs_catalog.catalog)}")
    logger.info(f"   • Файлов KML: {len(kml_catalog.catalog)}")
    
    logger.info("🔄 Запуск polling...")
    await dp.start_polling(bot, skip_updates=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")