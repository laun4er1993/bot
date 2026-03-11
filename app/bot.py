import asyncio
import logging
import os
import sys

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.client.default import DefaultBotProperties

# Токен берется из переменных окружения (обязательно для Railway)
BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    logging.critical("❌ КРИТИЧЕСКАЯ ОШИБКА: BOT_TOKEN не найден в переменных окружения!")
    logging.critical("Добавьте BOT_TOKEN в Variables на Railway и перезапустите бота")
    sys.exit(1)  # Завершаем программу с ошибкой

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    """Обработчик команды /start"""
    await message.answer(
        f"👋 Привет, {message.from_user.full_name}!\n"
        f"Я бот, работающий на Railway 24/7!\n\n"
        f"📋 Доступные команды:\n"
        f"/start - это сообщение\n"
        f"/help - список команд"
    )
    logger.info(f"User {message.from_user.id} started the bot")


@dp.message(Command("help"))
async def cmd_help(message: types.Message) -> None:
    """Обработчик команды /help"""
    await message.answer(
        "🤖 **О боте:**\n"
        "Это простой Telegram бот, работающий на Railway.\n\n"
        "**Команды:**\n"
        "• /start - Приветствие\n"
        "• /help - Это сообщение\n\n"
        "**Статус:** Бот работает 24/7 в облаке ☁️",
        parse_mode="Markdown"
    )
    logger.info(f"User {message.from_user.id} requested help")


@dp.message()
async def echo_message(message: types.Message) -> None:
    """Обработчик всех текстовых сообщений (эхо)"""
    # Игнорируем сообщения без текста (стикеры, фото и т.д.)
    if not message.text:
        return
    
    await message.answer(f"📝 Ты написал: {message.text}")
    logger.debug(f"Echo reply to user {message.from_user.id}")


async def delete_webhook_and_start() -> None:
    """Удаляет вебхук и запускает polling"""
    logger.info("🔄 Проверяем наличие активного webhook...")
    
    try:
        # Получаем информацию о текущем webhook
        webhook_info = await bot.get_webhook_info()
        
        if webhook_info.url:
            logger.warning(f"⚠️ Найден активный webhook: {webhook_info.url}")
            logger.info("🗑️ Удаляем webhook...")
            
            # Удаляем webhook
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("✅ Webhook успешно удален, ожидающие обновления сброшены")
        else:
            logger.info("✅ Активных webhook не найдено, можно запускать polling")
            
    except Exception as e:
        logger.error(f"❌ Ошибка при проверке/удалении webhook: {e}", exc_info=True)
        logger.info("⚠️ Продолжаем попытку запуска polling...")


async def main() -> None:
    """Главная функция запуска бота"""
    logger.info("🚀 Бот запускается...")
    
    try:
        # Информация о боте
        bot_info = await bot.get_me()
        logger.info(f"✅ Бот @{bot_info.username} успешно авторизован")
        
        # Удаляем webhook перед запуском polling
        await delete_webhook_and_start()
        
        logger.info("🔄 Начинаем polling...")
        
        # Запуск бота
        await dp.start_polling(
            bot,
            allowed_updates=["message", "callback_query"],  # Оптимизация: получаем только нужные типы обновлений
            skip_updates=True  # Пропускаем старые обновления, накопившиеся пока бот не работал
        )
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка при работе бота: {e}", exc_info=True)
    finally:
        logger.info("🛑 Бот остановлен")
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен пользователем (Ctrl+C)")
    except Exception as e:
        logger.critical(f"💥 Непредвиденная ошибка: {e}", exc_info=True)
        sys.exit(1)