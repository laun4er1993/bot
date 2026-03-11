import asyncio
import logging
import os
import sys

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command

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


async def main() -> None:
    """Главная функция запуска бота"""
    logger.info("🚀 Бот запускается...")
    
    # Информация о боте
    bot_info = await bot.get_me()
    logger.info(f"✅ Бот @{bot_info.username} успешно авторизован")
    logger.info("🔄 Начинаем polling...")
    
    # Запуск бота
    try:
        await dp.start_polling(bot)
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