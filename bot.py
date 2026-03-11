import asyncio
import logging
import os
import sys
from typing import Optional, Dict

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

# Токен берется из переменных окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    logging.critical("❌ ОШИБКА: BOT_TOKEN не найден!")
    sys.exit(1)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Инициализация бота
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ========== ПРОСТОЙ КЛАСС ДЛЯ РАБОТЫ С ТЕКСТОВЫМ ФАЙЛОМ ==========

class SimpleDatabase:
    """Очень простой класс для работы с текстовым файлом формата КЛЮЧ===ИНФО"""
    
    def __init__(self, file_path: str = "data/details.txt"):
        self.file_path = file_path
        self.data: Dict[str, str] = {}
        self.load_data()
    
    def load_data(self) -> None:
        """Загружает данные из текстового файла"""
        try:
            # Создаем папку data, если её нет
            os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
            
            if os.path.exists(self.file_path):
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Разделяем по === и собираем словарь
                entries = content.split('===')
                
                for i in range(len(entries) - 1):
                    # Получаем ключ (последняя строка перед ===)
                    lines = entries[i].strip().split('\n')
                    key = lines[-1].strip().lower()
                    
                    # Получаем значение (всё что после === до следующего === или конца файла)
                    value = entries[i + 1].strip()
                    
                    # Убираем комментарии из ключа
                    if not key.startswith('#'):
                        self.data[key] = value
                
                logger.info(f"✅ Загружено {len(self.data)} записей из {self.file_path}")
            else:
                # Создаем пример файла
                self._create_example()
                logger.info(f"📝 Создан пример файла {self.file_path}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке: {e}")
            self.data = {}
    
    def _create_example(self) -> None:
        """Создает пример файла"""
        example = '''# Пример базы знаний
# Формат: КЛЮЧ===ИНФОРМАЦИЯ

ноутбук===🖥️ ИНФОРМАЦИЯ О НОУТБУКЕ

Модель: ASUS ROG Strix G15
Цена: 129 999 ₽
Рейтинг: 4.7/5

Описание:
Игровой ноутбук с RTX 3060

пицца===🍕 ПИЦЦА МАРГАРИТА

Цена: 550 ₽
Вес: 400г
Состав: сыр, томаты, базилик
'''
        with open(self.file_path, 'w', encoding='utf-8') as f:
            f.write(example)
    
    def find_info(self, text: str) -> Optional[str]:
        """Ищет информацию по тексту"""
        if not text or not self.data:
            return None
        
        text_lower = text.lower().strip()
        
        # Прямое совпадение
        if text_lower in self.data:
            return self.data[text_lower]
        
        # Поиск по вхождению
        for key, value in self.data.items():
            if key in text_lower:
                return value
        
        return None

# Создаем базу данных
db = SimpleDatabase()

# ========== КЛАВИАТУРА ==========

def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Создает главную клавиатуру"""
    keyboard = [
        [KeyboardButton(text="🔍 Поиск"), KeyboardButton(text="📊 Список команд")],
        [KeyboardButton(text="❓ Помощь")]
    ]
    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        input_field_placeholder="Напишите слово для поиска..."
    )

# ========== ОБРАБОТЧИКИ ==========

@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    """Обработчик команды /start"""
    await message.answer(
        f"👋 Привет, {message.from_user.full_name}!\n\n"
        f"🔍 Просто напиши слово, и я покажу всю информацию о нём.\n"
        f"📋 Например: ноутбук, пицца, python\n\n"
        f"📊 Список команд - показать все доступные слова",
        reply_markup=get_main_keyboard()
    )
    logger.info(f"User {message.from_user.id} started the bot")

@dp.message(Command("help"))
@dp.message(lambda msg: msg.text == "❓ Помощь")
async def cmd_help(message: types.Message) -> None:
    """Обработчик команды /help"""
    await message.answer(
        "🤖 **Как пользоваться ботом:**\n\n"
        "1️⃣ Напишите слово (например: ноутбук)\n"
        "2️⃣ Бот покажет всю информацию по этому слову\n\n"
        "📋 **Список команд:**\n"
        "/start - Начать\n"
        "/help - Это сообщение\n"
        "/list - Список доступных слов\n\n"
        "📁 База данных хранится в файле data/details.txt",
        parse_mode="Markdown"
    )

@dp.message(Command("list"))
@dp.message(lambda msg: msg.text == "📊 Список команд")
async def cmd_list(message: types.Message) -> None:
    """Показывает список всех доступных слов"""
    if not db.data:
        await message.answer("📭 База данных пуста")
        return
    
    # Создаем список слов
    words = list(db.data.keys())
    words_list = "\n".join([f"• {word}" for word in words])
    
    await message.answer(
        f"📋 **Доступные слова ({len(words)} шт.):**\n\n"
        f"{words_list}\n\n"
        f"💡 Напишите любое слово, чтобы получить информацию!",
        parse_mode="Markdown"
    )

@dp.message()
async def handle_message(message: types.Message) -> None:
    """Основной обработчик - ищет информацию по слову"""
    text = message.text
    
    if not text:
        return
    
    # Обработка кнопки "Поиск"
    if text == "🔍 Поиск":
        await message.answer(
            "🔍 Режим поиска\n"
            "Напишите слово, и я покажу всю информацию о нём!"
        )
        return
    
    # Ищем информацию
    info = db.find_info(text)
    
    if info:
        # Отправляем найденную информацию
        await message.answer(
            f"✅ **Найдено по запросу: {text}**\n\n{info}",
            parse_mode="Markdown"
        )
        logger.info(f"Найдена информация для '{text}'")
    else:
        # Ничего не найдено
        await message.answer(
            f"❌ **Ничего не найдено**\n\n"
            f"Запрос: '{text}'\n\n"
            f"Этого слова нет в базе данных.\n"
            f"Посмотрите /list - список доступных слов",
            parse_mode="Markdown"
        )

# ========== ЗАПУСК ==========

async def delete_webhook_and_start() -> None:
    """Удаляет вебхук и запускает polling"""
    try:
        webhook_info = await bot.get_webhook_info()
        if webhook_info.url:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("✅ Webhook удален")
    except Exception as e:
        logger.error(f"Ошибка при удалении webhook: {e}")

async def main() -> None:
    """Главная функция запуска"""
    logger.info("🚀 Бот запускается...")
    
    try:
        bot_info = await bot.get_me()
        logger.info(f"✅ Бот @{bot_info.username} авторизован")
        logger.info(f"📊 В базе {len(db.data)} записей")
        
        await delete_webhook_and_start()
        
        logger.info("🔄 Начинаем polling...")
        await dp.start_polling(bot, skip_updates=True)
        
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")