import asyncio
import logging
import os
import sys
from typing import Optional, Dict, List

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup, 
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery
)

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

# ========== КЛАСС ДЛЯ РАБОТЫ С АССОЦИАЦИЯМИ ==========

class AssociationDatabase:
    """Класс для работы с ассоциациями и деталями"""
    
    def __init__(self, assoc_file: str = "data/associations.txt", details_file: str = "data/details.txt"):
        self.assoc_file = assoc_file
        self.details_file = details_file
        self.associations: Dict[str, List[str]] = {}  # ключ -> список ассоциаций
        self.details: Dict[str, str] = {}              # ассоциация -> детали
        self.load_data()
    
    def load_data(self) -> None:
        """Загружает все данные из файлов"""
        os.makedirs("data", exist_ok=True)
        self.load_associations()
        self.load_details()
    
    def load_associations(self) -> None:
        """Загружает ассоциации из файла"""
        try:
            if os.path.exists(self.assoc_file):
                with open(self.assoc_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue
                        
                        if '===' in line:
                            key, assoc_str = line.split('===', 1)
                            key = key.strip().lower()
                            associations = [a.strip() for a in assoc_str.split('###')]
                            self.associations[key] = associations
                
                logger.info(f"✅ Загружено {len(self.associations)} ключевых слов")
            else:
                self._create_example_associations()
                
        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке ассоциаций: {e}")
    
    def load_details(self) -> None:
        """Загружает детальную информацию из файла"""
        try:
            if os.path.exists(self.details_file):
                with open(self.details_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                entries = content.split('===')
                for i in range(len(entries) - 1):
                    lines = entries[i].strip().split('\n')
                    assoc = lines[-1].strip()
                    details = entries[i + 1].strip()
                    
                    if not assoc.startswith('#'):
                        self.details[assoc] = details
                
                logger.info(f"✅ Загружено {len(self.details)} ассоциаций с деталями")
            else:
                self._create_example_details()
                
        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке деталей: {e}")
    
    def _create_example_associations(self) -> None:
        """Создает пример файла ассоциаций"""
        example = '''# База ассоциаций
# Формат: КЛЮЧ===АССОЦИАЦИЯ1###АССОЦИАЦИЯ2###АССОЦИАЦИЯ3

ноутбук===💻 ИГРОВОЙ НОУТБУК###🖥️ ОФИСНЫЙ НОУТБУК###💼 Б/У НОУТБУК

пицца===🍕 МАРГАРИТА###🍄 ГРИБНАЯ###🥓 ПЕППЕРОНИ

python===🐍 ОСНОВЫ PYTHON###🌐 ВЕБ-РАЗРАБОТКА###🤖 МАШИННОЕ ОБУЧЕНИЕ
'''
        with open(self.assoc_file, 'w', encoding='utf-8') as f:
            f.write(example)
    
    def _create_example_details(self) -> None:
        """Создает пример файла деталей"""
        example = '''# Детальная информация для ассоциаций
# Формат: АССОЦИАЦИЯ===ПОДРОБНАЯ ИНФОРМАЦИЯ

💻 ИГРОВОЙ НОУТБУК===🖥️ **ИГРОВОЙ НОУТБУК**
Характеристики: RTX 3060, i7, 16GB RAM
Цена: 129 999 ₽

🖥️ ОФИСНЫЙ НОУТБУК===💼 **ОФИСНЫЙ НОУТБУК**
Характеристики: i5, 8GB RAM, SSD
Цена: 65 999 ₽
'''
        with open(self.details_file, 'w', encoding='utf-8') as f:
            f.write(example)
    
    def find_keyword(self, text: str) -> Optional[str]:
        """Ищет ключевое слово в тексте"""
        if not text or not self.associations:
            return None
        
        text_lower = text.lower().strip()
        
        # Прямое совпадение
        if text_lower in self.associations:
            return text_lower
        
        # Поиск по вхождению
        for key in self.associations.keys():
            if key in text_lower:
                return key
        
        return None
    
    def get_associations(self, key: str) -> Optional[List[str]]:
        """Возвращает список ассоциаций для ключа"""
        return self.associations.get(key)
    
    def get_details(self, association: str) -> Optional[str]:
        """Возвращает детали для ассоциации"""
        return self.details.get(association)

# Создаем базу данных
db = AssociationDatabase()

# ========== КЛАВИАТУРЫ ==========

def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Создает главную клавиатуру"""
    keyboard = [
        [KeyboardButton(text="🔍 Поиск"), KeyboardButton(text="📋 Список слов")],
        [KeyboardButton(text="❓ Помощь")]
    ]
    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        input_field_placeholder="Напишите слово для поиска..."
    )

def get_associations_keyboard(associations: List[str], keyword: str) -> InlineKeyboardMarkup:
    """Создает клавиатуру с ассоциациями"""
    keyboard = []
    
    # Добавляем кнопки для каждой ассоциации
    for assoc in associations:
        keyboard.append([InlineKeyboardButton(
            text=assoc,
            callback_data=f"assoc_{assoc}"
        )])
    
    # Добавляем кнопку отмены
    keyboard.append([InlineKeyboardButton(
        text="❌ Отмена", 
        callback_data=f"cancel_{keyword}"
    )])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# ========== ОБРАБОТЧИКИ КОМАНД ==========

@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    """Обработчик команды /start"""
    await message.answer(
        f"👋 Привет, {message.from_user.full_name}!\n\n"
        f"🔍 **Как это работает:**\n"
        f"1️⃣ Напиши любое слово (например: ноутбук)\n"
        f"2️⃣ Бот покажет все ассоциации с этим словом\n"
        f"3️⃣ Выбери нужную ассоциацию\n"
        f"4️⃣ Получи подробную информацию!\n\n"
        f"📋 Например: ноутбук, пицца, python, автомобиль, кофе",
        parse_mode="Markdown",
        reply_markup=get_main_keyboard()
    )
    logger.info(f"User {message.from_user.id} started the bot")

@dp.message(Command("help"))
@dp.message(lambda msg: msg.text == "❓ Помощь")
async def cmd_help(message: types.Message) -> None:
    """Обработчик команды /help"""
    await message.answer(
        "🤖 **Помощь по боту:**\n\n"
        "**Команды:**\n"
        "• /start - Начать работу\n"
        "• /help - Это сообщение\n"
        "• /list - Список доступных ключевых слов\n\n"
        "**Как пользоваться:**\n"
        "1. Напишите слово, например **ноутбук**\n"
        "2. Бот покажет все связанные ассоциации\n"
        "3. Выберите интересующую ассоциацию\n"
        "4. Получите подробную информацию!",
        parse_mode="Markdown"
    )

@dp.message(Command("list"))
@dp.message(lambda msg: msg.text == "📋 Список слов")
async def cmd_list(message: types.Message) -> None:
    """Показывает список всех доступных ключевых слов"""
    if not db.associations:
        await message.answer("📭 База данных пуста")
        return
    
    words = list(db.associations.keys())
    words_list = "\n".join([f"• {word}" for word in words])
    
    await message.answer(
        f"📋 **Доступные ключевые слова ({len(words)} шт.):**\n\n"
        f"{words_list}\n\n"
        f"💡 Напишите любое слово, чтобы увидеть ассоциации!",
        parse_mode="Markdown"
    )

# ========== ОБРАБОТЧИК ТЕКСТОВЫХ СООБЩЕНИЙ ==========

@dp.message()
async def handle_message(message: types.Message) -> None:
    """Обрабатывает текстовые сообщения"""
    text = message.text
    
    if not text:
        return
    
    # Обработка кнопки "Поиск"
    if text == "🔍 Поиск":
        await message.answer(
            "🔍 **Режим поиска**\n\n"
            "Напишите ключевое слово, я покажу все связанные ассоциации!",
            parse_mode="Markdown"
        )
        return
    
    # Ищем ключевое слово
    keyword = db.find_keyword(text)
    
    if keyword:
        # Получаем ассоциации
        associations = db.get_associations(keyword)
        
        if associations:
            # Показываем ассоциации
            assoc_list = "\n".join([f"• {a}" for a in associations])
            
            await message.answer(
                f"✅ **Ключевое слово найдено: {keyword}**\n\n"
                f"📌 **Связанные ассоциации ({len(associations)} шт.):**\n\n"
                f"{assoc_list}\n\n"
                f"👇 **Выберите ассоциацию ниже:**",
                parse_mode="Markdown",
                reply_markup=get_associations_keyboard(associations, keyword)
            )
            logger.info(f"Показаны ассоциации для '{keyword}' пользователю {message.from_user.id}")
        else:
            await message.answer(
                f"⚠️ Для слова '{keyword}' нет ассоциаций",
                parse_mode="Markdown"
            )
    else:
        # Слово не найдено
        await message.answer(
            f"❌ **Ключевое слово не найдено**\n\n"
            f"'{text}' - нет в базе данных.\n\n"
            f"📋 Посмотрите /list - список доступных слов",
            parse_mode="Markdown"
        )

# ========== ОБРАБОТЧИКИ ИНЛАЙН-КНОПОК ==========

@dp.callback_query(lambda c: c.data.startswith('assoc_'))
async def process_association(callback: CallbackQuery):
    """Показывает детальную информацию по выбранной ассоциации"""
    association = callback.data.replace('assoc_', '')
    
    # Получаем детали
    details = db.get_details(association)
    
    if details:
        await callback.message.edit_text(
            f"📖 **{association}**\n\n{details}",
            parse_mode="Markdown"
        )
    else:
        await callback.message.edit_text(
            f"⚠️ Информация для '{association}' не найдена",
            parse_mode="Markdown"
        )
    
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('cancel_'))
async def process_cancel(callback: CallbackQuery):
    """Отменяет выбор ассоциации"""
    keyword = callback.data.replace('cancel_', '')
    
    await callback.message.edit_text(
        f"❌ Выбор ассоциации для '{keyword}' отменен.\n\n"
        f"Можете попробовать другое слово!",
        parse_mode="Markdown"
    )
    await callback.answer()

# ========== ЗАПУСК БОТА ==========

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
        logger.info(f"📊 Ключевых слов: {len(db.associations)}")
        logger.info(f"📊 Ассоциаций с деталями: {len(db.details)}")
        
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