import asyncio
import logging
import os
import sys
from typing import Optional, Dict, List, Tuple, Set

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

# ========== КЛАСС ДЛЯ РАБОТЫ С МНОЖЕСТВОМ КЛЮЧЕЙ ==========

class MultiKeyToAssociationsDB:
    """
    Класс где несколько ключей ведут к одному набору ассоциаций
    Формат: ключ1,ключ2,ключ3|ассоциация1|ассоциация2|ассоциация3
    """
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.multi_keys_file = os.path.join(data_dir, "multi_keys.txt")
        self.details_file = os.path.join(data_dir, "details.txt")
        
        # Структуры данных
        self.key_to_group: Dict[str, str] = {}      # ключ -> ID группы
        self.group_to_keys: Dict[str, List[str]] = {}  # ID группы -> список ключей
        self.group_to_associations: Dict[str, List[str]] = {}  # ID группы -> список ассоциаций
        self.group_to_display: Dict[str, str] = {}  # ID группы -> отображаемое имя
        self.details: Dict[str, str] = {}            # ассоциация -> детали
        
        self.load_all_data()
    
    def load_all_data(self) -> None:
        """Загружает все данные из файлов"""
        os.makedirs(self.data_dir, exist_ok=True)
        self.load_multi_keys()
        self.load_details()
        self.log_stats()
    
    def load_multi_keys(self) -> None:
        """Загружает данные из файла с множественными ключами"""
        try:
            if os.path.exists(self.multi_keys_file):
                with open(self.multi_keys_file, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f):
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue
                        
                        if '|' in line:
                            keys_part, assoc_part = line.split('|', 1)
                            keys = [k.strip().lower() for k in keys_part.split(',') if k.strip()]
                            associations = [a.strip() for a in assoc_part.split('|') if a.strip()]
                            
                            if keys and associations:
                                group_id = f"{keys[0]}_{line_num}"
                                display_name = keys[0]
                                
                                self.group_to_keys[group_id] = keys
                                self.group_to_associations[group_id] = associations
                                self.group_to_display[group_id] = display_name
                                
                                for key in keys:
                                    self.key_to_group[key] = group_id
                
                logger.info(f"✅ Загружено {len(self.group_to_associations)} групп ключей")
                logger.info(f"✅ Всего ключей: {len(self.key_to_group)}")
            else:
                self._create_example_multi_keys()
                
        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке multi_keys: {e}")
    
    def load_details(self) -> None:
        """Загружает детальную информацию для ассоциаций"""
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
    
    def _create_example_multi_keys(self) -> None:
        """Создает пример файла с множественными ключами"""
        example = '''# Формат: КЛЮЧ,КЛЮЧ2,КЛЮЧ3|Ассоциация1|Ассоциация2|Ассоциация3

ноутбук,ноут,лэптоп,laptop,макбук,macbook|💻 Игровой|🖥️ Офисный|💼 Б/у|🍏 MacBook
пицца,pizza,итальянская|🍕 Маргарита|🍄 Грибная|🥓 Пепперони|🍖 Четыре сыра
python,питон,пайтон,python3|🐍 Основы|🌐 Веб|🤖 Машинное обучение|📊 Анализ данных
автомобиль,машина,авто,car,тачка|🚗 Седан|🚙 Кроссовер|🏎️ Спорткар|🚐 Микроавтобус
кофе,кофейный,coffee,капучино,латте|☕ Эспрессо|🥛 Латте|🍫 Капучино|🧊 Холодный
'''
        with open(self.multi_keys_file, 'w', encoding='utf-8') as f:
            f.write(example)
    
    def _create_example_details(self) -> None:
        """Создает пример файла с деталями"""
        example = '''# Детальная информация для ассоциаций

💻 Игровой===🖥️ **ИГРОВОЙ НОУТБУК ASUS ROG**

🔹 Характеристики:
• Процессор: Intel Core i7-12700H
• Видеокарта: NVIDIA RTX 3060 6GB
• Оперативная память: 16GB DDR5
• Накопитель: 512GB NVMe SSD
• Экран: 15.6" 240Hz IPS

💰 Цена: 129 999 ₽
⭐ Рейтинг: 4.7/5

✅ Плюсы:
• Отличная производительность в играх
• Качественный экран с высокой частотой

🍏 MacBook===🍏 **MacBook Pro 14" M3**

🔹 Характеристики:
• Процессор: Apple M3 Pro (11 ядер)
• Оперативная память: 18GB
• Накопитель: 512GB SSD
• Экран: 14.2" Liquid Retina XDR
• Вес: 1.6 кг

💰 Цена: 199 990 ₽
⭐ Рейтинг: 4.9/5

🍕 Маргарита===🍕 **ПИЦЦА МАРГАРИТА**

🥫 Состав:
• Томатный соус
• Моцарелла
• Свежий базилик

💰 Цена: 550 ₽
⚖️ Вес: 400 г
'''
        with open(self.details_file, 'w', encoding='utf-8') as f:
            f.write(example)
    
    def find_group_by_key(self, text: str) -> Optional[Tuple[str, List[str], List[str]]]:
        """
        Ищет группу по ключу
        Возвращает: (group_id, список ключей, список ассоциаций) или None
        """
        if not text or not self.key_to_group:
            return None
        
        text_lower = text.lower().strip()
        
        # Прямое совпадение с ключом
        if text_lower in self.key_to_group:
            group_id = self.key_to_group[text_lower]
            return group_id, self.group_to_keys[group_id], self.group_to_associations[group_id]
        
        # Поиск по вхождению ключа в текст
        for key, group_id in self.key_to_group.items():
            if key in text_lower:
                return group_id, self.group_to_keys[group_id], self.group_to_associations[group_id]
        
        return None
    
    def get_details(self, association: str) -> Optional[str]:
        """Возвращает детали для ассоциации"""
        return self.details.get(association)
    
    def log_stats(self):
        """Логирует статистику базы данных"""
        total_associations = sum(len(assoc) for assoc in self.group_to_associations.values())
        logger.info(f"📊 Статистика базы данных:")
        logger.info(f"   • Групп ключей: {len(self.group_to_associations)}")
        logger.info(f"   • Всего ключей: {len(self.key_to_group)}")
        logger.info(f"   • Всего ассоциаций: {total_associations}")
        logger.info(f"   • Ассоциаций с деталями: {len(self.details)}")

# Создаем базу данных
db = MultiKeyToAssociationsDB()

# ========== КЛАВИАТУРЫ ==========

def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Создает главную клавиатуру"""
    keyboard = [
        [KeyboardButton(text="🔍 Поиск"), KeyboardButton(text="🏠 К началу")],
        [KeyboardButton(text="❓ Помощь")]
    ]
    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        input_field_placeholder="Напишите что ищете..."
    )

def get_associations_keyboard(associations: List[str], group_id: str) -> InlineKeyboardMarkup:
    """Создает клавиатуру с ассоциациями"""
    keyboard = []
    
    # Добавляем кнопки для каждой ассоциации (по 2 в ряд)
    row = []
    for i, assoc in enumerate(associations):
        row.append(InlineKeyboardButton(
            text=assoc,
            callback_data=f"assoc_{assoc}"
        ))
        if len(row) == 2 or i == len(associations) - 1:
            keyboard.append(row)
            row = []
    
    # Добавляем кнопку "🏠 К началу" внизу
    keyboard.append([InlineKeyboardButton(
        text="🏠 К началу", 
        callback_data="back_to_start"
    )])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# ========== ОБРАБОТЧИКИ КОМАНД ==========

@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    """Обработчик команды /start"""
    welcome_text = (
        f"👋 Привет, {message.from_user.full_name}!\n\n"
        f"🔍 **Как это работает:**\n"
        f"1️⃣ Напиши что хочешь найти (например: ноут, машина, питон)\n"
        f"2️⃣ Бот покажет все связанные ассоциации\n"
        f"3️⃣ Выбери нужную ассоциацию\n"
        f"4️⃣ Получи подробную информацию!\n\n"
        f"📋 **Примеры запросов:**\n"
        f"• ноут, laptop, макбук → ноутбуки\n"
        f"• пицца, pizza → еда\n"
        f"• машина, car, авто → автомобили\n"
        f"• кофе, coffee → напитки\n\n"
        f"🏠 **Кнопка «К началу»** всегда вернёт тебя сюда!"
    )
    
    await message.answer(
        welcome_text,
        parse_mode="Markdown",
        reply_markup=get_main_keyboard()
    )
    logger.info(f"User {message.from_user.id} started the bot")

@dp.message(Command("help"))
@dp.message(lambda msg: msg.text == "❓ Помощь")
async def cmd_help(message: types.Message) -> None:
    """Обработчик команды /help"""
    help_text = (
        "🤖 **Помощь по боту:**\n\n"
        "**Команды:**\n"
        "• /start - Начать работу\n"
        "• /help - Это сообщение\n\n"
        "**Как пользоваться:**\n"
        "1. Напишите любое слово (например: **ноут**)\n"
        "2. Бот покажет все ассоциации для этой категории\n"
        "3. Нажмите на кнопку с ассоциацией\n"
        "4. Получите детальную информацию!\n\n"
        "**Примеры синонимов:**\n"
        "• ноут, laptop, макбук → категория **ноутбук**\n"
        "• питон, пайтон → категория **python**\n"
        "• машина, авто, car → категория **автомобиль**\n\n"
        "🏠 **Кнопка «К началу»** возвращает в главное меню"
    )
    
    await message.answer(help_text, parse_mode="Markdown")

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
            "Напишите любое слово, и я покажу все связанные ассоциации!\n\n"
            "📝 Например: ноут, машина, питон, кофе, пицца",
            parse_mode="Markdown"
        )
        return
    
    # Обработка кнопки "🏠 К началу"
    if text == "🏠 К началу":
        await cmd_start(message)
        return
    
    # Ищем группу по ключу
    result = db.find_group_by_key(text)
    
    if result:
        group_id, keys, associations = result
        display_name = keys[0]
        
        # Формируем список ассоциаций
        assoc_list = "\n".join([f"• {a}" for a in associations])
        
        # Показываем другие синонимы
        other_keys = [k for k in keys if k != display_name]
        syn_msg = f"\n✨ Также можно искать: {', '.join(other_keys[:5])}" if other_keys else ""
        
        await message.answer(
            f"✅ **Категория: {display_name}**{syn_msg}\n\n"
            f"📌 **Ассоциации ({len(associations)} шт.):**\n\n"
            f"{assoc_list}\n\n"
            f"👇 **Выберите интересующий вариант:**",
            parse_mode="Markdown",
            reply_markup=get_associations_keyboard(associations, group_id)
        )
        logger.info(f"User {message.from_user.id} searched '{text}' -> category '{display_name}'")
    else:
        # Ничего не найдено
        await message.answer(
            f"❌ **Ничего не найдено**\n\n"
            f"'{text}' - нет в базе данных.\n\n"
            f"💡 Попробуйте: ноут, машина, питон, кофе, пицца\n"
            f"🏠 Нажмите «К началу» для возврата в меню",
            parse_mode="Markdown"
        )

# ========== ОБРАБОТЧИКИ ИНЛАЙН-КНОПОК ==========

@dp.callback_query(lambda c: c.data.startswith('assoc_'))
async def process_association(callback: CallbackQuery):
    """Показывает детальную информацию по выбранной ассоциации"""
    association = callback.data.replace('assoc_', '')
    
    # Получаем детали
    details = db.get_details(association)
    
    # Создаем клавиатуру с кнопкой "К началу"
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 К началу", callback_data="back_to_start")]
    ])
    
    if details:
        await callback.message.edit_text(
            f"📖 **{association}**\n\n{details}",
            parse_mode="Markdown",
            reply_markup=keyboard
        )
    else:
        await callback.message.edit_text(
            f"⚠️ Информация для '{association}' не найдена\n\n"
            f"Но вы можете посмотреть другие ассоциации!",
            parse_mode="Markdown",
            reply_markup=keyboard
        )
    
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_start")
async def process_back_to_start(callback: CallbackQuery):
    """Возвращает в главное меню"""
    await callback.message.delete()  # Удаляем текущее сообщение
    
    # Отправляем новое приветствие
    welcome_text = (
        f"👋 С возвращением, {callback.from_user.full_name}!\n\n"
        f"🔍 Напишите слово для поиска или выберите действие:"
    )
    
    await callback.message.answer(
        welcome_text,
        parse_mode="Markdown",
        reply_markup=get_main_keyboard()
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