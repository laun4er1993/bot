import asyncio
import logging
import os
import sys
from typing import Optional, Dict, List, Tuple

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery
)

# Токен из переменных окружения
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

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ========== КЛАСС ДЛЯ РАБОТЫ С ДАННЫМИ ==========

class DataBase:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.multi_keys_file = os.path.join(data_dir, "multi_keys.txt")
        self.details_file = os.path.join(data_dir, "details.txt")
        
        # Данные
        self.key_to_groups: Dict[str, List[str]] = {}  # ключ -> список group_id
        self.groups: Dict[str, Dict] = {}  # group_id -> {keys, associations, category_name}
        self.details: Dict[str, str] = {}  # ассоциация -> детали
        
        # История пользователей
        self.user_last_group: Dict[int, str] = {}
        self.user_last_query: Dict[int, str] = {}
        
        self.load_all_data()
    
    def load_all_data(self) -> None:
        os.makedirs(self.data_dir, exist_ok=True)
        self.load_multi_keys()
        self.load_details()
        self.log_stats()
    
    def load_multi_keys(self) -> None:
        try:
            if os.path.exists(self.multi_keys_file):
                with open(self.multi_keys_file, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f):
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue
                        
                        if '|' in line:
                            # Формат: категория|ключ1,ключ2|ассоциация1|ассоциация2
                            parts = line.split('|')
                            if len(parts) >= 3:
                                category_name = parts[0].strip()
                                keys_part = parts[1].strip()
                                associations = [a.strip() for a in parts[2:] if a.strip()]
                                
                                keys = [k.strip().lower() for k in keys_part.split(',') if k.strip()]
                                
                                if keys and associations:
                                    group_id = f"group_{line_num}"
                                    
                                    # Сохраняем группу
                                    self.groups[group_id] = {
                                        'keys': keys,
                                        'associations': associations,
                                        'category_name': category_name
                                    }
                                    
                                    # Для каждого ключа добавляем ссылку на эту группу
                                    for key in keys:
                                        if key not in self.key_to_groups:
                                            self.key_to_groups[key] = []
                                        self.key_to_groups[key].append(group_id)
                
                logger.info(f"✅ Загружено {len(self.groups)} групп")
            else:
                logger.warning(f"⚠️ Файл {self.multi_keys_file} не найден")
                self._create_example_file()
                
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки: {e}")
    
    def _create_example_file(self) -> None:
        """Создает пример файла с данными"""
        example = '''# Формат: КАТЕГОРИЯ|ключ1,ключ2|ассоциация1|ассоциация2

Фрукты|яблоко,фрукт,apple|🍎 Красное яблоко|🍏 Зеленое яблоко
Apple|apple,iphone,mac|📱 iPhone 15|💻 MacBook Pro
Автомобили|машина,авто,car|🚗 Toyota Camry|🚙 Kia Sportage
Бытовая техника|машина,стиралка|🧺 Стиральная машина|☕ Кофемашина
Программирование|python,питон|🐍 Основы Python|🌐 Django
'''
        with open(self.multi_keys_file, 'w', encoding='utf-8') as f:
            f.write(example)
    
    def load_details(self) -> None:
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
                
                logger.info(f"✅ Загружено {len(self.details)} описаний")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки деталей: {e}")
    
    def find_groups(self, text: str) -> List[Tuple[str, str, List[str]]]:
        """
        Ищет все группы по ключу
        Возвращает список: (group_id, category_name, список ассоциаций)
        """
        if not text or not self.key_to_groups:
            return []
        
        text_lower = text.lower().strip()
        found_groups = []
        seen_group_ids = set()
        
        # Прямое совпадение
        if text_lower in self.key_to_groups:
            for group_id in self.key_to_groups[text_lower]:
                if group_id not in seen_group_ids:
                    group = self.groups[group_id]
                    found_groups.append((group_id, group['category_name'], group['associations']))
                    seen_group_ids.add(group_id)
        
        # Поиск по вхождению
        for key, group_ids in self.key_to_groups.items():
            if key in text_lower and key != text_lower:
                for group_id in group_ids:
                    if group_id not in seen_group_ids:
                        group = self.groups[group_id]
                        found_groups.append((group_id, group['category_name'], group['associations']))
                        seen_group_ids.add(group_id)
        
        return found_groups
    
    def get_group(self, group_id: str) -> Optional[Dict]:
        return self.groups.get(group_id)
    
    def get_details(self, association: str) -> Optional[str]:
        return self.details.get(association)
    
    def set_last_group(self, user_id: int, group_id: str):
        self.user_last_group[user_id] = group_id
    
    def get_last_group(self, user_id: int) -> Optional[str]:
        return self.user_last_group.get(user_id)
    
    def set_last_query(self, user_id: int, query: str):
        self.user_last_query[user_id] = query
    
    def get_last_query(self, user_id: int) -> Optional[str]:
        return self.user_last_query.get(user_id)
    
    def log_stats(self):
        logger.info(f"📊 Статистика:")
        logger.info(f"   • Групп: {len(self.groups)}")
        logger.info(f"   • Уникальных ключей: {len(self.key_to_groups)}")

db = DataBase()

# ========== КЛАВИАТУРЫ ==========

def back_to_main_keyboard() -> InlineKeyboardMarkup:
    """Кнопка возврата в главное меню"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 В начало", callback_data="back_to_main")]
    ])

def back_to_list_keyboard(group_id: str) -> InlineKeyboardMarkup:
    """Кнопка возврата к списку ассоциаций"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад к списку", callback_data=f"back_to_list_{group_id}")]
    ])

def groups_keyboard(groups: List[Tuple[str, str, List[str]]]) -> InlineKeyboardMarkup:
    """Клавиатура для выбора группы при дубликатах"""
    keyboard = []
    
    for group_id, category_name, associations in groups:
        keyboard.append([InlineKeyboardButton(
            text=category_name,
            callback_data=f"select_group_{group_id}"
        )])
    
    keyboard.append([InlineKeyboardButton(text="🔙 Назад к поиску", callback_data="back_to_search")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def associations_keyboard(associations: List[str], category_name: str, group_id: str) -> InlineKeyboardMarkup:
    """Клавиатура со списком ассоциаций - теперь с названием категории"""
    keyboard = []
    row = []
    
    for i, assoc in enumerate(associations):
        # Добавляем название категории к каждой ассоциации
        button_text = f"{category_name} - {assoc}"
        row.append(InlineKeyboardButton(text=button_text, callback_data=f"show_details_{assoc}"))
        if len(row) == 2 or i == len(associations) - 1:
            keyboard.append(row)
            row = []
    
    keyboard.append([InlineKeyboardButton(text="🔙 Назад к поиску", callback_data="back_to_search")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# ========== ОБРАБОТЧИКИ КОМАНД ==========

@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    """Обработчик команды /start"""
    await message.answer(
        f"👋 Привет, {message.from_user.full_name}!\n\n"
        f"🔍 Напиши слово для поиска.\n\n"
        f"📋 **Примеры:**\n"
        f"• яблоко - фрукт и бренд\n"
        f"• машина - автомобиль и устройство\n"
        f"• python - язык программирования\n\n"
        f"💡 Если слово встречается в нескольких категориях, "
        f"бот предложит выбрать нужную!",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardRemove()
    )

@dp.message(Command("help"))
async def cmd_help(message: types.Message) -> None:
    """Обработчик команды /help"""
    await message.answer(
        "🤖 **Помощь:**\n\n"
        "1️⃣ Напишите слово - бот покажет все категории\n"
        "2️⃣ Если категорий несколько - выберите нужную\n"
        "3️⃣ Выберите вариант из списка\n"
        "4️⃣ Получите подробную информацию\n\n"
        "🔙 Кнопки «Назад» возвращают к предыдущему шагу",
        parse_mode="Markdown"
    )

# ========== ОБРАБОТЧИК ТЕКСТА ==========

@dp.message()
async def handle_message(message: types.Message) -> None:
    """Обрабатывает текстовые сообщения"""
    text = message.text
    user_id = message.from_user.id
    
    if not text:
        return
    
    db.set_last_query(user_id, text)
    groups = db.find_groups(text)
    
    if groups:
        if len(groups) == 1:
            # Если одна группа - показываем сразу ассоциации
            group_id, category_name, associations = groups[0]
            db.set_last_group(user_id, group_id)
            
            assoc_list = "\n".join([f"• {category_name} - {a}" for a in associations])
            
            await message.answer(
                f"✅ **{category_name}**\n\n"
                f"📌 **Варианты:**\n\n{assoc_list}",
                parse_mode="Markdown",
                reply_markup=associations_keyboard(associations, category_name, group_id)
            )
        else:
            # Если несколько групп - показываем выбор
            await message.answer(
                f"🔍 **Найдено несколько категорий для '{text}':**\n\n"
                f"Выберите нужную:",
                parse_mode="Markdown",
                reply_markup=groups_keyboard(groups)
            )
        logger.info(f"User {user_id} searched '{text}' -> {len(groups)} groups")
    else:
        await message.answer(
            f"❌ Ничего не найдено для '{text}'\n\nПопробуйте другое слово",
            reply_markup=back_to_main_keyboard()
        )

# ========== ОБРАБОТЧИКИ ИНЛАЙН-КНОПОК ==========

@dp.callback_query(lambda c: c.data.startswith('select_group_'))
async def process_group_select(callback: CallbackQuery):
    """Обработка выбора группы при дубликатах"""
    group_id = callback.data.replace('select_group_', '')
    user_id = callback.from_user.id
    
    group = db.get_group(group_id)
    
    if group:
        db.set_last_group(user_id, group_id)
        associations = group['associations']
        category_name = group['category_name']
        
        assoc_list = "\n".join([f"• {category_name} - {a}" for a in associations])
        
        await callback.message.edit_text(
            f"✅ **{category_name}**\n\n"
            f"📌 **Варианты:**\n\n{assoc_list}",
            parse_mode="Markdown",
            reply_markup=associations_keyboard(associations, category_name, group_id)
        )
    
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('show_details_'))
async def process_show_details(callback: CallbackQuery):
    """Показывает детальную информацию по ассоциации"""
    association = callback.data.replace('show_details_', '')
    user_id = callback.from_user.id
    
    last_group = db.get_last_group(user_id)
    details = db.get_details(association)
    
    if details:
        text = f"📖 **{association}**\n\n{details}"
    else:
        text = f"📖 **{association}**\n\n*Нет подробного описания*"
    
    reply = back_to_list_keyboard(last_group) if last_group else back_to_main_keyboard()
    
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=reply)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('back_to_list_'))
async def process_back_to_list(callback: CallbackQuery):
    """Возвращает к списку ассоциаций"""
    group_id = callback.data.replace('back_to_list_', '')
    group = db.get_group(group_id)
    
    if group:
        associations = group['associations']
        category_name = group['category_name']
        assoc_list = "\n".join([f"• {category_name} - {a}" for a in associations])
        
        await callback.message.edit_text(
            f"✅ **{category_name}**\n\n📌 **Варианты:**\n\n{assoc_list}",
            parse_mode="Markdown",
            reply_markup=associations_keyboard(associations, category_name, group_id)
        )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_search")
async def process_back_to_search(callback: CallbackQuery):
    """Возвращает к поиску"""
    user_id = callback.from_user.id
    last_query = db.get_last_query(user_id)
    
    text = "🔍 Введите слово для поиска"
    if last_query:
        text += f"\n\nПоследний запрос: '{last_query}'"
    
    await callback.message.edit_text(text, reply_markup=back_to_main_keyboard())
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_main")
async def process_back_to_main(callback: CallbackQuery):
    """Возвращает в главное меню"""
    await callback.message.delete()
    await cmd_start(callback.message)
    await callback.answer()

# ========== ЗАПУСК ==========

async def delete_webhook() -> None:
    """Удаляет вебхук если есть"""
    try:
        info = await bot.get_webhook_info()
        if info.url:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("✅ Webhook удален")
    except Exception as e:
        logger.error(f"Ошибка удаления webhook: {e}")

async def main() -> None:
    """Главная функция"""
    logger.info("🚀 Бот запускается...")
    await delete_webhook()
    logger.info("🔄 Polling...")
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")