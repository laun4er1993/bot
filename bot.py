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
        
        # Теперь ключ может вести к нескольким группам
        self.key_to_groups: Dict[str, List[str]] = {}  # ключ -> список group_id
        self.groups: Dict[str, Dict] = {}  # group_id -> {keys, associations, display_name}
        self.details: Dict[str, str] = {}
        
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
                            keys_part, assoc_part = line.split('|', 1)
                            keys = [k.strip().lower() for k in keys_part.split(',') if k.strip()]
                            associations = [a.strip() for a in assoc_part.split('|') if a.strip()]
                            
                            if keys and associations:
                                group_id = f"group_{line_num}"
                                display_name = keys[0]  # Первый ключ как название группы
                                
                                # Сохраняем группу
                                self.groups[group_id] = {
                                    'keys': keys,
                                    'associations': associations,
                                    'display_name': display_name
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
        """Создает пример файла с дублирующимися ключами"""
        example = '''# Пример с дублирующимися ключами
# Формат: ключ1,ключ2,ключ3|ассоциация1|ассоциация2

# Яблоко как фрукт
яблоко,фрукт,apple|🍎 Красное|🍏 Зеленое|🍎 Голден

# Яблоко как бренд (Apple)
apple,айфон,iphone,mac|📱 iPhone|💻 MacBook|⌚ Watch

# Яблоко в кулинарии
яблоко,пирог,десерт|🥧 Шарлотка|🍎 Яблочный пирог|🧃 Сок

# Машина как автомобиль
машина,авто,car|🚗 Седан|🚙 Кроссовер|🏎️ Спорткар

# Машина как устройство
машина,стиралка,technics|🧺 Стиральная|☕ Кофемашина|🪡 Швейная

# Python как змея
python,змея,snake|🐍 Королевский|🐍 Сетчатый|🐍 Анаконда

# Python как язык
python,питон,programming|🐍 Основы|🌐 Веб|🤖 ML
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
        Возвращает список: (group_id, display_name, список ассоциаций)
        """
        if not text or not self.key_to_groups:
            return []
        
        text_lower = text.lower().strip()
        found_groups = []
        
        # Прямое совпадение
        if text_lower in self.key_to_groups:
            for group_id in self.key_to_groups[text_lower]:
                group = self.groups[group_id]
                found_groups.append((group_id, group['display_name'], group['associations']))
        
        # Поиск по вхождению
        for key, group_ids in self.key_to_groups.items():
            if key in text_lower and key != text_lower:  # Чтобы не дублировать прямые совпадения
                for group_id in group_ids:
                    group = self.groups[group_id]
                    # Проверяем, не добавили ли уже эту группу
                    if not any(g[0] == group_id for g in found_groups):
                        found_groups.append((group_id, group['display_name'], group['associations']))
        
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
        
        # Показываем ключи с дубликатами
        duplicates = {key: groups for key, groups in self.key_to_groups.items() if len(groups) > 1}
        if duplicates:
            logger.info(f"   • Ключей с дубликатами: {len(duplicates)}")
            for key, groups in list(duplicates.items())[:3]:  # Показываем первые 3
                logger.info(f"     - '{key}' встречается в {len(groups)} группах")

db = DataBase()

# ========== КЛАВИАТУРЫ ==========

def back_to_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 В начало", callback_data="back_to_main")]
    ])

def back_to_list_keyboard(group_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад к списку", callback_data=f"back_to_list_{group_id}")]
    ])

def groups_keyboard(groups: List[Tuple[str, str, List[str]]]) -> InlineKeyboardMarkup:
    """Клавиатура для выбора группы при дубликатах"""
    keyboard = []
    
    for group_id, display_name, associations in groups:
        # Показываем первые 2 ассоциации как пример
        examples = ", ".join(associations[:2])
        button_text = f"{display_name} ({examples}...)"
        keyboard.append([InlineKeyboardButton(
            text=button_text,
            callback_data=f"group_{group_id}"
        )])
    
    keyboard.append([InlineKeyboardButton(text="🔙 Назад к поиску", callback_data="back_to_search")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def associations_keyboard(associations: List[str], group_id: str) -> InlineKeyboardMarkup:
    keyboard = []
    row = []
    
    for i, assoc in enumerate(associations):
        row.append(InlineKeyboardButton(text=assoc, callback_data=f"assoc_{assoc}"))
        if len(row) == 2 or i == len(associations) - 1:
            keyboard.append(row)
            row = []
    
    keyboard.append([InlineKeyboardButton(text="🔙 Назад к поиску", callback_data="back_to_search")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# ========== ОБРАБОТЧИКИ КОМАНД ==========

@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    await message.answer(
        f"👋 Привет, {message.from_user.full_name}!\n\n"
        f"🔍 Напиши слово для поиска.\n\n"
        f"📋 **Примеры с дубликатами:**\n"
        f"• 'яблоко' - фрукт, бренд, кулинария\n"
        f"• 'apple' - фрукт, бренд\n"
        f"• 'машина' - автомобиль, устройство\n"
        f"• 'python' - змея, язык\n\n"
        f"💡 Если слово встречается в нескольких категориях, "
        f"бот предложит выбрать нужную!",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardRemove()
    )

@dp.message(Command("help"))
async def cmd_help(message: types.Message) -> None:
    await message.answer(
        "🤖 **Помощь:**\n\n"
        "• Напишите слово - бот покажет все категории\n"
        "• Если слово в нескольких категориях - выберите нужную\n"
        "• Затем выберите вариант\n"
        "• Кнопки назад вернут к выбору",
        parse_mode="Markdown"
    )

@dp.message(Command("stats"))
async def cmd_stats(message: types.Message) -> None:
    """Статистика базы данных"""
    stats = [
        f"📊 **Статистика:**",
        f"• Групп: {len(db.groups)}",
        f"• Уникальных ключей: {len(db.key_to_groups)}",
    ]
    
    # Считаем дубликаты
    duplicates = sum(1 for groups in db.key_to_groups.values() if len(groups) > 1)
    stats.append(f"• Ключей с дубликатами: {duplicates}")
    
    await message.answer("\n".join(stats), parse_mode="Markdown")

# ========== ОБРАБОТЧИК ТЕКСТА ==========

@dp.message()
async def handle_message(message: types.Message) -> None:
    text = message.text
    user_id = message.from_user.id
    
    if not text:
        return
    
    db.set_last_query(user_id, text)
    groups = db.find_groups(text)
    
    if groups:
        if len(groups) == 1:
            # Если одна группа - показываем сразу ассоциации
            group_id, display_name, associations = groups[0]
            db.set_last_group(user_id, group_id)
            
            assoc_list = "\n".join([f"• {a}" for a in associations])
            
            await message.answer(
                f"✅ **{display_name}**\n\n"
                f"📌 **Варианты:**\n\n{assoc_list}",
                parse_mode="Markdown",
                reply_markup=associations_keyboard(associations, group_id)
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

# ========== ОБРАБОТЧИКИ КНОПОК ==========

@dp.callback_query(lambda c: c.data.startswith('group_'))
async def process_group_select(callback: CallbackQuery):
    """Обработка выбора группы при дубликатах"""
    group_id = callback.data.replace('group_', '')
    user_id = callback.from_user.id
    
    group = db.get_group(group_id)
    
    if group:
        db.set_last_group(user_id, group_id)
        associations = group['associations']
        display_name = group['display_name']
        
        assoc_list = "\n".join([f"• {a}" for a in associations])
        
        await callback.message.edit_text(
            f"✅ **{display_name}**\n\n"
            f"📌 **Варианты:**\n\n{assoc_list}",
            parse_mode="Markdown",
            reply_markup=associations_keyboard(associations, group_id)
        )
    
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('assoc_'))
async def process_association(callback: CallbackQuery):
    association = callback.data.replace('assoc_', '')
    user_id = callback.from_user.id
    
    last_group = db.get_last_group(user_id)
    details = db.get_details(association)
    
    text = f"📖 **{association}**\n\n" + (details if details else "Нет подробной информации")
    reply = back_to_list_keyboard(last_group) if last_group else back_to_main_keyboard()
    
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=reply)
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('back_to_list_'))
async def process_back_to_list(callback: CallbackQuery):
    group_id = callback.data.replace('back_to_list_', '')
    group = db.get_group(group_id)
    
    if group:
        associations = group['associations']
        display_name = group['display_name']
        assoc_list = "\n".join([f"• {a}" for a in associations])
        
        await callback.message.edit_text(
            f"✅ **{display_name}**\n\n📌 **Варианты:**\n\n{assoc_list}",
            parse_mode="Markdown",
            reply_markup=associations_keyboard(associations, group_id)
        )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_search")
async def process_back_to_search(callback: CallbackQuery):
    user_id = callback.from_user.id
    last_query = db.get_last_query(user_id)
    
    text = "🔍 Введите слово для поиска"
    if last_query:
        text += f"\n\nПоследний запрос: '{last_query}'"
    
    await callback.message.edit_text(text, reply_markup=back_to_main_keyboard())
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_main")
async def process_back_to_main(callback: CallbackQuery):
    await callback.message.delete()
    await cmd_start(callback.message)
    await callback.answer()

# ========== ЗАПУСК ==========

async def delete_webhook() -> None:
    try:
        info = await bot.get_webhook_info()
        if info.url:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("✅ Webhook удален")
    except Exception as e:
        logger.error(f"Ошибка удаления webhook: {e}")

async def main() -> None:
    logger.info("🚀 Бот запускается...")
    await delete_webhook()
    logger.info("🔄 Polling...")
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")