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
        
        self.key_to_group: Dict[str, str] = {}
        self.group_to_keys: Dict[str, List[str]] = {}
        self.group_to_associations: Dict[str, List[str]] = {}
        self.details: Dict[str, str] = {}
        
        self.user_last_group: Dict[int, str] = {}
        self.user_last_query: Dict[int, str] = {}
        
        self.load_all_data()
    
    def load_all_data(self) -> None:
        os.makedirs(self.data_dir, exist_ok=True)
        self.load_multi_keys()
        self.load_details()
    
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
                                group_id = f"{keys[0]}_{line_num}"
                                
                                self.group_to_keys[group_id] = keys
                                self.group_to_associations[group_id] = associations
                                
                                for key in keys:
                                    self.key_to_group[key] = group_id
                
                logger.info(f"✅ Загружено {len(self.group_to_associations)} групп")
            else:
                logger.warning(f"⚠️ Файл {self.multi_keys_file} не найден")
                
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки: {e}")
    
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
    
    def find_group(self, text: str) -> Optional[Tuple[str, List[str], List[str]]]:
        if not text or not self.key_to_group:
            return None
        
        text_lower = text.lower().strip()
        
        if text_lower in self.key_to_group:
            group_id = self.key_to_group[text_lower]
            return group_id, self.group_to_keys[group_id], self.group_to_associations[group_id]
        
        for key, group_id in self.key_to_group.items():
            if key in text_lower:
                return group_id, self.group_to_keys[group_id], self.group_to_associations[group_id]
        
        return None
    
    def get_group(self, group_id: str) -> Optional[Tuple[List[str], List[str]]]:
        if group_id in self.group_to_keys:
            return self.group_to_keys[group_id], self.group_to_associations[group_id]
        return None
    
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
        f"📋 Примеры: ноут, пицца, машина, кофе, python",
        reply_markup=ReplyKeyboardRemove()
    )

@dp.message(Command("help"))
async def cmd_help(message: types.Message) -> None:
    await message.answer(
        "🤖 **Помощь:**\n\n"
        "• Напишите слово - бот покажет варианты\n"
        "• Выберите вариант - получите информацию\n"
        "• Кнопки назад вернут к выбору",
        parse_mode="Markdown"
    )

# ========== ОБРАБОТЧИК ТЕКСТА ==========

@dp.message()
async def handle_message(message: types.Message) -> None:
    text = message.text
    user_id = message.from_user.id
    
    if not text:
        return
    
    db.set_last_query(user_id, text)
    result = db.find_group(text)
    
    if result:
        group_id, keys, associations = result
        display_name = keys[0]
        db.set_last_group(user_id, group_id)
        
        assoc_list = "\n".join([f"• {a}" for a in associations])
        
        await message.answer(
            f"✅ **{display_name}**\n\n"
            f"📌 **Варианты:**\n\n{assoc_list}",
            parse_mode="Markdown",
            reply_markup=associations_keyboard(associations, group_id)
        )
    else:
        await message.answer(
            f"❌ Ничего не найдено\n\nПопробуйте другое слово",
            reply_markup=back_to_main_keyboard()
        )

# ========== ОБРАБОТЧИКИ КНОПОК ==========

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
        keys, associations = group
        display_name = keys[0]
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