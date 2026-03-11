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

# ========== КЛАСС ДЛЯ РАБОТЫ С БАЗОЙ СНИМКОВ ==========

class PhotosDatabase:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.photos_file = os.path.join(data_dir, "photos.txt")
        
        # Данные
        self.location_to_photos: Dict[str, List[str]] = {}  # локация (деревни) -> список снимков
        self.photo_to_locations: Dict[str, List[str]] = {}  # снимок -> список локаций
        self.all_locations: List[str] = []                  # все уникальные описания локаций
        
        # История пользователей
        self.user_last_locations: Dict[int, List[Tuple[str, str, List[str]]]] = {}
        self.user_last_query: Dict[int, str] = {}
        
        self.load_data()
    
    def load_data(self) -> None:
        os.makedirs(self.data_dir, exist_ok=True)
        
        try:
            if os.path.exists(self.photos_file):
                with open(self.photos_file, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f):
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue
                        
                        # Формат: локация|снимок1|снимок2|...
                        parts = line.split('|')
                        if len(parts) >= 3:
                            location_desc = parts[0].strip()  # список деревень
                            photos = [p.strip() for p in parts[1:] if p.strip()]
                            
                            if photos:
                                # Сохраняем связь локация -> снимки
                                self.location_to_photos[location_desc] = photos
                                self.all_locations.append(location_desc)
                                
                                # Сохраняем обратную связь снимок -> локации
                                for photo in photos:
                                    if photo not in self.photo_to_locations:
                                        self.photo_to_locations[photo] = []
                                    self.photo_to_locations[photo].append(location_desc)
                
                logger.info(f"✅ Загружено {len(self.location_to_photos)} локаций")
                logger.info(f"✅ Всего снимков: {len(self.photo_to_locations)}")
            else:
                logger.warning(f"⚠️ Файл {self.photos_file} не найден")
                self._create_example_file()
                
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки: {e}")
    
    def _create_example_file(self) -> None:
        """Создает пример файла с данными"""
        example = '''# Формат: ОПИСАНИЕ_ЛОКАЦИИ|НОМЕР_СНИМКА1|НОМЕР_СНИМКА2

Горбово,Нов.Ивановское,Ковынево,Скворцово,Дураково,Добрая,Мурылево,Ханино,Горы Казеки|N56E34-237-044|N56E34-237-045
Старшевицы,Бельково,Харино,Дешевка|N56E34-237-053
Полунино,Галахово,Тимофеево,Ердихино,Федорково|N56E34-237-048
Крупцово,Гущино,Иружа,Разница|N56E34-224-011
'''
        with open(self.photos_file, 'w', encoding='utf-8') as f:
            f.write(example)
    
    def find_locations(self, text: str) -> List[Tuple[str, List[str]]]:
        """
        Ищет локации по тексту (название деревни или номер снимка)
        Возвращает список: (описание локации, список снимков)
        """
        if not text:
            return []
        
        text_lower = text.lower().strip()
        found_locations = []
        seen = set()
        
        # Поиск по номеру снимка
        if text_lower in self.photo_to_locations:
            for loc_desc in self.photo_to_locations[text_lower]:
                if loc_desc not in seen:
                    found_locations.append((loc_desc, self.location_to_photos[loc_desc]))
                    seen.add(loc_desc)
        
        # Поиск по названию деревни (вхождение в описание локации)
        for loc_desc, photos in self.location_to_photos.items():
            if text_lower in loc_desc.lower():
                if loc_desc not in seen:
                    found_locations.append((loc_desc, photos))
                    seen.add(loc_desc)
        
        return found_locations
    
    def set_last_locations(self, user_id: int, locations: List[Tuple[str, List[str]]]):
        self.user_last_locations[user_id] = locations
    
    def get_last_locations(self, user_id: int) -> Optional[List[Tuple[str, List[str]]]]:
        return self.user_last_locations.get(user_id)
    
    def set_last_query(self, user_id: int, query: str):
        self.user_last_query[user_id] = query
    
    def get_last_query(self, user_id: int) -> Optional[str]:
        return self.user_last_query.get(user_id)

db = PhotosDatabase()

# ========== КЛАВИАТУРЫ ==========

def back_to_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 В начало", callback_data="back_to_main")]
    ])

def back_to_locations_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад к списку", callback_data="back_to_locations")]
    ])

def locations_keyboard(locations: List[Tuple[str, List[str]]]) -> InlineKeyboardMarkup:
    """Клавиатура для выбора локации"""
    keyboard = []
    
    for loc_desc, photos in locations:
        # Показываем первые несколько деревень для краткости
        villages = loc_desc.split(',')[:3]
        short_desc = ', '.join(villages)
        if len(loc_desc.split(',')) > 3:
            short_desc += f" и ещё {len(loc_desc.split(','))-3}"
        
        button_text = f"📌 {short_desc} ({len(photos)} снимков)"
        keyboard.append([InlineKeyboardButton(
            text=button_text,
            callback_data=f"loc_{loc_desc}"
        )])
    
    keyboard.append([InlineKeyboardButton(text="🔙 Новый поиск", callback_data="back_to_search")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def photos_keyboard(photos: List[str], loc_desc: str) -> InlineKeyboardMarkup:
    """Клавиатура со списком снимков"""
    keyboard = []
    row = []
    
    for i, photo in enumerate(photos):
        row.append(InlineKeyboardButton(text=photo, callback_data=f"photo_{photo}"))
        if len(row) == 3 or i == len(photos) - 1:
            keyboard.append(row)
            row = []
    
    keyboard.append([InlineKeyboardButton(text="🔙 Назад к списку", callback_data="back_to_locations")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# ========== ОБРАБОТЧИКИ КОМАНД ==========

@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    await message.answer(
        f"👋 Привет, {message.from_user.full_name}!\n\n"
        f"🛩️ **Поиск аэрофотоснимков Ржевского района**\n\n"
        f"🔍 Введите название деревни или номер снимка:\n\n"
        f"📋 **Примеры:**\n"
        f"• Горбово\n"
        f"• Полунино\n"
        f"• N56E34-237-044",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardRemove()
    )

@dp.message(Command("help"))
async def cmd_help(message: types.Message) -> None:
    await message.answer(
        "🛩️ **Помощь по поиску снимков:**\n\n"
        "1️⃣ Введите название деревни - найдете все снимки этого района\n"
        "2️⃣ Введите номер снимка - узнаете какие деревни на нем\n"
        "3️⃣ Выберите нужный вариант из списка\n"
        "4️⃣ Нажмите на номер снимка для просмотра\n\n"
        "🔙 Кнопки «Назад» возвращают к предыдущему шагу",
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
    locations = db.find_locations(text)
    
    if locations:
        db.set_last_locations(user_id, locations)
        
        if len(locations) == 1:
            # Одна локация - показываем сразу снимки
            loc_desc, photos = locations[0]
            
            await message.answer(
                f"✅ **Найден район:**\n{loc_desc}\n\n"
                f"📸 **Доступные снимки ({len(photos)} шт.):**",
                parse_mode="Markdown",
                reply_markup=photos_keyboard(photos, loc_desc)
            )
        else:
            # Несколько локаций - показываем выбор
            await message.answer(
                f"🔍 **Найдено {len(locations)} районов по запросу '{text}':**\n\n"
                f"Выберите нужный:",
                parse_mode="Markdown",
                reply_markup=locations_keyboard(locations)
            )
    else:
        await message.answer(
            f"❌ Ничего не найдено для '{text}'\n\n"
            f"Попробуйте другое название или номер снимка",
            reply_markup=back_to_main_keyboard()
        )

# ========== ОБРАБОТЧИКИ КНОПОК ==========

@dp.callback_query(lambda c: c.data.startswith('loc_'))
async def process_location_select(callback: CallbackQuery):
    loc_desc = callback.data.replace('loc_', '')
    photos = db.location_to_photos.get(loc_desc, [])
    
    if photos:
        await callback.message.edit_text(
            f"✅ **{loc_desc}**\n\n"
            f"📸 **Снимки ({len(photos)} шт.):**",
            parse_mode="Markdown",
            reply_markup=photos_keyboard(photos, loc_desc)
        )
    
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('photo_'))
async def process_photo_select(callback: CallbackQuery):
    photo = callback.data.replace('photo_', '')
    
    # Здесь можно добавить логику для показа preview снимка
    # Например, отправлять ссылку на изображение или файл
    
    await callback.message.edit_text(
        f"📸 **Снимок:** {photo}\n\n"
        f"🔗 Ссылка на снимок будет здесь\n\n"
        f"💡 В разработке...",
        parse_mode="Markdown",
        reply_markup=back_to_locations_keyboard()
    )
    
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_locations")
async def process_back_to_locations(callback: CallbackQuery):
    user_id = callback.from_user.id
    last_locations = db.get_last_locations(user_id)
    last_query = db.get_last_query(user_id)
    
    if last_locations and len(last_locations) > 1:
        await callback.message.edit_text(
            f"🔍 **Найдено {len(last_locations)} районов по запросу '{last_query}':**\n\n"
            f"Выберите нужный:",
            parse_mode="Markdown",
            reply_markup=locations_keyboard(last_locations)
        )
    elif last_locations and len(last_locations) == 1:
        loc_desc, photos = last_locations[0]
        await callback.message.edit_text(
            f"✅ **{loc_desc}**\n\n"
            f"📸 **Снимки ({len(photos)} шт.):**",
            parse_mode="Markdown",
            reply_markup=photos_keyboard(photos, loc_desc)
        )
    else:
        await callback.message.edit_text(
            "🔍 Введите название деревни или номер снимка",
            reply_markup=back_to_main_keyboard()
        )
    
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_search")
async def process_back_to_search(callback: CallbackQuery):
    await callback.message.edit_text(
        "🔍 Введите название деревни или номер снимка",
        reply_markup=back_to_main_keyboard()
    )
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
    logger.info("🛩️ Бот для поиска аэрофотоснимков запускается...")
    await delete_webhook()
    logger.info("🔄 Polling...")
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")