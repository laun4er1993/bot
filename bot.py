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

class PhotosDatabase:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.multi_keys_file = os.path.join(data_dir, "multi_keys.txt")
        self.details_file = os.path.join(data_dir, "details.txt")
        
        # Данные из multi_keys.txt
        self.location_to_photos: Dict[str, List[str]] = {}  # локация (деревни) -> список снимков
        self.photo_to_locations: Dict[str, List[str]] = {}  # снимок -> список локаций
        self.all_locations: List[str] = []                  # все уникальные описания локаций
        
        # Данные из details.txt
        self.photo_details: Dict[str, str] = {}             # снимок -> детальное описание
        
        # История пользователей
        self.user_last_locations: Dict[int, List[Tuple[str, List[str]]]] = {}
        self.user_last_query: Dict[int, str] = {}
        
        self.load_all_data()
    
    def load_all_data(self) -> None:
        os.makedirs(self.data_dir, exist_ok=True)
        self.load_multi_keys()
        self.load_details()
        self.log_stats()
    
    def load_multi_keys(self) -> None:
        """Загружает данные из multi_keys.txt"""
        try:
            if os.path.exists(self.multi_keys_file):
                with open(self.multi_keys_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    logger.info(f"📄 Читаем файл {self.multi_keys_file}, всего строк: {len(lines)}")
                    
                    for line_num, line in enumerate(lines):
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue
                        
                        # Формат: категория|описание|ключ1|ключ2|...
                        parts = line.split('|')
                        
                        if len(parts) >= 4:
                            category = parts[0].strip()      # Ржев
                            location_desc = parts[1].strip()  # список деревень
                            photos = [p.strip() for p in parts[2:] if p.strip()]  # номера снимков
                            
                            logger.info(f"  Строка {line_num}: '{location_desc[:30]}...' ({len(photos)} снимков)")
                            
                            if photos:
                                # Сохраняем связь локация -> снимки
                                self.location_to_photos[location_desc] = photos
                                self.all_locations.append(location_desc)
                                
                                # Сохраняем обратную связь снимок -> локации
                                for photo in photos:
                                    if photo not in self.photo_to_locations:
                                        self.photo_to_locations[photo] = []
                                    self.photo_to_locations[photo].append(location_desc)
                
                logger.info(f"✅ Загружено {len(self.location_to_photos)} локаций из multi_keys.txt")
            else:
                logger.warning(f"⚠️ Файл {self.multi_keys_file} не найден")
                
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки multi_keys: {e}")
    
    def load_details(self) -> None:
        """Загружает детальную информацию о снимках из details.txt"""
        try:
            if os.path.exists(self.details_file):
                with open(self.details_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue
                        
                        # Формат: НОМЕР_СНИМКА===ПОДРОБНОЕ_ОПИСАНИЕ
                        if '===' in line:
                            photo_num, details = line.split('===', 1)
                            photo_num = photo_num.strip()
                            details = details.strip()
                            
                            if photo_num and details:
                                self.photo_details[photo_num] = details
                
                logger.info(f"✅ Загружено {len(self.photo_details)} описаний снимков из details.txt")
            else:
                logger.warning(f"⚠️ Файл {self.details_file} не найден")
                self._create_example_details()
                
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки details: {e}")
    
    def _create_example_details(self) -> None:
        """Создает пример файла с деталями снимков"""
        example = '''# Формат: НОМЕР_СНИМКА===ПОДРОБНОЕ_ОПИСАНИЕ

N56E34-237-044===📸 **Снимок N56E34-237-044**
📅 Дата съемки: 1943-07-15
🛩️ Высота: 4500 м
🎞️ Масштаб: 1:15000
📋 Описание: Район деревень Горбово, Нов.Ивановское, Ковынево. Хорошо видны укрепления.

N56E34-237-045===📸 **Снимок N56E34-237-045**
📅 Дата съемки: 1943-07-15
🛩️ Высота: 4800 м
🎞️ Масштаб: 1:16000
📋 Описание: Продолжение предыдущего снимка. Видна линия окопов.

N56E34-237-053===📸 **Снимок N56E34-237-053**
📅 Дата съемки: 1943-08-02
🛩️ Высота: 4200 м
🎞️ Масштаб: 1:14000
📋 Описание: Район Старшевицы, Бельково, Харино. Заметны следы боев.
'''
        with open(self.details_file, 'w', encoding='utf-8') as f:
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
        
        logger.info(f"🔍 Поиск: '{text}'")
        
        # ПОИСК 1: По номеру снимка (точное совпадение)
        if text_lower in self.photo_to_locations:
            for loc_desc in self.photo_to_locations[text_lower]:
                if loc_desc not in seen:
                    found_locations.append((loc_desc, self.location_to_photos[loc_desc]))
                    seen.add(loc_desc)
                    logger.info(f"  ✓ Найдено по номеру снимка: {loc_desc[:50]}...")
        
        # ПОИСК 2: По точному названию деревни
        for loc_desc, photos in self.location_to_photos.items():
            # Разбиваем описание локации на отдельные деревни
            villages = [v.strip().lower() for v in loc_desc.split(',')]
            
            # Проверяем, есть ли искомый текст в списке деревень
            if text_lower in villages:
                if loc_desc not in seen:
                    found_locations.append((loc_desc, photos))
                    seen.add(loc_desc)
                    logger.info(f"  ✓ Точное совпадение с деревней: {loc_desc[:50]}...")
        
        # ПОИСК 3: По вхождению в название деревни (если еще не нашли)
        if len(found_locations) < 3:  # Ограничиваем, чтобы не было слишком много
            for loc_desc, photos in self.location_to_photos.items():
                # Проверяем каждую деревню в описании
                villages = [v.strip().lower() for v in loc_desc.split(',')]
                
                for village in villages:
                    if text_lower in village and len(text_lower) > 3:  # Не ищем по очень коротким словам
                        if loc_desc not in seen:
                            found_locations.append((loc_desc, photos))
                            seen.add(loc_desc)
                            logger.info(f"  ✓ Частичное совпадение: '{text}' в '{village}'")
                            break  # Нашли одну деревню в этой локации, переходим к следующей
        
        # ПОИСК 4: По вхождению в описание локации (самый широкий поиск)
        if not found_locations:  # Если ничего не нашли, пробуем самый широкий поиск
            for loc_desc, photos in self.location_to_photos.items():
                if text_lower in loc_desc.lower():
                    if loc_desc not in seen:
                        found_locations.append((loc_desc, photos))
                        seen.add(loc_desc)
                        logger.info(f"  ✓ Широкий поиск: '{text}' в {loc_desc[:50]}...")
        
        logger.info(f"✅ Всего найдено локаций: {len(found_locations)}")
        return found_locations
    
    def get_photo_details(self, photo_num: str) -> Optional[str]:
        """Возвращает детальное описание снимка"""
        return self.photo_details.get(photo_num)
    
    def set_last_locations(self, user_id: int, locations: List[Tuple[str, List[str]]]):
        self.user_last_locations[user_id] = locations
    
    def get_last_locations(self, user_id: int) -> Optional[List[Tuple[str, List[str]]]]:
        return self.user_last_locations.get(user_id)
    
    def set_last_query(self, user_id: int, query: str):
        self.user_last_query[user_id] = query
    
    def get_last_query(self, user_id: int) -> Optional[str]:
        return self.user_last_query.get(user_id)
    
    def log_stats(self):
        logger.info(f"📊 Статистика:")
        logger.info(f"   • Локаций в multi_keys.txt: {len(self.location_to_photos)}")
        logger.info(f"   • Уникальных снимков: {len(self.photo_to_locations)}")
        logger.info(f"   • Описаний в details.txt: {len(self.photo_details)}")

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
        "4️⃣ Нажмите на номер снимка для просмотра детальной информации\n\n"
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
            
            # Показываем краткое описание локации
            villages = loc_desc.split(',')
            if len(villages) > 5:
                short_desc = ', '.join(villages[:5]) + f" и ещё {len(villages)-5}"
            else:
                short_desc = loc_desc
            
            await message.answer(
                f"✅ **Найден район:**\n{short_desc}\n\n"
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
        # Показываем краткое описание локации
        villages = loc_desc.split(',')
        if len(villages) > 5:
            short_desc = ', '.join(villages[:5]) + f" и ещё {len(villages)-5}"
        else:
            short_desc = loc_desc
        
        await callback.message.edit_text(
            f"✅ **{short_desc}**\n\n"
            f"📸 **Снимки ({len(photos)} шт.):**",
            parse_mode="Markdown",
            reply_markup=photos_keyboard(photos, loc_desc)
        )
    
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('photo_'))
async def process_photo_select(callback: CallbackQuery):
    photo = callback.data.replace('photo_', '')
    
    # Получаем детальное описание снимка из details.txt
    details = db.get_photo_details(photo)
    
    if details:
        text = details
    else:
        # Если нет описания, показываем базовую информацию
        locations = db.photo_to_locations.get(photo, [])
        loc_text = ", ".join(locations) if locations else "неизвестно"
        
        text = f"📸 **Снимок {photo}**\n\n"
        text += f"📍 Район: {loc_text}\n"
        text += f"📝 Подробное описание отсутствует"
    
    await callback.message.edit_text(
        text,
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
        
        # Показываем краткое описание локации
        villages = loc_desc.split(',')
        if len(villages) > 5:
            short_desc = ', '.join(villages[:5]) + f" и ещё {len(villages)-5}"
        else:
            short_desc = loc_desc
        
        await callback.message.edit_text(
            f"✅ **{short_desc}**\n\n"
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