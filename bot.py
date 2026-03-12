import asyncio
import logging
import os
import sys
from typing import Optional, Dict, List, Set

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    ReplyKeyboardRemove
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

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

# Инициализация бота и хранилища состояний
storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=storage)

# ========== КЛАСС ДЛЯ РАБОТЫ С ДАННЫМИ ==========

class PhotosDatabase:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.multi_keys_file = os.path.join(data_dir, "multi_keys.txt")
        self.details_file = os.path.join(data_dir, "details.txt")
        
        # Данные из multi_keys.txt
        self.locations: List[Dict] = []
        self.all_villages: Set[str] = set()  # Все уникальные деревни
        
        # Данные из details.txt
        self.photo_details: Dict[str, str] = {}
        
        # История пользователей
        self.user_last_photos: Dict[int, List[str]] = {}
        self.user_last_villages: Dict[int, str] = {}
        self.user_last_query: Dict[int, str] = {}
        
        self.load_all_data()
    
    def load_all_data(self) -> None:
        os.makedirs(self.data_dir, exist_ok=True)
        self.load_multi_keys()
        self.load_details_multiline()
        self.log_stats()
    
    def load_multi_keys(self) -> None:
        """Загружает данные из multi_keys.txt"""
        try:
            if os.path.exists(self.multi_keys_file):
                with open(self.multi_keys_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    logger.info(f"📄 Читаем файл {self.multi_keys_file}, всего строк: {len(lines)}")
                    
                    for idx, line in enumerate(lines):
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue
                        
                        parts = line.split('|')
                        
                        if len(parts) >= 3:
                            villages_str = parts[1].strip()
                            photos = [p.strip() for p in parts[2:] if p.strip()]
                            villages = [v.strip() for v in villages_str.split(',') if v.strip()]
                            
                            # Добавляем деревни в общий список
                            for village in villages:
                                self.all_villages.add(village)
                            
                            record = {
                                'id': idx,
                                'villages': villages,
                                'villages_str': villages_str,
                                'photos': photos
                            }
                            self.locations.append(record)
                            
                            logger.info(f"  Строка {idx}: {len(villages)} деревень, {len(photos)} снимков")
                
                logger.info(f"✅ Загружено {len(self.locations)} записей из multi_keys.txt")
                logger.info(f"✅ Всего уникальных деревень: {len(self.all_villages)}")
            else:
                logger.warning(f"⚠️ Файл {self.multi_keys_file} не найден")
                
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки multi_keys: {e}")
    
    def load_details_multiline(self) -> None:
        """Загружает многострочные описания из details.txt"""
        try:
            if os.path.exists(self.details_file):
                with open(self.details_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    logger.info(f"📄 Читаем файл {self.details_file}, размер: {len(content)} байт")
                    
                    entries = content.split('===')
                    
                    for i in range(len(entries) - 1):
                        lines = entries[i].strip().split('\n')
                        photo_num = lines[-1].strip() if lines else ""
                        description = entries[i + 1].strip()
                        
                        if photo_num and description and not photo_num.startswith('#'):
                            self.photo_details[photo_num] = description
                            logger.info(f"  ✅ Загружен снимок: {photo_num}")
                    
                    logger.info(f"✅ Загружено {len(self.photo_details)} описаний снимков")
            else:
                logger.warning(f"⚠️ Файл {self.details_file} не найден")
                
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки details: {e}", exc_info=True)
    
    def search_by_village(self, query: str) -> List[Dict]:
        """Ищет записи по названию деревни"""
        if not query:
            return []
        
        query_lower = query.lower().strip()
        found_records = []
        seen_ids = set()
        
        for record in self.locations:
            for village in record['villages']:
                village_lower = village.lower()
                
                if village_lower == query_lower or (len(query_lower) > 2 and query_lower in village_lower):
                    if record['id'] not in seen_ids:
                        found_records.append(record)
                        seen_ids.add(record['id'])
                    break
        
        return found_records
    
    def get_all_photos(self, records: List[Dict]) -> List[str]:
        """Собирает все уникальные снимки из списка записей"""
        all_photos = []
        for record in records:
            all_photos.extend(record['photos'])
        
        unique_photos = []
        for photo in all_photos:
            if photo not in unique_photos:
                unique_photos.append(photo)
        return unique_photos
    
    def get_all_villages(self, records: List[Dict]) -> List[str]:
        """Собирает все уникальные деревни из списка записей"""
        all_villages = []
        for record in records:
            all_villages.extend(record['villages'])
        
        unique_villages = sorted(list(set(all_villages)))
        return unique_villages
    
    def get_photo_details(self, photo_num: str) -> Optional[str]:
        """Возвращает детальное описание снимка"""
        return self.photo_details.get(photo_num)
    
    def get_all_villages_list(self) -> List[str]:
        """Возвращает отсортированный список всех деревень"""
        return sorted(list(self.all_villages))
    
    def set_last_photos(self, user_id: int, photos: List[str]):
        self.user_last_photos[user_id] = photos
    
    def get_last_photos(self, user_id: int) -> Optional[List[str]]:
        return self.user_last_photos.get(user_id)
    
    def set_last_villages(self, user_id: int, villages_text: str):
        self.user_last_villages[user_id] = villages_text
    
    def get_last_villages(self, user_id: int) -> Optional[str]:
        return self.user_last_villages.get(user_id)
    
    def set_last_query(self, user_id: int, query: str):
        self.user_last_query[user_id] = query
    
    def get_last_query(self, user_id: int) -> Optional[str]:
        return self.user_last_query.get(user_id)
    
    def log_stats(self):
        logger.info(f"📊 Статистика:")
        logger.info(f"   • Записей в multi_keys.txt: {len(self.locations)}")
        logger.info(f"   • Уникальных деревень: {len(self.all_villages)}")
        logger.info(f"   • Описаний в details.txt: {len(self.photo_details)}")

db = PhotosDatabase()

# ========== СОСТОЯНИЯ ДЛЯ FSM ==========

class SearchStates(StatesGroup):
    waiting_for_village = State()

# ========== КЛАВИАТУРЫ ==========

def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Главное меню бота"""
    keyboard = [
        [KeyboardButton(text="🔍 ПОИСК"), KeyboardButton(text="📋 СПИСОК ДЕРЕВЕНЬ")],
        [KeyboardButton(text="📖 ИНСТРУКЦИЯ"), KeyboardButton(text="🗺️ КАРТА РЖЕВ")],
        [KeyboardButton(text="🗺️ LOCUS MAPS")]
    ]
    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        input_field_placeholder="Выберите действие..."
    )

def get_locus_maps_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для раздела Locus Maps"""
    keyboard = [
        [InlineKeyboardButton(text="📖 Инструкция по Locus Maps", callback_data="locus_instruction")],
        [InlineKeyboardButton(text="📥 Скачать карты для Locus", callback_data="locus_download")],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def back_to_main_keyboard() -> InlineKeyboardMarkup:
    """Инлайн-кнопка возврата в главное меню"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])

def back_to_photos_keyboard() -> InlineKeyboardMarkup:
    """Инлайн-кнопка возврата к списку снимков"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад к списку", callback_data="back_to_photos")]
    ])

def photos_keyboard(photos: List[str]) -> InlineKeyboardMarkup:
    """Клавиатура со списком снимков"""
    keyboard = []
    row = []
    
    for i, photo in enumerate(photos):
        row.append(InlineKeyboardButton(text=photo, callback_data=f"photo_{photo}"))
        if len(row) == 3 or i == len(photos) - 1:
            keyboard.append(row)
            row = []
    
    keyboard.append([InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# ========== ОБРАБОТЧИКИ КОМАНД ==========

@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    """Обработчик команды /start"""
    welcome_text = (
        f"👋 Добро пожаловать, {message.from_user.full_name}!\n\n"
        f"🛩️ **Бот для поиска информации об аэрофотоснимках Ржевского района**\n\n"
        f"📌 **Возможности бота:**\n"
        f"• 🔍 Поиск снимков по названию деревни\n"
        f"• 📋 Просмотр всех доступных деревень\n"
        f"• 📖 Подробная инструкция по использованию\n"
        f"• 🗺️ Скачивание карты Ржевского района\n"
        f"• 🗺️ Locus Maps - инструкция и карты для приложения\n\n"
        f"👇 **Выберите действие в меню ниже:**"
    )
    
    await message.answer(
        welcome_text,
        parse_mode="Markdown",
        reply_markup=get_main_keyboard()
    )

# ========== ОБРАБОТЧИКИ КНОПОК МЕНЮ ==========

@dp.message(F.text == "🔍 ПОИСК")
async def menu_search(message: types.Message, state: FSMContext):
    """Обработчик кнопки ПОИСК"""
    await message.answer(
        "🔍 **Режим поиска**\n\n"
        "Введите название деревни, и я найду все связанные с ней снимки.\n\n"
        "📝 Например: Горбово, Полунино, Дураково",
        parse_mode="Markdown"
    )
    await state.set_state(SearchStates.waiting_for_village)

@dp.message(F.text == "📋 СПИСОК ДЕРЕВЕНЬ")
async def menu_villages_list(message: types.Message):
    """Обработчик кнопки СПИСОК ДЕРЕВЕНЬ"""
    all_villages = db.get_all_villages_list()
    
    if not all_villages:
        await message.answer("📭 Список деревень пуст")
        return
    
    # Разбиваем на части для удобного отображения
    chunks = [all_villages[i:i+20] for i in range(0, len(all_villages), 20)]
    
    for i, chunk in enumerate(chunks):
        if i == 0:
            text = f"📋 **Все деревни в базе данных ({len(all_villages)} шт.):**\n\n"
        else:
            text = ""
        
        text += "\n".join([f"• {village}" for village in chunk])
        
        await message.answer(text, parse_mode="Markdown")
    
    await message.answer(
        "💡 Чтобы найти снимки по деревне, нажмите 🔍 ПОИСК",
        reply_markup=back_to_main_keyboard()
    )

@dp.message(F.text == "📖 ИНСТРУКЦИЯ")
async def menu_instruction(message: types.Message):
    """Обработчик кнопки ИНСТРУКЦИЯ"""
    instruction_text = (
        "📖 **Инструкция по использованию бота**\n\n"
        "🔍 **Поиск снимков:**\n"
        "1. Нажмите кнопку 🔍 ПОИСК\n"
        "2. Введите название деревни\n"
        "3. Бот покажет все снимки, где встречается эта деревня\n"
        "4. Нажмите на номер снимка для просмотра детальной информации\n\n"
        "📋 **Список деревень:**\n"
        "• Просмотр всех доступных для поиска деревень\n"
        "• Удобно, если не знаете точное название\n\n"
        "🗺️ **Карта Ржев:**\n"
        "• Скачивание карты Ржевского района\n"
        "• Полезно для ориентирования\n\n"
        "🗺️ **Locus Maps:**\n"
        "• Инструкция по использованию приложения Locus Maps\n"
        "• Скачивание готовых карт для приложения\n\n"
        "❓ **Дополнительно:**\n"
        "• После просмотра снимка можно вернуться к списку\n"
        "• Кнопка 🏠 В главное меню возвращает в начало\n\n"
        "🛩️ **Удачного поиска!**"
    )
    
    await message.answer(instruction_text, parse_mode="Markdown")

@dp.message(F.text == "🗺️ КАРТА РЖЕВ")
async def menu_map(message: types.Message):
    """Обработчик кнопки КАРТА РЖЕВ"""
    map_text = (
        "🗺️ **Карта Ржевского района**\n\n"
        "Ссылка для скачивания:\n"
        "https://example.com/rzhev-map.pdf\n\n"
        "Формат: PDF, 5.2 МБ\n\n"
        "На карте отмечены основные населенные пункты и районы съемки."
    )
    
    # Создаем инлайн-кнопку для скачивания
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Скачать карту", url="https://example.com/rzhev-map.pdf")],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])
    
    await message.answer(map_text, parse_mode="Markdown", reply_markup=keyboard)

@dp.message(F.text == "🗺️ LOCUS MAPS")
async def menu_locus_maps(message: types.Message):
    """Обработчик кнопки LOCUS MAPS"""
    locus_text = (
        "🗺️ **Locus Maps**\n\n"
        "Locus Maps - это мощное приложение для работы с картами в походах и экспедициях.\n\n"
        "Здесь вы можете:\n"
        "• 📖 Ознакомиться с инструкцией по использованию\n"
        "• 📥 Скачать готовые карты для загрузки в приложение\n\n"
        "👇 **Выберите действие:**"
    )
    
    await message.answer(
        locus_text,
        parse_mode="Markdown",
        reply_markup=get_locus_maps_keyboard()
    )

# ========== ОБРАБОТЧИКИ ИНЛАЙН-КНОПОК LOCUS MAPS ==========

@dp.callback_query(lambda c: c.data == "locus_instruction")
async def process_locus_instruction(callback: CallbackQuery):
    """Инструкция по Locus Maps"""
    instruction_text = (
        "📖 **Инструкция по использованию Locus Maps**\n\n"
        "**Установка приложения:**\n"
        "1. Скачайте Locus Maps из Google Play или App Store\n"
        "2. Установите приложение на устройство\n\n"
        "**Загрузка карт:**\n"
        "1. Нажмите кнопку «Скачать карты для Locus»\n"
        "2. Скачайте архив с картами\n"
        "3. Распакуйте архив в папку Locus/maps на вашем устройстве\n\n"
        "**Использование карт:**\n"
        "1. Откройте Locus Maps\n"
        "2. В меню выберите «Карты» → «Загруженные»\n"
        "3. Выберите нужную карту\n"
        "4. Используйте навигацию по карте\n\n"
        "**Особенности:**\n"
        "• Карты работают офлайн\n"
        "• Поддерживается поиск по координатам\n"
        "• Можно накладывать несколько слоев\n\n"
        "🔗 **Полезные ссылки:**\n"
        "• Официальный сайт: https://www.locusmap.eu\n"
        "• Документация: https://docs.locusmap.eu"
    )
    
    # Создаем клавиатуру с дополнительными кнопками
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Скачать карты", callback_data="locus_download")],
        [InlineKeyboardButton(text="🌐 Официальный сайт", url="https://www.locusmap.eu")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_locus")],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])
    
    await callback.message.edit_text(
        instruction_text,
        parse_mode="Markdown",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "locus_download")
async def process_locus_download(callback: CallbackQuery):
    """Скачивание карт для Locus Maps"""
    download_text = (
        "📥 **Скачать карты для Locus Maps**\n\n"
        "Доступны следующие карты Ржевского района:\n\n"
        "1️⃣ **Топографическая карта**\n"
        "   • Масштаб: 1:50000\n"
        "   • Формат: SQLiteDB\n"
        "   • Размер: 45 МБ\n"
        "   • Ссылка: https://example.com/locus/rzhev-topo.sqlitedb\n\n"
        "2️⃣ **Карта высот (рельеф)**\n"
        "   • Масштаб: 1:100000\n"
        "   • Формат: SQLiteDB\n"
        "   • Размер: 32 МБ\n"
        "   • Ссылка: https://example.com/locus/rzhev-elevation.sqlitedb\n\n"
        "3️⃣ **Спутниковые снимки 1942-1943**\n"
        "   • Масштаб: 1:25000\n"
        "   • Формат: MBTiles\n"
        "   • Размер: 128 МБ\n"
        "   • Ссылка: https://example.com/locus/rzhev-1942.mbtiles\n\n"
        "**Инструкция по установке:**\n"
        "1. Скачайте нужный файл\n"
        "2. Поместите его в папку Locus/maps/\n"
        "3. Откройте Locus Maps и выберите карту"
    )
    
    # Создаем клавиатуру со ссылками на скачивание
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗺️ Топографическая карта", url="https://example.com/locus/rzhev-topo.sqlitedb")],
        [InlineKeyboardButton(text="⛰️ Карта высот", url="https://example.com/locus/rzhev-elevation.sqlitedb")],
        [InlineKeyboardButton(text="🛩️ Снимки 1942-43", url="https://example.com/locus/rzhev-1942.mbtiles")],
        [InlineKeyboardButton(text="📖 Инструкция", callback_data="locus_instruction")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_locus")],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])
    
    await callback.message.edit_text(
        download_text,
        parse_mode="Markdown",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_locus")
async def process_back_to_locus(callback: CallbackQuery):
    """Возврат в меню Locus Maps"""
    locus_text = (
        "🗺️ **Locus Maps**\n\n"
        "Locus Maps - это мощное приложение для работы с картами в походах и экспедициях.\n\n"
        "Здесь вы можете:\n"
        "• 📖 Ознакомиться с инструкцией по использованию\n"
        "• 📥 Скачать готовые карты для загрузки в приложение\n\n"
        "👇 **Выберите действие:**"
    )
    
    await callback.message.edit_text(
        locus_text,
        parse_mode="Markdown",
        reply_markup=get_locus_maps_keyboard()
    )
    await callback.answer()

# ========== ОБРАБОТЧИК ПОИСКА ==========

@dp.message(SearchStates.waiting_for_village)
async def process_village_search(message: types.Message, state: FSMContext):
    """Обработчик ввода названия деревни"""
    text = message.text
    user_id = message.from_user.id
    
    if not text:
        await message.answer("❌ Пожалуйста, введите текст")
        return
    
    # Выходим из состояния поиска
    await state.clear()
    
    db.set_last_query(user_id, text)
    results = db.search_by_village(text)
    
    if results:
        all_photos = db.get_all_photos(results)
        all_villages = db.get_all_villages(results)
        
        # Формируем текст со списком деревень
        if len(all_villages) > 15:
            villages_text = ', '.join(all_villages[:15]) + f" и ещё {len(all_villages)-15}"
        else:
            villages_text = ', '.join(all_villages)
        
        # Сохраняем для кнопки "Назад"
        db.set_last_photos(user_id, all_photos)
        db.set_last_villages(user_id, villages_text)
        
        photos_list = "\n".join([f"• {photo}" for photo in all_photos])
        
        await message.answer(
            f"✅ **Найдено по запросу '{text}':**\n\n"
            f"📍 **Деревни в этом районе:**\n{villages_text}\n\n"
            f"📸 **Снимки ({len(all_photos)} шт.):**\n\n{photos_list}",
            parse_mode="Markdown",
            reply_markup=photos_keyboard(all_photos)
        )
    else:
        # Создаем клавиатуру с предложением попробовать еще раз
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔍 Попробовать снова", callback_data="try_again")],
            [InlineKeyboardButton(text="📋 Посмотреть список деревень", callback_data="show_villages")],
            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
        ])
        
        await message.answer(
            f"❌ Ничего не найдено для '{text}'\n\n"
            f"Попробуйте другое название или посмотрите список доступных деревень.",
            parse_mode="Markdown",
            reply_markup=keyboard
        )

# ========== ОБРАБОТЧИКИ ИНЛАЙН-КНОПОК ==========

@dp.callback_query(lambda c: c.data.startswith('photo_'))
async def process_photo_select(callback: CallbackQuery):
    """Показывает детальную информацию по снимку"""
    photo = callback.data.replace('photo_', '')
    details = db.get_photo_details(photo)
    
    if details:
        text = details
    else:
        text = f"📸 **Снимок {photo}**\n\n❌ Информация отсутствует"
    
    await callback.message.edit_text(
        text,
        parse_mode="Markdown",
        reply_markup=back_to_photos_keyboard()
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_photos")
async def process_back_to_photos(callback: CallbackQuery):
    """Возвращает к списку снимков"""
    user_id = callback.from_user.id
    last_photos = db.get_last_photos(user_id)
    last_villages = db.get_last_villages(user_id)
    last_query = db.get_last_query(user_id)
    
    if last_photos:
        photos_list = "\n".join([f"• {photo}" for photo in last_photos])
        
        await callback.message.edit_text(
            f"✅ **Найдено по запросу '{last_query}':**\n\n"
            f"📍 **Деревни в этом районе:**\n{last_villages}\n\n"
            f"📸 **Снимки ({len(last_photos)} шт.):**\n\n{photos_list}",
            parse_mode="Markdown",
            reply_markup=photos_keyboard(last_photos)
        )
    else:
        await callback.message.edit_text(
            "🔍 Введите название деревни",
            reply_markup=back_to_main_keyboard()
        )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "try_again")
async def process_try_again(callback: CallbackQuery, state: FSMContext):
    """Повторная попытка поиска"""
    await callback.message.delete()
    await callback.message.answer(
        "🔍 Введите название деревни:",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(SearchStates.waiting_for_village)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "show_villages")
async def process_show_villages(callback: CallbackQuery):
    """Показывает список деревень"""
    await callback.message.delete()
    all_villages = db.get_all_villages_list()
    
    if not all_villages:
        await callback.message.answer("📭 Список деревень пуст")
        return
    
    chunks = [all_villages[i:i+20] for i in range(0, len(all_villages), 20)]
    
    for i, chunk in enumerate(chunks):
        if i == 0:
            text = f"📋 **Все деревни в базе данных ({len(all_villages)} шт.):**\n\n"
        else:
            text = ""
        
        text += "\n".join([f"• {village}" for village in chunk])
        await callback.message.answer(text, parse_mode="Markdown")
    
    await callback.message.answer(
        "💡 Чтобы найти снимки по деревне, нажмите 🔍 ПОИСК",
        reply_markup=back_to_main_keyboard()
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_main")
async def process_back_to_main(callback: CallbackQuery, state: FSMContext):
    """Возвращает в главное меню"""
    await state.clear()
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
    logger.info("🛩️ Бот с меню запускается...")
    logger.info(f"📊 Загружено локаций: {len(db.locations)}")
    logger.info(f"📊 Уникальных деревень: {len(db.all_villages)}")
    logger.info(f"📊 Описаний снимков: {len(db.photo_details)}")
    
    await delete_webhook()
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")