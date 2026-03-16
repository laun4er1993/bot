import asyncio
import logging
import os
import sys
import re
from typing import Optional, Dict, List, Set
import requests
import io

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    ReplyKeyboardRemove,
    FSInputFile
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

# Токен из переменных окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")
YANDEX_DISK_TOKEN = os.getenv("YANDEX_DISK_TOKEN")

if not BOT_TOKEN:
    logging.critical("❌ ОШИБКА: BOT_TOKEN не найден!")
    sys.exit(1)

if not YANDEX_DISK_TOKEN:
    logging.critical("❌ ОШИБКА: YANDEX_DISK_TOKEN не найден!")
    sys.exit(1)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Инициализация
storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=storage)

# ========== КЛАСС ДЛЯ РАБОТЫ С ЯНДЕКС.ДИСКОМ ==========

class YandexDiskClient:
    def __init__(self, token: str):
        self.token = token
        self.base_url = "https://cloud-api.yandex.net/v1/disk"
        self.headers = {
            "Authorization": f"OAuth {token}",
            "Content-Type": "application/json"
        }
        self.file_cache: Dict[str, str] = {}
        
    def get_files_in_folder(self, folder_path: str) -> Optional[List[Dict]]:
        try:
            url = f"{self.base_url}/resources"
            params = {
                "path": folder_path,
                "limit": 100
            }
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code == 200:
                data = response.json()
                if "_embedded" in data and "items" in data["_embedded"]:
                    return data["_embedded"]["items"]
            logger.error(f"Ошибка получения файлов из {folder_path}: {response.status_code}")
            return None
        except Exception as e:
            logger.error(f"Ошибка при запросе к Яндекс.Диску: {e}")
            return None
    
    def folder_exists(self, folder_path: str) -> bool:
        try:
            url = f"{self.base_url}/resources"
            params = {"path": folder_path}
            response = requests.get(url, headers=self.headers, params=params)
            return response.status_code == 200
        except Exception:
            return False
    
    def get_file_download_link(self, file_path: str) -> Optional[str]:
        try:
            if file_path in self.file_cache:
                return self.file_cache[file_path]
            
            url = f"{self.base_url}/resources/download"
            params = {"path": file_path}
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code == 200:
                data = response.json()
                download_link = data.get("href")
                if download_link:
                    self.file_cache[file_path] = download_link
                    return download_link
            return None
        except Exception as e:
            logger.error(f"Ошибка получения ссылки на скачивание для {file_path}: {e}")
            return None
    
    def find_mbtiles_file(self, square: str, overlay: str, frame: str) -> Optional[Dict]:
        try:
            base_folder = f"АФС/Каталог ПО Сокол"
            square_folder = f"{base_folder}/{square}"
            overlay_folder = f"{square_folder}/{square}-{overlay}"
            full_name = f"{square}-{overlay}-{frame}"
            
            # ВАРИАНТ 1: Проверяем путь с подпапкой полного имени
            subfolder_path = f"{overlay_folder}/{full_name}"
            logger.info(f"  Проверяем путь с подпапкой: {subfolder_path}")
            
            if self.folder_exists(subfolder_path):
                files = self.get_files_in_folder(subfolder_path)
                if files:
                    mbtiles_files = self._filter_mbtiles_files(files, full_name)
                    if mbtiles_files:
                        logger.info(f"  ✅ Найдены файлы в подпапке: {len(mbtiles_files)} шт.")
                        mbtiles_files.sort(key=lambda x: x['version'], reverse=True)
                        selected = mbtiles_files[0]
                        
                        file_path = f"{subfolder_path}/{selected['name']}"
                        download_link = self.get_file_download_link(file_path)
                        
                        if download_link:
                            return {
                                'name': selected['name'],
                                'path': file_path,
                                'version': selected['version'],
                                'download_link': download_link
                            }
            
            # ВАРИАНТ 2: Проверяем путь без подпапки
            logger.info(f"  Проверяем путь без подпапки: {overlay_folder}")
            files = self.get_files_in_folder(overlay_folder)
            if files:
                mbtiles_files = self._filter_mbtiles_files(files, full_name)
                if mbtiles_files:
                    logger.info(f"  ✅ Найдены файлы в папке наложения: {len(mbtiles_files)} шт.")
                    mbtiles_files.sort(key=lambda x: x['version'], reverse=True)
                    selected = mbtiles_files[0]
                    
                    file_path = f"{overlay_folder}/{selected['name']}"
                    download_link = self.get_file_download_link(file_path)
                    
                    if download_link:
                        return {
                            'name': selected['name'],
                            'path': file_path,
                            'version': selected['version'],
                            'download_link': download_link
                        }
            
            logger.warning(f"❌ Файл не найден для {full_name}")
            return None
            
        except Exception as e:
            logger.error(f"Ошибка поиска файла для {square}-{overlay}-{frame}: {e}")
            return None
    
    def _filter_mbtiles_files(self, files: List[Dict], base_name: str) -> List[Dict]:
        mbtiles_files = []
        
        for file in files:
            name = file['name']
            if not name.endswith('.mbtiles') or not name.startswith(base_name):
                continue
            
            version = 0
            version_match = re.search(rf'{re.escape(base_name)}-(\d+)\.mbtiles$', name)
            if version_match:
                version = int(version_match.group(1))
            elif name == f"{base_name}.mbtiles":
                version = 0
            
            mbtiles_files.append({
                'name': name,
                'version': version
            })
        
        return mbtiles_files

# Инициализация клиента Яндекс.Диска
yd_client = YandexDiskClient(YANDEX_DISK_TOKEN)

# ========== КЛАСС ДЛЯ РАБОТЫ С ДАННЫМИ ==========

class PhotosDatabase:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.multi_keys_file = os.path.join(data_dir, "multi_keys.txt")
        self.details_file = os.path.join(data_dir, "details.txt")
        
        self.locations: List[Dict] = []
        self.all_villages: Set[str] = set()
        self.photo_details: Dict[str, str] = {}
        self.photo_links: Dict[str, str] = {}
        
        self.user_last_photos: Dict[int, List[str]] = {}
        self.user_last_villages: Dict[int, str] = {}
        self.user_last_query: Dict[int, str] = {}
        
        self.load_all_data()
    
    def load_all_data(self) -> None:
        os.makedirs(self.data_dir, exist_ok=True)
        self.load_multi_keys()
        self.load_details()
        self.load_photo_links()
        self.log_stats()
    
    def load_multi_keys(self) -> None:
        try:
            if os.path.exists(self.multi_keys_file):
                with open(self.multi_keys_file, 'r', encoding='utf-8') as f:
                    for idx, line in enumerate(f):
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue
                        
                        parts = line.split('|')
                        if len(parts) >= 3:
                            villages_str = parts[1].strip()
                            photos = [p.strip() for p in parts[2:] if p.strip()]
                            villages = [v.strip() for v in villages_str.split(',') if v.strip()]
                            
                            for village in villages:
                                self.all_villages.add(village)
                            
                            self.locations.append({
                                'id': idx,
                                'villages': villages,
                                'villages_str': villages_str,
                                'photos': photos
                            })
        except Exception as e:
            logger.error(f"Ошибка загрузки multi_keys: {e}")
    
    def load_details(self) -> None:
        try:
            if os.path.exists(self.details_file):
                with open(self.details_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    entries = content.split('===')
                    
                    for i in range(len(entries) - 1):
                        lines = entries[i].strip().split('\n')
                        photo_num = lines[-1].strip() if lines else ""
                        description = entries[i + 1].strip()
                        
                        if photo_num and description and not photo_num.startswith('#'):
                            self.photo_details[photo_num] = description
        except Exception as e:
            logger.error(f"Ошибка загрузки details: {e}")
    
    def load_photo_links(self) -> None:
        logger.info("🔍 Поиск файлов на Яндекс.Диске...")
        
        all_photos = set()
        for record in self.locations:
            for photo in record['photos']:
                all_photos.add(photo)
        
        logger.info(f"Найдено {len(all_photos)} уникальных снимков для поиска")
        
        for photo in all_photos:
            parts = photo.split('-')
            if len(parts) >= 4:
                square = f"{parts[0]}-{parts[1]}"
                overlay = parts[2]
                frame = parts[3]
            else:
                continue
            
            file_info = yd_client.find_mbtiles_file(
                square=square,
                overlay=overlay,
                frame=frame
            )
            
            if file_info:
                self.photo_links[photo] = file_info['download_link']
                logger.info(f"✅ Найден файл для {photo}: {file_info['name']} (версия {file_info['version']})")
            else:
                logger.warning(f"❌ Файл не найден для {photo}")
    
    def search_by_village(self, query: str) -> List[Dict]:
        if not query:
            return []
        
        query_lower = query.lower().strip()
        found = []
        seen = set()
        
        for record in self.locations:
            for village in record['villages']:
                if query_lower == village.lower() or (len(query_lower) > 2 and query_lower in village.lower()):
                    if record['id'] not in seen:
                        found.append(record)
                        seen.add(record['id'])
                    break
        return found
    
    def get_all_photos(self, records: List[Dict]) -> List[str]:
        photos = []
        for r in records:
            photos.extend(r['photos'])
        unique = []
        for p in photos:
            if p not in unique:
                unique.append(p)
        return unique
    
    def get_all_villages(self, records: List[Dict]) -> List[str]:
        villages = []
        for r in records:
            villages.extend(r['villages'])
        return sorted(list(set(villages)))
    
    def get_photo_details(self, photo_num: str) -> Optional[str]:
        details = self.photo_details.get(photo_num)
        download_link = self.photo_links.get(photo_num)
        
        if details:
            if download_link:
                details += f"\n\n📥 **Скачать MBTiles:**\n{download_link}"
            else:
                details += f"\n\n❌ **Файл MBTiles не найден на Яндекс.Диске**"
        
        return details
    
    def get_all_villages_list(self) -> List[str]:
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
        logger.info(f"📊 Статистика: {len(self.locations)} записей, {len(self.all_villages)} деревень, {len(self.photo_details)} описаний, {len(self.photo_links)} ссылок")

db = PhotosDatabase()

# ========== СОСТОЯНИЯ ==========

class SearchStates(StatesGroup):
    waiting_for_village = State()

# ========== КЛАВИАТУРЫ ==========

def get_main_keyboard() -> ReplyKeyboardMarkup:
    keyboard = [
        [KeyboardButton(text="🔍 ПОИСК"), KeyboardButton(text="📋 СПИСОК ДЕРЕВЕНЬ")],
        [KeyboardButton(text="📖 ИНСТРУКЦИЯ"), KeyboardButton(text="🗺️ КАРТА РЖЕВ")],
        [KeyboardButton(text="🗺️ LOCUS MAPS")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_locus_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📖 Инструкция", callback_data="locus_instruction")],
        [InlineKeyboardButton(text="📥 Скачать Locus Maps", callback_data="locus_download_app")],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])

def back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])

def back_to_photos_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад к списку", callback_data="back_to_photos")],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])

def photos_keyboard(photos: List[str]) -> InlineKeyboardMarkup:
    keyboard = []
    row = []
    for i, p in enumerate(photos):
        row.append(InlineKeyboardButton(text=p, callback_data=f"photo_{p}"))
        if len(row) == 3 or i == len(photos) - 1:
            keyboard.append(row)
            row = []
    keyboard.append([InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# ========== ОБРАБОТЧИКИ КОМАНД ==========

@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    welcome_text = (
        f"👋 **Добро пожаловать, {message.from_user.full_name}!**\n\n"
        f"🛩️ **Бот для поиска аэрофотоснимков Ржевского района**\n\n"
        f"📌 **Что я умею:**\n"
        f"• 🔍 **Поиск снимков** — введите название деревни, и я покажу все связанные с ней аэрофотоснимки\n"
        f"• 📋 **Список деревень** — покажу все деревни, которые есть в базе данных\n"
        f"• 📖 **Инструкция** — подробное описание всех функций бота\n"
        f"• 🗺️ **Карта Ржев** — скачать карту Ржевского района с привязкой к Locus Maps\n"
        f"• 🗺️ **Locus Maps** — инструкция и скачивание приложения\n\n"
        f"👇 **Выберите действие в меню ниже:**"
    )
    
    await message.answer(
        welcome_text,
        parse_mode="Markdown",
        reply_markup=get_main_keyboard()
    )

# ========== ОБРАБОТЧИКИ МЕНЮ ==========

@dp.message(F.text == "🔍 ПОИСК")
async def menu_search(message: types.Message, state: FSMContext):
    await message.answer(
        "🔍 **Режим поиска**\n\n"
        "Введите название деревни, и я найду все связанные с ней снимки.\n\n"
        "📝 **Примеры:** Горбово, Полунино, Дураково, Бельково",
        parse_mode="Markdown"
    )
    await state.set_state(SearchStates.waiting_for_village)

@dp.message(F.text == "📋 СПИСОК ДЕРЕВЕНЬ")
async def menu_villages(message: types.Message):
    villages = db.get_all_villages_list()
    if not villages:
        await message.answer("📭 Список деревень пуст")
        return
    
    chunks = [villages[i:i+20] for i in range(0, len(villages), 20)]
    for i, chunk in enumerate(chunks):
        text = f"📋 **Все деревни в базе данных ({len(villages)} шт.):**\n\n" if i == 0 else ""
        text += "\n".join([f"• {v}" for v in chunk])
        await message.answer(text, parse_mode="Markdown")
    await message.answer(
        "💡 Чтобы найти снимки по деревне, нажмите 🔍 ПОИСК",
        reply_markup=back_keyboard()
    )

@dp.message(F.text == "📖 ИНСТРУКЦИЯ")
async def menu_instruction(message: types.Message):
    instruction_text = (
        "📖 **ПОДРОБНАЯ ИНСТРУКЦИЯ ПО ИСПОЛЬЗОВАНИЮ БОТА**\n\n"
        
        "🔍 **1. ПОИСК СНИМКОВ**\n"
        "• Нажмите кнопку «🔍 ПОИСК» в главном меню\n"
        "• Введите название деревни (например: Горбово, Полунино)\n"
        "• Бот покажет все снимки, где встречается эта деревня\n"
        "• Нажмите на номер снимка для просмотра детальной информации\n"
        "• В деталях снимка будет ссылка на скачивание MBTiles с Яндекс.Диска\n\n"
        
        "📋 **2. СПИСОК ДЕРЕВЕНЬ**\n"
        "• Просмотр всех деревень, которые есть в базе данных\n"
        "• Удобно, если вы не знаете точное название\n\n"
        
        "🗺️ **3. КАРТА РЖЕВСКОГО РАЙОНА**\n"
        "• Скачивание карты Ржевского района с привязкой к Locus Maps\n"
        "• На карте отмечены основные населенные пункты\n\n"
        
        "🗺️ **4. LOCUS MAPS**\n"
        "• Раздел для работы с приложением Locus Maps\n"
        "• **Инструкция** — ссылка на руководство от ПО Сокол\n"
        "• **Скачать Locus Maps** — ссылка на скачивание приложения\n\n"
        
        "🔄 **5. НАВИГАЦИЯ**\n"
        "• После просмотра снимка можно вернуться к списку кнопкой «🔙 Назад к списку»\n"
        "• Кнопка «🏠 В главное меню» доступна на всех этапах\n\n"
        
        "🛩️ **ПРИЯТНОГО ИСПОЛЬЗОВАНИЯ!**"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])
    
    await message.answer(instruction_text, parse_mode="Markdown", reply_markup=keyboard)

@dp.message(F.text == "🗺️ КАРТА РЖЕВ")
async def menu_map(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Скачать карту для Locus", callback_data="download_map")],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])
    await message.answer(
        "🗺️ **Карта Ржевского района для Locus Maps**\n\n"
        "Нажмите кнопку ниже для скачивания карты с Яндекс.Диска.\n\n"
        "📌 Карта с привязкой для приложения Locus Maps.",
        parse_mode="Markdown",
        reply_markup=keyboard
    )

@dp.message(F.text == "🗺️ LOCUS MAPS")
async def menu_locus(message: types.Message):
    await message.answer(
        "🗺️ **Locus Maps**\n\n"
        "Выберите действие:",
        reply_markup=get_locus_keyboard()
    )

# ========== ОБРАБОТЧИКИ LOCUS ==========

@dp.callback_query(lambda c: c.data == "locus_instruction")
async def locus_instruction(callback: CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📖 Скачать инструкцию", callback_data="download_locus_instruction")],
        [InlineKeyboardButton(text="📥 Скачать Locus Maps", callback_data="locus_download_app")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_locus")],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])
    await callback.message.edit_text(
        "📖 **Инструкция по Locus Maps**\n\n"
        "Нажмите кнопку ниже для скачивания инструкции от ПО Сокол с Яндекс.Диска.",
        parse_mode="Markdown",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "locus_download_app")
async def locus_download_app(callback: CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Скачать Locus Maps", callback_data="download_locus_app")],
        [InlineKeyboardButton(text="📖 Инструкция", callback_data="locus_instruction")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_locus")],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])
    await callback.message.edit_text(
        "📥 **Скачать Locus Maps**\n\n"
        "Нажмите кнопку ниже для скачивания приложения Locus Maps с Яндекс.Диска.\n\n"
        "После установки приложения вы можете скачать карту Ржевского района "
        "в разделе «🗺️ КАРТА РЖЕВ».",
        parse_mode="Markdown",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_locus")
async def back_to_locus(callback: CallbackQuery):
    await callback.message.edit_text(
        "🗺️ **Locus Maps**\n\n"
        "Выберите действие:",
        reply_markup=get_locus_keyboard()
    )
    await callback.answer()

# ========== ОБРАБОТЧИКИ СКАЧИВАНИЯ С ЯНДЕКС.ДИСКА ==========

@dp.callback_query(lambda c: c.data == "download_map")
async def download_map(callback: CallbackQuery):
    await callback.message.edit_text(
        "⏳ **Загрузка...**\n\n"
        "Идет подготовка файла карты для скачивания...",
        parse_mode="Markdown"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Скачать карту", url="https://disk.yandex.ru/d/mrxZWJqLuAtnNA")],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])
    
    await callback.message.edit_text(
        "🗺️ **Карта Ржевского района для Locus Maps**\n\n"
        "Файл готов к скачиванию:\n"
        "https://disk.yandex.ru/d/mrxZWJqLuAtnNA\n\n"
        "📌 Нажмите кнопку ниже для скачивания.",
        parse_mode="Markdown",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "download_locus_instruction")
async def download_locus_instruction(callback: CallbackQuery):
    await callback.message.edit_text(
        "⏳ **Загрузка...**\n\n"
        "Идет подготовка инструкции для скачивания...",
        parse_mode="Markdown"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📖 Скачать инструкцию", url="https://disk.yandex.ru/i/sE2Jy99in7MCxw")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="locus_instruction")],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])
    
    await callback.message.edit_text(
        "📖 **Инструкция по Locus Maps**\n\n"
        "Файл готов к скачиванию:\n"
        "https://disk.yandex.ru/i/sE2Jy99in7MCxw\n\n"
        "📌 Нажмите кнопку ниже для скачивания.",
        parse_mode="Markdown",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "download_locus_app")
async def download_locus_app(callback: CallbackQuery):
    await callback.message.edit_text(
        "⏳ **Загрузка...**\n\n"
        "Идет подготовка приложения для скачивания...",
        parse_mode="Markdown"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Скачать Locus Maps", url="https://disk.yandex.ru/d/uUgVGkMoq3WITw")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="locus_download_app")],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])
    
    await callback.message.edit_text(
        "📥 **Скачать Locus Maps**\n\n"
        "Файл готов к скачиванию:\n"
        "https://disk.yandex.ru/d/uUgVGkMoq3WITw\n\n"
        "📌 Нажмите кнопку ниже для скачивания.",
        parse_mode="Markdown",
        reply_markup=keyboard
    )
    await callback.answer()

# ========== ОБРАБОТЧИК ПОИСКА ==========

@dp.message(SearchStates.waiting_for_village)
async def process_search(message: types.Message, state: FSMContext):
    text = message.text
    user_id = message.from_user.id
    
    if not text:
        return
    
    await state.clear()
    db.set_last_query(user_id, text)
    results = db.search_by_village(text)
    
    if results:
        photos = db.get_all_photos(results)
        villages = db.get_all_villages(results)
        villages_text = ', '.join(villages[:15]) + (f" и ещё {len(villages)-15}" if len(villages) > 15 else '')
        
        db.set_last_photos(user_id, photos)
        db.set_last_villages(user_id, villages_text)
        
        photos_list = "\n".join([f"• {p}" for p in photos])
        
        await message.answer(
            f"✅ **Найдено по запросу '{text}':**\n\n"
            f"📍 **Деревни в этом районе:** {villages_text}\n\n"
            f"📸 **Снимки ({len(photos)} шт.):**\n{photos_list}",
            parse_mode="Markdown",
            reply_markup=photos_keyboard(photos)
        )
    else:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔍 Попробовать снова", callback_data="try_again")],
            [InlineKeyboardButton(text="📋 Список деревень", callback_data="show_villages")],
            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
        ])
        await message.answer(
            f"❌ Ничего не найдено для '{text}'",
            reply_markup=keyboard
        )

# ========== ОБРАБОТЧИКИ КНОПОК ==========

@dp.callback_query(lambda c: c.data.startswith('photo_'))
async def process_photo(callback: CallbackQuery):
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
async def back_to_photos(callback: CallbackQuery):
    user_id = callback.from_user.id
    photos = db.get_last_photos(user_id)
    villages = db.get_last_villages(user_id)
    query = db.get_last_query(user_id)
    
    if photos:
        photos_list = "\n".join([f"• {p}" for p in photos])
        await callback.message.edit_text(
            f"✅ **Найдено по запросу '{query}':**\n\n"
            f"📍 **Деревни в этом районе:** {villages}\n\n"
            f"📸 **Снимки ({len(photos)} шт.):**\n{photos_list}",
            parse_mode="Markdown",
            reply_markup=photos_keyboard(photos)
        )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "try_again")
async def try_again(callback: CallbackQuery, state: FSMContext):
    await callback.message.delete()
    await callback.message.answer("🔍 Введите название деревни:")
    await state.set_state(SearchStates.waiting_for_village)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "show_villages")
async def show_villages(callback: CallbackQuery):
    await callback.message.delete()
    villages = db.get_all_villages_list()
    chunks = [villages[i:i+20] for i in range(0, len(villages), 20)]
    for i, chunk in enumerate(chunks):
        text = f"📋 **Все деревни в базе данных ({len(villages)} шт.):**\n\n" if i == 0 else ""
        text += "\n".join([f"• {v}" for v in chunk])
        await callback.message.answer(text, parse_mode="Markdown")
    await callback.message.answer("💡 Нажмите 🔍 ПОИСК для поиска", reply_markup=back_keyboard())
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_main")
async def back_to_main(callback: CallbackQuery, state: FSMContext):
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
    except Exception as e:
        logger.error(f"Ошибка удаления webhook: {e}")

async def main() -> None:
    logger.info("🚀 Бот с поддержкой Яндекс.Диска запускается...")
    db.log_stats()
    logger.info(f"✅ Яндекс.Диск токен загружен")
    await delete_webhook()
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")