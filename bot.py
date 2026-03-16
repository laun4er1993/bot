import asyncio
import logging
import os
import sys
import re
import urllib.parse
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
        
    def _make_request(self, url: str, params: dict = None) -> Optional[Dict]:
        """Выполняет запрос к API с обработкой ошибок"""
        try:
            response = requests.get(url, headers=self.headers, params=params)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                logger.error("  ❌ Ошибка 401: Неверный токен или нет прав доступа")
            elif response.status_code == 404:
                # Не логируем 404 ошибки - они ожидаемы при проверке вариантов
                pass
            else:
                logger.error(f"  ❌ Ошибка {response.status_code}: {response.text}")
            
            return None
        except Exception as e:
            logger.error(f"  ❌ Исключение при запросе: {e}")
            return None
    
    def check_root_access(self) -> bool:
        """Проверяет доступ к корню диска"""
        logger.info("🔍 Проверка доступа к Яндекс.Диску...")
        data = self._make_request(f"{self.base_url}/")
        if data:
            logger.info("✅ Доступ к диску получен")
            return True
        return False
    
    def get_files_in_folder(self, folder_path: str, quiet: bool = False) -> Optional[List[Dict]]:
        """Получает список файлов в папке с возможностью отключить логирование"""
        url = f"{self.base_url}/resources"
        params = {
            "path": f"/{folder_path}",
            "limit": 100
        }
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code == 200:
                data = response.json()
                if "_embedded" in data and "items" in data["_embedded"]:
                    items = data["_embedded"]["items"]
                    if not quiet:
                        logger.info(f"  ✅ Найдено {len(items)} элементов в папке: /{folder_path}")
                    return items
            elif response.status_code == 404:
                if not quiet:
                    logger.info(f"  Папка не существует: /{folder_path}")
                return None
            else:
                if not quiet:
                    logger.error(f"  ❌ Ошибка {response.status_code} при запросе папки: /{folder_path}")
                return None
        except Exception as e:
            if not quiet:
                logger.error(f"  ❌ Ошибка при запросе папки: {e}")
            return None
    
    def folder_exists(self, folder_path: str, quiet: bool = False) -> bool:
        """Проверяет существование папки с возможностью отключить логирование 404 ошибок"""
        url = f"{self.base_url}/resources"
        params = {"path": f"/{folder_path}"}
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code == 200:
                data = response.json()
                exists = data.get("type") == "dir"
                if not quiet:
                    logger.info(f"  Папка {'существует' if exists else 'не существует'}: /{folder_path}")
                return exists
            elif response.status_code == 404:
                if not quiet:
                    logger.info(f"  Папка не существует: /{folder_path}")
                return False
            else:
                if not quiet:
                    logger.error(f"  ❌ Ошибка {response.status_code} при проверке папки: /{folder_path}")
                return False
        except Exception as e:
            if not quiet:
                logger.error(f"  ❌ Исключение при проверке папки: {e}")
            return False
    
    def get_file_download_link(self, file_path: str) -> Optional[str]:
        """Получает ссылку на скачивание файла"""
        if file_path in self.file_cache:
            return self.file_cache[file_path]
        
        url = f"{self.base_url}/resources/download"
        params = {"path": f"/{file_path}"}
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code == 200:
                data = response.json()
                if "href" in data:
                    self.file_cache[file_path] = data["href"]
                    return data["href"]
            return None
        except Exception as e:
            logger.error(f"  ❌ Ошибка получения ссылки: {e}")
            return None
    
    def find_map_files(self, square: str, overlay: str, frame: str) -> Dict[str, List[Dict]]:
        """Ищет MBTILES и KMZ файлы для снимка, возвращает все версии"""
        try:
            # Формируем базовые части
            base_folder = f"CatalogSokol/АФС/КаталогПОСокол"
            square_folder = f"{base_folder}/{square}"
            overlay_folder = f"{square_folder}/{square}-{overlay}"
            full_name = f"{square}-{overlay}-{frame}"
            
            logger.info(f"\n🔍 Поиск файлов для {full_name}:")
            
            result = {
                'mbtiles': [],
                'kmz': []
            }
            
            # ВАРИАНТ 1: Путь с подпапкой наложения и подпапкой полного имени
            subfolder_path = f"{overlay_folder}/{full_name}"
            logger.info(f"  Вариант 1: {subfolder_path}")
            
            if self.folder_exists(subfolder_path, quiet=True):
                files = self.get_files_in_folder(subfolder_path, quiet=True)
                if files:
                    logger.info(f"  ✅ Вариант 1: папка существует")
                    mbtiles_versions = self._extract_file_versions(files, full_name, '.mbtiles', subfolder_path)
                    kmz_versions = self._extract_file_versions(files, full_name, '.kmz', subfolder_path)
                    result['mbtiles'].extend(mbtiles_versions)
                    result['kmz'].extend(kmz_versions)
            
            # ВАРИАНТ 2: Путь без подпапки полного имени
            logger.info(f"  Вариант 2: {overlay_folder}")
            files = self.get_files_in_folder(overlay_folder, quiet=True)
            if files:
                logger.info(f"  ✅ Вариант 2: папка существует")
                mbtiles_versions = self._extract_file_versions(files, full_name, '.mbtiles', overlay_folder)
                kmz_versions = self._extract_file_versions(files, full_name, '.kmz', overlay_folder)
                result['mbtiles'].extend(mbtiles_versions)
                result['kmz'].extend(kmz_versions)
            
            # ВАРИАНТ 3: Путь с полным именем прямо в квадрате
            full_folder_path = f"{square_folder}/{full_name}"
            logger.info(f"  Вариант 3: {full_folder_path}")
            
            if self.folder_exists(full_folder_path, quiet=True):
                files = self.get_files_in_folder(full_folder_path, quiet=True)
                if files:
                    logger.info(f"  ✅ Вариант 3: папка существует")
                    mbtiles_versions = self._extract_file_versions(files, full_name, '.mbtiles', full_folder_path)
                    kmz_versions = self._extract_file_versions(files, full_name, '.kmz', full_folder_path)
                    result['mbtiles'].extend(mbtiles_versions)
                    result['kmz'].extend(kmz_versions)
            
            # Сортируем по версии (от большей к меньшей)
            result['mbtiles'].sort(key=lambda x: x['version'], reverse=True)
            result['kmz'].sort(key=lambda x: x['version'], reverse=True)
            
            if result['mbtiles']:
                logger.info(f"  ✅ Найдено MBTILES: {len(result['mbtiles'])} версий")
            if result['kmz']:
                logger.info(f"  ✅ Найдено KMZ: {len(result['kmz'])} версий")
            if not result['mbtiles'] and not result['kmz']:
                logger.warning(f"  ❌ Файлы не найдены для {full_name}")
            
            return result
            
        except Exception as e:
            logger.error(f"  ❌ Ошибка поиска файлов для {square}-{overlay}-{frame}: {e}")
            return {'mbtiles': [], 'kmz': []}
    
    def _extract_file_versions(self, files: List[Dict], base_name: str, extension: str, folder_path: str) -> List[Dict]:
        """Извлекает информацию о версиях файлов из списка, фильтрует по размеру (>= 10 МБ)"""
        versions = []
        
        for file in files:
            name = file['name']
            if not name.endswith(extension) or not name.startswith(base_name):
                continue
            
            version = 0
            version_match = re.search(rf'{re.escape(base_name)}-(\d+){re.escape(extension)}$', name)
            if version_match:
                version = int(version_match.group(1))
            elif name == f"{base_name}{extension}":
                version = 0
            
            file_path = f"{folder_path}/{name}"
            download_link = self.get_file_download_link(file_path)
            
            if download_link:
                # Форматируем дату из поля created
                created = file.get('created', '')
                if created:
                    date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', created)
                    if date_match:
                        year, month, day = date_match.groups()
                        formatted_date = f"{day}.{month}.{year}"
                    else:
                        formatted_date = "unknown"
                else:
                    formatted_date = "unknown"
                
                # Размер в МБ (целое число)
                size_mb = round(file.get('size', 0) / (1024 * 1024))
                
                # Добавляем только если размер >= 10 МБ
                if size_mb >= 10:
                    versions.append({
                        'name': name,
                        'version': version,
                        'download_link': download_link,
                        'date': formatted_date,
                        'size_mb': size_mb
                    })
                    logger.info(f"    ✅ Добавлен файл: {name} (версия {version}, {formatted_date}, {size_mb} МБ)")
                else:
                    logger.info(f"    ⏭️ Пропущен файл (меньше 10 МБ): {name} ({size_mb} МБ)")
        
        return versions

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
        self.photo_files: Dict[str, Dict] = {}
        
        self.user_last_photos: Dict[int, List[str]] = {}
        self.user_last_villages: Dict[int, str] = {}
        self.user_last_query: Dict[int, str] = {}
        
        self.load_all_data()
    
    def load_all_data(self) -> None:
        os.makedirs(self.data_dir, exist_ok=True)
        self.load_multi_keys()
        self.load_details()
        
        # Проверяем доступ к Яндекс.Диску перед загрузкой ссылок
        if yd_client.check_root_access():
            self.load_photo_files()
        else:
            logger.error("❌ Нет доступа к Яндекс.Диску, пропускаем загрузку ссылок")
        
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
    
    def load_photo_files(self) -> None:
        logger.info("🔍 Поиск файлов на Яндекс.Диске...")
        
        all_photos = set()
        for record in self.locations:
            for photo in record['photos']:
                all_photos.add(photo)
        
        logger.info(f"Найдено {len(all_photos)} уникальных снимков для поиска")
        
        for photo in all_photos:
            logger.info(f"  🔍 Обработка снимка: {photo}")
            
            # Разбиваем номер снимка на части
            parts = photo.split('-')
            
            if len(parts) >= 3:
                square = parts[0]
                overlay = parts[1]
                frame = parts[2]
                
                logger.info(f"    square={square}, overlay={overlay}, frame={frame}")
            else:
                logger.warning(f"    ❌ Неправильный формат: {photo}")
                continue
            
            files_info = yd_client.find_map_files(
                square=square,
                overlay=overlay,
                frame=frame
            )
            
            if files_info['mbtiles'] or files_info['kmz']:
                self.photo_files[photo] = files_info
                if files_info['mbtiles']:
                    logger.info(f"  ✅ Найдено MBTILES для {photo}: {len(files_info['mbtiles'])} версий")
                if files_info['kmz']:
                    logger.info(f"  ✅ Найдено KMZ для {photo}: {len(files_info['kmz'])} версий")
            else:
                logger.warning(f"  ❌ Файлы не найдены для {photo}")
    
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
        logger.info(f"📸 ЗАПРОШЕН СНИМОК: {photo_num}")
        logger.info(f"  Есть в photo_details: {photo_num in self.photo_details}")
        logger.info(f"  Есть в photo_files: {photo_num in self.photo_files}")
        
        details = self.photo_details.get(photo_num)
        files = self.photo_files.get(photo_num, {})
        
        if details:
            download_links = []
            
            # Добавляем MBTILES версии
            if files.get('mbtiles'):
                for v in files['mbtiles']:
                    version_text = f"версия {v['version']}" if v['version'] > 0 else ""
                    date_text = f"от {v['date']}" if v['date'] != "unknown" else ""
                    size_text = f"({v['size_mb']} МБ)"
                    
                    link_text = f"📥 Загрузить для Locus Maps {version_text} {date_text} {size_text}".strip()
                    download_links.append(f"<a href='{v['download_link']}'>{link_text}</a>")
            
            # Добавляем KMZ версии
            if files.get('kmz'):
                for v in files['kmz']:
                    version_text = f"версия {v['version']}" if v['version'] > 0 else ""
                    date_text = f"от {v['date']}" if v['date'] != "unknown" else ""
                    size_text = f"({v['size_mb']} МБ)"
                    
                    link_text = f"📥 Загрузить для Google Earth KMZ {version_text} {date_text} {size_text}".strip()
                    download_links.append(f"<a href='{v['download_link']}'>{link_text}</a>")
            
            if download_links:
                details += "\n\n" + "\n".join(download_links)
                logger.info(f"  ✅ Добавлены ссылки: {len(download_links)}")
            else:
                details += f"\n\n❌ <b>Файлы не найдены на Яндекс.Диске</b>"
                logger.info(f"  ❌ Файлы не найдены")
        else:
            logger.warning(f"  ❌ Нет описания для {photo_num}")
        
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
        logger.info(f"📊 Статистика: {len(self.locations)} записей, {len(self.all_villages)} деревень, {len(self.photo_details)} описаний, {len(self.photo_files)} файловых записей")

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
        f"👋 <b>Добро пожаловать, {message.from_user.full_name}!</b>\n\n"
        f"🛩️ <b>Бот для поиска аэрофотоснимков Ржевского района</b>\n\n"
        f"📌 <b>Что я умею:</b>\n"
        f"• 🔍 <b>Поиск снимков</b> — введите название деревни, и я покажу все связанные с ней аэрофотоснимки\n"
        f"• 📋 <b>Список деревень</b> — покажу все деревни, которые есть в базе данных\n"
        f"• 📖 <b>Инструкция</b> — подробное описание всех функций бота\n"
        f"• 🗺️ <b>Карта Ржев</b> — скачать карту Ржевского района с привязкой к Locus Maps\n"
        f"• 🗺️ <b>Locus Maps</b> — инструкция и скачивание приложения\n\n"
        f"👇 <b>Выберите действие в меню ниже:</b>"
    )
    
    await message.answer(
        welcome_text,
        parse_mode="HTML",
        reply_markup=get_main_keyboard()
    )

# ========== ОБРАБОТЧИКИ МЕНЮ ==========

@dp.message(F.text == "🔍 ПОИСК")
async def menu_search(message: types.Message, state: FSMContext):
    await message.answer(
        "🔍 <b>Режим поиска</b>\n\n"
        "Введите название деревни, и я найду все связанные с ней снимки.\n\n"
        "📝 <b>Примеры:</b> Горбово, Полунино, Дураково, Бельково",
        parse_mode="HTML"
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
        text = f"📋 <b>Все деревни в базе данных ({len(villages)} шт.):</b>\n\n" if i == 0 else ""
        text += "\n".join([f"• {v}" for v in chunk])
        await message.answer(text, parse_mode="HTML")
    await message.answer(
        "💡 Чтобы найти снимки по деревне, нажмите 🔍 ПОИСК",
        reply_markup=back_keyboard()
    )

@dp.message(F.text == "📖 ИНСТРУКЦИЯ")
async def menu_instruction(message: types.Message):
    instruction_text = (
        "📖 <b>ПОДРОБНАЯ ИНСТРУКЦИЯ ПО ИСПОЛЬЗОВАНИЮ БОТА</b>\n\n"
        
        "🔍 <b>1. ПОИСК СНИМКОВ</b>\n"
        "• Нажмите кнопку «🔍 ПОИСК» в главном меню\n"
        "• Введите название деревни (например: Горбово, Полунино)\n"
        "• Бот покажет все снимки, где встречается эта деревня\n"
        "• Нажмите на номер снимка для просмотра детальной информации\n"
        "• В деталях снимка будут ссылки на скачивание MBTiles и KMZ файлов\n\n"
        
        "📋 <b>2. СПИСОК ДЕРЕВЕНЬ</b>\n"
        "• Просмотр всех деревень, которые есть в базе данных\n"
        "• Удобно, если вы не знаете точное название\n\n"
        
        "🗺️ <b>3. КАРТА РЖЕВСКОГО РАЙОНА</b>\n"
        "• Скачивание карты Ржевского района с привязкой к Locus Maps\n"
        "• На карте отмечены основные населенные пункты\n\n"
        
        "🗺️ <b>4. LOCUS MAPS</b>\n"
        "• Раздел для работы с приложением Locus Maps\n"
        "• <b>Инструкция</b> — ссылка на руководство от ПО Сокол\n"
        "• <b>Скачать Locus Maps</b> — ссылка на скачивание приложения\n\n"
        
        "🔄 <b>5. НАВИГАЦИЯ</b>\n"
        "• После просмотра снимка можно вернуться к списку кнопкой «🔙 Назад к списку»\n"
        "• Кнопка «🏠 В главное меню» доступна на всех этапах\n\n"
        
        "🛩️ <b>ПРИЯТНОГО ИСПОЛЬЗОВАНИЯ!</b>"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])
    
    await message.answer(instruction_text, parse_mode="HTML", reply_markup=keyboard)

@dp.message(F.text == "🗺️ КАРТА РЖЕВ")
async def menu_map(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Скачать карту для Locus", callback_data="download_map")],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])
    await message.answer(
        "🗺️ <b>Карта Ржевского района для Locus Maps</b>\n\n"
        "Нажмите кнопку ниже для скачивания карты с Яндекс.Диска.\n\n"
        "📌 Карта с привязкой для приложения Locus Maps.",
        parse_mode="HTML",
        reply_markup=keyboard
    )

@dp.message(F.text == "🗺️ LOCUS MAPS")
async def menu_locus(message: types.Message):
    await message.answer(
        "🗺️ <b>Locus Maps</b>\n\n"
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
        "📖 <b>Инструкция по Locus Maps</b>\n\n"
        "Нажмите кнопку ниже для скачивания инструкции от ПО Сокол с Яндекс.Диска.",
        parse_mode="HTML",
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
        "📥 <b>Скачать Locus Maps</b>\n\n"
        "Нажмите кнопку ниже для скачивания приложения Locus Maps с Яндекс.Диска.\n\n"
        "После установки приложения вы можете скачать карту Ржевского района "
        "в разделе «🗺️ КАРТА РЖЕВ».",
        parse_mode="HTML",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_locus")
async def back_to_locus(callback: CallbackQuery):
    await callback.message.edit_text(
        "🗺️ <b>Locus Maps</b>\n\n"
        "Выберите действие:",
        reply_markup=get_locus_keyboard()
    )
    await callback.answer()

# ========== ОБРАБОТЧИКИ СКАЧИВАНИЯ С ЯНДЕКС.ДИСКА ==========

@dp.callback_query(lambda c: c.data == "download_map")
async def download_map(callback: CallbackQuery):
    await callback.message.edit_text(
        "⏳ <b>Загрузка...</b>\n\n"
        "Идет подготовка файла карты для скачивания...",
        parse_mode="HTML"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Скачать карту", url="https://disk.yandex.ru/d/mrxZWJqLuAtnNA")],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])
    
    await callback.message.edit_text(
        "🗺️ <b>Карта Ржевского района для Locus Maps</b>\n\n"
        "Файл готов к скачиванию:\n"
        "https://disk.yandex.ru/d/mrxZWJqLuAtnNA\n\n"
        "📌 Нажмите кнопку ниже для скачивания.",
        parse_mode="HTML",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "download_locus_instruction")
async def download_locus_instruction(callback: CallbackQuery):
    await callback.message.edit_text(
        "⏳ <b>Загрузка...</b>\n\n"
        "Идет подготовка инструкции для скачивания...",
        parse_mode="HTML"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📖 Скачать инструкцию", url="https://disk.yandex.ru/i/sE2Jy99in7MCxw")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="locus_instruction")],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])
    
    await callback.message.edit_text(
        "📖 <b>Инструкция по Locus Maps</b>\n\n"
        "Файл готов к скачиванию:\n"
        "https://disk.yandex.ru/i/sE2Jy99in7MCxw\n\n"
        "📌 Нажмите кнопку ниже для скачивания.",
        parse_mode="HTML",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "download_locus_app")
async def download_locus_app(callback: CallbackQuery):
    await callback.message.edit_text(
        "⏳ <b>Загрузка...</b>\n\n"
        "Идет подготовка приложения для скачивания...",
        parse_mode="HTML"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Скачать Locus Maps", url="https://disk.yandex.ru/d/uUgVGkMoq3WITw")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="locus_download_app")],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])
    
    await callback.message.edit_text(
        "📥 <b>Скачать Locus Maps</b>\n\n"
        "Файл готов к скачиванию:\n"
        "https://disk.yandex.ru/d/uUgVGkMoq3WITw\n\n"
        "📌 Нажмите кнопку ниже для скачивания.",
        parse_mode="HTML",
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
            f"✅ <b>Найдено по запросу '{text}':</b>\n\n"
            f"📍 <b>Деревни в этом районе:</b> {villages_text}\n\n"
            f"📸 <b>Снимки ({len(photos)} шт.):</b>\n{photos_list}",
            parse_mode="HTML",
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
        text = f"📸 <b>Снимок {photo}</b>\n\n❌ Информация отсутствует"
    
    await callback.message.edit_text(
        text,
        parse_mode="HTML",
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
            f"✅ <b>Найдено по запросу '{query}':</b>\n\n"
            f"📍 <b>Деревни в этом районе:</b> {villages}\n\n"
            f"📸 <b>Снимки ({len(photos)} шт.):</b>\n{photos_list}",
            parse_mode="HTML",
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
        text = f"📋 <b>Все деревни в базе данных ({len(villages)} шт.):</b>\n\n" if i == 0 else ""
        text += "\n".join([f"• {v}" for v in chunk])
        await callback.message.answer(text, parse_mode="HTML")
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
    logger.info("🚀 Бот для поиска аэрофотоснимков запускается...")
    logger.info(f"📊 Загружено локаций: {len(db.locations)}")
    logger.info(f"📊 Уникальных деревень: {len(db.all_villages)}")
    logger.info(f"📊 Описаний снимков: {len(db.photo_details)}")
    logger.info(f"✅ Яндекс.Диск токен загружен")
    await delete_webhook()
    logger.info("🔄 Polling...")
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")