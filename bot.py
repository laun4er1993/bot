import asyncio
import logging
import os
import sys
import re
import tempfile
import csv
import io
import time
from typing import Optional, Dict, List, Set, Tuple
import requests

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    FSInputFile
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from shapely.geometry import Polygon, Point
from bs4 import BeautifulSoup

from api_sources import APISourceManager, AVAILABLE_DISTRICTS

# ========== КОНФИГУРАЦИЯ ==========

BOT_TOKEN = os.getenv("BOT_TOKEN")
YANDEX_DISK_TOKEN = os.getenv("YANDEX_DISK_TOKEN")

if not BOT_TOKEN:
    logging.critical("❌ ОШИБКА: BOT_TOKEN не найден!")
    sys.exit(1)

if not YANDEX_DISK_TOKEN:
    logging.critical("❌ ОШИБКА: YANDEX_DISK_TOKEN не найден!")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

storage = MemoryStorage()
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=storage)


# ========== КЛАСС ДЛЯ РАБОТЫ С ЯНДЕКС.ДИСКОМ ==========

class YandexDiskClient:
    """Клиент для работы с Яндекс.Диском"""
    
    def __init__(self, token: str):
        self.token = token
        self.base_url = "https://cloud-api.yandex.net/v1/disk"
        self.headers = {
            "Authorization": f"OAuth {token}",
            "Content-Type": "application/json"
        }
    
    def _request(self, url: str, params: dict = None) -> Optional[Dict]:
        """Выполняет запрос к API"""
        try:
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            logger.error(f"Ошибка запроса: {e}")
            return None
    
    def check_root_access(self) -> bool:
        """Проверяет доступ к диску"""
        data = self._request(f"{self.base_url}/")
        return data is not None
    
    def get_file_download_link(self, file_path: str) -> Optional[str]:
        """Получает ссылку на скачивание файла"""
        if ' ' in os.path.basename(file_path):
            return None
        
        url = f"{self.base_url}/resources/download"
        data = self._request(url, {"path": f"/{file_path}"})
        
        if data and "href" in data:
            return data["href"]
        return None
    
    def find_map_files(self, square: str, overlay: str, frame: str) -> Dict[str, List[Dict]]:
        """Ищет MBTILES и KMZ файлы для снимка"""
        base_folder = "Компьютер DESKTOP-JMVJ4CL/АФС/КаталогПОСокол"
        full_name = f"{square}-{overlay}-{frame}"
        result = {'mbtiles': [], 'kmz': []}
        
        # Проверяем возможные пути
        paths = [
            f"{base_folder}/{square}/{square}-{overlay}/{full_name}",
            f"{base_folder}/{square}/{square}-{overlay}",
            f"{base_folder}/{square}/{full_name}"
        ]
        
        for path in paths:
            files = self._get_files_in_folder(path)
            if files:
                for ext in ['.mbtiles', '.kmz']:
                    versions = self._extract_versions(files, full_name, ext, path)
                    result[ext.replace('.', '')].extend(versions)
        
        for key in result:
            result[key].sort(key=lambda x: x['version'], reverse=True)
        
        return result
    
    def _get_files_in_folder(self, folder_path: str) -> Optional[List[Dict]]:
        """Получает список файлов в папке"""
        url = f"{self.base_url}/resources"
        data = self._request(url, {"path": f"/{folder_path}"})
        
        if data and "_embedded" in data:
            return data["_embedded"].get("items", [])
        return None
    
    def _extract_versions(self, files: List[Dict], base_name: str, ext: str, folder: str) -> List[Dict]:
        """Извлекает информацию о версиях файлов"""
        versions = []
        for f in files:
            name = f['name']
            if not name.startswith(base_name) or not name.endswith(ext):
                continue
            
            version = 0
            match = re.search(rf'{re.escape(base_name)}-(\d+){re.escape(ext)}$', name)
            if match:
                version = int(match.group(1))
            
            link = self.get_file_download_link(f"{folder}/{name}")
            if link:
                size_mb = round(f.get('size', 0) / (1024 * 1024))
                if size_mb >= 10:
                    versions.append({
                        'name': name, 'version': version, 'download_link': link,
                        'size_mb': size_mb
                    })
        return versions


yd_client = YandexDiskClient(YANDEX_DISK_TOKEN)


# ========== КЛАСС ДЛЯ РАБОТЫ С БАЗОЙ НАСЕЛЕННЫХ ПУНКТОВ ==========

class VillageDatabase:
    """База данных населенных пунктов"""
    
    def __init__(self, csv_path: str = "data/villages.csv"):
        self.csv_path = csv_path
        self.villages: List[Dict] = []
        self.villages_by_name: Dict[str, List[Dict]] = {}
        self.stats = {'total': 0, 'with_coords': 0, 'last_update': None, 'source_file': None}
        self._load()
    
    def _load(self):
        """Загружает данные из CSV"""
        if not os.path.exists(self.csv_path):
            self._create_empty()
            return
        
        try:
            with open(self.csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                self.villages = list(reader)
            
            self.villages_by_name.clear()
            with_coords = 0
            
            for v in self.villages:
                name_lower = v['name'].lower()
                if name_lower not in self.villages_by_name:
                    self.villages_by_name[name_lower] = []
                self.villages_by_name[name_lower].append(v)
                
                if v.get('lat') and v.get('lon') and v['lat'].strip() and v['lon'].strip():
                    with_coords += 1
            
            self.stats['total'] = len(self.villages)
            self.stats['with_coords'] = with_coords
            logger.info(f"✅ Загружено {self.stats['total']} населенных пунктов")
            
        except Exception as e:
            logger.error(f"Ошибка загрузки: {e}")
            self._create_empty()
    
    def _create_empty(self):
        """Создает пустую базу"""
        os.makedirs(os.path.dirname(self.csv_path), exist_ok=True)
        with open(self.csv_path, 'w', encoding='utf-8') as f:
            f.write("name,type,lat,lon,district\n")
        self.villages = []
        self.villages_by_name = {}
        self.stats = {'total': 0, 'with_coords': 0, 'last_update': None, 'source_file': None}
    
    def _save(self):
        """Сохраняет данные в CSV"""
        if not self.villages:
            return
        
        with open(self.csv_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['name', 'type', 'lat', 'lon', 'district'])
            writer.writeheader()
            writer.writerows(self.villages)
    
    def search(self, query: str) -> List[Dict]:
        """Ищет населенные пункты по названию"""
        if not query:
            return []
        
        query_lower = query.lower().strip()
        results = []
        seen = set()
        
        # Точное совпадение
        if query_lower in self.villages_by_name:
            for v in self.villages_by_name[query_lower]:
                if v['name'] not in seen:
                    results.append(v)
                    seen.add(v['name'])
        
        # Частичное совпадение
        for name, villages in self.villages_by_name.items():
            if query_lower in name and name != query_lower:
                for v in villages:
                    if v['name'] not in seen:
                        results.append(v)
                        seen.add(v['name'])
        
        return results
    
    def replace_with_catalog(self, csv_content: str, source_filename: str) -> Dict:
        """Полностью заменяет каталог новым"""
        stats = {'loaded': 0, 'with_coords': 0, 'errors': 0}
        
        try:
            reader = csv.DictReader(io.StringIO(csv_content))
            required = ['name', 'type', 'lat', 'lon', 'district']
            
            if not all(f in reader.fieldnames for f in required):
                missing = [f for f in required if f not in reader.fieldnames]
                raise ValueError(f"Отсутствуют поля: {', '.join(missing)}")
            
            new_villages = []
            for row in reader:
                if row['name'].strip():
                    if row['lat'].strip() and row['lon'].strip():
                        try:
                            float(row['lat'])
                            float(row['lon'])
                            stats['with_coords'] += 1
                        except ValueError:
                            stats['errors'] += 1
                            continue
                    new_villages.append(row)
                    stats['loaded'] += 1
            
            self.villages = new_villages
            self._save()
            
            # Обновляем индекс
            self.villages_by_name.clear()
            for v in self.villages:
                name_lower = v['name'].lower()
                if name_lower not in self.villages_by_name:
                    self.villages_by_name[name_lower] = []
                self.villages_by_name[name_lower].append(v)
            
            self.stats['total'] = len(self.villages)
            self.stats['with_coords'] = stats['with_coords']
            self.stats['last_update'] = time.strftime('%Y-%m-%d %H:%M:%S')
            self.stats['source_file'] = source_filename
            
            return stats
            
        except Exception as e:
            logger.error(f"Ошибка замены каталога: {e}")
            raise
    
    def get_stats(self) -> Dict:
        """Возвращает статистику"""
        return self.stats.copy()


village_db = VillageDatabase()


# ========== КЛАСС ДЛЯ РАБОТЫ С KML ==========

class KMLProcessor:
    """Обработчик KML файлов"""
    
    def __init__(self):
        self.logger = logging.getLogger('KMLProcessor')
    
    def process_kml_file(self, kml_path: str, margin_m: float = 100.0) -> List[Dict]:
        """Обрабатывает KML файл и находит НП для каждого снимка"""
        with open(kml_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f.read(), 'xml')
        
        results = []
        for placemark in soup.find_all('Placemark'):
            name_elem = placemark.find('name')
            if not name_elem or not name_elem.text.startswith('Frame-'):
                continue
            
            photo_num = name_elem.text.replace('Frame-', '')
            
            polygon = placemark.find('Polygon')
            if not polygon:
                continue
            
            coords_elem = polygon.find('coordinates')
            if not coords_elem:
                continue
            
            coordinates = self._parse_coords(coords_elem.text.strip())
            if coordinates:
                results.append(self._process_polygon(photo_num, coordinates, margin_m))
        
        return results
    
    def _parse_coords(self, coords_text: str) -> List[Tuple[float, float]]:
        """Парсит координаты из KML"""
        coords = []
        for point in coords_text.strip().split():
            parts = point.split(',')
            if len(parts) >= 2:
                coords.append((float(parts[1]), float(parts[0])))
        return coords
    
    def _process_polygon(self, photo_num: str, coordinates: List[Tuple[float, float]], margin_m: float) -> Dict:
        """Обрабатывает один полигон"""
        margin_deg = margin_m / 111000
        
        lats = [c[0] for c in coordinates]
        lons = [c[1] for c in coordinates]
        bbox = (min(lats) - margin_deg, max(lats) + margin_deg,
                min(lons) - margin_deg, max(lons) + margin_deg)
        
        polygon = Polygon([(lon, lat) for lat, lon in coordinates])
        buffered = polygon.buffer(margin_deg)
        
        villages_in_photo = []
        for v in village_db.villages:
            if not v.get('lat') or not v.get('lon'):
                continue
            try:
                lat = float(v['lat'])
                lon = float(v['lon'])
                if bbox[0] <= lat <= bbox[1] and bbox[2] <= lon <= bbox[3]:
                    if buffered.contains(Point(lon, lat)):
                        villages_in_photo.append(v['name'])
            except:
                continue
        
        return {
            'photo_num': photo_num,
            'villages': villages_in_photo,
            'village_count': len(villages_in_photo)
        }


kml_processor = KMLProcessor()


# ========== КЛАСС ДЛЯ РАБОТЫ С ДАННЫМИ СНИМКОВ ==========

class PhotosDatabase:
    """База данных аэрофотоснимков"""
    
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
        
        self._load()
    
    def _load(self):
        os.makedirs(self.data_dir, exist_ok=True)
        self._load_multi_keys()
        self._load_details()
        
        if yd_client.check_root_access():
            self._load_photo_files()
    
    def _load_multi_keys(self):
        """Загружает связи деревень со снимками"""
        if not os.path.exists(self.multi_keys_file):
            return
        
        with open(self.multi_keys_file, 'r', encoding='utf-8') as f:
            for idx, line in enumerate(f):
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                parts = line.split('|')
                if len(parts) >= 3:
                    villages = [v.strip() for v in parts[1].split(',') if v.strip()]
                    photos = [p.strip() for p in parts[2:] if p.strip()]
                    
                    for v in villages:
                        self.all_villages.add(v)
                    
                    self.locations.append({
                        'id': idx, 'villages': villages, 'photos': photos
                    })
    
    def _load_details(self):
        """Загружает описания снимков"""
        if not os.path.exists(self.details_file):
            return
        
        with open(self.details_file, 'r', encoding='utf-8') as f:
            content = f.read()
            entries = content.split('===')
            
            for i in range(len(entries) - 1):
                lines = entries[i].strip().split('\n')
                photo_num = lines[-1].strip() if lines else ""
                description = entries[i + 1].strip()
                
                if photo_num and description and not photo_num.startswith('#'):
                    self.photo_details[photo_num] = description
    
    def _load_photo_files(self):
        """Загружает ссылки на файлы с Яндекс.Диска"""
        all_photos = set()
        for record in self.locations:
            for photo in record['photos']:
                all_photos.add(photo)
        
        for photo in all_photos:
            parts = photo.split('-')
            if len(parts) >= 3:
                files = yd_client.find_map_files(parts[0], parts[1], parts[2])
                if files['mbtiles'] or files['kmz']:
                    self.photo_files[photo] = files
    
    def search_by_village(self, query: str) -> List[Dict]:
        """Ищет снимки по названию деревни"""
        if not query:
            return []
        
        query_lower = query.lower().strip()
        found = []
        seen = set()
        
        # Поиск в каталоге НП
        villages = village_db.search(query)
        for village in villages:
            for record in self.locations:
                for v in record['villages']:
                    if query_lower in v.lower():
                        if record['id'] not in seen:
                            found.append(record)
                            seen.add(record['id'])
                        break
        
        # Поиск в multi_keys
        for record in self.locations:
            for v in record['villages']:
                if query_lower == v.lower() or (len(query_lower) > 2 and query_lower in v.lower()):
                    if record['id'] not in seen:
                        found.append(record)
                        seen.add(record['id'])
                    break
        
        return found
    
    def get_photo_details(self, photo_num: str) -> Optional[str]:
        """Возвращает описание снимка со ссылками"""
        details = self.photo_details.get(photo_num)
        if not details:
            return None
        
        files = self.photo_files.get(photo_num, {})
        links = []
        
        for file_type, label in [('mbtiles', 'Locus Maps'), ('kmz', 'Google Earth KMZ')]:
            for v in files.get(file_type, []):
                version = f"версия {v['version']}" if v['version'] > 0 else ""
                size = f"({v['size_mb']} МБ)"
                links.append(f"<a href='{v['download_link']}'>📥 Загрузить для {label} {version} {size}</a>")
        
        if links:
            details += "\n\n" + "\n".join(links)
        else:
            details += "\n\n❌ Файлы не найдены на Яндекс.Диске"
        
        return details
    
    def get_all_villages_list(self) -> List[str]:
        """Возвращает список всех деревень"""
        all_villages = set(self.all_villages)
        for v in village_db.villages:
            all_villages.add(v['name'])
        return sorted(all_villages)
    
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


db = PhotosDatabase()


# ========== ФУНКЦИИ ДЛЯ РАБОТЫ С TXT ==========

def generate_txt_from_data(data: List[Dict], filename: str) -> str:
    """Генерирует TXT файл с данными"""
    cleaned = []
    for v in data:
        cleaned.append({
            'name': v.get('name', ''),
            'type': v.get('type', ''),
            'lat': v.get('lat', ''),
            'lon': v.get('lon', ''),
            'district': v.get('district', '')
        })
    
    cleaned.sort(key=lambda x: x['name'])
    
    export_dir = "data/export"
    os.makedirs(export_dir, exist_ok=True)
    file_path = os.path.join(export_dir, filename)
    
    with open(file_path, 'w', encoding='cp1251', newline='') as f:
        f.write('\t'.join(['Название', 'Тип', 'Широта', 'Долгота', 'Район']) + '\n')
        for item in cleaned:
            f.write('\t'.join([item['name'], item['type'], item['lat'], item['lon'], item['district']]) + '\n')
    
    return file_path


# ========== СОСТОЯНИЯ ==========

class SearchStates(StatesGroup):
    waiting_for_village = State()
    waiting_for_kml = State()
    waiting_for_csv_upload = State()
    waiting_for_district_select = State()


# ========== КЛАВИАТУРЫ ==========

def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Главное меню"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔍 ПОИСК"), KeyboardButton(text="📋 СПИСОК ДЕРЕВЕНЬ")],
            [KeyboardButton(text="📖 ИНСТРУКЦИЯ"), KeyboardButton(text="🗺️ КАРТА РЖЕВ")],
            [KeyboardButton(text="🗺️ LOCUS MAPS"), KeyboardButton(text="🔄 ОБРАБОТАТЬ KML")],
            [KeyboardButton(text="⚙️ НАСТРОЙКИ")]
        ],
        resize_keyboard=True
    )


def get_settings_keyboard() -> InlineKeyboardMarkup:
    """Меню настроек"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Загрузить каталог (CSV)", callback_data="update_villages")],
        [InlineKeyboardButton(text="🌐 Загрузить из интернета", callback_data="download_from_web_start")],
        [InlineKeyboardButton(text="📊 Статистика каталога", callback_data="village_stats")],
        [InlineKeyboardButton(text="📤 Скачать каталог (TXT)", callback_data="download_villages_txt")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
    ])


def get_district_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора района"""
    keyboard = []
    for district in AVAILABLE_DISTRICTS:
        keyboard.append([InlineKeyboardButton(text=f"📍 {district} район", callback_data=f"select_district_{district}")])
    keyboard.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_merge_keyboard(district: str) -> InlineKeyboardMarkup:
    """Клавиатура выбора действия с загруженными данными"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Заменить каталог", callback_data=f"merge_replace_{district}")],
        [InlineKeyboardButton(text="➕ Дополнить каталог", callback_data=f"merge_append_{district}")],
        [InlineKeyboardButton(text="📥 Скачать результат (TXT)", callback_data=f"merge_download_{district}")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="back_to_main")]
    ])


def back_keyboard() -> InlineKeyboardMarkup:
    """Кнопка возврата в главное меню"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])


def photos_keyboard(photos: List[str]) -> InlineKeyboardMarkup:
    """Клавиатура со списком снимков"""
    keyboard = []
    row = []
    for p in photos:
        row.append(InlineKeyboardButton(text=p, callback_data=f"photo_{p}"))
        if len(row) == 3:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


# ========== ОБРАБОТЧИКИ ==========

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    """Обработчик команды /start"""
    await message.answer(
        f"👋 <b>Добро пожаловать, {message.from_user.full_name}!</b>\n\n"
        f"🛩️ <b>Бот для поиска аэрофотоснимков</b>\n\n"
        f"📌 <b>Что я умею:</b>\n"
        f"• 🔍 <b>ПОИСК</b> — найдите снимки по названию деревни\n"
        f"• 📋 <b>СПИСОК ДЕРЕВЕНЬ</b> — все доступные деревни\n"
        f"• 📖 <b>ИНСТРУКЦИЯ</b> — помощь по боту\n"
        f"• 🗺️ <b>КАРТА РЖЕВ</b> — скачать карту для Locus Maps\n"
        f"• 🗺️ <b>LOCUS MAPS</b> — инструкция и скачивание приложения\n"
        f"• 🔄 <b>ОБРАБОТАТЬ KML</b> — загрузить каталог снимков\n"
        f"• ⚙️ <b>НАСТРОЙКИ</b> — управление каталогом населенных пунктов\n\n"
        f"👇 <b>Выберите действие:</b>",
        parse_mode="HTML",
        reply_markup=get_main_keyboard()
    )


@dp.message(F.text == "🔍 ПОИСК")
async def menu_search(message: types.Message, state: FSMContext):
    """Обработчик кнопки ПОИСК"""
    await message.answer(
        "🔍 <b>Режим поиска</b>\n\n"
        "Введите название деревни, и я найду все связанные с ней снимки.\n\n"
        "📝 <b>Примеры:</b> Горбово, Полунино, Дураково",
        parse_mode="HTML"
    )
    await state.set_state(SearchStates.waiting_for_village)


@dp.message(F.text == "📋 СПИСОК ДЕРЕВЕНЬ")
async def menu_villages(message: types.Message):
    """Обработчик кнопки СПИСОК ДЕРЕВЕНЬ"""
    villages = db.get_all_villages_list()
    if not villages:
        await message.answer("📭 Список деревень пуст")
        return
    
    chunks = [villages[i:i+25] for i in range(0, len(villages), 25)]
    for i, chunk in enumerate(chunks):
        text = f"📋 <b>Все деревни ({len(villages)} шт.):</b>\n\n" if i == 0 else ""
        text += "\n".join([f"• {v}" for v in chunk])
        await message.answer(text, parse_mode="HTML")
    
    await message.answer(
        "💡 Чтобы найти снимки, нажмите 🔍 ПОИСК",
        reply_markup=back_keyboard()
    )


@dp.message(F.text == "📖 ИНСТРУКЦИЯ")
async def menu_instruction(message: types.Message):
    """Обработчик кнопки ИНСТРУКЦИЯ"""
    await message.answer(
        "📖 <b>ИНСТРУКЦИЯ</b>\n\n"
        "🔍 <b>ПОИСК СНИМКОВ</b>\n"
        "• Нажмите «🔍 ПОИСК»\n"
        "• Введите название деревни\n"
        "• Нажмите на номер снимка для просмотра\n\n"
        "🗺️ <b>LOCUS MAPS</b>\n"
        "• Скачайте приложение из меню «🗺️ LOCUS MAPS»\n"
        "• Загрузите карту и снимки\n"
        "• Откройте MBTILES файл в приложении\n\n"
        "🔄 <b>ОБРАБОТКА KML</b>\n"
        "• Загрузите KML файл с каталогом снимков\n"
        "• Бот найдет населенные пункты в каждом кадре\n\n"
        "⚙️ <b>НАСТРОЙКИ</b>\n"
        "• Загрузка каталога населенных пунктов (CSV)\n"
        "• Автоматическая загрузка из интернета\n"
        "• Просмотр статистики\n\n"
        "🛩️ <b>ПРИЯТНОГО ИСПОЛЬЗОВАНИЯ!</b>",
        parse_mode="HTML",
        reply_markup=back_keyboard()
    )


@dp.message(F.text == "🗺️ КАРТА РЖЕВ")
async def menu_map(message: types.Message):
    """Обработчик кнопки КАРТА РЖЕВ"""
    await message.answer(
        "🗺️ <b>Карта Ржевского района для Locus Maps</b>\n\n"
        "Нажмите кнопку для скачивания:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📥 Скачать карту", url="https://disk.yandex.ru/d/mrxZWJqLuAtnNA")],
            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
        ])
    )


@dp.message(F.text == "🗺️ LOCUS MAPS")
async def menu_locus(message: types.Message):
    """Обработчик кнопки LOCUS MAPS"""
    await message.answer(
        "🗺️ <b>Locus Maps</b>\n\n"
        "Выберите действие:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📖 Инструкция", callback_data="locus_instruction")],
            [InlineKeyboardButton(text="📥 Скачать Locus Maps", callback_data="locus_download")],
            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
        ])
    )


@dp.message(F.text == "🔄 ОБРАБОТАТЬ KML")
async def menu_process_kml(message: types.Message, state: FSMContext):
    """Обработчик кнопки ОБРАБОТАТЬ KML"""
    await message.answer(
        "📤 <b>Загрузите KML файл</b>\n\n"
        "Отправьте мне KML файл с каталогом снимков.\n"
        "После загрузки я найду населенные пункты в каждом кадре.",
        parse_mode="HTML"
    )
    await state.set_state(SearchStates.waiting_for_kml)


@dp.message(F.text == "⚙️ НАСТРОЙКИ")
async def menu_settings(message: types.Message):
    """Обработчик кнопки НАСТРОЙКИ"""
    stats = village_db.get_stats()
    
    text = (
        f"⚙️ <b>Настройки базы населенных пунктов</b>\n\n"
        f"📊 <b>Статистика:</b>\n"
        f"• Всего записей: {stats['total']}\n"
        f"• С координатами: {stats['with_coords']}\n"
    )
    if stats['last_update']:
        text += f"• Обновлено: {stats['last_update']}\n"
    if stats['source_file']:
        text += f"• Источник: {stats['source_file']}\n"
    
    await message.answer(text, parse_mode="HTML", reply_markup=get_settings_keyboard())


# ========== ОБРАБОТЧИКИ CALLBACK ==========

@dp.callback_query(lambda c: c.data == "locus_instruction")
async def locus_instruction(callback: CallbackQuery):
    """Инструкция по Locus Maps"""
    await callback.message.edit_text(
        "📖 <b>Инструкция по Locus Maps</b>\n\n"
        "1. Скачайте приложение Locus Maps\n"
        "2. Скачайте карту Ржевского района\n"
        "3. Скачайте MBTILES файл снимка\n"
        "4. Откройте MBTILES файл в приложении\n\n"
        "📥 <b>Скачать инструкцию:</b>",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📖 Скачать инструкцию", url="https://disk.yandex.ru/i/sE2Jy99in7MCxw")],
            [InlineKeyboardButton(text="📥 Скачать Locus Maps", callback_data="locus_download")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_locus")],
            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
        ])
    )
    await callback.answer()


@dp.callback_query(lambda c: c.data == "locus_download")
async def locus_download(callback: CallbackQuery):
    """Скачивание Locus Maps"""
    await callback.message.edit_text(
        "📥 <b>Скачать Locus Maps</b>\n\n"
        "Нажмите кнопку для скачивания:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📥 Скачать Locus Maps", url="https://disk.yandex.ru/d/uUgVGkMoq3WITw")],
            [InlineKeyboardButton(text="📖 Инструкция", callback_data="locus_instruction")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_locus")],
            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
        ])
    )
    await callback.answer()


@dp.callback_query(lambda c: c.data == "back_to_locus")
async def back_to_locus(callback: CallbackQuery):
    """Возврат в меню Locus Maps"""
    await callback.message.edit_text(
        "🗺️ <b>Locus Maps</b>\n\nВыберите действие:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📖 Инструкция", callback_data="locus_instruction")],
            [InlineKeyboardButton(text="📥 Скачать Locus Maps", callback_data="locus_download")],
            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
        ])
    )
    await callback.answer()


@dp.callback_query(lambda c: c.data == "village_stats")
async def show_stats(callback: CallbackQuery):
    """Показывает статистику каталога"""
    stats = village_db.get_stats()
    
    text = (
        f"📊 <b>Статистика каталога НП</b>\n\n"
        f"• Всего записей: {stats['total']}\n"
        f"• С координатами: {stats['with_coords']}\n"
        f"• Без координат: {stats['total'] - stats['with_coords']}\n"
    )
    if stats['last_update']:
        text += f"• Обновлено: {stats['last_update']}\n"
    if stats['source_file']:
        text += f"• Источник: {stats['source_file']}\n\n"
    
    if village_db.villages:
        text += f"\n📝 <b>Примеры:</b>\n"
        for v in village_db.villages[:10]:
            coords = f"({v['lat']}, {v['lon']})" if v['lat'] and v['lon'] else "(без координат)"
            text += f"• {v['name']} ({v['type']}) {coords}\n"
    
    await callback.message.edit_text(
        text, parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_settings")],
            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
        ])
    )
    await callback.answer()


@dp.callback_query(lambda c: c.data == "update_villages")
async def update_villages_start(callback: CallbackQuery, state: FSMContext):
    """Начало загрузки каталога"""
    await callback.message.edit_text(
        "📤 <b>Загрузка каталога населенных пунктов</b>\n\n"
        "⚠️ ВНИМАНИЕ: Это действие ЗАМЕНИТ текущую базу данных!\n\n"
        "Отправьте CSV файл со структурой:\n"
        "<code>name,type,lat,lon,district</code>\n\n"
        "Пример:\n"
        "<code>Горбово,деревня,56.2345,34.1234,Ржевский</code>\n\n"
        "Поля lat, lon могут быть пустыми.",
        parse_mode="HTML"
    )
    await state.set_state(SearchStates.waiting_for_csv_upload)
    await callback.answer()


@dp.callback_query(lambda c: c.data == "download_villages_txt")
async def download_villages_txt(callback: CallbackQuery):
    """Скачивание каталога в TXT"""
    if not os.path.exists(village_db.csv_path) or village_db.stats['total'] == 0:
        await callback.message.answer("❌ Каталог пуст. Сначала загрузите данные.")
        await callback.answer()
        return
    
    try:
        with open(village_db.csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            data = list(reader)
        
        filename = f"населенные_пункты_{time.strftime('%Y%m%d')}.txt"
        filepath = generate_txt_from_data(data, filename)
        
        await callback.message.answer_document(
            FSInputFile(filepath, filename=filename),
            caption=f"📁 <b>Каталог населенных пунктов</b>\nВсего: {village_db.stats['total']} записей",
            parse_mode="HTML"
        )
        os.unlink(filepath)
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await callback.message.answer("❌ Ошибка при создании файла.")
    
    await callback.answer()


@dp.callback_query(lambda c: c.data == "download_from_web_start")
async def download_from_web_start(callback: CallbackQuery, state: FSMContext):
    """Начало загрузки из интернета"""
    districts_list = "\n".join([f"• {d} район" for d in AVAILABLE_DISTRICTS])
    
    await callback.message.edit_text(
        "🌐 <b>Загрузка данных из интернета</b>\n\n"
        f"Выберите район:\n\n{districts_list}\n\n"
        "<i>Бот выполнит поиск на dic.academic.ru и Wikipedia</i>",
        parse_mode="HTML",
        reply_markup=get_district_keyboard()
    )
    await state.set_state(SearchStates.waiting_for_district_select)
    await callback.answer()


@dp.callback_query(lambda c: c.data.startswith("select_district_"))
async def process_district_select(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора района"""
    district = callback.data.replace("select_district_", "")
    
    await callback.message.edit_text(
        f"⏳ <b>Загрузка данных для {district} района...</b>\n\n"
        f"Это может занять 10-15 минут.\n"
        f"<i>Пожалуйста, подождите...</i>",
        parse_mode="HTML"
    )
    await callback.answer("⏳ Начинаю загрузку...")
    
    try:
        api_manager = APISourceManager()
        villages = await asyncio.wait_for(
            api_manager.fetch_district_data(district),
            timeout=1500.0
        )
        await api_manager.close_session()
        
        if not villages:
            await callback.message.edit_text(
                f"❌ <b>Не удалось загрузить данные для {district} района</b>\n\n"
                f"Попробуйте другой район или загрузите CSV вручную.",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            await callback.answer()
            return
        
        # Сохраняем данные
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        temp_dir = "data/temp"
        os.makedirs(temp_dir, exist_ok=True)
        temp_csv = os.path.join(temp_dir, f"{district}_{timestamp}.csv")
        
        with open(temp_csv, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['name', 'type', 'lat', 'lon', 'district'])
            writer.writeheader()
            writer.writerows(villages)
        
        txt_path = generate_txt_from_data(villages, f"населенные_пункты_{district}_{timestamp}.txt")
        
        await state.update_data(temp_csv=temp_csv, temp_txt=txt_path, villages=villages)
        
        with_coords = sum(1 for v in villages if v.get('lat'))
        
        await callback.message.edit_text(
            f"✅ <b>Данные для {district} района загружены!</b>\n\n"
            f"📊 Всего: {len(villages)} записей\n"
            f"📍 С координатами: {with_coords}\n"
            f"❌ Без координат: {len(villages) - with_coords}\n\n"
            f"<b>Что сделать с этими данными?</b>",
            parse_mode="HTML",
            reply_markup=get_merge_keyboard(district)
        )
        
    except asyncio.TimeoutError:
        await callback.message.edit_text(
            "❌ <b>Превышено время ожидания</b>\n\nПопробуйте позже.",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await callback.message.edit_text(
            f"❌ <b>Ошибка</b>\n\n{str(e)}",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
    finally:
        if 'api_manager' in locals():
            await api_manager.close_session()


@dp.callback_query(lambda c: c.data.startswith("merge_"))
async def process_merge(callback: CallbackQuery, state: FSMContext):
    """Обработка действий с загруженными данными"""
    action, district = callback.data.replace("merge_", "").split("_", 1)
    data = await state.get_data()
    
    temp_csv = data.get('temp_csv')
    villages = data.get('villages', [])
    
    if not temp_csv or not os.path.exists(temp_csv):
        await callback.message.edit_text(
            "❌ Временный файл не найден. Попробуйте заново.",
            reply_markup=back_keyboard()
        )
        await callback.answer()
        return
    
    if action == "download":
        txt_path = data.get('temp_txt')
        if txt_path and os.path.exists(txt_path):
            await callback.message.answer_document(
                FSInputFile(txt_path, filename=os.path.basename(txt_path)),
                caption=f"📁 Данные для {district} района"
            )
        await callback.answer()
        return
    
    elif action == "replace":
        try:
            with open(temp_csv, 'r', encoding='utf-8') as f:
                csv_content = f.read()
            
            stats = village_db.replace_with_catalog(csv_content, f"internet_{district}_catalog.csv")
            
            os.unlink(temp_csv)
            if data.get('temp_txt'):
                os.unlink(data['temp_txt'])
            
            await state.clear()
            
            await callback.message.edit_text(
                f"✅ <b>Каталог заменен данными {district} района!</b>\n\n"
                f"📊 Загружено: {stats['loaded']} записей\n"
                f"📍 С координатами: {stats['with_coords']}",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            await callback.message.edit_text(f"❌ Ошибка: {e}", reply_markup=back_keyboard())
        
    elif action == "append":
        try:
            existing_names = {v['name'] for v in village_db.villages}
            added = 0
            updated = 0
            
            for new_v in villages:
                name = new_v['name']
                if name not in existing_names:
                    village_db.villages.append(new_v)
                    added += 1
                    existing_names.add(name)
                else:
                    for i, existing in enumerate(village_db.villages):
                        if existing['name'] == name:
                            if (not existing.get('lat') and new_v.get('lat')):
                                village_db.villages[i] = new_v
                                updated += 1
                            break
            
            village_db._save()
            village_db.villages_by_name.clear()
            for v in village_db.villages:
                name_lower = v['name'].lower()
                if name_lower not in village_db.villages_by_name:
                    village_db.villages_by_name[name_lower] = []
                village_db.villages_by_name[name_lower].append(v)
            
            with_coords = sum(1 for v in village_db.villages if v.get('lat') and v['lat'].strip())
            village_db.stats['with_coords'] = with_coords
            village_db.stats['total'] = len(village_db.villages)
            village_db.stats['last_update'] = time.strftime('%Y-%m-%d %H:%M:%S')
            village_db.stats['source_file'] = f"appended_{district}_catalog.csv"
            
            os.unlink(temp_csv)
            if data.get('temp_txt'):
                os.unlink(data['temp_txt'])
            
            await state.clear()
            
            await callback.message.edit_text(
                f"✅ <b>Каталог дополнен данными {district} района!</b>\n\n"
                f"📊 Добавлено: {added} новых записей\n"
                f"🔄 Обновлено: {updated} записей\n"
                f"📈 Всего записей: {village_db.stats['total']}\n"
                f"📍 С координатами: {village_db.stats['with_coords']}",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            await callback.message.edit_text(f"❌ Ошибка: {e}", reply_markup=back_keyboard())
    
    await callback.answer()


@dp.callback_query(lambda c: c.data == "back_to_settings")
async def back_to_settings(callback: CallbackQuery):
    """Возврат в настройки"""
    stats = village_db.get_stats()
    
    text = (
        f"⚙️ <b>Настройки базы населенных пунктов</b>\n\n"
        f"📊 <b>Статистика:</b>\n"
        f"• Всего записей: {stats['total']}\n"
        f"• С координатами: {stats['with_coords']}\n"
    )
    if stats['last_update']:
        text += f"• Обновлено: {stats['last_update']}\n"
    
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=get_settings_keyboard())
    await callback.answer()


@dp.callback_query(lambda c: c.data.startswith("photo_"))
async def process_photo(callback: CallbackQuery):
    """Показ деталей снимка"""
    photo = callback.data.replace("photo_", "")
    details = db.get_photo_details(photo)
    
    await callback.message.edit_text(
        details or f"📸 <b>Снимок {photo}</b>\n\n❌ Информация отсутствует",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад к списку", callback_data="back_to_photos")],
            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
        ])
    )
    await callback.answer()


@dp.callback_query(lambda c: c.data == "back_to_photos")
async def back_to_photos(callback: CallbackQuery):
    """Возврат к списку снимков"""
    user_id = callback.from_user.id
    photos = db.get_last_photos(user_id)
    villages = db.get_last_villages(user_id)
    query = db.get_last_query(user_id)
    
    if photos:
        await callback.message.edit_text(
            f"✅ <b>Найдено по запросу '{query}':</b>\n\n"
            f"📍 <b>Деревни:</b> {villages}\n\n"
            f"📸 <b>Снимки ({len(photos)} шт.):</b>\n" + "\n".join([f"• {p}" for p in photos]),
            parse_mode="HTML",
            reply_markup=photos_keyboard(photos)
        )
    await callback.answer()


@dp.callback_query(lambda c: c.data == "back_to_main")
async def back_to_main(callback: CallbackQuery, state: FSMContext):
    """Возврат в главное меню"""
    await state.clear()
    await callback.message.delete()
    await cmd_start(callback.message)
    await callback.answer()


# ========== ОБРАБОТЧИКИ СООБЩЕНИЙ ==========

@dp.message(SearchStates.waiting_for_village)
async def process_search(message: types.Message, state: FSMContext):
    """Поиск по названию деревни"""
    query = message.text
    if not query:
        return
    
    await state.clear()
    user_id = message.from_user.id
    
    db.set_last_query(user_id, query)
    results = db.search_by_village(query)
    
    if results:
        photos = []
        for r in results:
            photos.extend(r['photos'])
        photos = list(dict.fromkeys(photos))
        
        villages = []
        for r in results:
            villages.extend(r['villages'])
        villages = sorted(list(set(villages)))
        villages_text = ', '.join(villages[:15])
        if len(villages) > 15:
            villages_text += f" и ещё {len(villages)-15}"
        
        db.set_last_photos(user_id, photos)
        db.set_last_villages(user_id, villages_text)
        
        await message.answer(
            f"✅ <b>Найдено по запросу '{query}':</b>\n\n"
            f"📍 <b>Деревни:</b> {villages_text}\n\n"
            f"📸 <b>Снимки ({len(photos)} шт.):</b>\n" + "\n".join([f"• {p}" for p in photos]),
            parse_mode="HTML",
            reply_markup=photos_keyboard(photos)
        )
    else:
        await message.answer(
            f"❌ Ничего не найдено для '{query}'",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔍 Попробовать снова", callback_data="try_again")],
                [InlineKeyboardButton(text="📋 Список деревень", callback_data="show_villages")],
                [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
            ])
        )


@dp.message(SearchStates.waiting_for_kml, F.document)
async def process_kml_upload(message: types.Message, state: FSMContext):
    """Обработка загрузки KML"""
    if not message.document.file_name.endswith('.kml'):
        await message.answer("❌ Неверный формат. Отправьте файл .kml")
        await state.clear()
        return
    
    await message.answer("⏳ Обработка файла...")
    
    try:
        file_info = await bot.get_file(message.document.file_id)
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.kml') as tmp:
            await bot.download_file(file_info.file_path, tmp)
            tmp_path = tmp.name
        
        results = kml_processor.process_kml_file(tmp_path)
        os.unlink(tmp_path)
        
        if results:
            total = sum(r.get('village_count', 0) for r in results)
            photos_with_np = sum(1 for r in results if r.get('village_count', 0) > 0)
            
            await message.answer(
                f"✅ <b>Обработка завершена!</b>\n\n"
                f"📊 Снимков: {len(results)}\n"
                f"📍 Снимков с НП: {photos_with_np}\n"
                f"🔗 Всего связей: {total}",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
        else:
            await message.answer("❌ В файле не найдено снимков", reply_markup=back_keyboard())
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await message.answer(f"❌ Ошибка: {e}")
    
    await state.clear()


@dp.message(SearchStates.waiting_for_kml)
async def process_kml_invalid(message: types.Message, state: FSMContext):
    """Неверный ввод при ожидании KML"""
    await message.answer("❌ Отправьте KML файл")
    await state.clear()


@dp.message(SearchStates.waiting_for_csv_upload, F.document)
async def process_csv_upload(message: types.Message, state: FSMContext):
    """Обработка загрузки CSV"""
    if not message.document.file_name.endswith('.csv'):
        await message.answer("❌ Отправьте CSV файл")
        await state.clear()
        return
    
    await message.answer("⏳ Загрузка каталога...")
    
    try:
        file_info = await bot.get_file(message.document.file_id)
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp:
            await bot.download_file(file_info.file_path, tmp)
            tmp_path = tmp.name
        
        with open(tmp_path, 'r', encoding='utf-8') as f:
            csv_content = f.read()
        
        os.unlink(tmp_path)
        
        stats = village_db.replace_with_catalog(csv_content, message.document.file_name)
        
        await message.answer(
            f"✅ <b>Каталог загружен!</b>\n\n"
            f"📊 Загружено: {stats['loaded']} записей\n"
            f"📍 С координатами: {stats['with_coords']}\n"
            f"❌ Ошибок: {stats['errors']}",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await message.answer(f"❌ Ошибка: {e}")
    
    await state.clear()


@dp.message(SearchStates.waiting_for_csv_upload)
async def process_csv_invalid(message: types.Message, state: FSMContext):
    """Неверный ввод при ожидании CSV"""
    await message.answer("❌ Отправьте CSV файл")
    await state.clear()


@dp.callback_query(lambda c: c.data == "try_again")
async def try_again(callback: CallbackQuery, state: FSMContext):
    """Повторный поиск"""
    await callback.message.delete()
    await callback.message.answer("🔍 Введите название деревни:")
    await state.set_state(SearchStates.waiting_for_village)
    await callback.answer()


@dp.callback_query(lambda c: c.data == "show_villages")
async def show_villages(callback: CallbackQuery):
    """Показ списка деревень"""
    await callback.message.delete()
    villages = db.get_all_villages_list()
    
    if not villages:
        await callback.message.answer("📭 Список деревень пуст")
        await callback.answer()
        return
    
    chunks = [villages[i:i+25] for i in range(0, len(villages), 25)]
    for i, chunk in enumerate(chunks):
        text = f"📋 <b>Все деревни ({len(villages)} шт.):</b>\n\n" if i == 0 else ""
        text += "\n".join([f"• {v}" for v in chunk])
        await callback.message.answer(text, parse_mode="HTML")
    
    await callback.message.answer("💡 Нажмите 🔍 ПОИСК", reply_markup=back_keyboard())
    await callback.answer()


# ========== ЗАПУСК ==========

async def delete_webhook() -> None:
    """Удаляет webhook перед запуском polling"""
    try:
        info = await bot.get_webhook_info()
        if info.url:
            await bot.delete_webhook(drop_pending_updates=True)
    except Exception as e:
        logger.error(f"Ошибка удаления webhook: {e}")


async def main() -> None:
    """Запуск бота"""
    logger.info("🚀 Бот запускается...")
    logger.info(f"📊 Загружено локаций: {len(db.locations)}")
    logger.info(f"📊 Уникальных деревень: {len(db.all_villages)}")
    logger.info(f"📊 Описаний снимков: {len(db.photo_details)}")
    logger.info(f"📊 НП в каталоге: {village_db.stats['total']}")
    
    await delete_webhook()
    logger.info("🔄 Запуск polling...")
    await dp.start_polling(bot, skip_updates=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")