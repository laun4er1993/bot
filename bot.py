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
from aiogram.exceptions import TelegramBadRequest
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


# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========

async def safe_edit_text(message, text: str, parse_mode: str = "HTML", reply_markup=None):
    """Безопасное редактирование сообщения с обработкой ошибок"""
    try:
        await message.edit_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            pass
        elif "message can't be edited" in str(e):
            await message.answer(text, parse_mode=parse_mode, reply_markup=reply_markup)
        else:
            raise


async def safe_answer_callback(callback: CallbackQuery, text: str = None, show_alert: bool = False):
    """Безопасный ответ на callback с обработкой устаревших запросов"""
    try:
        if text:
            await callback.answer(text, show_alert=show_alert)
        else:
            await callback.answer()
    except TelegramBadRequest as e:
        if "query is too old" in str(e) or "timeout expired" in str(e):
            logger.debug(f"Callback expired: {e}")
        else:
            logger.warning(f"Callback answer error: {e}")


async def safe_delete_message(message):
    """Безопасное удаление сообщения"""
    try:
        await message.delete()
    except Exception:
        pass


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
        self.logger = logging.getLogger('YandexDisk')
    
    def _request(self, url: str, params: dict = None) -> Optional[Dict]:
        """Выполняет GET запрос к API Яндекс.Диска"""
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                self.logger.error("  ❌ Ошибка 401: Неверный токен")
            elif response.status_code == 404:
                self.logger.debug(f"  Ресурс не найден: {url}")
            return None
        except Exception as e:
            self.logger.error(f"  ❌ Ошибка запроса: {e}")
            return None
    
    def check_root_access(self) -> bool:
        """Проверяет доступ к корню Яндекс.Диска"""
        self.logger.info("🔍 Проверка доступа к Яндекс.Диску...")
        data = self._request(f"{self.base_url}/")
        if data:
            self.logger.info("✅ Доступ к диску получен")
            return True
        self.logger.error("❌ Нет доступа к Яндекс.Диску")
        return False
    
    def get_file_download_link(self, file_path: str) -> Optional[str]:
        """Получает прямую ссылку для скачивания файла"""
        file_name = os.path.basename(file_path)
        if ' ' in file_name:
            self.logger.warning(f"  ⚠️ Пропускаем файл с пробелом: {file_name}")
            return None
        
        url = f"{self.base_url}/resources/download"
        data = self._request(url, {"path": f"/{file_path}"})
        if data and "href" in data:
            self.logger.debug(f"  ✅ Получена ссылка для {file_name}")
            return data["href"]
        return None
    
    def get_files_in_folder(self, folder_path: str, quiet: bool = False) -> Optional[List[Dict]]:
        """Получает список файлов в папке"""
        url = f"{self.base_url}/resources"
        data = self._request(url, {"path": f"/{folder_path}"})
        if data and "_embedded" in data:
            items = data["_embedded"].get("items", [])
            if not quiet:
                self.logger.info(f"  ✅ Найдено {len(items)} элементов в папке: {folder_path}")
            return items
        if not quiet:
            self.logger.debug(f"  Папка не найдена: {folder_path}")
        return None
    
    def folder_exists(self, folder_path: str, quiet: bool = False) -> bool:
        """Проверяет существование папки"""
        url = f"{self.base_url}/resources"
        data = self._request(url, {"path": f"/{folder_path}"})
        exists = data and data.get("type") == "dir"
        if not quiet:
            self.logger.debug(f"  Папка {'существует' if exists else 'не существует'}: {folder_path}")
        return exists
    
    def find_map_files(self, square: str, overlay: str, frame: str) -> Dict[str, List[Dict]]:
        """Ищет MBTILES и KMZ файлы для указанного снимка"""
        try:
            base_folder = "Компьютер DESKTOP-JMVJ4CL/АФС/КаталогПОСокол"
            square_folder = f"{base_folder}/{square}"
            overlay_folder = f"{square_folder}/{square}-{overlay}"
            full_name = f"{square}-{overlay}-{frame}"
            
            self.logger.info(f"\n🔍 Поиск файлов для {full_name}:")
            result = {'mbtiles': [], 'kmz': []}
            
            paths = [
                f"{overlay_folder}/{full_name}",
                overlay_folder,
                f"{square_folder}/{full_name}"
            ]
            
            for i, path in enumerate(paths, 1):
                self.logger.info(f"  Вариант {i}: {path}")
                if self.folder_exists(path, quiet=True):
                    files = self.get_files_in_folder(path, quiet=True)
                    if files:
                        self.logger.info(f"  ✅ Вариант {i}: папка существует")
                        mbtiles = self._extract_versions(files, full_name, '.mbtiles', path)
                        kmz = self._extract_versions(files, full_name, '.kmz', path)
                        result['mbtiles'].extend(mbtiles)
                        result['kmz'].extend(kmz)
            
            result['mbtiles'].sort(key=lambda x: x['version'], reverse=True)
            result['kmz'].sort(key=lambda x: x['version'], reverse=True)
            
            if result['mbtiles']:
                self.logger.info(f"  ✅ Найдено MBTILES: {len(result['mbtiles'])} версий")
            if result['kmz']:
                self.logger.info(f"  ✅ Найдено KMZ: {len(result['kmz'])} версий")
            if not result['mbtiles'] and not result['kmz']:
                self.logger.warning(f"  ❌ Файлы не найдены для {full_name}")
            
            return result
        except Exception as e:
            self.logger.error(f"  ❌ Ошибка поиска: {e}")
            return {'mbtiles': [], 'kmz': []}
    
    def _extract_versions(self, files: List[Dict], base_name: str, ext: str, folder: str) -> List[Dict]:
        """Извлекает версии файлов из списка"""
        versions = []
        for f in files:
            name = f['name']
            if not name.startswith(base_name) or not name.endswith(ext):
                continue
            if ' ' in name:
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
                    self.logger.info(f"    ✅ Добавлен файл: {name} (версия {version}, {size_mb} МБ)")
        return versions


yd_client = YandexDiskClient(YANDEX_DISK_TOKEN)


# ========== КЛАСС ДЛЯ РАБОТЫ С БАЗОЙ НАСЕЛЕННЫХ ПУНКТОВ ==========

class VillageDatabase:
    """База данных населенных пунктов"""
    
    def __init__(self, csv_path: str = "data/villages.csv"):
        self.csv_path = csv_path
        self.villages: List[Dict] = []
        self.villages_by_name: Dict[str, List[Dict]] = {}
        self.villages_by_district: Dict[str, List[Dict]] = {}
        self.stats = {'total': 0, 'with_coords': 0, 'last_update': None, 'source_file': None}
        self._load()
    
    def _load(self):
        """Загружает базу из CSV файла"""
        if not os.path.exists(self.csv_path):
            self._create_empty()
            return
        try:
            with open(self.csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                self.villages = list(reader)
            
            self.villages_by_name.clear()
            self.villages_by_district.clear()
            with_coords = 0
            for v in self.villages:
                name_lower = v['name'].lower()
                if name_lower not in self.villages_by_name:
                    self.villages_by_name[name_lower] = []
                self.villages_by_name[name_lower].append(v)
                
                district = v.get('district', '')
                if district:
                    if district not in self.villages_by_district:
                        self.villages_by_district[district] = []
                    self.villages_by_district[district].append(v)
                
                if v.get('lat') and v.get('lon') and v['lat'].strip() and v['lon'].strip():
                    with_coords += 1
            
            self.stats['total'] = len(self.villages)
            self.stats['with_coords'] = with_coords
            logger.info(f"✅ Загружено {self.stats['total']} населенных пунктов")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки: {e}")
            self._create_empty()
    
    def _create_empty(self):
        """Создает пустую базу данных"""
        os.makedirs(os.path.dirname(self.csv_path), exist_ok=True)
        with open(self.csv_path, 'w', encoding='utf-8') as f:
            f.write("name,type,lat,lon,district\n")
        self.villages = []
        self.villages_by_name = {}
        self.villages_by_district = {}
        self.stats = {'total': 0, 'with_coords': 0, 'last_update': None, 'source_file': None}
    
    def _save(self):
        """Сохраняет базу в CSV файл"""
        if not self.villages:
            return
        with open(self.csv_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['name', 'type', 'lat', 'lon', 'district'])
            writer.writeheader()
            writer.writerows(self.villages)
    
    def search(self, query: str) -> List[Dict]:
        """Поиск населенных пунктов по названию"""
        if not query:
            return []
        query_lower = query.lower().strip()
        results = []
        seen = set()
        if query_lower in self.villages_by_name:
            for v in self.villages_by_name[query_lower]:
                if v['name'] not in seen:
                    results.append(v)
                    seen.add(v['name'])
        for name, villages in self.villages_by_name.items():
            if query_lower in name and name != query_lower:
                for v in villages:
                    if v['name'] not in seen:
                        results.append(v)
                        seen.add(v['name'])
        return results
    
    def add_village(self, village: Dict) -> Tuple[bool, str]:
        """Добавляет один населенный пункт в каталог с проверкой на дубликат"""
        name = village.get('name', '').strip()
        if not name:
            return False, "Название не указано"
        
        # Проверка на существование
        name_lower = name.lower()
        if name_lower in self.villages_by_name:
            return False, f"Населенный пункт '{name}' уже существует в каталоге"
        
        # Проверка обязательных полей
        if not village.get('type'):
            village['type'] = 'деревня'
        if not village.get('district'):
            village['district'] = ''
        if not village.get('lat'):
            village['lat'] = ''
        if not village.get('lon'):
            village['lon'] = ''
        
        self.villages.append(village)
        
        # Обновляем индексы
        name_lower = village['name'].lower()
        if name_lower not in self.villages_by_name:
            self.villages_by_name[name_lower] = []
        self.villages_by_name[name_lower].append(village)
        
        district = village.get('district', '')
        if district:
            if district not in self.villages_by_district:
                self.villages_by_district[district] = []
            self.villages_by_district[district].append(village)
        
        if village.get('lat') and village.get('lon') and village['lat'].strip() and village['lon'].strip():
            self.stats['with_coords'] += 1
        
        self.stats['total'] = len(self.villages)
        self._save()
        
        return True, f"Населенный пункт '{name}' добавлен"
    
    def add_villages_batch(self, villages: List[Dict]) -> Dict:
        """Добавляет список населенных пунктов с проверкой на дубликаты"""
        stats = {'added': 0, 'duplicates': 0, 'errors': 0, 'villages': []}
        
        for village in villages:
            name = village.get('name', '').strip()
            if not name:
                stats['errors'] += 1
                continue
            
            name_lower = name.lower()
            if name_lower in self.villages_by_name:
                stats['duplicates'] += 1
                continue
            
            if not village.get('type'):
                village['type'] = 'деревня'
            if not village.get('district'):
                village['district'] = ''
            if not village.get('lat'):
                village['lat'] = ''
            if not village.get('lon'):
                village['lon'] = ''
            
            self.villages.append(village)
            stats['villages'].append(village)
            stats['added'] += 1
            
            # Обновляем индексы
            name_lower = village['name'].lower()
            if name_lower not in self.villages_by_name:
                self.villages_by_name[name_lower] = []
            self.villages_by_name[name_lower].append(village)
            
            district = village.get('district', '')
            if district:
                if district not in self.villages_by_district:
                    self.villages_by_district[district] = []
                self.villages_by_district[district].append(village)
        
        self.stats['total'] = len(self.villages)
        self.stats['with_coords'] = sum(1 for v in self.villages if v.get('lat') and v.get('lon') and v['lat'].strip() and v['lon'].strip())
        self._save()
        
        return stats
    
    def remove_district(self, district: str) -> Tuple[int, int]:
        """Удаляет все населенные пункты указанного района"""
        if district not in self.villages_by_district:
            return 0, 0
        
        removed_villages = self.villages_by_district[district]
        removed_count = len(removed_villages)
        
        # Удаляем из основного списка
        self.villages = [v for v in self.villages if v.get('district', '') != district]
        
        # Обновляем индексы
        self.villages_by_name.clear()
        self.villages_by_district.clear()
        
        for v in self.villages:
            name_lower = v['name'].lower()
            if name_lower not in self.villages_by_name:
                self.villages_by_name[name_lower] = []
            self.villages_by_name[name_lower].append(v)
            
            dist = v.get('district', '')
            if dist:
                if dist not in self.villages_by_district:
                    self.villages_by_district[dist] = []
                self.villages_by_district[dist].append(v)
        
        self.stats['total'] = len(self.villages)
        self.stats['with_coords'] = sum(1 for v in self.villages if v.get('lat') and v.get('lon') and v['lat'].strip() and v['lon'].strip())
        self._save()
        
        return removed_count, sum(1 for v in removed_villages if v.get('lat') and v.get('lon') and v['lat'].strip() and v['lon'].strip())
    
    def clear_all(self) -> int:
        """Очищает весь каталог"""
        removed_count = len(self.villages)
        self.villages = []
        self.villages_by_name.clear()
        self.villages_by_district.clear()
        self.stats = {'total': 0, 'with_coords': 0, 'last_update': None, 'source_file': None}
        self._save()
        return removed_count
    
    def get_villages_by_district(self, district: str) -> List[Dict]:
        """Возвращает населенные пункты по району"""
        return self.villages_by_district.get(district, [])
    
    def get_districts(self) -> List[str]:
        """Возвращает список районов в каталоге"""
        return sorted(self.villages_by_district.keys())
    
    def replace_with_catalog(self, csv_content: str, source_filename: str) -> Dict:
        """Заменяет текущий каталог новым из CSV"""
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
            
            self.villages_by_name.clear()
            self.villages_by_district.clear()
            for v in self.villages:
                name_lower = v['name'].lower()
                if name_lower not in self.villages_by_name:
                    self.villages_by_name[name_lower] = []
                self.villages_by_name[name_lower].append(v)
                
                district = v.get('district', '')
                if district:
                    if district not in self.villages_by_district:
                        self.villages_by_district[district] = []
                    self.villages_by_district[district].append(v)
            
            self.stats['total'] = len(self.villages)
            self.stats['with_coords'] = stats['with_coords']
            self.stats['last_update'] = time.strftime('%Y-%m-%d %H:%M:%S')
            self.stats['source_file'] = source_filename
            return stats
        except Exception as e:
            logger.error(f"❌ Ошибка замены каталога: {e}")
            raise
    
    def get_stats(self) -> Dict:
        """Возвращает статистику каталога"""
        return self.stats.copy()


village_db = VillageDatabase()


# ========== КЛАСС ДЛЯ РАБОТЫ С KML ==========

class KMLProcessor:
    """Обработчик KML файлов для поиска населенных пунктов в кадрах"""
    
    def process_kml_file(self, kml_path: str, margin_m: float = 100.0) -> List[Dict]:
        """Обрабатывает KML файл и возвращает список снимков с населенными пунктами"""
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
        """Обрабатывает полигон и находит населенные пункты внутри"""
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
        
        return {'photo_num': photo_num, 'villages': villages_in_photo, 'village_count': len(villages_in_photo)}


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
        """Загружает данные из файлов"""
        os.makedirs(self.data_dir, exist_ok=True)
        self._load_multi_keys()
        self._load_details()
        if yd_client.check_root_access():
            self._load_photo_files()
        self._log_stats()
    
    def _load_multi_keys(self):
        """Загружает связи деревень и снимков"""
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
                    self.locations.append({'id': idx, 'villages': villages, 'photos': photos})
    
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
        """Загружает информацию о файлах снимков с Яндекс.Диска"""
        logger.info("🔍 Поиск файлов на Яндекс.Диске...")
        all_photos = set()
        for record in self.locations:
            for photo in record['photos']:
                all_photos.add(photo)
        logger.info(f"Найдено {len(all_photos)} уникальных снимков")
        
        for photo in all_photos:
            parts = photo.split('-')
            if len(parts) >= 3:
                logger.info(f"  🔍 Обработка снимка: {photo}")
                files = yd_client.find_map_files(parts[0], parts[1], parts[2])
                if files['mbtiles'] or files['kmz']:
                    self.photo_files[photo] = files
                else:
                    logger.warning(f"  ❌ Файлы не найдены для {photo}")
        logger.info(f"✅ Загрузка завершена. Найдено {len(self.photo_files)} снимков")
    
    def _log_stats(self):
        """Выводит статистику базы"""
        logger.info(f"📊 Статистика:")
        logger.info(f"   • Записей в multi_keys: {len(self.locations)}")
        logger.info(f"   • Деревень в multi_keys: {len(self.all_villages)}")
        logger.info(f"   • Описаний снимков: {len(self.photo_details)}")
        logger.info(f"   • Файловых записей: {len(self.photo_files)}")
    
    def search_by_village(self, query: str) -> List[Dict]:
        """Поиск снимков по названию деревни"""
        if not query:
            return []
        query_lower = query.lower().strip()
        found = []
        seen = set()
        
        villages = village_db.search(query)
        for village in villages:
            for record in self.locations:
                for v in record['villages']:
                    if query_lower in v.lower():
                        if record['id'] not in seen:
                            found.append(record)
                            seen.add(record['id'])
                        break
        
        for record in self.locations:
            for v in record['villages']:
                if query_lower == v.lower() or (len(query_lower) > 2 and query_lower in v.lower()):
                    if record['id'] not in seen:
                        found.append(record)
                        seen.add(record['id'])
                    break
        
        return found
    
    def get_photo_details(self, photo_num: str) -> Optional[str]:
        """Возвращает описание снимка и ссылки на файлы"""
        logger.info(f"📸 ЗАПРОШЕН СНИМОК: {photo_num}")
        details = self.photo_details.get(photo_num)
        if not details:
            return None
        
        files = self.photo_files.get(photo_num, {})
        links = []
        for file_type, label in [('mbtiles', '🗺️ Locus Maps'), ('kmz', '🌍 Google Earth KMZ')]:
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
        """Возвращает отсортированный список всех деревень"""
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
    """Генерирует TXT файл с данными о населенных пунктах"""
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
        f.write('Название Тип Широта Долгота Район\n')
        for item in cleaned:
            f.write(f"{item['name']} {item['type']} {item['lat']} {item['lon']} {item['district']}\n")
    return file_path


# ========== СОСТОЯНИЯ ==========

class SearchStates(StatesGroup):
    waiting_for_village = State()
    waiting_for_kml = State()
    waiting_for_csv_upload = State()
    waiting_for_district_select = State()
    waiting_for_add_village_name = State()
    waiting_for_add_village_type = State()
    waiting_for_add_village_coords = State()
    waiting_for_add_village_district = State()


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
        [InlineKeyboardButton(text="📥 Добавить НП вручную", callback_data="add_village_manual")],
        [InlineKeyboardButton(text="📤 Добавить НП из CSV", callback_data="add_villages_csv")],
        [InlineKeyboardButton(text="🌐 Загрузить из интернета", callback_data="download_from_web_start")],
        [InlineKeyboardButton(text="🗑️ Удалить район", callback_data="delete_district_start")],
        [InlineKeyboardButton(text="🗑️ Очистить весь каталог", callback_data="clear_all_catalog")],
        [InlineKeyboardButton(text="📊 Статистика каталога", callback_data="village_stats")],
        [InlineKeyboardButton(text="📤 Скачать каталог (TXT)", callback_data="download_villages_txt")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
    ])


def get_district_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора района"""
    keyboard = []
    for district in AVAILABLE_DISTRICTS[:5]:
        keyboard.append([InlineKeyboardButton(text=f"📍 {district} район", callback_data=f"select_district_{district}")])
    
    remaining_districts = AVAILABLE_DISTRICTS[5:]
    if remaining_districts:
        district_buttons = []
        for district in remaining_districts:
            district_buttons.append([InlineKeyboardButton(text=f"📍 {district} район", callback_data=f"select_district_{district}")])
        keyboard.append([InlineKeyboardButton(text="📋 Ещё районы ▼", callback_data="show_more_districts")])
    
    keyboard.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_settings")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_more_districts_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура со всеми районами"""
    keyboard = []
    for district in AVAILABLE_DISTRICTS[5:]:
        keyboard.append([InlineKeyboardButton(text=f"📍 {district} район", callback_data=f"select_district_{district}")])
    keyboard.append([InlineKeyboardButton(text="🔙 Назад к районам", callback_data="back_to_districts")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_delete_district_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора района для удаления"""
    districts = village_db.get_districts()
    if not districts:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📭 Нет районов для удаления", callback_data="no_op")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_settings")]
        ])
    
    keyboard = []
    for district in districts:
        count = len(village_db.get_villages_by_district(district))
        keyboard.append([InlineKeyboardButton(text=f"🗑️ {district} район ({count} НП)", callback_data=f"delete_district_confirm_{district}")])
    keyboard.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_settings")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_confirm_delete_district_keyboard(district: str) -> InlineKeyboardMarkup:
    """Клавиатура подтверждения удаления района"""
    count = len(village_db.get_villages_by_district(district))
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"✅ Да, удалить {district} район ({count} НП)", callback_data=f"confirm_delete_district_{district}")],
        [InlineKeyboardButton(text="❌ Нет, отмена", callback_data="delete_district_start")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_settings")]
    ])


def get_confirm_clear_all_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура подтверждения очистки всего каталога"""
    total = village_db.stats['total']
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"⚠️ ДА, УДАЛИТЬ ВСЕ {total} ЗАПИСЕЙ", callback_data="confirm_clear_all")],
        [InlineKeyboardButton(text="❌ Нет, отмена", callback_data="back_to_settings")]
    ])


def get_merge_keyboard(district: str) -> InlineKeyboardMarkup:
    """Клавиатура для выбора действия с загруженными данными"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Дополнить каталог", callback_data=f"merge_append_{district}")],
        [InlineKeyboardButton(text="📥 Скачать результат (TXT)", callback_data=f"merge_download_{district}")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="back_to_main")]
    ])


def back_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура возврата в главное меню"""
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
    welcome_text = (
        f"✈️ <b>Добро пожаловать, {message.from_user.full_name}!</b>\n\n"
        f"<b>🛩️ Бот для поиска аэрофотоснимков</b>\n\n"
        f"📌 <b>Основные возможности:</b>\n"
        f"• 🔍 <b>ПОИСК</b> — найдите снимки по названию деревни\n"
        f"• 📋 <b>СПИСОК ДЕРЕВЕНЬ</b> — все доступные населенные пункты\n"
        f"• 📖 <b>ИНСТРУКЦИЯ</b> — подробная помощь по боту\n"
        f"• 🗺️ <b>КАРТА РЖЕВ</b> — скачать карту для Locus Maps\n"
        f"• 🗺️ <b>LOCUS MAPS</b> — инструкция и скачивание приложения\n"
        f"• 🔄 <b>ОБРАБОТАТЬ KML</b> — загрузить каталог снимков\n"
        f"• ⚙️ <b>НАСТРОЙКИ</b> — управление каталогом населенных пунктов\n\n"
        f"👇 <b>Выберите действие:</b>"
    )
    await message.answer(welcome_text, parse_mode="HTML", reply_markup=get_main_keyboard())


@dp.message(F.text == "🔍 ПОИСК")
async def menu_search(message: types.Message, state: FSMContext):
    """Меню поиска снимков"""
    await message.answer(
        "🔍 <b>Режим поиска аэрофотоснимков</b>\n\n"
        "Введите название деревни, и я найду все связанные с ней снимки.\n\n"
        "📝 <b>Примеры:</b> Горбово, Полунино, Дураково\n\n"
        "💡 <i>Можно вводить как полное название, так и его часть</i>",
        parse_mode="HTML"
    )
    await state.set_state(SearchStates.waiting_for_village)


@dp.message(F.text == "📋 СПИСОК ДЕРЕВЕНЬ")
async def menu_villages(message: types.Message):
    """Вывод списка всех деревень"""
    villages = db.get_all_villages_list()
    if not villages:
        await message.answer("📭 Список деревень пуст. Добавьте населенные пункты через ⚙️ НАСТРОЙКИ")
        return
    
    chunks = [villages[i:i+25] for i in range(0, len(villages), 25)]
    for i, chunk in enumerate(chunks):
        text = f"📋 <b>Все деревни ({len(villages)} шт.):</b>\n\n" if i == 0 else ""
        text += "\n".join([f"• {v}" for v in chunk])
        await message.answer(text, parse_mode="HTML")
    
    await message.answer(
        "💡 Чтобы найти снимки, нажмите 🔍 ПОИСК и введите название деревни",
        reply_markup=back_keyboard()
    )


@dp.message(F.text == "📖 ИНСТРУКЦИЯ")
async def menu_instruction(message: types.Message):
    """Вывод подробной инструкции"""
    instruction_text = (
        "📖 <b>ИНСТРУКЦИЯ ПО ИСПОЛЬЗОВАНИЮ БОТА</b>\n\n"
        "🔍 <b>ПОИСК СНИМКОВ</b>\n"
        "• Нажмите «🔍 ПОИСК»\n"
        "• Введите название деревни (можно часть названия)\n"
        "• Нажмите на номер снимка для просмотра описания и скачивания\n\n"
        "🗺️ <b>LOCUS MAPS</b>\n"
        "• Скачайте приложение из меню «🗺️ LOCUS MAPS»\n"
        "• Загрузите карту Ржевского района\n"
        "• Скачайте MBTILES файл снимка\n"
        "• Откройте MBTILES файл в приложении для просмотра\n\n"
        "🔄 <b>ОБРАБОТКА KML</b>\n"
        "• Загрузите KML файл с каталогом снимков\n"
        "• Бот найдет населенные пункты в каждом кадре\n"
        "• Результат покажет статистику по кадрам\n\n"
        "⚙️ <b>НАСТРОЙКИ</b>\n"
        "• Добавление НП вручную или из CSV\n"
        "• Автоматическая загрузка из интернета (dic.academic.ru + Wikipedia)\n"
        "• Удаление районов или очистка всего каталога\n"
        "• Просмотр статистики каталога\n"
        "• Экспорт каталога в TXT\n\n"
        "🛩️ <b>ПРИЯТНОГО ИСПОЛЬЗОВАНИЯ!</b>"
    )
    await message.answer(instruction_text, parse_mode="HTML", reply_markup=back_keyboard())


@dp.message(F.text == "🗺️ КАРТА РЖЕВ")
async def menu_map(message: types.Message):
    """Скачивание карты Ржевского района"""
    await message.answer(
        "🗺️ <b>Карта Ржевского района для Locus Maps</b>\n\n"
        "Нажмите кнопку для скачивания карты:",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📥 Скачать карту", url="https://disk.yandex.ru/d/mrxZWJqLuAtnNA")],
            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
        ])
    )


@dp.message(F.text == "🗺️ LOCUS MAPS")
async def menu_locus(message: types.Message):
    """Меню Locus Maps"""
    await message.answer(
        "🗺️ <b>Locus Maps</b>\n\n"
        "Выберите действие:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📖 Инструкция по Locus", callback_data="locus_instruction")],
            [InlineKeyboardButton(text="📥 Скачать Locus Maps", callback_data="locus_download")],
            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
        ])
    )


@dp.message(F.text == "🔄 ОБРАБОТАТЬ KML")
async def menu_process_kml(message: types.Message, state: FSMContext):
    """Обработка KML файла"""
    await message.answer(
        "📤 <b>Загрузите KML файл</b>\n\n"
        "Отправьте мне KML файл с каталогом снимков.\n"
        "После загрузки я найду населенные пункты в каждом кадре.\n\n"
        "📌 <i>Файл должен содержать Placemark с названиями Frame-XXX</i>",
        parse_mode="HTML"
    )
    await state.set_state(SearchStates.waiting_for_kml)


@dp.message(F.text == "⚙️ НАСТРОЙКИ")
async def menu_settings(message: types.Message):
    """Меню настроек"""
    stats = village_db.get_stats()
    text = (
        f"⚙️ <b>Настройки базы населенных пунктов</b>\n\n"
        f"📊 <b>Статистика каталога:</b>\n"
        f"• Всего записей: {stats['total']}\n"
        f"• С координатами: {stats['with_coords']}\n"
        f"• Без координат: {stats['total'] - stats['with_coords']}\n"
    )
    if stats['last_update']:
        text += f"• Обновлено: {stats['last_update']}\n"
    if stats['source_file']:
        text += f"• Источник: {stats['source_file']}\n"
    
    districts = village_db.get_districts()
    if districts:
        text += f"\n📍 <b>Районы в каталоге:</b>\n"
        for d in districts:
            count = len(village_db.get_villages_by_district(d))
            text += f"• {d} район: {count} НП\n"
    
    await message.answer(text, parse_mode="HTML", reply_markup=get_settings_keyboard())


# ========== ОБРАБОТЧИКИ ДОБАВЛЕНИЯ НП ==========

@dp.callback_query(lambda c: c.data == "add_village_manual")
async def add_village_manual_start(callback: CallbackQuery, state: FSMContext):
    """Начало добавления НП вручную"""
    await safe_edit_text(
        callback.message,
        "📝 <b>Добавление населенного пункта вручную</b>\n\n"
        "Введите название населенного пункта:\n\n"
        "📌 <i>Примеры: Горбово, Полунино, Дураково</i>",
        parse_mode="HTML"
    )
    await state.set_state(SearchStates.waiting_for_add_village_name)
    await safe_answer_callback(callback)


@dp.message(SearchStates.waiting_for_add_village_name)
async def add_village_name(message: types.Message, state: FSMContext):
    """Получение названия НП"""
    name = message.text.strip()
    if len(name) < 2 or len(name) > 50:
        await message.answer("❌ Название должно быть от 2 до 50 символов. Попробуйте снова:")
        return
    
    await state.update_data(village_name=name)
    await message.answer(
        f"📝 <b>Название: {name}</b>\n\n"
        f"Введите тип населенного пункта:\n\n"
        f"📌 <i>Варианты: деревня, село, посёлок, хутор, станция, урочище</i>\n"
        f"<i>Если оставить пустым, будет установлено 'деревня'</i>",
        parse_mode="HTML"
    )
    await state.set_state(SearchStates.waiting_for_add_village_type)


@dp.message(SearchStates.waiting_for_add_village_type)
async def add_village_type(message: types.Message, state: FSMContext):
    """Получение типа НП"""
    type_text = message.text.strip() if message.text else "деревня"
    if not type_text:
        type_text = "деревня"
    
    valid_types = ['деревня', 'село', 'посёлок', 'хутор', 'станция', 'урочище']
    if type_text not in valid_types:
        await message.answer(f"❌ Неверный тип. Доступные: {', '.join(valid_types)}\nПопробуйте снова:")
        return
    
    await state.update_data(village_type=type_text)
    await message.answer(
        f"📝 <b>Тип: {type_text}</b>\n\n"
        f"Введите координаты (широта и долгота) через пробел:\n\n"
        f"📌 <i>Пример: 56.2345 34.1234</i>\n"
        f"<i>Если координаты неизвестны, оставьте пустым</i>",
        parse_mode="HTML"
    )
    await state.set_state(SearchStates.waiting_for_add_village_coords)


@dp.message(SearchStates.waiting_for_add_village_coords)
async def add_village_coords(message: types.Message, state: FSMContext):
    """Получение координат НП"""
    coords_text = message.text.strip()
    lat, lon = "", ""
    
    if coords_text:
        parts = coords_text.split()
        if len(parts) >= 2:
            try:
                lat = str(float(parts[0]))
                lon = str(float(parts[1]))
                if not (-90 <= float(lat) <= 90 and -180 <= float(lon) <= 180):
                    await message.answer("❌ Координаты вне допустимого диапазона. Попробуйте снова:")
                    return
            except ValueError:
                await message.answer("❌ Неверный формат координат. Введите два числа через пробел:\nПример: 56.2345 34.1234")
                return
    
    await state.update_data(village_lat=lat, village_lon=lon)
    await message.answer(
        f"📝 <b>Координаты: {lat if lat else 'не указаны'}, {lon if lon else ''}</b>\n\n"
        f"Введите район (для Ржевского района введите 'Ржевский'):\n\n"
        f"📌 <i>Доступные районы: {', '.join(AVAILABLE_DISTRICTS[:5])} и другие</i>",
        parse_mode="HTML"
    )
    await state.set_state(SearchStates.waiting_for_add_village_district)


@dp.message(SearchStates.waiting_for_add_village_district)
async def add_village_district(message: types.Message, state: FSMContext):
    """Получение района и добавление НП"""
    district = message.text.strip()
    
    # Проверяем, что район существует в списке
    district_normalized = district.replace(" район", "").strip()
    if district_normalized not in AVAILABLE_DISTRICTS:
        await message.answer(
            f"❌ Район '{district}' не найден в списке.\n\n"
            f"Доступные районы: {', '.join(AVAILABLE_DISTRICTS[:10])}...\n"
            f"Введите полное название района (например: Ржевский):"
        )
        return
    
    data = await state.get_data()
    
    village = {
        "name": data['village_name'],
        "type": data['village_type'],
        "lat": data['village_lat'],
        "lon": data['village_lon'],
        "district": district_normalized
    }
    
    success, msg = village_db.add_village(village)
    
    if success:
        await message.answer(
            f"✅ {msg}\n\n"
            f"📊 <b>Данные добавленного НП:</b>\n"
            f"• Название: {village['name']}\n"
            f"• Тип: {village['type']}\n"
            f"• Координаты: {village['lat'] if village['lat'] else 'не указаны'} {village['lon'] if village['lon'] else ''}\n"
            f"• Район: {village['district']}\n\n"
            f"Теперь этот НП доступен для поиска снимков!",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
    else:
        await message.answer(f"❌ {msg}", reply_markup=back_keyboard())
    
    await state.clear()


# ========== ОБРАБОТЧИКИ ДОБАВЛЕНИЯ ИЗ CSV ==========

@dp.callback_query(lambda c: c.data == "add_villages_csv")
async def add_villages_csv_start(callback: CallbackQuery, state: FSMContext):
    """Начало добавления НП из CSV"""
    await safe_edit_text(
        callback.message,
        "📤 <b>Добавление населенных пунктов из CSV</b>\n\n"
        "Отправьте CSV файл со структурой:\n"
        "<code>name,type,lat,lon,district</code>\n\n"
        "📌 <b>Пример строки:</b>\n"
        "<code>Горбово,деревня,56.2345,34.1234,Ржевский</code>\n\n"
        "⚠️ <b>Важно:</b>\n"
        "• Если НП уже существует в каталоге, он будет пропущен\n"
        "• Поля lat, lon могут быть пустыми\n"
        "• Добавятся только новые НП",
        parse_mode="HTML"
    )
    await state.set_state(SearchStates.waiting_for_csv_upload)
    await safe_answer_callback(callback)


# ========== ОБРАБОТЧИКИ УДАЛЕНИЯ ==========

@dp.callback_query(lambda c: c.data == "delete_district_start")
async def delete_district_start(callback: CallbackQuery):
    """Начало удаления района"""
    districts = village_db.get_districts()
    if not districts:
        await safe_edit_text(
            callback.message,
            "📭 В каталоге нет районов для удаления.",
            reply_markup=back_keyboard()
        )
        await safe_answer_callback(callback)
        return
    
    await safe_edit_text(
        callback.message,
        "🗑️ <b>Удаление района</b>\n\n"
        "Выберите район для удаления:",
        parse_mode="HTML",
        reply_markup=get_delete_district_keyboard()
    )
    await safe_answer_callback(callback)


@dp.callback_query(lambda c: c.data.startswith("delete_district_confirm_"))
async def delete_district_confirm(callback: CallbackQuery):
    """Подтверждение удаления района"""
    district = callback.data.replace("delete_district_confirm_", "")
    await safe_edit_text(
        callback.message,
        f"🗑️ <b>Удаление района {district}</b>\n\n"
        f"⚠️ <b>ВНИМАНИЕ!</b> Это действие удалит все населенные пункты района {district} из каталога.\n\n"
        f"Вы уверены?",
        parse_mode="HTML",
        reply_markup=get_confirm_delete_district_keyboard(district)
    )
    await safe_answer_callback(callback)


@dp.callback_query(lambda c: c.data.startswith("confirm_delete_district_"))
async def delete_district_execute(callback: CallbackQuery):
    """Выполнение удаления района"""
    district = callback.data.replace("confirm_delete_district_", "")
    removed, with_coords = village_db.remove_district(district)
    
    await safe_edit_text(
        callback.message,
        f"✅ <b>Район {district} удален!</b>\n\n"
        f"📊 <b>Результат:</b>\n"
        f"• Удалено записей: {removed}\n"
        f"• Из них с координатами: {with_coords}\n\n"
        f"Текущее состояние каталога:\n"
        f"• Всего записей: {village_db.stats['total']}\n"
        f"• С координатами: {village_db.stats['with_coords']}",
        parse_mode="HTML",
        reply_markup=back_keyboard()
    )
    await safe_answer_callback(callback)


@dp.callback_query(lambda c: c.data == "clear_all_catalog")
async def clear_all_catalog_confirm(callback: CallbackQuery):
    """Подтверждение очистки всего каталога"""
    total = village_db.stats['total']
    if total == 0:
        await safe_edit_text(
            callback.message,
            "📭 Каталог уже пуст.",
            reply_markup=back_keyboard()
        )
        await safe_answer_callback(callback)
        return
    
    await safe_edit_text(
        callback.message,
        f"⚠️ <b>ОЧИСТКА ВСЕГО КАТАЛОГА</b>\n\n"
        f"В каталоге находится {total} населенных пунктов.\n\n"
        f"<b>Это действие НЕОБРАТИМО!</b>\n\n"
        f"Вы уверены, что хотите удалить все данные?",
        parse_mode="HTML",
        reply_markup=get_confirm_clear_all_keyboard()
    )
    await safe_answer_callback(callback)


@dp.callback_query(lambda c: c.data == "confirm_clear_all")
async def clear_all_catalog_execute(callback: CallbackQuery):
    """Выполнение очистки всего каталога"""
    removed = village_db.clear_all()
    
    await safe_edit_text(
        callback.message,
        f"✅ <b>Каталог полностью очищен!</b>\n\n"
        f"📊 <b>Результат:</b>\n"
        f"• Удалено записей: {removed}\n\n"
        f"Теперь каталог пуст. Вы можете добавить новые НП через настройки.",
        parse_mode="HTML",
        reply_markup=back_keyboard()
    )
    await safe_answer_callback(callback)


# ========== ОБРАБОТЧИКИ ЗАГРУЗКИ ИЗ ИНТЕРНЕТА ==========

@dp.callback_query(lambda c: c.data == "download_from_web_start")
async def download_from_web_start(callback: CallbackQuery, state: FSMContext):
    """Начало загрузки из интернета - выбор района"""
    await safe_edit_text(
        callback.message,
        "🌐 <b>Загрузка данных из интернета</b>\n\n"
        "Бот выполнит поиск на dic.academic.ru и Wikipedia.\n"
        "Это может занять 10-15 минут.\n\n"
        "<b>Выберите район:</b>",
        parse_mode="HTML",
        reply_markup=get_district_keyboard()
    )
    await state.set_state(SearchStates.waiting_for_district_select)
    await safe_answer_callback(callback)


@dp.callback_query(lambda c: c.data.startswith("select_district_"))
async def process_district_select(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора района и запуск загрузки"""
    district = callback.data.replace("select_district_", "")
    
    await safe_edit_text(
        callback.message,
        f"⏳ <b>Загрузка данных для {district} района...</b>\n\n"
        f"🔍 Выполняется поиск на dic.academic.ru и Wikipedia.\n"
        f"⏱️ Это может занять 10-15 минут.\n"
        f"<i>Пожалуйста, подождите...</i>",
        parse_mode="HTML"
    )
    await safe_answer_callback(callback, f"⏳ Начинаю загрузку для {district} района...")
    
    try:
        api_manager = APISourceManager()
        villages = await asyncio.wait_for(
            api_manager.fetch_district_data(district),
            timeout=1500.0
        )
        await api_manager.close_session()
        
        if not villages:
            await safe_edit_text(
                callback.message,
                f"❌ <b>Не удалось загрузить данные для {district} района</b>\n\n"
                f"Возможные причины:\n"
                f"• Нет данных в источниках\n"
                f"• Проблемы с подключением\n"
                f"• Превышено время ожидания\n\n"
                f"Попробуйте другой район или добавьте CSV вручную.",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            await safe_answer_callback(callback)
            return
        
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
        
        with_coords = sum(1 for v in villages if v.get('lat') and v.get('lon'))
        
        await safe_edit_text(
            callback.message,
            f"✅ <b>Данные для {district} района загружены!</b>\n\n"
            f"📊 <b>Статистика:</b>\n"
            f"• Всего населенных пунктов: {len(villages)}\n"
            f"• С координатами: {with_coords}\n"
            f"• Без координат: {len(villages) - with_coords}\n\n"
            f"<b>Что сделать с этими данными?</b>",
            parse_mode="HTML",
            reply_markup=get_merge_keyboard(district)
        )
        
    except asyncio.TimeoutError:
        await safe_edit_text(
            callback.message,
            "❌ <b>Превышено время ожидания</b>\n\n"
            "Загрузка данных заняла слишком много времени.\n"
            "Попробуйте позже или выберите другой район.",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await safe_edit_text(
            callback.message,
            f"❌ <b>Ошибка при загрузке данных</b>\n\n"
            f"{str(e)}",
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
        await safe_edit_text(
            callback.message,
            "❌ Временный файл не найден. Попробуйте загрузить данные заново.",
            reply_markup=back_keyboard()
        )
        await safe_answer_callback(callback)
        return
    
    if action == "download":
        txt_path = data.get('temp_txt')
        if txt_path and os.path.exists(txt_path):
            await callback.message.answer_document(
                FSInputFile(txt_path, filename=os.path.basename(txt_path)),
                caption=f"📁 Данные для {district} района"
            )
        await safe_answer_callback(callback)
        return
    
    elif action == "append":
        try:
            # Добавляем НП с проверкой на дубликаты
            stats = village_db.add_villages_batch(villages)
            
            os.unlink(temp_csv)
            if data.get('temp_txt'):
                os.unlink(data['temp_txt'])
            
            await state.clear()
            
            await safe_edit_text(
                callback.message,
                f"✅ <b>Каталог дополнен данными {district} района!</b>\n\n"
                f"📊 <b>Результат:</b>\n"
                f"• Добавлено новых записей: {stats['added']}\n"
                f"• Пропущено дубликатов: {stats['duplicates']}\n"
                f"• Ошибок: {stats['errors']}\n\n"
                f"📊 <b>Текущее состояние каталога:</b>\n"
                f"• Всего записей: {village_db.stats['total']}\n"
                f"• С координатами: {village_db.stats['with_coords']}",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            await safe_edit_text(
                callback.message,
                f"❌ Ошибка при дополнении каталога:\n{str(e)}",
                reply_markup=back_keyboard()
            )
    
    await safe_answer_callback(callback)


# ========== ОСТАЛЬНЫЕ ОБРАБОТЧИКИ ==========

@dp.callback_query(lambda c: c.data == "locus_instruction")
async def locus_instruction(callback: CallbackQuery):
    """Инструкция по Locus Maps"""
    await safe_edit_text(
        callback.message,
        "📖 <b>Инструкция по работе с Locus Maps</b>\n\n"
        "1️⃣ Скачайте приложение Locus Maps из магазина приложений\n"
        "2️⃣ Скачайте карту Ржевского района по ссылке ниже\n"
        "3️⃣ Скачайте MBTILES файл нужного снимка\n"
        "4️⃣ Откройте MBTILES файл в приложении Locus Maps\n"
        "5️⃣ Снимок отобразится на карте как дополнительный слой\n\n"
        "📥 <b>Полезные ссылки:</b>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📖 Полная инструкция (PDF)", url="https://disk.yandex.ru/i/sE2Jy99in7MCxw")],
            [InlineKeyboardButton(text="📥 Скачать Locus Maps", callback_data="locus_download")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_locus")],
            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
        ])
    )
    await safe_answer_callback(callback)


@dp.callback_query(lambda c: c.data == "locus_download")
async def locus_download(callback: CallbackQuery):
    """Скачивание Locus Maps"""
    await safe_edit_text(
        callback.message,
        "📥 <b>Скачать Locus Maps</b>\n\n"
        "Нажмите кнопку для скачивания приложения:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📥 Locus Maps (Android)", url="https://disk.yandex.ru/d/uUgVGkMoq3WITw")],
            [InlineKeyboardButton(text="📖 Инструкция", callback_data="locus_instruction")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_locus")],
            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
        ])
    )
    await safe_answer_callback(callback)


@dp.callback_query(lambda c: c.data == "back_to_locus")
async def back_to_locus(callback: CallbackQuery):
    """Возврат в меню Locus Maps"""
    await safe_edit_text(
        callback.message,
        "🗺️ <b>Locus Maps</b>\n\nВыберите действие:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📖 Инструкция", callback_data="locus_instruction")],
            [InlineKeyboardButton(text="📥 Скачать Locus Maps", callback_data="locus_download")],
            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
        ])
    )
    await safe_answer_callback(callback)


@dp.callback_query(lambda c: c.data == "show_more_districts")
async def show_more_districts(callback: CallbackQuery):
    """Показывает все районы в выпадающем списке"""
    await safe_edit_text(
        callback.message,
        "🌐 <b>Выберите район для загрузки</b>\n\n"
        f"Всего доступно районов: {len(AVAILABLE_DISTRICTS)}\n"
        f"Выберите из списка ниже:",
        parse_mode="HTML",
        reply_markup=get_more_districts_keyboard()
    )
    await safe_answer_callback(callback)


@dp.callback_query(lambda c: c.data == "back_to_districts")
async def back_to_districts(callback: CallbackQuery):
    """Возврат к выбору района"""
    await safe_edit_text(
        callback.message,
        "🌐 <b>Выберите район для загрузки</b>\n\n"
        "Выберите район из списка ниже:",
        parse_mode="HTML",
        reply_markup=get_district_keyboard()
    )
    await safe_answer_callback(callback)


@dp.callback_query(lambda c: c.data == "back_to_settings")
async def back_to_settings(callback: CallbackQuery):
    """Возврат в меню настроек"""
    stats = village_db.get_stats()
    text = (
        f"⚙️ <b>Настройки базы населенных пунктов</b>\n\n"
        f"📊 <b>Статистика:</b>\n"
        f"• Всего записей: {stats['total']}\n"
        f"• С координатами: {stats['with_coords']}\n"
    )
    if stats['last_update']:
        text += f"• Обновлено: {stats['last_update']}\n"
    
    districts = village_db.get_districts()
    if districts:
        text += f"\n📍 <b>Районы в каталоге:</b>\n"
        for d in districts:
            count = len(village_db.get_villages_by_district(d))
            text += f"• {d} район: {count} НП\n"
    
    await safe_edit_text(
        callback.message,
        text,
        parse_mode="HTML",
        reply_markup=get_settings_keyboard()
    )
    await safe_answer_callback(callback)


@dp.callback_query(lambda c: c.data == "village_stats")
async def show_stats(callback: CallbackQuery):
    """Показывает статистику каталога"""
    stats = village_db.get_stats()
    text = (
        f"📊 <b>Статистика каталога населенных пунктов</b>\n\n"
        f"• Всего записей: {stats['total']}\n"
        f"• С координатами: {stats['with_coords']}\n"
        f"• Без координат: {stats['total'] - stats['with_coords']}\n"
    )
    if stats['last_update']:
        text += f"• Обновлено: {stats['last_update']}\n"
    if stats['source_file']:
        text += f"• Источник: {stats['source_file']}\n\n"
    
    districts = village_db.get_districts()
    if districts:
        text += f"📍 <b>Районы в каталоге:</b>\n"
        for d in districts:
            count = len(village_db.get_villages_by_district(d))
            with_coords = sum(1 for v in village_db.get_villages_by_district(d) if v.get('lat') and v.get('lon') and v['lat'].strip() and v['lon'].strip())
            text += f"• {d} район: {count} НП (из них с координатами: {with_coords})\n"
    
    if village_db.villages:
        text += f"\n📝 <b>Примеры записей (первые 10):</b>\n"
        for v in village_db.villages[:10]:
            coords = f"({v['lat']}, {v['lon']})" if v['lat'] and v['lon'] else "(без координат)"
            text += f"• {v['name']} ({v['type']}) - {v['district']} район {coords}\n"
    
    await safe_edit_text(
        callback.message,
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_settings")],
            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
        ])
    )
    await safe_answer_callback(callback)


@dp.callback_query(lambda c: c.data == "download_villages_txt")
async def download_villages_txt(callback: CallbackQuery):
    """Экспорт каталога в TXT"""
    if not village_db.villages:
        await callback.message.answer("❌ Каталог пуст. Сначала добавьте данные.")
        await safe_answer_callback(callback)
        return
    
    try:
        filename = f"населенные_пункты_{time.strftime('%Y%m%d_%H%M%S')}.txt"
        filepath = generate_txt_from_data(village_db.villages, filename)
        
        await callback.message.answer_document(
            FSInputFile(filepath, filename=filename),
            caption=f"📁 <b>Каталог населенных пунктов</b>\nВсего: {village_db.stats['total']} записей\nС координатами: {village_db.stats['with_coords']}",
            parse_mode="HTML"
        )
        os.unlink(filepath)
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await callback.message.answer("❌ Ошибка при создании файла.")
    
    await safe_answer_callback(callback)


@dp.callback_query(lambda c: c.data == "no_op")
async def no_op(callback: CallbackQuery):
    """Пустой обработчик для кнопки без действия"""
    await safe_answer_callback(callback)


@dp.callback_query(lambda c: c.data.startswith("photo_"))
async def process_photo(callback: CallbackQuery):
    """Показ информации о снимке"""
    photo = callback.data.replace("photo_", "")
    details = db.get_photo_details(photo)
    
    await safe_edit_text(
        callback.message,
        details or f"📸 <b>Снимок {photo}</b>\n\n❌ Информация отсутствует",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 Назад к списку", callback_data="back_to_photos")],
            [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
        ])
    )
    await safe_answer_callback(callback)


@dp.callback_query(lambda c: c.data == "back_to_photos")
async def back_to_photos(callback: CallbackQuery):
    """Возврат к списку снимков"""
    user_id = callback.from_user.id
    photos = db.get_last_photos(user_id)
    villages = db.get_last_villages(user_id)
    query = db.get_last_query(user_id)
    
    if photos:
        await safe_edit_text(
            callback.message,
            f"✅ <b>Найдено по запросу '{query}':</b>\n\n"
            f"📍 <b>Деревни:</b> {villages}\n\n"
            f"📸 <b>Снимки ({len(photos)} шт.):</b>\n" + "\n".join([f"• {p}" for p in photos]),
            parse_mode="HTML",
            reply_markup=photos_keyboard(photos)
        )
    await safe_answer_callback(callback)


@dp.callback_query(lambda c: c.data == "back_to_main")
async def back_to_main(callback: CallbackQuery, state: FSMContext):
    """Возврат в главное меню"""
    await state.clear()
    await safe_delete_message(callback.message)
    await cmd_start(callback.message)
    await safe_answer_callback(callback)


# ========== ОБРАБОТЧИКИ СООБЩЕНИЙ ==========

@dp.message(SearchStates.waiting_for_village)
async def process_search(message: types.Message, state: FSMContext):
    """Поиск снимков по названию деревни"""
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
            f"📍 <b>Населенные пункты:</b> {villages_text}\n\n"
            f"📸 <b>Снимки ({len(photos)} шт.):</b>\n" + "\n".join([f"• {p}" for p in photos]),
            parse_mode="HTML",
            reply_markup=photos_keyboard(photos)
        )
    else:
        await message.answer(
            f"❌ <b>Ничего не найдено для '{query}'</b>\n\n"
            f"Попробуйте:\n"
            f"• Ввести полное название деревни\n"
            f"• Проверить правильность написания\n"
            f"• Посмотреть список всех деревень в меню",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔍 Попробовать снова", callback_data="try_again")],
                [InlineKeyboardButton(text="📋 Список деревень", callback_data="show_villages")],
                [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
            ])
        )


@dp.message(SearchStates.waiting_for_kml, F.document)
async def process_kml_upload(message: types.Message, state: FSMContext):
    """Обработка загруженного KML файла"""
    if not message.document.file_name.endswith('.kml'):
        await message.answer("❌ Неверный формат. Отправьте файл с расширением .kml")
        await state.clear()
        return
    
    await message.answer("⏳ Обработка файла... Это может занять несколько секунд.")
    
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
                f"✅ <b>Обработка KML завершена!</b>\n\n"
                f"📊 <b>Статистика:</b>\n"
                f"• Всего снимков: {len(results)}\n"
                f"• Снимков с населенными пунктами: {photos_with_np}\n"
                f"• Всего связей (НП в кадрах): {total}\n\n"
                f"<i>Результат сохранен в памяти бота</i>",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
        else:
            await message.answer("❌ В KML файле не найдено снимков с названиями Frame-XXX", reply_markup=back_keyboard())
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await message.answer(f"❌ Ошибка при обработке KML:\n{str(e)}")
    
    await state.clear()


@dp.message(SearchStates.waiting_for_kml)
async def process_kml_invalid(message: types.Message, state: FSMContext):
    """Обработка неверного ввода в режиме ожидания KML"""
    await message.answer("❌ Отправьте KML файл (с расширением .kml)")
    await state.clear()


@dp.message(SearchStates.waiting_for_csv_upload, F.document)
async def process_csv_upload(message: types.Message, state: FSMContext):
    """Обработка загруженного CSV для добавления НП"""
    if not message.document.file_name.endswith('.csv'):
        await message.answer("❌ Отправьте CSV файл (с расширением .csv)")
        await state.clear()
        return
    
    await message.answer("⏳ Загрузка и обработка файла...")
    
    try:
        file_info = await bot.get_file(message.document.file_id)
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp:
            await bot.download_file(file_info.file_path, tmp)
            tmp_path = tmp.name
        
        with open(tmp_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            villages = list(reader)
        os.unlink(tmp_path)
        
        stats = village_db.add_villages_batch(villages)
        
        await message.answer(
            f"✅ <b>Обработка CSV завершена!</b>\n\n"
            f"📊 <b>Результат:</b>\n"
            f"• Добавлено новых записей: {stats['added']}\n"
            f"• Пропущено дубликатов: {stats['duplicates']}\n"
            f"• Ошибок: {stats['errors']}\n\n"
            f"📊 <b>Текущее состояние каталога:</b>\n"
            f"• Всего записей: {village_db.stats['total']}\n"
            f"• С координатами: {village_db.stats['with_coords']}",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await message.answer(f"❌ Ошибка при загрузке CSV:\n{str(e)}")
    
    await state.clear()


@dp.message(SearchStates.waiting_for_csv_upload)
async def process_csv_invalid(message: types.Message, state: FSMContext):
    """Обработка неверного ввода в режиме ожидания CSV"""
    await message.answer("❌ Отправьте CSV файл с расширением .csv")
    await state.clear()


@dp.callback_query(lambda c: c.data == "try_again")
async def try_again(callback: CallbackQuery, state: FSMContext):
    """Повторный поиск"""
    await safe_delete_message(callback.message)
    await callback.message.answer("🔍 Введите название деревни:")
    await state.set_state(SearchStates.waiting_for_village)
    await safe_answer_callback(callback)


@dp.callback_query(lambda c: c.data == "show_villages")
async def show_villages(callback: CallbackQuery):
    """Показ списка всех деревень"""
    await safe_delete_message(callback.message)
    
    villages = db.get_all_villages_list()
    if not villages:
        await callback.message.answer("📭 Список деревень пуст. Добавьте населенные пункты через ⚙️ НАСТРОЙКИ")
        await safe_answer_callback(callback)
        return
    
    chunks = [villages[i:i+25] for i in range(0, len(villages), 25)]
    for i, chunk in enumerate(chunks):
        text = f"📋 <b>Все деревни ({len(villages)} шт.):</b>\n\n" if i == 0 else ""
        text += "\n".join([f"• {v}" for v in chunk])
        await callback.message.answer(text, parse_mode="HTML")
    
    await callback.message.answer("💡 Нажмите 🔍 ПОИСК и введите название деревни", reply_markup=back_keyboard())
    await safe_answer_callback(callback)


# ========== ЗАПУСК ==========

async def delete_webhook() -> None:
    """Удаляет вебхук перед запуском polling"""
    try:
        info = await bot.get_webhook_info()
        if info.url:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("✅ Webhook удален")
    except Exception as e:
        logger.error(f"Ошибка удаления webhook: {e}")


async def main() -> None:
    """Главная функция запуска бота"""
    logger.info("🚀 Запуск бота...")
    logger.info(f"📊 Статистика загрузки:")
    logger.info(f"   • Локаций (связей): {len(db.locations)}")
    logger.info(f"   • Уникальных деревень: {len(db.all_villages)}")
    logger.info(f"   • Описаний снимков: {len(db.photo_details)}")
    logger.info(f"   • Населенных пунктов в каталоге: {village_db.stats['total']}")
    
    await delete_webhook()
    logger.info("🔄 Запуск polling...")
    await dp.start_polling(bot, skip_updates=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")