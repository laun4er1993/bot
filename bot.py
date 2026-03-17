import asyncio
import logging
import os
import sys
import re
import urllib.parse
import zipfile
import tempfile
import json
import time
from typing import Optional, Dict, List, Set, Tuple, Any, Union
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
from shapely.geometry import Polygon, Point
from bs4 import BeautifulSoup

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
        
    def _make_request(self, url: str, params: dict = None, method: str = "GET") -> Optional[Dict]:
        """Выполняет запрос к API с обработкой ошибок"""
        try:
            if method == "GET":
                response = requests.get(url, headers=self.headers, params=params)
            elif method == "PUT":
                response = requests.put(url, headers=self.headers, params=params)
            elif method == "POST":
                response = requests.post(url, headers=self.headers, json=params)
            
            if response.status_code in [200, 201, 202]:
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
    
    def get_file_info(self, file_path: str) -> Optional[Dict]:
        """Получает информацию о файле, включая дату создания"""
        url = f"{self.base_url}/resources"
        params = {"path": f"/{file_path}"}
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            logger.error(f"  ❌ Ошибка получения информации о файле {file_path}: {e}")
            return None
    
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
        """
        Получает НОВУЮ временную ссылку на скачивание файла через API.
        Каждый раз создается свежая ссылка.
        :param file_path: путь к файлу
        :return: временная ссылка на скачивание
        """
        # Извлекаем только имя файла из пути
        file_name = os.path.basename(file_path)
        
        # Проверяем, есть ли пробелы только в имени файла
        if ' ' in file_name:
            logger.warning(f"  ⚠️ Пропускаем файл с пробелом в имени: {file_name}")
            return None
        
        url = f"{self.base_url}/resources/download"
        params = {"path": f"/{file_path}"}
        
        try:
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code == 200:
                data = response.json()
                if "href" in data:
                    download_link = data["href"]
                    logger.info(f"  ✅ Получена новая временная ссылка для {file_name}")
                    return download_link
                else:
                    logger.error(f"  ❌ В ответе нет href: {data}")
            else:
                logger.error(f"  ❌ Ошибка получения ссылки: {response.status_code} - {response.text}")
            return None
        except Exception as e:
            logger.error(f"  ❌ Ошибка при запросе ссылки: {e}")
            return None
    
    def find_map_files(self, square: str, overlay: str, frame: str) -> Dict[str, List[Dict]]:
        """Ищет MBTILES и KMZ файлы для снимка, возвращает все версии"""
        try:
            # Формируем базовые части с новым путем
            base_folder = f"Компьютер DESKTOP-JMVJ4CL/АФС/КаталогПОСокол"
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
            
            # Проверяем только имя файла на наличие пробелов
            if ' ' in name:
                logger.info(f"    ⏭️ Пропущен файл с пробелом в имени: {name}")
                continue
            
            version = 0
            version_match = re.search(rf'{re.escape(base_name)}-(\d+){re.escape(extension)}$', name)
            if version_match:
                version = int(version_match.group(1))
            elif name == f"{base_name}{extension}":
                version = 0
            
            file_path = f"{folder_path}/{name}"
            
            # Получаем НОВУЮ временную ссылку
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
            else:
                logger.info(f"    ⏭️ Пропущен файл (не удалось получить ссылку): {name}")
        
        return versions

# Инициализация клиента Яндекс.Диска
yd_client = YandexDiskClient(YANDEX_DISK_TOKEN)

# ========== КЛАСС ДЛЯ ОБРАБОТКИ KML ФАЙЛОВ ==========

class KMLProcessor:
    def __init__(self, nominatim_endpoint: str = "https://nominatim.openstreetmap.org/search"):
        """
        Инициализация процессора KML файлов
        :param nominatim_endpoint: API endpoint для Nominatim
        """
        self.nominatim_endpoint = nominatim_endpoint
        self.user_agent = "WW2AerialPhotoBot/1.0 (your_email@example.com)"
        self.results_file = "data/kml_processed_results.json"
        self.villages_file = "data/photo_villages.json"
        self.log_file = "data/kml_processor.log"
        
        # Настройка отдельного логгера для KML процессора
        self.logger = logging.getLogger('KMLProcessor')
        self.logger.setLevel(logging.INFO)
        
        # Создаем обработчик для файла
        fh = logging.FileHandler(self.log_file, encoding='utf-8')
        fh.setLevel(logging.INFO)
        
        # Создаем форматтер
        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')
        fh.setFormatter(formatter)
        
        # Добавляем обработчик к логгеру
        self.logger.addHandler(fh)
        
        # Загружаем существующие данные
        self.load_results()
        self.load_villages_data()
        
    def load_results(self):
        """Загружает результаты предыдущих обработок"""
        if os.path.exists(self.results_file):
            with open(self.results_file, 'r', encoding='utf-8') as f:
                self.results = json.load(f)
        else:
            self.results = {"metadata": {"total_processed": 0}, "photos": []}
    
    def save_results(self):
        """Сохраняет результаты"""
        os.makedirs(os.path.dirname(self.results_file), exist_ok=True)
        with open(self.results_file, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, ensure_ascii=False, indent=2)
    
    def load_villages_data(self):
        """Загружает данные о связях снимков и НП"""
        if os.path.exists(self.villages_file):
            with open(self.villages_file, 'r', encoding='utf-8') as f:
                self.villages_data = json.load(f)
        else:
            self.villages_data = {}
    
    def save_villages_data(self):
        """Сохраняет данные о связях снимков и НП"""
        os.makedirs(os.path.dirname(self.villages_file), exist_ok=True)
        with open(self.villages_file, 'w', encoding='utf-8') as f:
            json.dump(self.villages_data, f, ensure_ascii=False, indent=2)
    
    def parse_kml_file(self, kml_path: str) -> List[Dict]:
        """
        Парсит KML файл и извлекает все Placemark с полигонами
        :param kml_path: путь к KML файлу
        :return: список словарей с информацией о снимках
        """
        self.logger.info(f"📄 Чтение KML файла: {kml_path}")
        
        with open(kml_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        soup = BeautifulSoup(content, 'xml')
        placemarks = soup.find_all('Placemark')
        
        results = []
        for placemark in placemarks:
            # Извлекаем название
            name_elem = placemark.find('name')
            if not name_elem:
                continue
            
            name = name_elem.text.strip()
            
            # Проверяем, что это снимок (начинается с Frame-)
            if not name.startswith('Frame-'):
                continue
            
            # Извлекаем номер снимка
            photo_num = name.replace('Frame-', '')
            
            # Извлекаем описание
            desc_elem = placemark.find('description')
            description = desc_elem.text.strip() if desc_elem else ""
            
            # Извлекаем полигон
            polygon_elem = placemark.find('Polygon')
            if not polygon_elem:
                continue
            
            # Извлекаем координаты
            coords_elem = polygon_elem.find('coordinates')
            if not coords_elem:
                continue
            
            coordinates = self._parse_coordinates(coords_elem.text.strip())
            
            if coordinates:
                photo_data = {
                    'photo_num': photo_num,
                    'name': name,
                    'description': description,
                    'coordinates': coordinates,
                    'coordinate_count': len(coordinates)
                }
                results.append(photo_data)
                self.logger.info(f"  ✅ Найден снимок: {photo_num} ({len(coordinates)} координат)")
        
        self.logger.info(f"📸 Всего найдено {len(results)} снимков")
        return results
    
    def _parse_coordinates(self, coords_text: str) -> List[Tuple[float, float]]:
        """
        Парсит координаты из KML (формат: lon,lat,alt lon,lat,alt ...)
        :param coords_text: строка с координатами
        :return: список координат (lat, lon)
        """
        coords = []
        for point in coords_text.strip().split():
            parts = point.split(',')
            if len(parts) >= 2:
                lon = float(parts[0])
                lat = float(parts[1])
                coords.append((lat, lon))
        return coords
    
    def calculate_bounding_box(self, coordinates: List[Tuple[float, float]], margin_m: float = 100.0) -> Tuple[float, float, float, float]:
        """
        Вычисляет bounding box с запасом в метрах
        :param coordinates: список координат
        :param margin_m: запас в метрах
        :return: (min_lat, max_lat, min_lon, max_lon)
        """
        lats = [c[0] for c in coordinates]
        lons = [c[1] for c in coordinates]
        margin_deg = margin_m / 111000  # 1 градус ≈ 111 км
        
        return (min(lats) - margin_deg, max(lats) + margin_deg,
                min(lons) - margin_deg, max(lons) + margin_deg)
    
    def search_nominatim(self, bbox: Tuple[float, float, float, float]) -> List[Dict]:
        """
        Ищет населенные пункты через Nominatim API
        :param bbox: (min_lat, max_lat, min_lon, max_lon)
        :return: список найденных населенных пунктов
        """
        params = {
            'q': '',
            'format': 'json',
            'bounded': 1,
            'viewbox': f"{bbox[2]},{bbox[1]},{bbox[3]},{bbox[0]}",
            'addressdetails': 1,
            'limit': 50,
            'accept-language': 'ru'
        }
        
        try:
            response = requests.get(
                self.nominatim_endpoint, 
                params=params, 
                headers={'User-Agent': self.user_agent},
                timeout=10
            )
            if response.status_code == 200:
                data = response.json()
                # Фильтруем только населенные пункты
                villages = []
                for item in data:
                    if self._is_valid_place_type(item):
                        villages.append({
                            'name': item.get('display_name', '').split(',')[0],
                            'lat': float(item['lat']),
                            'lon': float(item['lon']),
                            'type': item.get('type', '')
                        })
                return villages
            return []
        except Exception as e:
            self.logger.error(f"Ошибка Nominatim: {e}")
            return []
    
    def _is_valid_place_type(self, item: Dict) -> bool:
        """
        Проверяет, относится ли объект к нужным типам населенных пунктов
        :param item: объект из Nominatim
        :return: True если подходит
        """
        place_type = item.get('type', '')
        class_type = item.get('class', '')
        
        # Исключаем нежелательные типы
        if place_type in ['suburb', 'neighbourhood']:
            return False
        
        # Разрешенные типы
        allowed_types = ['city', 'town', 'village', 'hamlet', 'isolated_dwelling', 'locality', 'farm']
        
        return class_type == 'place' or place_type in allowed_types
    
    def point_in_polygon(self, point: Tuple[float, float], polygon_coords: List[Tuple[float, float]], margin_m: float = 100.0) -> bool:
        """
        Проверяет, попадает ли точка в полигон с учетом запаса
        :param point: координаты точки (lat, lon)
        :param polygon_coords: координаты полигона
        :param margin_m: запас в метрах
        :return: True если попадает
        """
        polygon = Polygon(polygon_coords)
        point_obj = Point(point[0], point[1])
        margin_deg = margin_m / 111000
        return polygon.buffer(margin_deg).contains(point_obj)
    
    def process_kml_file(self, kml_path: str, margin_m: float = 100.0) -> Dict:
        """
        Основной метод: обрабатывает KML файл и находит НП для каждого снимка
        :param kml_path: путь к KML файлу
        :param margin_m: запас в метрах
        :return: словарь с результатами обработки
        """
        self.logger.info(f"🚀 Начало обработки KML файла: {kml_path}")
        
        # Парсим все снимки из KML
        photos = self.parse_kml_file(kml_path)
        
        if not photos:
            self.logger.warning("❌ В файле не найдено снимков")
            return {"metadata": {"total_photos": 0}, "photos": []}
        
        # Обрабатываем каждый снимок
        results = []
        new_villages_data = {}
        
        for i, photo in enumerate(photos):
            self.logger.info(f"🔄 Обработка {i+1}/{len(photos)}: {photo['photo_num']}")
            
            # Вычисляем область поиска
            bbox = self.calculate_bounding_box(photo['coordinates'], margin_m)
            
            # Ищем НП в этом районе
            candidates = self.search_nominatim(bbox)
            self.logger.info(f"    Найдено кандидатов: {len(candidates)}")
            
            # Проверяем каждый кандидат
            villages_in_photo = []
            for village in candidates:
                if self.point_in_polygon(
                    (village['lat'], village['lon']), 
                    photo['coordinates'], 
                    margin_m
                ):
                    villages_in_photo.append(village['name'])
                    self.logger.info(f"      ✅ Попадает: {village['name']}")
            
            # Сохраняем результаты
            photo['villages'] = list(set(villages_in_photo))
            photo['village_count'] = len(photo['villages'])
            results.append(photo)
            
            # Обновляем словарь для быстрого доступа
            new_villages_data[photo['photo_num']] = photo['villages']
            
            self.logger.info(f"  ✅ Найдено {photo['village_count']} НП")
            
            # Задержка для соблюдения лимитов API
            if i < len(photos) - 1:
                time.sleep(1)
        
        # Сохраняем результаты
        self.results = {
            'metadata': {
                'source_file': os.path.basename(kml_path),
                'processing_date': time.strftime('%Y-%m-%d %H:%M:%S'),
                'total_photos': len(results),
                'photos_with_villages': sum(1 for p in results if p['village_count'] > 0),
                'total_villages_found': sum(p['village_count'] for p in results)
            },
            'photos': results
        }
        self.save_results()
        
        # Обновляем данные в памяти и сохраняем в файл
        self.villages_data.update(new_villages_data)
        self.save_villages_data()
        
        self.logger.info(f"✅ Обработка завершена. Результаты сохранены")
        return self.results
    
    def get_photo_villages(self, photo_num: Optional[str] = None) -> Union[List[str], Dict]:
        """
        Возвращает НП для конкретного снимка или весь словарь
        :param photo_num: номер снимка (если None, возвращает весь словарь)
        :return: 
            - если photo_num указан: список НП для конкретного снимка
            - если photo_num не указан: весь словарь {photo_num: [villages]}
        """
        self.load_villages_data()  # Перезагружаем на всякий случай
        if photo_num:
            return self.villages_data.get(photo_num, [])
        return self.villages_data

# Инициализация KML процессора
kml_processor = KMLProcessor()

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
        
        # Получаем данные из KML (теперь работает без аргумента)
        kml_villages_dict = kml_processor.get_photo_villages()  # без аргумента - возвращает словарь
        
        # Сначала ищем в данных из KML
        if isinstance(kml_villages_dict, dict):
            for photo_num, villages in kml_villages_dict.items():
                if villages:  # если есть населенные пункты
                    for village in villages:
                        if query_lower in village.lower():
                            # Находим запись в locations
                            for record in self.locations:
                                if photo_num in record['photos'] and record['id'] not in seen:
                                    found.append(record)
                                    seen.add(record['id'])
                                    logger.info(f"✅ Найден снимок {photo_num} по KML для '{village}'")
                                    break
        
        # Затем ищем в исходных данных из multi_keys
        for record in self.locations:
            for village in record['villages']:
                if query_lower == village.lower() or (len(query_lower) > 2 and query_lower in village.lower()):
                    if record['id'] not in seen:
                        found.append(record)
                        seen.add(record['id'])
                        logger.info(f"✅ Найден снимок по multi_keys для '{village}'")
                    break
        
        logger.info(f"🔍 По запросу '{query}' найдено {len(found)} записей")
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
        
        # Добавляем населенные пункты из KML для этих снимков
        for photo in self.get_all_photos(records):
            kml_villages = kml_processor.get_photo_villages(photo)  # с аргументом - возвращает список
            if kml_villages:
                villages.extend(kml_villages)
        
        return sorted(list(set(villages)))
    
    def get_photo_details(self, photo_num: str) -> Optional[str]:
        logger.info(f"📸 ЗАПРОШЕН СНИМОК: {photo_num}")
        logger.info(f"  Есть в photo_details: {photo_num in self.photo_details}")
        logger.info(f"  Есть в photo_files: {photo_num in self.photo_files}")
        
        details = self.photo_details.get(photo_num)
        files = self.photo_files.get(photo_num, {})
        
        if details:
            download_links = []
            
            # Добавляем информацию о населенных пунктах из KML
            kml_villages = kml_processor.get_photo_villages(photo_num)  # с аргументом - возвращает список
            if kml_villages:
                village_text = f"\n📍 <b>Населенные пункты в кадре:</b>\n" + "\n".join([f"• {v}" for v in kml_villages[:10]])
                if len(kml_villages) > 10:
                    village_text += f"\n  и ещё {len(kml_villages)-10}"
                details += village_text
                logger.info(f"  ✅ Добавлены НП из KML: {len(kml_villages)}")
            
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
        # Объединяем деревни из multi_keys и из KML
        all_villages = set(self.all_villages)
        
        # Получаем данные из KML (без аргумента - возвращает словарь)
        kml_villages_dict = kml_processor.get_photo_villages()
        if isinstance(kml_villages_dict, dict):
            for villages in kml_villages_dict.values():
                all_villages.update(villages)
        
        return sorted(list(all_villages))
    
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
        logger.info(f"   • Записей в multi_keys: {len(self.locations)}")
        logger.info(f"   • Деревень в multi_keys: {len(self.all_villages)}")
        logger.info(f"   • Описаний снимков: {len(self.photo_details)}")
        logger.info(f"   • Файловых записей: {len(self.photo_files)}")

db = PhotosDatabase()

# ========== СОСТОЯНИЯ ==========

class SearchStates(StatesGroup):
    waiting_for_village = State()
    waiting_for_kml = State()
    waiting_for_file_download = State()

# ========== КЛАВИАТУРЫ ==========

def get_main_keyboard() -> ReplyKeyboardMarkup:
    keyboard = [
        [KeyboardButton(text="🔍 ПОИСК"), KeyboardButton(text="📋 СПИСОК ДЕРЕВЕНЬ")],
        [KeyboardButton(text="📖 ИНСТРУКЦИЯ"), KeyboardButton(text="🗺️ КАРТА РЖЕВ")],
        [KeyboardButton(text="🗺️ LOCUS MAPS"), KeyboardButton(text="🔄 ОБРАБОТАТЬ KML")]
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

def get_download_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Скачать photo_villages.json", callback_data="download_photo_villages")],
        [InlineKeyboardButton(text="📥 Скачать kml_processed_results.json", callback_data="download_kml_results")],
        [InlineKeyboardButton(text="📥 Скачать kml_processor.log", callback_data="download_kml_log")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
    ])

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
        f"• 🗺️ <b>Locus Maps</b> — инструкция и скачивание приложения\n"
        f"• 🔄 <b>Обработать KML</b> — загрузить и обработать KML файл с каталогом снимков\n\n"
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
        
        "🔄 <b>5. ОБРАБОТКА KML</b>\n"
        "• Загрузите KML файл с каталогом снимков\n"
        "• Бот извлечет все полигоны и определит населенные пункты\n"
        "• Данные будут добавлены в базу для более точного поиска\n"
        "• После обработки вы сможете скачать все созданные файлы\n\n"
        
        "🔄 <b>6. НАВИГАЦИЯ</b>\n"
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

@dp.message(F.text == "🔄 ОБРАБОТАТЬ KML")
async def menu_process_kml(message: types.Message, state: FSMContext):
    await message.answer(
        "📤 <b>Загрузите KML файл</b>\n\n"
        "Отправьте мне KML файл с каталогом снимков для обработки.\n\n"
        "После загрузки я:\n"
        "1. Извлеку все полигоны снимков\n"
        "2. Найду населенные пункты в каждом кадре\n"
        "3. Обновлю базу данных для более точного поиска\n"
        "4. Создам файлы с данными для скачивания\n\n"
        "⏱️ Обработка может занять несколько минут.",
        parse_mode="HTML"
    )
    await state.set_state(SearchStates.waiting_for_kml)

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

# ========== ОБРАБОТЧИК ЗАГРУЗКИ KML ==========

@dp.message(SearchStates.waiting_for_kml, F.document)
async def process_kml_upload(message: types.Message, state: FSMContext):
    document = message.document
    
    # Проверяем расширение файла (только .kml)
    if not document.file_name.endswith('.kml'):
        await message.answer(
            "❌ <b>Неверный формат файла</b>\n\n"
            "Пожалуйста, загрузите файл с расширением .kml",
            parse_mode="HTML"
        )
        await state.clear()
        return
    
    await message.answer(
        "⏳ <b>Файл получен. Начинаю обработку...</b>\n\n"
        "Это может занять несколько минут. Я сообщу, когда всё будет готово.",
        parse_mode="HTML"
    )
    
    try:
        # Скачиваем файл
        file_info = await bot.get_file(document.file_id)
        file_path = file_info.file_path
        
        # Создаем временный файл
        with tempfile.NamedTemporaryFile(delete=False, suffix='.kml') as tmp_file:
            await bot.download_file(file_path, tmp_file)
            tmp_path = tmp_file.name
        
        # Обрабатываем KML файл
        results = kml_processor.process_kml_file(tmp_path, margin_m=100.0)
        
        # Удаляем временный файл
        os.unlink(tmp_path)
        
        # Показываем результаты
        meta = results['metadata']
        
        response = f"✅ <b>Обработка завершена!</b>\n\n"
        response += f"📊 <b>Результаты:</b>\n"
        response += f"• Обработано снимков: {meta['total_photos']}\n"
        response += f"• Снимков с НП: {meta['photos_with_villages']}\n"
        response += f"• Всего связей: {meta['total_villages_found']}\n\n"
        response += f"📁 <b>Созданы файлы:</b>\n"
        response += f"• data/photo_villages.json - для быстрого поиска\n"
        response += f"• data/kml_processed_results.json - полные данные\n"
        response += f"• data/kml_processor.log - лог обработки\n\n"
        response += f"👇 <b>Нажмите кнопку для скачивания файлов:</b>"
        
        await message.answer(
            response,
            parse_mode="HTML",
            reply_markup=get_download_keyboard()
        )
        
    except Exception as e:
        logger.error(f"Ошибка при обработке KML: {e}")
        await message.answer(
            f"❌ <b>Ошибка при обработке файла</b>\n\n{str(e)}",
            parse_mode="HTML"
        )
    
    await state.clear()

@dp.message(SearchStates.waiting_for_kml)
async def process_kml_upload_invalid(message: types.Message, state: FSMContext):
    await message.answer(
        "❌ <b>Ожидался файл</b>\n\n"
        "Пожалуйста, отправьте KML файл для обработки.",
        parse_mode="HTML"
    )
    await state.clear()

# ========== ОБРАБОТЧИКИ СКАЧИВАНИЯ ФАЙЛОВ ==========

@dp.callback_query(lambda c: c.data.startswith('download_'))
async def process_file_download(callback: CallbackQuery):
    file_map = {
        'download_photo_villages': ('data/photo_villages.json', 'photo_villages.json'),
        'download_kml_results': ('data/kml_processed_results.json', 'kml_processed_results.json'),
        'download_kml_log': ('data/kml_processor.log', 'kml_processor.log')
    }
    
    file_key = callback.data
    if file_key not in file_map:
        await callback.answer("❌ Файл не найден")
        return
    
    file_path, file_name = file_map[file_key]
    
    try:
        if os.path.exists(file_path):
            document = FSInputFile(file_path, filename=file_name)
            await callback.message.answer_document(
                document,
                caption=f"📁 <b>Файл {file_name}</b>",
                parse_mode="HTML"
            )
        else:
            await callback.message.answer(f"❌ Файл {file_name} еще не создан")
    except Exception as e:
        logger.error(f"Ошибка при отправке файла {file_path}: {e}")
        await callback.message.answer(f"❌ Ошибка при отправке файла")
    
    await callback.answer()

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