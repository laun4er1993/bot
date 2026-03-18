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
import csv
import io
from typing import Optional, Dict, List, Set, Tuple, Any, Union
from collections import defaultdict
import requests
import shutil

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

# Импортируем наш модуль для работы с API
from api_sources import APISourceManager, DISTRICTS

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

# ========== КЛАСС ДЛЯ РАБОТЫ С БАЗОЙ НАСЕЛЕННЫХ ПУНКТОВ ==========

class VillageDatabase:
    def __init__(self, csv_path: str = "data/villages.csv"):
        self.csv_path = csv_path
        self.villages: List[Dict] = []
        self.villages_by_name: Dict[str, List[Dict]] = {}
        self.stats = {
            'total': 0,
            'with_coords': 0,
            'last_update': None,
            'source_file': None
        }
        self.load_data()
    
    def load_data(self):
        """Загружает данные из CSV файла"""
        if os.path.exists(self.csv_path):
            try:
                with open(self.csv_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    self.villages = list(reader)
                
                # Строим индекс для быстрого поиска
                self.villages_by_name.clear()
                with_coords = 0
                
                for v in self.villages:
                    name_lower = v['name'].lower()
                    if name_lower not in self.villages_by_name:
                        self.villages_by_name[name_lower] = []
                    self.villages_by_name[name_lower].append(v)
                    
                    # Считаем записи с координатами
                    if v.get('lat') and v.get('lon') and v['lat'].strip() and v['lon'].strip():
                        with_coords += 1
                
                self.stats['total'] = len(self.villages)
                self.stats['with_coords'] = with_coords
                
                logger.info(f"✅ Загружено {self.stats['total']} населенных пунктов из каталога")
                logger.info(f"   • С координатами: {self.stats['with_coords']}")
                
            except Exception as e:
                logger.error(f"❌ Ошибка загрузки каталога: {e}")
                self._create_empty_db()
        else:
            logger.warning(f"⚠️ Файл каталога {self.csv_path} не найден")
            self._create_empty_db()
    
    def _create_empty_db(self):
        """Создает пустой файл каталога"""
        os.makedirs(os.path.dirname(self.csv_path), exist_ok=True)
        with open(self.csv_path, 'w', encoding='utf-8') as f:
            f.write("name,type,lat,lon,source,district,status,notes\n")
        self.villages = []
        self.villages_by_name = {}
        self.stats = {'total': 0, 'with_coords': 0, 'last_update': None, 'source_file': None}
    
    def search_villages(self, query: str) -> List[Dict]:
        """
        Ищет населенные пункты по названию
        :param query: текст запроса
        :return: список найденных НП
        """
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
    
    def get_villages_in_bbox(self, bbox: Tuple[float, float, float, float]) -> List[Dict]:
        """
        Ищет населенные пункты внутри bounding box
        :param bbox: (min_lat, max_lat, min_lon, max_lon)
        :return: список НП
        """
        min_lat, max_lat, min_lon, max_lon = bbox
        results = []
        
        for v in self.villages:
            try:
                if not v.get('lat') or not v.get('lon'):
                    continue
                    
                lat = float(v['lat']) if v['lat'].strip() else None
                lon = float(v['lon']) if v['lon'].strip() else None
                
                if lat and lon:
                    if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
                        results.append(v)
            except (ValueError, TypeError):
                continue
        
        return results
    
    def replace_with_catalog(self, csv_content: str, source_filename: str) -> Dict:
        """
        ПОЛНОСТЬЮ ЗАМЕНЯЕТ текущую базу новым каталогом
        :param csv_content: содержимое CSV файла
        :param source_filename: имя исходного файла
        :return: статистика загрузки
        """
        stats = {
            'loaded': 0,
            'with_coords': 0,
            'errors': 0
        }
        
        try:
            # Читаем новый CSV
            new_villages = []
            reader = csv.DictReader(io.StringIO(csv_content))
            
            # Проверяем наличие обязательных полей
            required_fields = ['name', 'type', 'lat', 'lon', 'source', 'district', 'status', 'notes']
            if not all(field in reader.fieldnames for field in required_fields):
                missing = [f for f in required_fields if f not in reader.fieldnames]
                raise ValueError(f"Отсутствуют обязательные поля: {', '.join(missing)}")
            
            for row in reader:
                if row['name'].strip():
                    # Валидация координат
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
            
            # ПОЛНОСТЬЮ заменяем текущую базу
            self.villages = new_villages
            
            # Сохраняем в файл
            self._save_to_csv()
            
            # Обновляем индексы
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
            logger.error(f"Ошибка загрузки каталога: {e}")
            raise
    
    def _save_to_csv(self):
        """Сохраняет данные в CSV файл"""
        if not self.villages:
            return
        
        fieldnames = ['name', 'type', 'lat', 'lon', 'source', 'district', 'status', 'notes']
        
        with open(self.csv_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.villages)
    
    def get_stats(self) -> Dict:
        """Возвращает статистику базы данных"""
        return self.stats.copy()
    
    def use_generated_catalog(self, file_path: str) -> Dict:
        """
        Использует сгенерированный каталог как основной
        :param file_path: путь к сгенерированному файлу
        :return: статистика загрузки
        """
        stats = {'loaded': 0, 'with_coords': 0}
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                new_villages = []
                
                for row in reader:
                    if row['name'].strip():
                        # Преобразуем в формат основного каталога
                        village = {
                            'name': row['name'],
                            'type': row['type'],
                            'lat': row['lat'],
                            'lon': row['lon'],
                            'source': row['source'],
                            'district': row['district'],
                            'status': row.get('status', 'existing'),
                            'notes': row.get('notes', '')
                        }
                        new_villages.append(village)
                        
                        if village['lat'] and village['lon'] and village['lat'].strip() and village['lon'].strip():
                            stats['with_coords'] += 1
                        stats['loaded'] += 1
            
            if new_villages:
                self.villages = new_villages
                self._save_to_csv()
                
                # Перестраиваем индексы
                self.villages_by_name.clear()
                for v in self.villages:
                    name_lower = v['name'].lower()
                    if name_lower not in self.villages_by_name:
                        self.villages_by_name[name_lower] = []
                    self.villages_by_name[name_lower].append(v)
                
                self.stats['total'] = len(self.villages)
                self.stats['with_coords'] = stats['with_coords']
                self.stats['last_update'] = time.strftime('%Y-%m-%d %H:%M:%S')
                self.stats['source_file'] = 'generated_catalog.csv'
            
            return stats
            
        except Exception as e:
            logger.error(f"Ошибка при загрузке сгенерированного каталога: {e}")
            raise
    
    def generate_full_catalog(self) -> Dict:
        """
        Генерирует полный каталог из всех источников
        :return: статистика генерации и путь к файлу
        """
        stats = {
            'total': 0,
            'from_catalog': 0,
            'from_multi_keys': 0,
            'with_coords': 0,
            'file_path': None
        }
        
        # Собираем все уникальные названия
        all_entries = []
        seen_names = set()
        
        # 1. Добавляем из текущего каталога
        for v in self.villages:
            entry = v.copy()
            entry['photo_count'] = 0
            all_entries.append(entry)
            seen_names.add(v['name'])
            stats['from_catalog'] += 1
            
            if v.get('lat') and v.get('lon') and v['lat'].strip() and v['lon'].strip():
                stats['with_coords'] += 1
        
        # 2. Добавляем из multi_keys.txt (деревни, которые есть в снимках)
        for record in db.locations:
            for village in record['villages']:
                if village not in seen_names:
                    all_entries.append({
                        'name': village,
                        'type': 'деревня',
                        'lat': '',
                        'lon': '',
                        'source': 'multi_keys',
                        'district': 'Ржевский',
                        'status': 'existing',
                        'notes': '',
                        'photo_count': 0
                    })
                    seen_names.add(village)
                    stats['from_multi_keys'] += 1
        
        # 3. Подсчитываем количество снимков для каждого НП
        photo_counter = defaultdict(int)
        for record in db.locations:
            for village in record['villages']:
                photo_counter[village] += 1
        
        # Обновляем photo_count для всех записей
        for entry in all_entries:
            entry['photo_count'] = photo_counter.get(entry['name'], 0)
        
        stats['total'] = len(all_entries)
        
        # Создаем папку export, если её нет
        export_dir = "data/export"
        os.makedirs(export_dir, exist_ok=True)
        
        # Генерируем файл
        file_path = os.path.join(export_dir, "villages_full.csv")
        
        fieldnames = ['name', 'type', 'lat', 'lon', 'source', 'district', 'status', 'notes', 'photo_count']
        
        with open(file_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            
            # Сортируем по названию
            sorted_entries = sorted(all_entries, key=lambda x: x['name'])
            for entry in sorted_entries:
                row = {
                    'name': entry.get('name', ''),
                    'type': entry.get('type', ''),
                    'lat': entry.get('lat', ''),
                    'lon': entry.get('lon', ''),
                    'source': entry.get('source', ''),
                    'district': entry.get('district', ''),
                    'status': entry.get('status', 'existing'),
                    'notes': entry.get('notes', ''),
                    'photo_count': entry.get('photo_count', 0)
                }
                writer.writerow(row)
        
        stats['file_path'] = file_path
        return stats

# Инициализация базы данных НП
village_db = VillageDatabase()

# ========== КЛАСС ДЛЯ ОБРАБОТКИ KML ФАЙЛОВ ==========

class KMLProcessor:
    def __init__(self):
        self.log_file = "data/kml_processor.log"
        
        # Настройка отдельного логгера для KML процессора
        self.logger = logging.getLogger('KMLProcessor')
        self.logger.setLevel(logging.INFO)
        
        # Создаем обработчик для файла
        fh = logging.FileHandler(self.log_file, encoding='utf-8')
        fh.setLevel(logging.INFO)
        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)
    
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
    
    def search_villages_in_bbox(self, bbox: Tuple[float, float, float, float]) -> List[Dict]:
        """
        Ищет населенные пункты в bounding box из локальной базы
        :param bbox: (min_lat, max_lat, min_lon, max_lon)
        :return: список НП
        """
        min_lat, max_lat, min_lon, max_lon = bbox
        results = []
        
        for v in village_db.villages:
            try:
                if not v.get('lat') or not v.get('lon'):
                    continue
                    
                lat = float(v['lat']) if v['lat'].strip() else None
                lon = float(v['lon']) if v['lon'].strip() else None
                
                if lat and lon:
                    if min_lat <= lat <= max_lat and min_lon <= lon <= max_lon:
                        results.append({
                            'name': v['name'],
                            'type': v.get('type', 'НП'),
                            'lat': lat,
                            'lon': lon,
                            'source': v.get('source', 'unknown')
                        })
            except (ValueError, TypeError):
                continue
        
        self.logger.info(f"    Найдено в локальной БД: {len(results)} кандидатов")
        return results
    
    def point_in_polygon(self, point: Tuple[float, float], polygon_coords: List[Tuple[float, float]], margin_m: float = 100.0) -> bool:
        """
        Проверяет, попадает ли точка в полигон с учетом запаса
        :param point: координаты точки (lat, lon)
        :param polygon_coords: координаты полигона
        :param margin_m: запас в метрах
        :return: True если попадает
        """
        polygon = Polygon([(lon, lat) for lat, lon in polygon_coords])
        point_obj = Point(point[1], point[0])  # Shapely ожидает (x,y) = (lon,lat)
        margin_deg = margin_m / 111000
        return polygon.buffer(margin_deg).contains(point_obj)
    
    def process_single_polygon(self, photo_data: Dict, margin_m: float = 100.0) -> Dict:
        """
        Обрабатывает один полигон и находит попадающие в него НП
        :param photo_data: данные о снимке
        :param margin_m: запас в метрах
        :return: обогащенные данные о снимке
        """
        try:
            # Создаем полигон из координат
            polygon = Polygon([(lon, lat) for lat, lon in photo_data['coordinates']])
            
            # Конвертируем запас в градусы (приблизительно)
            margin_deg = margin_m / 111000
            
            # Вычисляем bounding box с запасом
            lats = [c[0] for c in photo_data['coordinates']]
            lons = [c[1] for c in photo_data['coordinates']]
            bbox = (min(lats) - margin_deg, max(lats) + margin_deg,
                    min(lons) - margin_deg, max(lons) + margin_deg)
            
            # Ищем НП в этом районе из локальной базы
            candidates = self.search_villages_in_bbox(bbox)
            self.logger.info(f"    Найдено кандидатов: {len(candidates)}")
            
            # Фильтруем НП, попадающие в полигон (с учетом запаса)
            villages_in_photo = []
            for village in candidates:
                point = Point(village['lon'], village['lat'])
                buffered_polygon = polygon.buffer(margin_deg)
                if buffered_polygon.contains(point):
                    villages_in_photo.append(village['name'])
                    self.logger.info(f"      ✅ Попадает: {village['name']}")
            
            photo_data['villages'] = list(set(villages_in_photo))
            photo_data['village_count'] = len(photo_data['villages'])
            
            self.logger.info(f"  ✅ Снимок {photo_data['photo_num']}: найдено {photo_data['village_count']} НП")
            return photo_data
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обработки полигона {photo_data.get('photo_num')}: {e}")
            photo_data['villages'] = []
            photo_data['village_count'] = 0
            return photo_data
    
    def process_kml_file(self, kml_path: str, margin_m: float = 100.0) -> List[Dict]:
        """
        Основной метод: обрабатывает KML файл и находит НП для каждого снимка
        :param kml_path: путь к KML файлу
        :param margin_m: запас в метрах
        :return: список обработанных снимков
        """
        self.logger.info(f"🚀 Начало обработки KML файла: {kml_path}")
        
        # Парсим все снимки из KML
        photos = self.parse_kml_file(kml_path)
        
        if not photos:
            self.logger.warning("❌ В файле не найдено снимков")
            return []
        
        # Обрабатываем каждый снимок
        results = []
        
        for i, photo in enumerate(photos):
            self.logger.info(f"🔄 Обработка {i+1}/{len(photos)}: {photo['photo_num']}")
            processed = self.process_single_polygon(photo, margin_m)
            results.append(processed)
        
        self.logger.info(f"✅ Обработка завершена. Обработано {len(results)} снимков")
        return results

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
        
        # Ищем в локальной базе НП
        villages = village_db.search_villages(query)
        
        if villages:
            logger.info(f"🔍 Найдено {len(villages)} населенных пунктов в каталоге")
            # Для каждого найденного НП ищем связанные снимки
            for village in villages:
                for record in self.locations:
                    for v in record['villages']:
                        if query_lower in v.lower():
                            if record['id'] not in seen:
                                found.append(record)
                                seen.add(record['id'])
                                logger.info(f"✅ Найден снимок по каталогу для '{v}'")
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
        # Объединяем деревни из multi_keys и из каталога
        all_villages = set(self.all_villages)
        
        # Добавляем названия из каталога
        for v in village_db.villages:
            all_villages.add(v['name'])
        
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
        logger.info(f"   • Населенных пунктов в каталоге: {village_db.stats['total']}")

db = PhotosDatabase()

# ========== СОСТОЯНИЯ ==========

class SearchStates(StatesGroup):
    waiting_for_village = State()
    waiting_for_kml = State()
    waiting_for_csv_upload = State()
    waiting_for_district_select = State()  # Новое состояние для выбора района
    waiting_for_merge_action = State()     # Новое состояние для выбора действия с данными

# ========== КЛАВИАТУРЫ ==========

def get_main_keyboard() -> ReplyKeyboardMarkup:
    keyboard = [
        [KeyboardButton(text="🔍 ПОИСК"), KeyboardButton(text="📋 СПИСОК ДЕРЕВЕНЬ")],
        [KeyboardButton(text="📖 ИНСТРУКЦИЯ"), KeyboardButton(text="🗺️ КАРТА РЖЕВ")],
        [KeyboardButton(text="🗺️ LOCUS MAPS"), KeyboardButton(text="🔄 ОБРАБОТАТЬ KML")],
        [KeyboardButton(text="⚙️ НАСТРОЙКИ")]
    ]
    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

def get_locus_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📖 Инструкция", callback_data="locus_instruction")],
        [InlineKeyboardButton(text="📥 Скачать Locus Maps", callback_data="locus_download_app")],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])

def get_settings_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Загрузить новый каталог", callback_data="update_villages")],
        [InlineKeyboardButton(text="🌐 Загрузить из интернета", callback_data="download_from_web_start")],
        [InlineKeyboardButton(text="📊 Статистика каталога", callback_data="village_stats")],
        [InlineKeyboardButton(text="📤 Скачать текущий каталог", callback_data="download_villages")],
        [InlineKeyboardButton(text="📋 Сгенерировать полный каталог", callback_data="generate_catalog")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_settings")],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])

def get_district_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для выбора района"""
    keyboard = []
    
    # Добавляем кнопки для всех доступных районов
    for district in DISTRICTS.keys():
        keyboard.append([InlineKeyboardButton(
            text=f"📍 {district} район", 
            callback_data=f"select_district_{district}"
        )])
    
    keyboard.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_settings")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def get_merge_action_keyboard(district: str) -> InlineKeyboardMarkup:
    """Клавиатура для выбора действия с загруженными данными"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Полностью заменить каталог", callback_data=f"merge_replace_{district}")],
        [InlineKeyboardButton(text="➕ Дополнить существующий каталог", callback_data=f"merge_append_{district}")],
        [InlineKeyboardButton(text="📊 Посмотреть статистику", callback_data=f"merge_stats_{district}")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="back_to_settings")]
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

def back_to_settings_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад в настройки", callback_data="back_to_settings")],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])

# ========== ОБРАБОТЧИКИ КОМАНД ==========

@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    welcome_text = (
        f"👋 <b>Добро пожаловать, {message.from_user.full_name}!</b>\n\n"
        f"🛩️ <b>Бот для поиска аэрофотоснимков Ржевского района</b>\n\n"
        f"📌 <b>Что я умею:</b>\n"
        f"• 🔍 <b>Поиск снимков</b> — введите название деревни\n"
        f"• 📋 <b>Список деревень</b> — все доступные деревни\n"
        f"• 📖 <b>Инструкция</b> — помощь по боту\n"
        f"• 🗺️ <b>Карта Ржев</b> — скачать карту\n"
        f"• 🗺️ <b>Locus Maps</b> — инструкция и приложение\n"
        f"• 🔄 <b>Обработать KML</b> — загрузить каталог снимков\n"
        f"• ⚙️ <b>Настройки</b> — управление каталогом НП\n\n"
        f"👇 <b>Выберите действие:</b>"
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
        "• Введите название деревни\n"
        "• Бот покажет все снимки, где встречается эта деревня\n"
        "• Нажмите на номер снимка для просмотра детальной информации\n\n"
        
        "📋 <b>2. СПИСОК ДЕРЕВЕНЬ</b>\n"
        "• Просмотр всех деревень в базе данных\n\n"
        
        "🗺️ <b>3. КАРТА РЖЕВСКОГО РАЙОНА</b>\n"
        "• Скачивание карты для Locus Maps\n\n"
        
        "🗺️ <b>4. LOCUS MAPS</b>\n"
        "• Инструкция и скачивание приложения\n\n"
        
        "🔄 <b>5. ОБРАБОТКА KML</b>\n"
        "• Загрузите KML файл с каталогом снимков\n"
        "• Бот найдет населенные пункты в каждом кадре\n\n"
        
        "⚙️ <b>6. НАСТРОЙКИ</b>\n"
        "• Загрузка официального каталога населенных пунктов\n"
        "• Просмотр статистики базы\n"
        "• Генерация полного каталога\n"
        "• 🌐 Загрузка из интернета (dic.academic.ru для Бельского района)\n"
        "   - Возможность дополнить или заменить каталог\n\n"
        
        "🛩️ <b>ПРИЯТНОГО ИСПОЛЬЗОВАНИЯ!</b>"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])
    
    await message.answer(instruction_text, parse_mode="HTML", reply_markup=keyboard)

@dp.message(F.text == "🗺️ КАРТА РЖЕВ")
async def menu_map(message: types.Message):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Скачать карту", url="https://disk.yandex.ru/d/mrxZWJqLuAtnNA")],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])
    await message.answer(
        "🗺️ <b>Карта Ржевского района для Locus Maps</b>\n\n"
        "Нажмите кнопку для скачивания.",
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
        "После загрузки я найду населенные пункты в каждом кадре "
        "на основе загруженного каталога НП.",
        parse_mode="HTML"
    )
    await state.set_state(SearchStates.waiting_for_kml)

@dp.message(F.text == "⚙️ НАСТРОЙКИ")
async def menu_settings(message: types.Message):
    """Меню настроек"""
    stats = village_db.get_stats()
    
    text = (
        f"⚙️ <b>Настройки базы населенных пунктов</b>\n\n"
        f"📊 <b>Текущая статистика:</b>\n"
        f"• Всего записей: {stats['total']}\n"
        f"• С координатами: {stats['with_coords']}\n"
    )
    
    if stats['last_update']:
        text += f"• Последнее обновление: {stats['last_update']}\n"
    if stats['source_file']:
        text += f"• Источник: {stats['source_file']}\n\n"
    else:
        text += f"• База данных пуста\n\n"
    
    text += f"👇 <b>Выберите действие:</b>"
    
    await message.answer(text, parse_mode="HTML", reply_markup=get_settings_keyboard())

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
        "Нажмите кнопку ниже для скачивания инструкции.",
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
        "Нажмите кнопку ниже для скачивания приложения.",
        parse_mode="HTML",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_locus")
async def back_to_locus(callback: CallbackQuery):
    await callback.message.edit_text(
        "🗺️ <b>Locus Maps</b>\n\nВыберите действие:",
        reply_markup=get_locus_keyboard()
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "download_locus_instruction")
async def download_locus_instruction(callback: CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📖 Скачать инструкцию", url="https://disk.yandex.ru/i/sE2Jy99in7MCxw")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="locus_instruction")],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])
    await callback.message.edit_text(
        "📖 <b>Инструкция по Locus Maps</b>\n\n"
        "Файл готов к скачиванию.",
        parse_mode="HTML",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "download_locus_app")
async def download_locus_app(callback: CallbackQuery):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Скачать Locus Maps", url="https://disk.yandex.ru/d/uUgVGkMoq3WITw")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="locus_download_app")],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])
    await callback.message.edit_text(
        "📥 <b>Скачать Locus Maps</b>\n\n"
        "Файл готов к скачиванию.",
        parse_mode="HTML",
        reply_markup=keyboard
    )
    await callback.answer()

# ========== ОБРАБОТЧИКИ НАСТРОЕК ==========

@dp.callback_query(lambda c: c.data == "village_stats")
async def show_village_stats(callback: CallbackQuery):
    """Показывает подробную статистику базы"""
    stats = village_db.get_stats()
    
    text = (
        f"📊 <b>Детальная статистика каталога НП</b>\n\n"
        f"• Всего записей: {stats['total']}\n"
        f"• С координатами: {stats['with_coords']}\n"
        f"• Без координат: {stats['total'] - stats['with_coords']}\n"
    )
    
    if stats['last_update']:
        text += f"• Последнее обновление: {stats['last_update']}\n"
    if stats['source_file']:
        text += f"• Источник: {stats['source_file']}\n\n"
    
    # Показываем первые 10 записей для примера
    if village_db.villages:
        text += f"\n📋 <b>Примеры записей:</b>\n"
        for v in village_db.villages[:10]:
            coords = f"({v['lat']}, {v['lon']})" if v['lat'] and v['lon'] and v['lat'].strip() and v['lon'].strip() else "(без координат)"
            status = "🏚️" if v.get('status') == 'abandoned' else "🏡"
            text += f"• {status} {v['name']} ({v['type']}) {coords}\n"
            if v.get('notes'):
                text += f"  <i>{v['notes'][:50]}...</i>\n"
        if len(village_db.villages) > 10:
            text += f"  и ещё {len(village_db.villages) - 10}..."
    
    await callback.message.edit_text(text, parse_mode="HTML", 
                                    reply_markup=back_to_settings_keyboard())
    await callback.answer()

@dp.callback_query(lambda c: c.data == "update_villages")
async def update_villages_start(callback: CallbackQuery, state: FSMContext):
    """Начинает процесс загрузки нового каталога"""
    await callback.message.edit_text(
        "📤 <b>Загрузка официального каталога населенных пунктов</b>\n\n"
        "⚠️ <b>ВНИМАНИЕ:</b> Это действие ПОЛНОСТЬЮ ЗАМЕНИТ текущую базу данных!\n\n"
        "Пожалуйста, отправьте CSV файл со следующей структурой:\n\n"
        "<code>name,type,lat,lon,source,district,status,notes</code>\n\n"
        "Пример:\n"
        "<code>Горбово,деревня,56.2345,34.1234,google_places,Ржевский,existing,</code>\n"
        "<code>Авсюково,деревня,55.8324,33.3499,academic_ru,Бельский,abandoned,&lt;i&gt;Источник: dic.academic.ru&lt;/i&gt;</code>\n\n"
        "Поля <b>lat, lon</b> могут быть пустыми для записей без координат.\n\n"
        "Все существующие данные будут удалены и заменены новыми.",
        parse_mode="HTML"
    )
    await state.set_state(SearchStates.waiting_for_csv_upload)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "download_villages")
async def download_villages(callback: CallbackQuery):
    """Отправляет текущий каталог"""
    if os.path.exists(village_db.csv_path) and village_db.stats['total'] > 0:
        document = FSInputFile(village_db.csv_path, filename="villages.csv")
        await callback.message.answer_document(
            document,
            caption=f"📁 <b>Текущий каталог населенных пунктов</b>\n"
                    f"Всего записей: {village_db.stats['total']}",
            parse_mode="HTML"
        )
    else:
        await callback.message.answer("❌ Файл каталога пуст или не найден. Сначала загрузите данные.")
    
    await callback.answer()

@dp.callback_query(lambda c: c.data == "download_from_web_start")
async def download_from_web_start(callback: CallbackQuery, state: FSMContext):
    """Начинает процесс загрузки данных из интернета - выбор района"""
    
    await callback.message.edit_text(
        "🌐 <b>Загрузка данных из интернета</b>\n\n"
        "Выберите район, для которого нужно загрузить данные:\n\n"
        "📌 <b>Доступные районы:</b>\n"
        "• Бельский - бывшие населенные пункты (dic.academic.ru)\n"
        "• Ржевский - (источники пока отсутствуют)\n"
        "• Оленинский - (источники пока отсутствуют)\n"
        "• Зубцовский - (источники пока отсутствуют)\n\n"
        "<i>В текущей версии доступен только Бельский район через dic.academic.ru</i>",
        parse_mode="HTML",
        reply_markup=get_district_keyboard()
    )
    
    await state.set_state(SearchStates.waiting_for_district_select)
    await callback.answer()

# ОБРАБОТЧИК ВЫБОРА РАЙОНА
@dp.callback_query(lambda c: c.data.startswith("select_district_"))
async def process_district_select(callback: CallbackQuery, state: FSMContext):
    """Обрабатывает выбор района"""
    district = callback.data.replace("select_district_", "")
    
    await state.update_data(selected_district=district)
    
    await callback.message.edit_text(
        f"⏳ <b>Загрузка данных для {district} района...</b>\n\n"
        f"Это может занять до 30 секунд. Я сообщу, когда данные будут готовы.",
        parse_mode="HTML"
    )
    
    await callback.answer("⏳ Начинаю загрузку...")
    
    try:
        # Создаем менеджер API
        api_manager = APISourceManager()
        
        # Загружаем данные для выбранного района
        results = await asyncio.wait_for(
            api_manager.fetch_district_data(district),
            timeout=45.0  # Увеличиваем таймаут для обработки 3000+ записей
        )
        
        await api_manager.close_session()
        
        # Сохраняем результаты в состоянии
        await state.update_data(
            downloaded_data=results["total"],
            google_count=len(results["google_places"]),
            academic_count=len(results.get("academic_ru", [])),
            total_count=len(results["total"])
        )
        
        # Формируем статистику
        stats_text = f"📊 <b>Результаты для {district} района:</b>\n\n"
        
        if results.get("academic_ru"):
            stats_text += f"• dic.academic.ru: {len(results['academic_ru'])} записей (бывшие НП)\n"
        
        stats_text += f"• Всего уникальных: {len(results['total'])} записей\n"
        
        # Подсчет по типам
        type_counts = {}
        for v in results["total"]:
            t = v.get('type', 'неизвестно')
            type_counts[t] = type_counts.get(t, 0) + 1
        
        if type_counts:
            stats_text += f"\n📋 <b>По типам:</b>\n"
            for t, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
                stats_text += f"• {t}: {count}\n"
        
        stats_text += f"\n<i>Источники данных указаны в поле notes мелким шрифтом.</i>"
        
        # Создаем временный файл для предпросмотра
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        temp_dir = "data/temp"
        os.makedirs(temp_dir, exist_ok=True)
        temp_file = os.path.join(temp_dir, f"{district}_{timestamp}.csv")
        
        with open(temp_file, 'w', encoding='utf-8', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['name', 'type', 'lat', 'lon', 'source', 'district', 'status', 'notes'])
            writer.writeheader()
            writer.writerows(results["total"])
        
        await state.update_data(temp_file=temp_file)
        
        # Показываем меню выбора действия
        await callback.message.edit_text(
            f"✅ <b>Данные для {district} района загружены!</b>\n\n"
            f"{stats_text}\n"
            f"📁 Временный файл: <code>{temp_file}</code>\n\n"
            f"<b>Что сделать с этими данными?</b>",
            parse_mode="HTML",
            reply_markup=get_merge_action_keyboard(district)
        )
        
    except asyncio.TimeoutError:
        logger.error("Таймаут при загрузке данных")
        await callback.message.edit_text(
            "❌ <b>Ошибка загрузки</b>\n\n"
            "Превышено время ожидания ответа от серверов.\n"
            "Попробуйте позже.",
            parse_mode="HTML",
            reply_markup=back_to_settings_keyboard()
        )
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {e}")
        await callback.message.edit_text(
            f"❌ <b>Ошибка загрузки</b>\n\n{str(e)}",
            parse_mode="HTML",
            reply_markup=back_to_settings_keyboard()
        )
    finally:
        if 'api_manager' in locals():
            await api_manager.close_session()

# ОБРАБОТЧИК ВЫБОРА ДЕЙСТВИЯ С ДАННЫМИ
@dp.callback_query(lambda c: c.data.startswith("merge_"))
async def process_merge_action(callback: CallbackQuery, state: FSMContext):
    """Обрабатывает выбор действия с загруженными данными"""
    
    action, district = callback.data.replace("merge_", "").split("_", 1)
    
    data = await state.get_data()
    temp_file = data.get('temp_file')
    downloaded_data = data.get('downloaded_data', [])
    
    if not temp_file or not os.path.exists(temp_file):
        await callback.message.edit_text(
            "❌ <b>Ошибка</b>\n\nВременный файл не найден. Попробуйте загрузить данные заново.",
            parse_mode="HTML",
            reply_markup=back_to_settings_keyboard()
        )
        await callback.answer()
        return
    
    if action == "stats":
        # Показываем подробную статистику
        await show_detailed_stats(callback, state, downloaded_data)
        return
    
    elif action == "replace":
        # Полная замена каталога
        await perform_replace_catalog(callback, state, temp_file, downloaded_data, district)
    
    elif action == "append":
        # Дополнение существующего каталога
        await perform_append_catalog(callback, state, temp_file, downloaded_data, district)

async def show_detailed_stats(callback: CallbackQuery, state: FSMContext, data: List[Dict]):
    """Показывает детальную статистику по загруженным данным"""
    
    with_coords = sum(1 for v in data if v.get('lat') and v.get('lon'))
    
    # Статистика по источникам
    source_stats = {}
    for v in data:
        source = v.get('source', 'unknown')
        source_stats[source] = source_stats.get(source, 0) + 1
    
    # Статистика по районам
    district_stats = {}
    for v in data:
        dist = v.get('district', 'неизвестно')
        district_stats[dist] = district_stats.get(dist, 0) + 1
    
    # Статистика по статусам
    status_stats = {}
    status_names = {
        'existing': 'существующие',
        'abandoned': 'бывшие/исторические',
        'natural': 'природные',
        'historical': 'исторические'
    }
    for v in data:
        status = v.get('status', 'unknown')
        status_stats[status] = status_stats.get(status, 0) + 1
    
    text = (
        f"📊 <b>Детальная статистика</b>\n\n"
        f"• Всего записей: {len(data)}\n"
        f"• С координатами: {with_coords}\n"
        f"• Без координат: {len(data) - with_coords}\n\n"
        
        f"📋 <b>По источникам:</b>\n"
    )
    
    for source, count in sorted(source_stats.items(), key=lambda x: x[1], reverse=True):
        text += f"  • {source}: {count}\n"
    
    text += f"\n📍 <b>По районам:</b>\n"
    for dist, count in sorted(district_stats.items(), key=lambda x: x[1], reverse=True):
        text += f"  • {dist}: {count}\n"
    
    text += f"\n🔄 <b>По статусам:</b>\n"
    for status, count in status_stats.items():
        status_text = status_names.get(status, status)
        text += f"  • {status_text}: {count}\n"
    
    # Показываем примеры записей с источниками
    if data:
        text += f"\n📝 <b>Пример записи:</b>\n"
        sample = data[0]
        text += f"  • {sample.get('name')} ({sample.get('type')})\n"
        if sample.get('notes'):
            # Убираем HTML теги для отображения в статистике
            notes = sample.get('notes', '').replace('<i>', '').replace('</i>', '').replace('<br>', ' ')
            text += f"    {notes[:100]}...\n"
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Заменить каталог", callback_data="merge_replace_continue")],
        [InlineKeyboardButton(text="➕ Дополнить каталог", callback_data="merge_append_continue")],
        [InlineKeyboardButton(text="🔙 Назад к выбору", callback_data="merge_back_to_action")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="back_to_settings")]
    ])
    
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await callback.answer()

async def perform_replace_catalog(callback: CallbackQuery, state: FSMContext, 
                                   temp_file: str, data: List[Dict], district: str):
    """Выполняет полную замену каталога"""
    
    await callback.message.edit_text(
        f"⏳ <b>Заменяю каталог данными {district} района...</b>",
        parse_mode="HTML"
    )
    
    try:
        # Читаем временный файл
        with open(temp_file, 'r', encoding='utf-8') as f:
            csv_content = f.read()
        
        # Заменяем каталог
        stats = village_db.replace_with_catalog(csv_content, f"internet_{district}_catalog.csv")
        
        # Удаляем временный файл
        os.unlink(temp_file)
        await state.clear()
        
        # Формируем ответ
        text = (
            f"✅ <b>Каталог полностью заменен данными {district} района!</b>\n\n"
            f"📊 <b>Результаты:</b>\n"
            f"• Загружено записей: {stats['loaded']}\n"
            f"• С координатами: {stats['with_coords']}\n\n"
            f"<i>Все старые данные удалены. Теперь поиск использует новый каталог.</i>"
        )
        
        await callback.message.edit_text(
            text,
            parse_mode="HTML",
            reply_markup=back_to_settings_keyboard()
        )
        
    except Exception as e:
        logger.error(f"Ошибка при замене каталога: {e}")
        await callback.message.edit_text(
            f"❌ <b>Ошибка при замене каталога</b>\n\n{str(e)}",
            parse_mode="HTML",
            reply_markup=back_to_settings_keyboard()
        )
    
    await callback.answer()

async def perform_append_catalog(callback: CallbackQuery, state: FSMContext,
                                  temp_file: str, new_data: List[Dict], district: str):
    """Дополняет существующий каталог новыми данными"""
    
    await callback.message.edit_text(
        f"⏳ <b>Дополняю каталог данными {district} района...</b>",
        parse_mode="HTML"
    )
    
    try:
        # Получаем существующие данные
        existing_villages = village_db.villages.copy()
        existing_names = {v['name'] for v in existing_villages}
        
        # Счетчики для статистики
        added = 0
        updated = 0
        skipped = 0
        
        # Обрабатываем новые данные
        for new_village in new_data:
            name = new_village['name']
            
            if name not in existing_names:
                # Добавляем новую запись
                existing_villages.append(new_village)
                added += 1
                existing_names.add(name)
            else:
                # Находим существующую запись и обновляем, если у новой есть координаты
                for i, existing in enumerate(existing_villages):
                    if existing['name'] == name:
                        # Если у существующей нет координат, а у новой есть - обновляем
                        if (not existing.get('lat') or not existing.get('lon') or 
                            not existing['lat'].strip() or not existing['lon'].strip()) and \
                           (new_village.get('lat') and new_village.get('lon') and 
                            new_village['lat'].strip() and new_village['lon'].strip()):
                            # Сохраняем заметки из обоих источников
                            if existing.get('notes') and new_village.get('notes'):
                                new_village['notes'] = existing['notes'] + "<br>" + new_village['notes']
                            existing_villages[i] = new_village
                            updated += 1
                        else:
                            # Если у новой записи есть дополнительная информация в notes
                            if new_village.get('notes') and not existing.get('notes'):
                                existing['notes'] = new_village['notes']
                                updated += 1
                            else:
                                skipped += 1
                        break
        
        # Заменяем каталог объединенными данными
        village_db.villages = existing_villages
        village_db._save_to_csv()
        
        # Перестраиваем индексы
        village_db.villages_by_name.clear()
        for v in village_db.villages:
            name_lower = v['name'].lower()
            if name_lower not in village_db.villages_by_name:
                village_db.villages_by_name[name_lower] = []
            village_db.villages_by_name[name_lower].append(v)
        
        # Обновляем статистику
        with_coords = sum(1 for v in village_db.villages if v.get('lat') and v.get('lon') and v['lat'].strip() and v['lon'].strip())
        village_db.stats['total'] = len(village_db.villages)
        village_db.stats['with_coords'] = with_coords
        village_db.stats['last_update'] = time.strftime('%Y-%m-%d %H:%M:%S')
        village_db.stats['source_file'] = f"appended_{district}_catalog.csv"
        
        # Удаляем временный файл
        os.unlink(temp_file)
        await state.clear()
        
        # Формируем ответ
        text = (
            f"✅ <b>Каталог дополнен данными {district} района!</b>\n\n"
            f"📊 <b>Результаты:</b>\n"
            f"• Добавлено новых записей: {added}\n"
            f"• Обновлено существующих: {updated}\n"
            f"• Пропущено (уже есть): {skipped}\n\n"
            f"📈 <b>Новая статистика каталога:</b>\n"
            f"• Всего записей: {village_db.stats['total']}\n"
            f"• С координатами: {village_db.stats['with_coords']}\n"
            f"• Источников: {len(set(v['source'] for v in village_db.villages if v.get('source')))}"
        )
        
        await callback.message.edit_text(
            text,
            parse_mode="HTML",
            reply_markup=back_to_settings_keyboard()
        )
        
    except Exception as e:
        logger.error(f"Ошибка при дополнении каталога: {e}")
        await callback.message.edit_text(
            f"❌ <b>Ошибка при дополнении каталога</b>\n\n{str(e)}",
            parse_mode="HTML",
            reply_markup=back_to_settings_keyboard()
        )
    
    await callback.answer()

# Дополнительные обработчики для навигации
@dp.callback_query(lambda c: c.data == "merge_back_to_action")
async def merge_back_to_action(callback: CallbackQuery, state: FSMContext):
    """Возврат к выбору действия"""
    data = await state.get_data()
    district = data.get('selected_district', 'Ржевский')
    temp_file = data.get('temp_file')
    downloaded_data = data.get('downloaded_data', [])
    total = len(downloaded_data)
    
    text = (
        f"✅ <b>Данные для {district} района загружены!</b>\n\n"
        f"📊 Всего записей: {total}\n"
        f"📁 Файл: <code>{temp_file}</code>\n\n"
        f"<b>Что сделать с этими данными?</b>"
    )
    
    await callback.message.edit_text(
        text,
        parse_mode="HTML",
        reply_markup=get_merge_action_keyboard(district)
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "merge_replace_continue")
async def merge_replace_continue(callback: CallbackQuery, state: FSMContext):
    """Продолжить замену после просмотра статистики"""
    data = await state.get_data()
    district = data.get('selected_district', 'Ржевский')
    temp_file = data.get('temp_file')
    downloaded_data = data.get('downloaded_data', [])
    
    await perform_replace_catalog(callback, state, temp_file, downloaded_data, district)

@dp.callback_query(lambda c: c.data == "merge_append_continue")
async def merge_append_continue(callback: CallbackQuery, state: FSMContext):
    """Продолжить дополнение после просмотра статистики"""
    data = await state.get_data()
    district = data.get('selected_district', 'Ржевский')
    temp_file = data.get('temp_file')
    downloaded_data = data.get('downloaded_data', [])
    
    await perform_append_catalog(callback, state, temp_file, downloaded_data, district)

@dp.callback_query(lambda c: c.data == "generate_catalog")
async def generate_catalog_start(callback: CallbackQuery):
    """Показывает предварительную статистику перед генерацией"""
    
    # Собираем предварительную статистику
    from_catalog = len(village_db.villages)
    
    # Уникальные деревни из multi_keys
    multi_keys_villages = set()
    for record in db.locations:
        for village in record['villages']:
            multi_keys_villages.add(village)
    
    text = (
        f"📋 <b>Генерация полного каталога</b>\n\n"
        f"Будет создан файл со следующими данными:\n\n"
        f"📊 <b>Источники:</b>\n"
        f"• Из текущего каталога: {from_catalog} записей\n"
        f"• Из multi_keys: {len(multi_keys_villages)} уникальных деревень\n\n"
        f"📁 Файл будет сохранен в: <code>data/export/villages_full.csv</code>\n\n"
        f"Продолжить генерацию?"
    )
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, сгенерировать", callback_data="generate_catalog_confirm")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="back_to_settings")]
    ])
    
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
    await callback.answer()

@dp.callback_query(lambda c: c.data == "generate_catalog_confirm")
async def generate_catalog_confirm(callback: CallbackQuery):
    """Генерирует полный каталог"""
    
    await callback.message.edit_text(
        "⏳ <b>Генерация каталога...</b>\n\n"
        "Это может занять несколько секунд.",
        parse_mode="HTML"
    )
    
    try:
        # Генерируем каталог
        stats = village_db.generate_full_catalog()
        
        text = (
            f"✅ <b>Каталог успешно сгенерирован!</b>\n\n"
            f"📊 <b>Статистика:</b>\n"
            f"• Всего записей: {stats['total']}\n"
            f"• Из каталога: {stats['from_catalog']}\n"
            f"• Из multi_keys: {stats['from_multi_keys']}\n"
            f"• С координатами: {stats['with_coords']}\n\n"
            f"📁 Файл сохранен:\n"
            f"<code>{stats['file_path']}</code>\n\n"
            f"Хотите использовать этот каталог как основной?"
        )
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Использовать как основной", callback_data="use_generated_catalog")],
            [InlineKeyboardButton(text="📥 Только скачать", callback_data="download_generated_catalog")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_settings")]
        ])
        
        await callback.message.edit_text(text, parse_mode="HTML", reply_markup=keyboard)
        
    except Exception as e:
        logger.error(f"Ошибка при генерации каталога: {e}")
        await callback.message.edit_text(
            f"❌ <b>Ошибка при генерации каталога</b>\n\n{str(e)}",
            parse_mode="HTML",
            reply_markup=back_to_settings_keyboard()
        )
    
    await callback.answer()

@dp.callback_query(lambda c: c.data == "use_generated_catalog")
async def use_generated_catalog(callback: CallbackQuery):
    """Использует сгенерированный каталог как основной"""
    
    await callback.message.edit_text(
        "⏳ <b>Загружаю сгенерированный каталог...</b>\n\n"
        "Это может занять несколько секунд.",
        parse_mode="HTML"
    )
    
    try:
        file_path = "data/export/villages_full.csv"
        if not os.path.exists(file_path):
            raise Exception("Сгенерированный файл не найден. Сначала выполните генерацию.")
        
        stats = village_db.use_generated_catalog(file_path)
        
        text = (
            f"✅ <b>Каталог успешно загружен!</b>\n\n"
            f"📊 <b>Результаты:</b>\n"
            f"• Загружено записей: {stats['loaded']}\n"
            f"• С координатами: {stats['with_coords']}\n\n"
            f"Теперь поиск будет использовать этот каталог!"
        )
        
        await callback.message.edit_text(text, parse_mode="HTML", 
                                        reply_markup=back_to_settings_keyboard())
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке сгенерированного каталога: {e}")
        await callback.message.edit_text(
            f"❌ <b>Ошибка при загрузке</b>\n\n{str(e)}",
            parse_mode="HTML",
            reply_markup=back_to_settings_keyboard()
        )
    
    await callback.answer()

@dp.callback_query(lambda c: c.data == "download_generated_catalog")
async def download_generated_catalog(callback: CallbackQuery):
    """Отправляет сгенерированный каталог"""
    file_path = "data/export/villages_full.csv"
    
    if os.path.exists(file_path):
        document = FSInputFile(file_path, filename="villages_full.csv")
        await callback.message.answer_document(
            document,
            caption=f"📁 <b>Полный каталог населенных пунктов</b>",
            parse_mode="HTML"
        )
    else:
        await callback.message.answer("❌ Файл не найден. Сначала сгенерируйте каталог.")
    
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_settings")
async def back_to_settings(callback: CallbackQuery):
    """Возврат в меню настроек"""
    stats = village_db.get_stats()
    
    text = (
        f"⚙️ <b>Настройки базы населенных пунктов</b>\n\n"
        f"📊 <b>Текущая статистика:</b>\n"
        f"• Всего записей: {stats['total']}\n"
        f"• С координатами: {stats['with_coords']}\n"
    )
    
    if stats['last_update']:
        text += f"• Последнее обновление: {stats['last_update']}\n"
    if stats['source_file']:
        text += f"• Источник: {stats['source_file']}\n\n"
    else:
        text += f"• База данных пуста\n\n"
    
    text += f"👇 <b>Выберите действие:</b>"
    
    await callback.message.edit_text(text, parse_mode="HTML", reply_markup=get_settings_keyboard())
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
        
        if results:
            # Считаем статистику
            total_np = sum(r.get('village_count', 0) for r in results)
            photos_with_np = sum(1 for r in results if r.get('village_count', 0) > 0)
            
            await message.answer(
                f"✅ <b>Обработка завершена!</b>\n\n"
                f"📊 <b>Результаты:</b>\n"
                f"• Обработано снимков: {len(results)}\n"
                f"• Снимков с НП: {photos_with_np}\n"
                f"• Всего найдено связей: {total_np}\n\n"
                f"Данные сохранены во временные файлы.",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
        else:
            await message.answer(
                "❌ В файле не найдено снимков для обработки",
                reply_markup=back_keyboard()
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

# ========== ОБРАБОТЧИК ЗАГРУЗКИ CSV ==========

@dp.message(SearchStates.waiting_for_csv_upload, F.document)
async def process_csv_upload(message: types.Message, state: FSMContext):
    """Обрабатывает загрузку нового каталога"""
    document = message.document
    
    # Проверяем расширение файла
    if not document.file_name.endswith('.csv'):
        await message.answer(
            "❌ <b>Неверный формат файла</b>\n\n"
            "Пожалуйста, загрузите файл с расширением .csv",
            parse_mode="HTML"
        )
        await state.clear()
        return
    
    await message.answer(
        "⏳ <b>Файл получен. Загружаю новый каталог...</b>\n\n"
        "Это может занять несколько секунд.",
        parse_mode="HTML"
    )
    
    try:
        # Скачиваем файл
        file_info = await bot.get_file(document.file_id)
        file_path = file_info.file_path
        
        # Читаем содержимое
        with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
            await bot.download_file(file_path, tmp_file)
            tmp_path = tmp_file.name
        
        with open(tmp_path, 'r', encoding='utf-8') as f:
            csv_content = f.read()
        
        os.unlink(tmp_path)
        
        # Заменяем базу новым каталогом
        stats = village_db.replace_with_catalog(csv_content, document.file_name)
        
        await message.answer(
            f"✅ <b>Каталог успешно загружен!</b>\n\n"
            f"📊 <b>Результаты:</b>\n"
            f"• Загружено записей: {stats['loaded']}\n"
            f"• С координатами: {stats['with_coords']}\n"
            f"• Ошибок: {stats['errors']}\n"
            f"• Источник: {document.file_name}\n\n"
            f"Теперь поиск будет использовать этот каталог!",
            parse_mode="HTML",
            reply_markup=back_to_settings_keyboard()
        )
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке каталога: {e}")
        await message.answer(
            f"❌ <b>Ошибка при загрузке каталога</b>\n\n{str(e)}",
            parse_mode="HTML"
        )
    
    await state.clear()

@dp.message(SearchStates.waiting_for_csv_upload)
async def process_csv_upload_invalid(message: types.Message, state: FSMContext):
    """Обрабатывает неверный ввод при ожидании CSV"""
    await message.answer(
        "❌ <b>Ожидался CSV файл</b>\n\n"
        "Пожалуйста, отправьте файл с расширением .csv",
        parse_mode="HTML"
    )
    await state.clear()

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
    logger.info(f"📊 Населенных пунктов в каталоге: {village_db.stats['total']}")
    logger.info(f"✅ Яндекс.Диск токен загружен")
    await delete_webhook()
    logger.info("🔄 Polling...")
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")