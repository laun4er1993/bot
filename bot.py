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
from typing import Optional, Dict, List, Set, Tuple, Any
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
        self.file_cache: Dict[str, str] = {}      # Кэш для временных ссылок
        self.public_cache: Dict[str, Dict] = {}   # Кэш для публичных ссылок с метаданными
        
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
    
    def check_and_update_link(self, file_path: str) -> Optional[str]:
        """
        Проверяет актуальность ссылки и обновляет её при необходимости
        Возвращает актуальную ссылку
        """
        try:
            # Получаем текущую информацию о файле
            file_info = self.get_file_info(file_path)
            if not file_info:
                logger.error(f"  ❌ Не удалось получить информацию о файле: {file_path}")
                return None
            
            current_modified = file_info.get('modified', '')
            current_created = file_info.get('created', '')
            
            # Проверяем, есть ли файл в кэше
            if file_path in self.public_cache:
                cached_data = self.public_cache[file_path]
                cached_modified = cached_data.get('modified', '')
                cached_created = cached_data.get('created', '')
                
                # Сравниваем даты модификации и создания
                if (cached_modified == current_modified and 
                    cached_created == current_created):
                    # Файл не изменился, возвращаем сохраненную ссылку
                    logger.info(f"  ✅ Ссылка актуальна для {file_path}")
                    return cached_data.get('download_link')
                else:
                    # Файл изменился, нужно создать новую ссылку
                    logger.info(f"  🔄 Файл изменился, создаем новую ссылку: {file_path}")
            
            # Создаем новую публикацию
            return self._create_new_publication(file_path, file_info)
            
        except Exception as e:
            logger.error(f"  ❌ Ошибка при проверке ссылки {file_path}: {e}")
            return None
    
    def _create_new_publication(self, file_path: str, file_info: Dict) -> Optional[str]:
        """Создает новую публикацию файла и сохраняет в кэш"""
        try:
            # Проверяем, есть ли уже публикация
            if file_info.get('public_url'):
                public_url = file_info.get('public_url')
                logger.info(f"  ✅ Используем существующую публикацию: {file_path}")
                
                # Получаем ссылку на скачивание
                download_link = self._get_public_download_link(public_url)
                
                if download_link:
                    # Сохраняем в кэш с метаданными
                    self.public_cache[file_path] = {
                        'download_link': download_link,
                        'modified': file_info.get('modified', ''),
                        'created': file_info.get('created', ''),
                        'public_url': public_url,
                        'public_key': file_info.get('public_key', '')
                    }
                    logger.info(f"  ✅ Использована существующая публикация для {file_path}")
                    return download_link
                else:
                    logger.warning(f"  ⚠️ Не удалось получить ссылку из существующей публикации, создаем новую")
            
            # Если нет публикации или не удалось получить ссылку - создаем новую
            logger.info(f"  📤 Создаем новую публикацию для {file_path}")
            url = f"{self.base_url}/resources/publish"
            params = {"path": f"/{file_path}"}
            data = self._make_request(url, params, method="PUT")
            
            if data:
                # Получаем обновленную информацию о файле
                updated_info = self.get_file_info(file_path)
                if updated_info and updated_info.get('public_url'):
                    public_url = updated_info.get('public_url')
                    download_link = self._get_public_download_link(public_url)
                    
                    if download_link:
                        # Сохраняем в кэш с метаданными
                        self.public_cache[file_path] = {
                            'download_link': download_link,
                            'modified': updated_info.get('modified', ''),
                            'created': updated_info.get('created', ''),
                            'public_url': public_url,
                            'public_key': updated_info.get('public_key', '')
                        }
                        logger.info(f"  ✅ Создана новая публикация для {file_path}")
                        return download_link
            
            logger.error(f"  ❌ Не удалось создать публикацию для {file_path}")
            return None
            
        except Exception as e:
            logger.error(f"  ❌ Ошибка при создании публикации {file_path}: {e}")
            return None
    
    def publish_file(self, file_path: str) -> Optional[str]:
        """
        Публикует файл и возвращает публичную ссылку на скачивание
        С проверкой актуальности ссылки
        """
        try:
            # Проверяем актуальность ссылки и обновляем при необходимости
            return self.check_and_update_link(file_path)
            
        except Exception as e:
            logger.error(f"  ❌ Ошибка при публикации файла {file_path}: {e}")
            return None
    
    def _get_public_download_link(self, public_url: str) -> Optional[str]:
        """
        Конвертирует публичную ссылку на папку/файл в прямую ссылку на скачивание
        Формат: https://disk.yandex.ru/d/XXX -> https://downloader.disk.yandex.ru/disk/...
        """
        try:
            # Извлекаем public_key из URL
            import re
            match = re.search(r'(?:disk\.yandex\.ru/d/|yadi\.sk/d/)([a-zA-Z0-9_-]+)', public_url)
            if not match:
                logger.error(f"  ❌ Не удалось извлечь public_key из URL: {public_url}")
                return None
            
            public_key = match.group(1)
            
            # Получаем ссылку на скачивание через API
            url = f"{self.base_url}/public/resources/download"
            params = {"public_key": public_key}
            data = self._make_request(url, params)
            
            if data and "href" in data:
                return data["href"]
            
            logger.error(f"  ❌ Не удалось получить ссылку на скачивание для public_key: {public_key}")
            return None
            
        except Exception as e:
            logger.error(f"  ❌ Ошибка при получении ссылки на скачивание: {e}")
            return None
    
    def get_file_download_link(self, file_path: str, use_public: bool = True) -> Optional[str]:
        """
        Получает ссылку на скачивание файла.
        Если use_public=True, публикует файл и возвращает постоянную ссылку.
        Иначе возвращает временную ссылку через /download.
        """
        if use_public:
            return self.publish_file(file_path)
        
        # Старый метод с временными ссылками
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
            
            version = 0
            version_match = re.search(rf'{re.escape(base_name)}-(\d+){re.escape(extension)}$', name)
            if version_match:
                version = int(version_match.group(1))
            elif name == f"{base_name}{extension}":
                version = 0
            
            file_path = f"{folder_path}/{name}"
            
            # Получаем ПОСТОЯННУЮ публичную ссылку (use_public=True)
            download_link = self.get_file_download_link(file_path, use_public=True)
            
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

# ========== КЛАСС ДЛЯ ОБРАБОТКИ KMZ ФАЙЛОВ ==========

class KMZProcessor:
    def __init__(self, nominatim_endpoint: str = "https://nominatim.openstreetmap.org/search"):
        """
        Инициализация процессора KMZ файлов
        :param nominatim_endpoint: API endpoint для Nominatim
        """
        self.nominatim_endpoint = nominatim_endpoint
        self.user_agent = "WW2AerialPhotoBot/1.0 (your_email@example.com)"
        self.cache_file = "data/np_cache.json"
        self.photo_villages_file = "data/photo_villages.json"
        self.kmz_data_file = "data/kmz_extracted_data.json"
        self.kmz_processed_file = "data/kmz_processed.json"
        self.kmz_history_file = "data/kmz_history.json"
        self.log_file = "data/kmz_processor.log"
        
        # Настройка отдельного логгера для KMZ процессора
        self.logger = logging.getLogger('KMZProcessor')
        self.logger.setLevel(logging.INFO)
        
        # Создаем обработчик для файла
        fh = logging.FileHandler(self.log_file, encoding='utf-8')
        fh.setLevel(logging.INFO)
        
        # Создаем форматтер
        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] %(message)s')
        fh.setFormatter(formatter)
        
        # Добавляем обработчик к логгеру
        self.logger.addHandler(fh)
        
        # Загружаем данные
        self.load_cache()
        self.load_photo_villages()
        self.load_kmz_data()
        self.load_kmz_processed()
        self.load_kmz_history()
        
    def load_cache(self):
        """Загружает кэш населенных пунктов"""
        if os.path.exists(self.cache_file):
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                self.np_cache = json.load(f)
        else:
            self.np_cache = {}
    
    def save_cache(self):
        """Сохраняет кэш населенных пунктов"""
        os.makedirs(os.path.dirname(self.cache_file), exist_ok=True)
        with open(self.cache_file, 'w', encoding='utf-8') as f:
            json.dump(self.np_cache, f, ensure_ascii=False, indent=2)
    
    def load_photo_villages(self):
        """Загружает данные о связях снимков и населенных пунктов"""
        if os.path.exists(self.photo_villages_file):
            with open(self.photo_villages_file, 'r', encoding='utf-8') as f:
                self.photo_villages = json.load(f)
        else:
            self.photo_villages = {}
    
    def save_photo_villages(self):
        """Сохраняет данные о связях снимков и населенных пунктов"""
        os.makedirs(os.path.dirname(self.photo_villages_file), exist_ok=True)
        with open(self.photo_villages_file, 'w', encoding='utf-8') as f:
            json.dump(self.photo_villages, f, ensure_ascii=False, indent=2)
    
    def load_kmz_data(self):
        """Загружает выгруженные данные из KMZ"""
        if os.path.exists(self.kmz_data_file):
            with open(self.kmz_data_file, 'r', encoding='utf-8') as f:
                self.kmz_data = json.load(f)
        else:
            self.kmz_data = {"metadata": {"total_photos": 0}, "photos": []}
    
    def save_kmz_data(self):
        """Сохраняет выгруженные данные из KMZ"""
        os.makedirs(os.path.dirname(self.kmz_data_file), exist_ok=True)
        with open(self.kmz_data_file, 'w', encoding='utf-8') as f:
            json.dump(self.kmz_data, f, ensure_ascii=False, indent=2, default=str)
    
    def load_kmz_processed(self):
        """Загружает информацию о обработанных KMZ файлах"""
        if os.path.exists(self.kmz_processed_file):
            with open(self.kmz_processed_file, 'r', encoding='utf-8') as f:
                self.kmz_processed = json.load(f)
        else:
            self.kmz_processed = {}
    
    def save_kmz_processed(self):
        """Сохраняет информацию о обработанных KMZ файлах"""
        os.makedirs(os.path.dirname(self.kmz_processed_file), exist_ok=True)
        with open(self.kmz_processed_file, 'w', encoding='utf-8') as f:
            json.dump(self.kmz_processed, f, ensure_ascii=False, indent=2)
    
    def load_kmz_history(self):
        """Загружает историю изменений KMZ"""
        if os.path.exists(self.kmz_history_file):
            with open(self.kmz_history_file, 'r', encoding='utf-8') as f:
                self.kmz_history = json.load(f)
        else:
            self.kmz_history = []
    
    def save_kmz_history(self):
        """Сохраняет историю изменений KMZ"""
        os.makedirs(os.path.dirname(self.kmz_history_file), exist_ok=True)
        with open(self.kmz_history_file, 'w', encoding='utf-8') as f:
            json.dump(self.kmz_history, f, ensure_ascii=False, indent=2)
    
    def should_process_kmz(self, kmz_path: str, file_size: int) -> bool:
        """
        Проверяет, нужно ли обрабатывать KMZ файл
        :param kmz_path: путь к файлу
        :param file_size: размер файла
        :return: True если нужно обработать, False если уже обработан
        """
        file_stat = os.stat(kmz_path)
        file_mtime = file_stat.st_mtime
        
        if kmz_path in self.kmz_processed:
            last_processed = self.kmz_processed[kmz_path]
            if (last_processed['mtime'] == file_mtime and 
                last_processed['size'] == file_size):
                self.logger.info(f"✅ Файл {kmz_path} уже обработан, пропускаем")
                return False
        
        return True
    
    def mark_kmz_processed(self, kmz_path: str):
        """Отмечает KMZ файл как обработанный"""
        file_stat = os.stat(kmz_path)
        self.kmz_processed[kmz_path] = {
            'mtime': file_stat.st_mtime,
            'size': file_stat.st_size,
            'processed_date': time.strftime('%Y-%m-%d %H:%M:%S')
        }
        self.save_kmz_processed()
    
    def extract_kml_from_kmz(self, kmz_path: str) -> str:
        """
        Извлекает KML файл из KMZ архива
        :param kmz_path: путь к KMZ файлу
        :return: содержимое KML файла
        """
        with zipfile.ZipFile(kmz_path, 'r') as kmz:
            # Ищем KML файл (обычно doc.kml)
            kml_files = [f for f in kmz.namelist() if f.endswith('.kml')]
            if not kml_files:
                raise ValueError("KML файл не найден в KMZ архиве")
            
            # Берем первый KML файл
            with kmz.open(kml_files[0], 'r') as kml_file:
                return kml_file.read().decode('utf-8')
    
    def parse_kml_polygons(self, kml_content: str) -> List[Dict]:
        """
        Парсит KML и извлекает полигоны снимков
        :param kml_content: содержимое KML файла
        :return: список словарей с информацией о снимках
        """
        soup = BeautifulSoup(kml_content, 'xml')
        placemarks = soup.find_all('Placemark')
        
        results = []
        for placemark in placemarks:
            name_elem = placemark.find('name')
            if not name_elem:
                continue
            
            name = name_elem.text.strip()
            
            # Ищем placemark с frame- в названии или с номером снимка
            photo_num = None
            
            # Проверяем разные форматы
            if name.startswith('frame-'):
                photo_num = name.replace('frame-', '')
            elif re.match(r'N56E34-\d+-\d+', name):
                photo_num = name
            else:
                # Пропускаем, если не похоже на снимок
                continue
            
            # Ищем описание
            desc_elem = placemark.find('description')
            description = desc_elem.text.strip() if desc_elem else ""
            
            # Ищем стиль
            style_elem = placemark.find('styleUrl')
            style = style_elem.text.strip() if style_elem else ""
            
            # Ищем полигон
            polygon_elem = placemark.find('Polygon')
            if not polygon_elem:
                continue
            
            # Извлекаем координаты
            coords_elem = polygon_elem.find('coordinates')
            if not coords_elem:
                continue
            
            # Парсим координаты
            coords_text = coords_elem.text.strip()
            coordinates = self._parse_coordinates(coords_text)
            
            if coordinates:
                results.append({
                    'photo_num': photo_num,
                    'name': name,
                    'description': description,
                    'style': style,
                    'coordinates': coordinates,
                    'coordinate_count': len(coordinates)
                })
                self.logger.info(f"  📸 Найден снимок: {photo_num} (координат: {len(coordinates)})")
        
        self.logger.info(f"📸 Всего найдено {len(results)} снимков в KML")
        return results
    
    def _parse_coordinates(self, coords_text: str) -> List[Tuple[float, float]]:
        """
        Парсит строку с координатами
        :param coords_text: строка вида "lon,lat,alt lon,lat,alt ..."
        :return: список координат (lat, lon)
        """
        coords = []
        # Разделяем по пробелам
        for point in coords_text.strip().split():
            # Каждая точка: lon,lat,alt
            parts = point.split(',')
            if len(parts) >= 2:
                lon = float(parts[0])
                lat = float(parts[1])
                coords.append((lat, lon))
        return coords
    
    def get_bounding_box(self, coordinates: List[Tuple[float, float]], margin_km: float = 1.0) -> Tuple[float, float, float, float]:
        """
        Вычисляет bounding box для полигона с запасом
        :param coordinates: список координат
        :param margin_km: запас в километрах
        :return: (min_lat, max_lat, min_lon, max_lon)
        """
        lats = [c[0] for c in coordinates]
        lons = [c[1] for c in coordinates]
        
        min_lat = min(lats)
        max_lat = max(lats)
        min_lon = min(lons)
        max_lon = max(lons)
        
        # Добавляем запас (примерно 0.01 градуса = 1.1 км)
        margin_deg = margin_km / 111.0
        
        return (min_lat - margin_deg, max_lat + margin_deg, 
                min_lon - margin_deg, max_lon + margin_deg)
    
    def _make_nominatim_request(self, params: Dict) -> Optional[List[Dict]]:
        """
        Выполняет запрос к Nominatim API с повторными попытками
        :param params: параметры запроса
        :return: ответ API или None
        """
        max_retries = 3
        retry_delay = 2
        
        for attempt in range(max_retries):
            try:
                response = requests.get(
                    self.nominatim_endpoint, 
                    params=params, 
                    headers={'User-Agent': self.user_agent},
                    timeout=10
                )
                
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:
                    self.logger.warning(f"⚠️ Превышен лимит запросов, попытка {attempt+1}/{max_retries}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay * (attempt + 1))
                    continue
                else:
                    self.logger.error(f"❌ Ошибка Nominatim API: {response.status_code}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                    continue
                    
            except requests.exceptions.Timeout:
                self.logger.warning(f"⚠️ Таймаут, попытка {attempt+1}/{max_retries}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                continue
            except Exception as e:
                self.logger.error(f"❌ Исключение при запросе: {e}")
                if attempt < max_retries - 1:
                    time.sleep(retry_delay)
                continue
        
        self.logger.error(f"❌ Все попытки запроса исчерпаны")
        return None
    
    def _is_valid_place_type(self, item: Dict) -> bool:
        """
        Проверяет, относится ли объект к нужным типам населенных пунктов
        """
        place_type = item.get('type', '')
        class_type = item.get('class', '')
        
        # Исключаем нежелательные типы
        if place_type in ['suburb', 'neighbourhood']:
            return False
        
        # Разрешенные типы
        allowed_types = [
            'city', 'town',
            'village',
            'hamlet', 'isolated_dwelling',
            'locality',
            'farm'
        ]
        
        return class_type == 'place' or place_type in allowed_types
    
    def calculate_center_and_radius(self, coordinates: List[Tuple[float, float]]) -> Dict:
        """
        Вычисляет центр полигона и приблизительный радиус
        :param coordinates: список координат вершин
        :return: словарь с центром и радиусом
        """
        polygon = Polygon(coordinates)
        center = polygon.centroid
        
        # Вычисляем максимальное расстояние от центра до вершин
        max_distance = 0
        for coord in coordinates:
            point = Point(coord[0], coord[1])
            distance = center.distance(point)
            # Конвертируем градусы в метры (приблизительно)
            distance_m = distance * 111000  # 1° ≈ 111 км
            max_distance = max(max_distance, distance_m)
        
        return {
            'center_lat': center.y,
            'center_lon': center.x,
            'radius_m': round(max_distance, 2)
        }
    
    def process_single_polygon(self, photo_data: Dict, margin_m: float = 100.0) -> Dict:
        """
        Обрабатывает один полигон и находит попадающие в него НП
        :param photo_data: данные о снимке
        :param margin_m: запас в метрах
        :return: обогащенные данные о снимке
        """
        try:
            # Создаем полигон из координат
            polygon = Polygon(photo_data['coordinates'])
            
            # Вычисляем центр и радиус
            geo_data = self.calculate_center_and_radius(photo_data['coordinates'])
            
            # Конвертируем запас в градусы (приблизительно)
            margin_deg = margin_m / 111000
            
            # Вычисляем bounding box с запасом (в км для обратной совместимости)
            bbox = self.get_bounding_box(photo_data['coordinates'], margin_m / 1000)
            
            # Ищем НП в этом районе
            candidates = self._make_nominatim_request({
                'q': '',
                'format': 'json',
                'bounded': 1,
                'viewbox': f"{bbox[2]},{bbox[1]},{bbox[3]},{bbox[0]}",
                'addressdetails': 1,
                'limit': 50,
                'accept-language': 'ru'
            })
            
            # Фильтруем НП, попадающие в полигон (с учетом запаса)
            villages_in_polygon = []
            if candidates:
                self.logger.info(f"    Найдено кандидатов: {len(candidates)}")
                for village in candidates:
                    if not self._is_valid_place_type(village):
                        continue
                    
                    point = Point(float(village['lat']), float(village['lon']))
                    # Добавляем небольшой буфер к полигону
                    buffered_polygon = polygon.buffer(margin_deg)
                    if buffered_polygon.contains(point):
                        village_name = village.get('display_name', '').split(',')[0]
                        villages_in_polygon.append(village_name)
                        self.logger.info(f"      ✅ Попадает: {village_name}")
            
            photo_data['villages'] = list(set(villages_in_polygon))
            photo_data['village_count'] = len(photo_data['villages'])
            photo_data['center'] = {'lat': geo_data['center_lat'], 'lon': geo_data['center_lon']}
            photo_data['radius_m'] = geo_data['radius_m']
            photo_data['bbox'] = bbox
            photo_data['all_candidates_count'] = len(candidates) if candidates else 0
            
            self.logger.info(f"✅ Снимок {photo_data['photo_num']}: найдено {len(photo_data['villages'])} НП")
            return photo_data
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обработки полигона {photo_data.get('photo_num')}: {e}")
            photo_data['villages'] = []
            photo_data['village_count'] = 0
            photo_data['error'] = str(e)
            return photo_data
    
    def merge_photo_villages(self, photo_num: str, new_villages: List[str]) -> List[str]:
        """
        Объединяет старые и новые списки населенных пунктов
        :param photo_num: номер снимка
        :param new_villages: новые населенные пункты из текущей обработки
        :return: объединенный список
        """
        old_villages = self.photo_villages.get(photo_num, [])
        
        # Объединяем старые и новые, убираем дубликаты
        merged = list(set(old_villages + new_villages))
        
        if merged != old_villages:
            self.logger.info(f"🔄 Снимок {photo_num}: добавлено {len(merged) - len(old_villages)} новых НП")
        
        return merged
    
    def update_with_new_kmz(self, new_results: List[Dict], kmz_filename: str) -> Dict:
        """
        Обновляет базу данных новыми результатами
        :param new_results: новые результаты обработки
        :param kmz_filename: имя обработанного файла
        :return: словарь со статистикой изменений
        """
        self.logger.info(f"📊 Обновление базы данных новыми результатами из {kmz_filename}")
        
        changes = {
            'added': 0,
            'updated': 0,
            'unchanged': 0,
            'new_villages_added': 0
        }
        
        # Создаем словарь для быстрого доступа по имени
        new_photos_by_name = {photo['name']: photo for photo in new_results}
        
        # Обновляем существующие и добавляем новые
        for name, new_photo in new_photos_by_name.items():
            photo_num = new_photo['photo_num']
            
            if photo_num in self.photo_villages:
                # Снимок существует - обновляем
                old_villages = self.photo_villages[photo_num]
                new_villages = new_photo.get('villages', [])
                
                # Объединяем старые и новые НП
                merged_villages = self.merge_photo_villages(photo_num, new_villages)
                self.photo_villages[photo_num] = merged_villages
                
                # Считаем изменения
                if len(merged_villages) > len(old_villages):
                    changes['new_villages_added'] += len(merged_villages) - len(old_villages)
                    changes['updated'] += 1
                else:
                    changes['unchanged'] += 1
                
                # Обновляем данные в kmz_data
                self._update_photo_in_kmz_data(photo_num, new_photo)
                
            else:
                # Новый снимок - добавляем
                self.photo_villages[photo_num] = new_photo.get('villages', [])
                self._add_photo_to_kmz_data(new_photo)
                changes['added'] += 1
        
        # Сохраняем обновленные данные
        self.save_photo_villages()
        self.save_kmz_data()
        
        # Записываем историю изменений
        history_entry = {
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'filename': kmz_filename,
            'changes': changes,
            'total_photos': len(self.photo_villages)
        }
        self.kmz_history.append(history_entry)
        self.save_kmz_history()
        
        self.logger.info(f"✅ Обновление завершено: +{changes['added']} новых, "
                        f"обновлено {changes['updated']}, "
                        f"добавлено {changes['new_villages_added']} новых связей")
        
        return changes
    
    def _update_photo_in_kmz_data(self, photo_num: str, new_photo: Dict):
        """Обновляет данные снимка в kmz_data"""
        for i, photo in enumerate(self.kmz_data.get('photos', [])):
            if photo['photo_num'] == photo_num:
                # Обновляем, сохраняя историю населенных пунктов
                old_villages = photo.get('villages_found', [])
                new_villages = new_photo.get('villages', [])
                
                # Объединяем списки
                merged_villages = list(set(old_villages + new_villages))
                
                self.kmz_data['photos'][i] = {
                    'photo_num': photo_num,
                    'name': new_photo['name'],
                    'description': new_photo['description'],
                    'style': new_photo.get('style', ''),
                    'center': new_photo.get('center', {'lat': 0, 'lon': 0}),
                    'radius_m': new_photo.get('radius_m', 0),
                    'bbox': new_photo.get('bbox', (0,0,0,0)),
                    'villages_found': merged_villages,
                    'village_count': len(merged_villages),
                    'all_candidates_count': new_photo.get('all_candidates_count', 0),
                    'processing_time': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'last_update': time.strftime('%Y-%m-%d %H:%M:%S')
                }
                return
        
        # Если не нашли - добавляем новый
        self._add_photo_to_kmz_data(new_photo)
    
    def _add_photo_to_kmz_data(self, photo: Dict):
        """Добавляет новый снимок в kmz_data"""
        if 'photos' not in self.kmz_data:
            self.kmz_data['photos'] = []
        
        self.kmz_data['photos'].append({
            'photo_num': photo['photo_num'],
            'name': photo['name'],
            'description': photo['description'],
            'style': photo.get('style', ''),
            'center': photo.get('center', {'lat': 0, 'lon': 0}),
            'radius_m': photo.get('radius_m', 0),
            'bbox': photo.get('bbox', (0,0,0,0)),
            'villages_found': photo.get('villages', []),
            'village_count': len(photo.get('villages', [])),
            'all_candidates_count': photo.get('all_candidates_count', 0),
            'processing_time': time.strftime('%Y-%m-%d %H:%M:%S'),
            'first_seen': time.strftime('%Y-%m-%d %H:%M:%S')
        })
        
        # Обновляем метаданные
        self.kmz_data['metadata']['total_photos'] = len(self.kmz_data['photos'])
        self.kmz_data['metadata']['last_update'] = time.strftime('%Y-%m-%d %H:%M:%S')
    
    def process_kmz_file(self, kmz_path: str, margin_m: float = 100.0) -> List[Dict]:
        """
        Основной метод: обрабатывает KMZ файл и сохраняет результаты
        :param kmz_path: путь к KMZ файлу
        :param margin_m: запас в метрах
        :return: список обработанных снимков
        """
        self.logger.info(f"🚀 Начало обработки KMZ файла: {kmz_path}")
        kmz_filename = os.path.basename(kmz_path)
        
        # Проверяем, нужно ли обрабатывать файл
        file_size = os.path.getsize(kmz_path)
        if not self.should_process_kmz(kmz_path, file_size):
            self.logger.info(f"⏭️ Файл {kmz_filename} уже обработан, пропускаем")
            return []
        
        try:
            # Извлекаем KML
            kml_content = self.extract_kml_from_kmz(kmz_path)
            
            # Парсим полигоны
            photos = self.parse_kml_polygons(kml_content)
            self.logger.info(f"📸 Найдено {len(photos)} снимков для обработки")
            
            # Обрабатываем каждый снимок
            results = []
            for i, photo in enumerate(photos):
                self.logger.info(f"🔄 Обработка {i+1}/{len(photos)}: {photo['photo_num']}")
                
                processed = self.process_single_polygon(photo, margin_m)
                results.append(processed)
                
                # Задержка для соблюдения лимитов Nominatim
                if i < len(photos) - 1:
                    time.sleep(1)
            
            # Обновляем базу данных с новой логикой
            changes = self.update_with_new_kmz(results, kmz_filename)
            
            # Отмечаем файл как обработанный
            self.mark_kmz_processed(kmz_path)
            
            self.logger.info(f"✅ Обработка завершена. Изменения: {changes}")
            return results
            
        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка при обработке KMZ: {e}")
            raise
    
    def get_villages_for_photo(self, photo_num: str) -> List[str]:
        """Возвращает список населенных пунктов для снимка"""
        return self.photo_villages.get(photo_num, [])
    
    def get_kmz_data(self) -> Dict:
        """Возвращает выгруженные данные из KMZ"""
        return self.kmz_data
    
    def get_stats(self) -> Dict:
        """Возвращает статистику по обработанным данным"""
        total_photos = len(self.photo_villages)
        total_with_villages = sum(1 for v in self.photo_villages.values() if v)
        total_villages = sum(len(v) for v in self.photo_villages.values())
        
        return {
            'total_photos': total_photos,
            'photos_with_villages': total_with_villages,
            'total_village_entries': total_villages,
            'avg_villages_per_photo': round(total_villages / total_photos, 2) if total_photos > 0 else 0
        }
    
    def get_history(self) -> List[Dict]:
        """Возвращает историю изменений"""
        return self.kmz_history

# Инициализация KMZ процессора
kmz_processor = KMZProcessor()

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
        
        # Сначала ищем в данных из KMZ
        for photo_num, villages in kmz_processor.photo_villages.items():
            for village in villages:
                if query_lower in village.lower():
                    # Находим запись в locations
                    for record in self.locations:
                        if photo_num in record['photos'] and record['id'] not in seen:
                            found.append(record)
                            seen.add(record['id'])
                            break
        
        # Затем ищем в исходных данных из multi_keys
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
        
        # Добавляем населенные пункты из KMZ для этих снимков
        for photo in self.get_all_photos(records):
            kmz_villages = kmz_processor.get_villages_for_photo(photo)
            villages.extend(kmz_villages)
        
        return sorted(list(set(villages)))
    
    def get_photo_details(self, photo_num: str) -> Optional[str]:
        logger.info(f"📸 ЗАПРОШЕН СНИМОК: {photo_num}")
        logger.info(f"  Есть в photo_details: {photo_num in self.photo_details}")
        logger.info(f"  Есть в photo_files: {photo_num in self.photo_files}")
        
        details = self.photo_details.get(photo_num)
        files = self.photo_files.get(photo_num, {})
        
        if details:
            download_links = []
            
            # Добавляем информацию о населенных пунктах из KMZ
            kmz_villages = kmz_processor.get_villages_for_photo(photo_num)
            if kmz_villages:
                village_text = f"\n📍 <b>Населенные пункты в кадре:</b>\n" + "\n".join([f"• {v}" for v in kmz_villages[:10]])
                if len(kmz_villages) > 10:
                    village_text += f"\n  и ещё {len(kmz_villages)-10}"
                details += village_text
            
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
        # Объединяем деревни из multi_keys и из KMZ
        all_villages = set(self.all_villages)
        for villages in kmz_processor.photo_villages.values():
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
        kmz_stats = kmz_processor.get_stats()
        logger.info(f"📊 Статистика:")
        logger.info(f"   • Записей в multi_keys: {len(self.locations)}")
        logger.info(f"   • Деревень в multi_keys: {len(self.all_villages)}")
        logger.info(f"   • Описаний снимков: {len(self.photo_details)}")
        logger.info(f"   • Файловых записей: {len(self.photo_files)}")
        logger.info(f"   • KMZ: {kmz_stats['total_photos']} снимков, {kmz_stats['total_village_entries']} связей")

db = PhotosDatabase()

# ========== СОСТОЯНИЯ ==========

class SearchStates(StatesGroup):
    waiting_for_village = State()
    waiting_for_kmz = State()
    waiting_for_file_download = State()

# ========== КЛАВИАТУРЫ ==========

def get_main_keyboard() -> ReplyKeyboardMarkup:
    keyboard = [
        [KeyboardButton(text="🔍 ПОИСК"), KeyboardButton(text="📋 СПИСОК ДЕРЕВЕНЬ")],
        [KeyboardButton(text="📖 ИНСТРУКЦИЯ"), KeyboardButton(text="🗺️ КАРТА РЖЕВ")],
        [KeyboardButton(text="🗺️ LOCUS MAPS"), KeyboardButton(text="🔄 ОБРАБОТАТЬ KMZ")]
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
        [InlineKeyboardButton(text="📥 Скачать kmz_extracted_data.json", callback_data="download_kmz_data")],
        [InlineKeyboardButton(text="📥 Скачать kmz_history.json", callback_data="download_kmz_history")],
        [InlineKeyboardButton(text="📥 Скачать kmz_processor.log", callback_data="download_kmz_log")],
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
        f"• 🔄 <b>Обработать KMZ</b> — загрузить и обработать KMZ файл с каталогом снимков\n\n"
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
        
        "🔄 <b>5. ОБРАБОТКА KMZ</b>\n"
        "• Загрузите KMZ файл с каталогом снимков\n"
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

@dp.message(F.text == "🔄 ОБРАБОТАТЬ KMZ")
async def menu_process_kmz(message: types.Message, state: FSMContext):
    await message.answer(
        "📤 <b>Загрузите KMZ файл</b>\n\n"
        "Отправьте мне KMZ файл с каталогом снимков для обработки.\n\n"
        "После загрузки я:\n"
        "1. Извлеку все полигоны снимков\n"
        "2. Найду населенные пункты в каждом кадре\n"
        "3. Обновлю базу данных для более точного поиска\n"
        "4. Создам файлы с данными и историей изменений\n\n"
        "⏱️ Обработка может занять несколько минут.",
        parse_mode="HTML"
    )
    await state.set_state(SearchStates.waiting_for_kmz)

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

# ========== ОБРАБОТЧИК ЗАГРУЗКИ KMZ ==========

@dp.message(SearchStates.waiting_for_kmz, F.document)
async def process_kmz_upload(message: types.Message, state: FSMContext):
    document = message.document
    
    # Проверяем расширение файла
    if not document.file_name.endswith('.kmz'):
        await message.answer(
            "❌ <b>Неверный формат файла</b>\n\n"
            "Пожалуйста, загрузите файл с расширением .kmz",
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
        with tempfile.NamedTemporaryFile(delete=False, suffix='.kmz') as tmp_file:
            await bot.download_file(file_path, tmp_file)
            tmp_path = tmp_file.name
        
        # Обрабатываем KMZ
        results = kmz_processor.process_kmz_file(tmp_path, margin_m=100.0)
        
        # Удаляем временный файл
        os.unlink(tmp_path)
        
        # Получаем статистику
        stats = kmz_processor.get_stats()
        
        # Получаем историю
        history = kmz_processor.get_history()
        last_history = history[-1] if history else {}
        
        await message.answer(
            f"✅ <b>Обработка завершена!</b>\n\n"
            f"📊 <b>Результаты:</b>\n"
            f"• Обработано снимков: {stats['total_photos']}\n"
            f"• Снимков с НП: {stats['photos_with_villages']}\n"
            f"• Всего связей: {stats['total_village_entries']}\n"
            f"• В среднем: {stats['avg_villages_per_photo']} НП на снимок\n\n"
            f"📁 <b>Созданы файлы:</b>\n"
            f"• data/photo_villages.json - связи снимков и НП\n"
            f"• data/kmz_extracted_data.json - полные выгруженные данные\n"
            f"• data/kmz_history.json - история изменений\n"
            f"• data/kmz_processor.log - лог обработки\n\n"
            f"🔄 <b>Изменения в этом обновлении:</b>\n"
            f"• Новых снимков: +{last_history.get('changes', {}).get('added', 0)}\n"
            f"• Обновлено снимков: {last_history.get('changes', {}).get('updated', 0)}\n"
            f"• Добавлено новых связей: +{last_history.get('changes', {}).get('new_villages_added', 0)}\n\n"
            f"👇 <b>Нажмите кнопку ниже, чтобы скачать файлы:</b>",
            parse_mode="HTML",
            reply_markup=get_download_keyboard()
        )
        
    except Exception as e:
        logger.error(f"Ошибка при обработке KMZ: {e}")
        await message.answer(
            f"❌ <b>Ошибка при обработке файла</b>\n\n"
            f"{str(e)}",
            parse_mode="HTML"
        )
    
    await state.clear()

@dp.message(SearchStates.waiting_for_kmz)
async def process_kmz_upload_invalid(message: types.Message, state: FSMContext):
    await message.answer(
        "❌ <b>Ожидался файл</b>\n\n"
        "Пожалуйста, отправьте KMZ файл для обработки.",
        parse_mode="HTML"
    )
    await state.clear()

# ========== ОБРАБОТЧИКИ СКАЧИВАНИЯ ФАЙЛОВ ==========

@dp.callback_query(lambda c: c.data.startswith('download_'))
async def process_file_download(callback: CallbackQuery):
    file_map = {
        'download_photo_villages': ('data/photo_villages.json', 'photo_villages.json'),
        'download_kmz_data': ('data/kmz_extracted_data.json', 'kmz_extracted_data.json'),
        'download_kmz_history': ('data/kmz_history.json', 'kmz_history.json'),
        'download_kmz_log': ('data/kmz_processor.log', 'kmz_processor.log')
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