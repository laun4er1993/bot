# services/yandex_disk.py
import os
import re
import logging
import requests
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)


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
        self.logger.info("🔍 Проверка доступа к Яндекс.Диску...")
        data = self._request(f"{self.base_url}/")
        if data:
            self.logger.info("✅ Доступ к диску получен")
            return True
        self.logger.error("❌ Нет доступа к Яндекс.Диску")
        return False
    
    def get_file_download_link(self, file_path: str) -> Optional[str]:
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
        url = f"{self.base_url}/resources"
        data = self._request(url, {"path": f"/{folder_path}"})
        exists = data and data.get("type") == "dir"
        if not quiet:
            self.logger.debug(f"  Папка {'существует' if exists else 'не существует'}: {folder_path}")
        return exists
    
    def find_map_files(self, square: str, overlay: str, frame: str) -> Dict[str, List[Dict]]:
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