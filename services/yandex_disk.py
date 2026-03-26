import os
import re
import logging
import requests
from typing import Optional, Dict, List
from datetime import datetime

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
                self.logger.error("  ❌ Неверный токен")
            return None
        except Exception as e:
            self.logger.error(f"  ❌ Ошибка запроса: {e}")
            return None
    
    def check_root_access(self) -> bool:
        data = self._request(f"{self.base_url}/")
        return data is not None
    
    def get_file_download_link(self, file_path: str) -> Optional[Dict]:
        """Возвращает ссылку на скачивание и метаданные файла"""
        file_name = os.path.basename(file_path)
        if ' ' in file_name:
            return None
        
        url = f"{self.base_url}/resources/download"
        data = self._request(url, {"path": f"/{file_path}"})
        if data and "href" in data:
            return {
                'download_link': data["href"],
                'modified': data.get('modified', '')
            }
        return None
    
    def get_files_in_folder(self, folder_path: str, quiet: bool = False) -> Optional[List[Dict]]:
        url = f"{self.base_url}/resources"
        data = self._request(url, {"path": f"/{folder_path}"})
        if data and "_embedded" in data:
            return data["_embedded"].get("items", [])
        return None
    
    def folder_exists(self, folder_path: str, quiet: bool = False) -> bool:
        url = f"{self.base_url}/resources"
        data = self._request(url, {"path": f"/{folder_path}"})
        return data and data.get("type") == "dir"
    
    def find_map_files(self, square: str, overlay: str, frame: str) -> Dict[str, List[Dict]]:
        """Поиск файлов MBTILES и KMZ с метаданными"""
        base_folder = "Компьютер DESKTOP-JMVJ4CL/АФС/КаталогПОСокол"
        full_name = f"{square}-{overlay}-{frame}"
        
        result = {'mbtiles': [], 'kmz': []}
        
        paths = [
            f"{base_folder}/{square}/{square}-{overlay}/{full_name}",
            f"{base_folder}/{square}/{square}-{overlay}",
            f"{base_folder}/{square}/{full_name}"
        ]
        
        for path in paths:
            if not self.folder_exists(path, quiet=True):
                continue
            
            files = self.get_files_in_folder(path, quiet=True)
            if not files:
                continue
            
            result['mbtiles'].extend(self._extract_versions(files, full_name, '.mbtiles', path))
            result['kmz'].extend(self._extract_versions(files, full_name, '.kmz', path))
        
        result['mbtiles'].sort(key=lambda x: x['version'], reverse=True)
        result['kmz'].sort(key=lambda x: x['version'], reverse=True)
        
        return result
    
    def _format_date(self, date_str: str) -> str:
        """Форматирует дату в читаемый вид: ДД.ММ.ГГГГ"""
        if not date_str:
            return ""
        try:
            dt = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            return dt.strftime("%d.%m.%Y")
        except:
            return date_str[:10] if len(date_str) > 10 else date_str
    
    def _extract_versions(self, files: List[Dict], base_name: str, ext: str, folder: str) -> List[Dict]:
        versions = []
        for f in files:
            name = f['name']
            if not name.startswith(base_name) or not name.endswith(ext) or ' ' in name:
                continue
            
            version = 0
            match = re.search(rf'{re.escape(base_name)}-(\d+){re.escape(ext)}$', name)
            if match:
                version = int(match.group(1))
            
            file_info = self.get_file_download_link(f"{folder}/{name}")
            if file_info:
                size_mb = round(f.get('size', 0) / (1024 * 1024))
                if size_mb >= 10:
                    versions.append({
                        'name': name,
                        'version': version,
                        'download_link': file_info['download_link'],
                        'size_mb': size_mb,
                        'modified': self._format_date(file_info.get('modified', ''))
                    })
        return versions