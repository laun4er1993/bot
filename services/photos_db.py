# services/photos_db.py
import os
import logging
from typing import List, Dict, Optional, Set

from config import DATA_DIR, MULTI_KEYS_FILE, DETAILS_FILE
from services.yandex_disk import YandexDiskClient

logger = logging.getLogger(__name__)


class PhotosDatabase:
    """База данных аэрофотоснимков"""
    
    def __init__(self, yd_client: YandexDiskClient, village_db):
        self.data_dir = DATA_DIR
        self.multi_keys_file = MULTI_KEYS_FILE
        self.details_file = DETAILS_FILE
        self.yd_client = yd_client
        self.village_db = village_db
        
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
        if self.yd_client.check_root_access():
            self._load_photo_files()
        self._log_stats()
    
    def _load_multi_keys(self):
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
                files = self.yd_client.find_map_files(parts[0], parts[1], parts[2])
                if files['mbtiles'] or files['kmz']:
                    self.photo_files[photo] = files
                else:
                    logger.warning(f"  ❌ Файлы не найдены для {photo}")
        logger.info(f"✅ Загрузка завершена. Найдено {len(self.photo_files)} снимков")
    
    def _log_stats(self):
        logger.info(f"📊 Статистика:")
        logger.info(f"   • Записей в multi_keys: {len(self.locations)}")
        logger.info(f"   • Деревень в multi_keys: {len(self.all_villages)}")
        logger.info(f"   • Описаний снимков: {len(self.photo_details)}")
        logger.info(f"   • Файловых записей: {len(self.photo_files)}")
    
    def search_by_village(self, query: str) -> List[Dict]:
        if not query:
            return []
        query_lower = query.lower().strip()
        found = []
        seen = set()
        
        villages = self.village_db.search(query)
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
        all_villages = set(self.all_villages)
        for v in self.village_db.villages:
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