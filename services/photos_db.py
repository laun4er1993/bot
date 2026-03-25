# services/photos_db.py
import os
import logging
from typing import List, Dict, Optional, Set

from config import DATA_DIR, MULTI_KEYS_FILE, DETAILS_FILE
from services.yandex_disk import YandexDiskClient

logger = logging.getLogger(__name__)


class PhotosDatabase:
    """База данных аэрофотоснимков - использует каталог АФС и Яндекс.Диск"""
    
    def __init__(self, yd_client: YandexDiskClient, village_db, afs_catalog):
        self.data_dir = DATA_DIR
        self.multi_keys_file = MULTI_KEYS_FILE
        self.details_file = DETAILS_FILE
        self.yd_client = yd_client
        self.village_db = village_db
        self.afs_catalog = afs_catalog
        
        self.photo_files: Dict[str, Dict] = {}
        
        self.user_last_photos: Dict[int, List[str]] = {}
        self.user_last_villages: Dict[int, str] = {}
        self.user_last_query: Dict[int, str] = {}
        
        self._load_photo_files()
        self._log_stats()
    
    def _load_photo_files(self):
        """Загружает информацию о файлах снимков с Яндекс.Диска"""
        if not self.yd_client.check_root_access():
            return
        
        logger.info("🔍 Поиск файлов на Яндекс.Диске...")
        
        all_photos = [item['frame'] for item in self.afs_catalog.catalog]
        logger.info(f"Найдено {len(all_photos)} снимков в каталоге АФС")
        
        for photo in all_photos:
            parts = photo.split('-')
            if len(parts) >= 3:
                logger.info(f"  🔍 Обработка снимка: {photo}")
                files = self.yd_client.find_map_files(parts[0], parts[1], parts[2])
                if files['mbtiles'] or files['kmz']:
                    self.photo_files[photo] = files
                else:
                    logger.warning(f"  ❌ Файлы не найдены для {photo}")
        logger.info(f"✅ Загрузка завершена. Найдено {len(self.photo_files)} снимков с файлами")
    
    def _log_stats(self):
        logger.info(f"📊 Статистика:")
        logger.info(f"   • Снимков в каталоге АФС: {len(self.afs_catalog.catalog)}")
        logger.info(f"   • Снимков с файлами на Яндекс.Диске: {len(self.photo_files)}")
    
    def search_by_village(self, query: str) -> List[Dict]:
        """
        Поиск снимков по названию деревни.
        Использует каталог населенных пунктов и каталог АФС.
        """
        if not query:
            return []
        
        query_lower = query.lower().strip()
        
        villages = self.village_db.search(query_lower)
        
        if not villages:
            return []
        
        result = []
        for village in villages:
            result.append({
                'id': hash(village['name']),
                'villages': [village['name']],
                'photos': [item['frame'] for item in self.afs_catalog.catalog]
            })
        
        return result
    
    def get_photo_details(self, photo_num: str) -> Optional[str]:
        """Возвращает описание снимка и ссылки на файлы"""
        logger.info(f"📸 ЗАПРОШЕН СНИМОК: {photo_num}")
        
        details = self.afs_catalog.get_photo_details(photo_num)
        
        if not details:
            details = f"📸 Снимок {photo_num}"
        
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
        """Возвращает отсортированный список всех деревень из каталога НП"""
        return sorted([v['name'] for v in self.village_db.villages])
    
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