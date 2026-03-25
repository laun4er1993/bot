import os
import logging
import re
from typing import List, Dict, Optional, Tuple

from services.yandex_disk import YandexDiskClient

logger = logging.getLogger(__name__)


class PhotosDatabase:
    """База данных аэрофотоснимков - использует каталог АФС и Яндекс.Диск"""
    
    def __init__(self, yd_client: YandexDiskClient, village_db, afs_catalog):
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
        if not self.yd_client.check_root_access():
            return
        
        logger.info("🔍 ПОИСК ФАЙЛОВ НА ЯНДЕКС.ДИСКЕ")
        
        all_photos = [item['frame'] for item in self.afs_catalog.catalog]
        found_count = 0
        
        for photo in all_photos:
            parts = photo.split('-')
            if len(parts) >= 3:
                files = self.yd_client.find_map_files(parts[0], parts[1], parts[2])
                if files['mbtiles'] or files['kmz']:
                    self.photo_files[photo] = files
                    found_count += 1
        
        logger.info(f"✅ Найдено {found_count} снимков с файлами")
    
    def _log_stats(self):
        logger.info(f"📊 Снимков в АФС: {len(self.afs_catalog.catalog)}, с файлами: {len(self.photo_files)}")
    
    def find_files_for_photo(self, photo_num: str) -> Dict[str, List[Dict]]:
        """Поиск файлов для конкретного снимка на Яндекс.Диске"""
        parts = photo_num.split('-')
        if len(parts) >= 3:
            files = self.yd_client.find_map_files(parts[0], parts[1], parts[2])
            if files['mbtiles'] or files['kmz']:
                self.photo_files[photo_num] = files
                return files
        return {'mbtiles': [], 'kmz': []}
    
    def refresh_all_photo_links(self) -> Dict:
        """Обновляет ссылки для всех снимков в каталоге АФС"""
        stats = {'total': 0, 'found': 0, 'not_found': 0}
        
        for item in self.afs_catalog.catalog:
            photo_num = item['frame']
            stats['total'] += 1
            
            if photo_num in self.photo_files and (self.photo_files[photo_num].get('mbtiles') or self.photo_files[photo_num].get('kmz')):
                stats['found'] += 1
                continue
            
            files = self.find_files_for_photo(photo_num)
            if files.get('mbtiles') or files.get('kmz'):
                stats['found'] += 1
            else:
                stats['not_found'] += 1
        
        return stats
    
    def search_by_village(self, query: str) -> List[Dict]:
        if not query:
            return []
        
        villages = self.village_db.search(query.lower().strip())
        if not villages:
            return []
        
        all_photos = []
        seen_frames = set()
        
        for village in villages:
            results = self.afs_catalog.search_by_village_name(village['name'])
            for result in results:
                if result['frame'] not in seen_frames:
                    all_photos.append(result['frame'])
                    seen_frames.add(result['frame'])
        
        if not all_photos:
            return []
        
        return [{'id': hash(query), 'villages': [v['name'] for v in villages], 'photos': all_photos}]
    
    def _format_file_link(self, file_info: Dict, label: str) -> str:
        """Форматирует ссылку на файл с датой изменения"""
        version = f"версия {file_info['version']}" if file_info['version'] > 0 else ""
        size = f"{file_info['size_mb']} МБ"
        date = file_info.get('modified', '')
        date_str = f" [{date}]" if date else ""
        
        return f"<a href='{file_info['download_link']}'>📥 {label} {version} {size}{date_str}</a>"
    
    def get_photo_details(self, photo_num: str) -> Optional[str]:
        """Возвращает описание снимка со ссылками и списком НП"""
        details = self.afs_catalog.get_photo_details(photo_num)
        villages = self.afs_catalog.get_villages_for_frame(photo_num)
        
        # Заголовок
        result_text = f"📸 <b>{photo_num}</b>\n\n"
        
        # Описание
        if details and details != f"📸 Снимок {photo_num}":
            result_text += f"{details}\n\n"
        else:
            parts = photo_num.split('-')
            if len(parts) >= 3:
                result_text += f"📍 Квадрат: {parts[0]}\n🖼️ Налет: {parts[1]}\n🎞️ Кадр: {parts[2]}\n\n"
        
        # Список НП (в спойлере)
        if villages:
            result_text += f"<details>\n<summary>📍 <b>Населенные пункты в кадре ({len(villages)})</b> (▼ нажмите для раскрытия)</summary>\n\n"
            for i, v in enumerate(villages, 1):
                result_text += f"{i}. {v}\n"
            result_text += f"\n</details>\n\n"
        else:
            result_text += f"📍 <b>Населенные пункты:</b> ℹ️ Нет данных\n\n"
        
        # Ссылки на файлы
        files = self.photo_files.get(photo_num)
        if not files or (not files.get('mbtiles') and not files.get('kmz')):
            files = self.find_files_for_photo(photo_num)
        
        links = []
        for file_type, label in [('mbtiles', '🗺️ Locus Maps'), ('kmz', '🌍 Google Earth')]:
            for v in files.get(file_type, []):
                links.append(self._format_file_link(v, label))
        
        if links:
            result_text += "📥 <b>Скачать:</b>\n" + "\n".join(links)
        else:
            result_text += "❌ <b>Файлы не найдены</b>\n\n💡 Возможные причины:\n• Снимок не загружен на диск\n• Изменилась структура папок"
        
        return result_text
    
    def get_all_villages_list(self) -> List[str]:
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