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
        self.yandex_disk_available = False
        
        self.user_last_photos: Dict[int, List[str]] = {}
        self.user_last_villages: Dict[int, str] = {}
        self.user_last_query: Dict[int, str] = {}
        
        self._check_yandex_disk()
        self._load_photo_files()
        self._log_stats()
    
    def _check_yandex_disk(self):
        """Проверяет доступность Яндекс.Диска"""
        try:
            self.yandex_disk_available = self.yd_client.check_root_access()
            if self.yandex_disk_available:
                logger.info("✅ Яндекс.Диск доступен")
            else:
                logger.warning("⚠️ Яндекс.Диск недоступен")
        except Exception as e:
            logger.error(f"❌ Ошибка проверки Яндекс.Диска: {e}")
            self.yandex_disk_available = False
    
    def _load_photo_files(self):
        if not self.yandex_disk_available:
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
        if not self.yandex_disk_available:
            return {'mbtiles': [], 'kmz': []}
        
        parts = photo_num.split('-')
        if len(parts) >= 3:
            files = self.yd_client.find_map_files(parts[0], parts[1], parts[2])
            if files['mbtiles'] or files['kmz']:
                self.photo_files[photo_num] = files
                return files
        return {'mbtiles': [], 'kmz': []}
    
    def refresh_all_photo_links(self, progress_callback=None) -> Dict:
        """Обновляет ссылки для всех снимков в каталоге АФС с прогрессом"""
        if not self.yandex_disk_available:
            return {'total': 0, 'found': 0, 'not_found': 0, 'yandex_disk_unavailable': True}
        
        stats = {'total': 0, 'found': 0, 'not_found': 0, 'current': 0}
        
        total_items = len(self.afs_catalog.catalog)
        stats['total'] = total_items
        
        for i, item in enumerate(self.afs_catalog.catalog):
            photo_num = item['frame']
            stats['current'] = i + 1
            
            if progress_callback:
                await progress_callback(stats['current'], stats['total'], photo_num)
            
            if photo_num in self.photo_files and (self.photo_files[photo_num].get('mbtiles') or self.photo_files[photo_num].get('kmz')):
                stats['found'] += 1
                continue
            
            files = self.find_files_for_photo(photo_num)
            if files.get('mbtiles') or files.get('kmz'):
                stats['found'] += 1
            else:
                stats['not_found'] += 1
        
        return stats
    
    async def refresh_all_photo_links_with_progress(self, progress_callback) -> Dict:
        """Обновляет ссылки для всех снимков в каталоге АФС с прогрессом (асинхронная версия)"""
        if not self.yandex_disk_available:
            return {'total': 0, 'found': 0, 'not_found': 0, 'yandex_disk_unavailable': True}
        
        stats = {'total': 0, 'found': 0, 'not_found': 0, 'current': 0}
        
        total_items = len(self.afs_catalog.catalog)
        stats['total'] = total_items
        
        for i, item in enumerate(self.afs_catalog.catalog):
            photo_num = item['frame']
            stats['current'] = i + 1
            
            if progress_callback:
                await progress_callback(stats['current'], stats['total'], photo_num)
            
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
    
    def _format_file_link(self, file_info: Dict, label: str, icon: str) -> str:
        """Форматирует ссылку на файл с красивым отображением"""
        version = f"версия {file_info['version']}" if file_info['version'] > 0 else ""
        size = f"{file_info['size_mb']} МБ"
        date = file_info.get('modified', '')
        
        date_str = f" 📅 {date}" if date else ""
        version_str = f" [{version}]" if version else ""
        
        return f"<a href='{file_info['download_link']}'>{icon} {label}{version_str} {size}{date_str}</a>"
    
    def get_photo_details(self, photo_num: str) -> Optional[str]:
        """
        Возвращает описание снимка со ссылками и списком НП.
        Список НП выводится в сокращенном виде, полный список доступен по кнопке.
        """
        logger.info(f"📸 ЗАПРОШЕН СНИМОК: {photo_num}")
        
        details = self.afs_catalog.get_photo_details(photo_num)
        villages = self.afs_catalog.get_villages_for_frame(photo_num)
        
        result_text = f"📸 <b>{photo_num}</b>\n\n"
        
        if details and details != f"📸 Снимок {photo_num}":
            result_text += f"{details}\n\n"
        else:
            parts = photo_num.split('-')
            if len(parts) >= 3:
                result_text += f"📍 Квадрат: {parts[0]}\n"
                result_text += f"🖼️ Налет: {parts[1]}\n"
                result_text += f"🎞️ Кадр: {parts[2]}\n\n"
        
        if villages:
            result_text += f"📍 <b>Населенные пункты в кадре ({len(villages)}):</b>\n\n"
            
            for i, v in enumerate(villages[:5], 1):
                result_text += f"{i}. {v}\n"
            
            if len(villages) > 5:
                result_text += f"\n<i>... и ещё {len(villages) - 5} населенных пунктов</i>\n"
                result_text += f"🔽 <b>Нажмите кнопку ниже, чтобы увидеть полный список</b>\n"
        else:
            result_text += f"📍 <b>Населенные пункты:</b> ℹ️ Нет данных\n"
        
        result_text += "\n"
        
        files = self.photo_files.get(photo_num)
        if not files or (not files.get('mbtiles') and not files.get('kmz')):
            files = self.find_files_for_photo(photo_num)
        
        links = []
        
        for v in files.get('mbtiles', []):
            links.append(self._format_file_link(v, "Locus Maps", "🗺️"))
        
        for v in files.get('kmz', []):
            links.append(self._format_file_link(v, "Google Earth", "🌍"))
        
        if links:
            result_text += "📥 <b>Скачать файлы:</b>\n" + "\n".join(links)
        else:
            if not self.yandex_disk_available:
                result_text += "❌ <b>Яндекс.Диск недоступен</b>\n\n"
                result_text += "💡 Проверьте подключение к интернету или токен доступа"
            else:
                result_text += "❌ <b>Файлы не найдены на Яндекс.Диске</b>\n\n"
                result_text += "💡 Возможные причины:\n"
                result_text += "• Снимок не загружен на диск\n"
                result_text += "• Изменилась структура папок\n"
                result_text += "• Файлы еще не проиндексированы"
        
        return result_text
    
    def get_photo_details_with_full_villages(self, photo_num: str) -> Optional[str]:
        """
        Возвращает описание снимка с ПОЛНЫМ списком населенных пунктов.
        Используется для кнопки "Показать все НП"
        """
        logger.info(f"📸 ЗАПРОШЕН ПОЛНЫЙ СПИСОК НП ДЛЯ СНИМКА: {photo_num}")
        
        details = self.afs_catalog.get_photo_details(photo_num)
        villages = self.afs_catalog.get_villages_for_frame(photo_num)
        
        result_text = f"📸 <b>{photo_num}</b>\n\n"
        
        if details and details != f"📸 Снимок {photo_num}":
            result_text += f"{details}\n\n"
        else:
            parts = photo_num.split('-')
            if len(parts) >= 3:
                result_text += f"📍 Квадрат: {parts[0]}\n"
                result_text += f"🖼️ Налет: {parts[1]}\n"
                result_text += f"🎞️ Кадр: {parts[2]}\n\n"
        
        if villages:
            result_text += f"📍 <b>Все населенные пункты в кадре ({len(villages)}):</b>\n\n"
            for i, v in enumerate(villages, 1):
                result_text += f"{i}. {v}\n"
        else:
            result_text += f"📍 <b>Населенные пункты:</b> ℹ️ Нет данных\n"
        
        result_text += "\n"
        
        files = self.photo_files.get(photo_num)
        if not files or (not files.get('mbtiles') and not files.get('kmz')):
            files = self.find_files_for_photo(photo_num)
        
        links = []
        
        for v in files.get('mbtiles', []):
            links.append(self._format_file_link(v, "Locus Maps", "🗺️"))
        
        for v in files.get('kmz', []):
            links.append(self._format_file_link(v, "Google Earth", "🌍"))
        
        if links:
            result_text += "📥 <b>Скачать файлы:</b>\n" + "\n".join(links)
        else:
            if not self.yandex_disk_available:
                result_text += "❌ <b>Яндекс.Диск недоступен</b>"
            else:
                result_text += "❌ <b>Файлы не найдены на Яндекс.Диске</b>"
        
        return result_text
    
    def get_all_villages_list(self) -> List[str]:
        return sorted([v['name'] for v in self.village_db.villages])
    
    def get_yandex_disk_status(self) -> bool:
        """Возвращает статус доступности Яндекс.Диска"""
        return self.yandex_disk_available
    
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