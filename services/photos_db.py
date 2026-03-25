# services/photos_db.py
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
        """Загружает информацию о файлах снимков с Яндекс.Диска"""
        if not self.yd_client.check_root_access():
            return
        
        logger.info("🔍 ПОИСК ФАЙЛОВ НА ЯНДЕКС.ДИСКЕ")
        logger.info("=" * 50)
        
        all_photos = [item['frame'] for item in self.afs_catalog.catalog]
        logger.info(f"📊 Найдено {len(all_photos)} снимков в каталоге АФС")
        
        found_count = 0
        for photo in all_photos:
            parts = photo.split('-')
            if len(parts) >= 3:
                logger.info(f"  🔍 Обработка снимка: {photo}")
                files = self.yd_client.find_map_files(parts[0], parts[1], parts[2])
                if files['mbtiles'] or files['kmz']:
                    self.photo_files[photo] = files
                    found_count += 1
                    logger.info(f"  ✅ Найдены файлы для {photo}")
                else:
                    logger.warning(f"  ❌ Файлы не найдены для {photo}")
        
        logger.info("=" * 50)
        logger.info(f"✅ ЗАГРУЗКА ЗАВЕРШЕНА: найдено {found_count} снимков с файлами")
    
    def _log_stats(self):
        logger.info(f"📊 СТАТИСТИКА:")
        logger.info(f"   • Снимков в каталоге АФС: {len(self.afs_catalog.catalog)}")
        logger.info(f"   • Снимков с файлами: {len(self.photo_files)}")
    
    def search_by_village(self, query: str) -> List[Dict]:
        if not query:
            return []
        
        query_lower = query.lower().strip()
        
        # Поиск по названию деревни
        villages = self.village_db.search(query_lower)
        
        if not villages:
            logger.info(f"❌ Деревня '{query}' не найдена в каталоге населенных пунктов")
            return []
        
        logger.info(f"🔍 ПОИСК СНИМКОВ ДЛЯ ДЕРЕВНИ: {query}")
        logger.info(f"📍 Найдено в каталоге НП: {len(villages)} записей")
        
        all_photos = []
        seen_frames = set()
        all_villages_found = []
        
        for village in villages:
            village_name = village['name']
            logger.info(f"  🔎 Обработка деревни: {village_name}")
            
            results = self.afs_catalog.search_by_village_name(village_name)
            
            for result in results:
                if result['frame'] not in seen_frames:
                    all_photos.append(result['frame'])
                    seen_frames.add(result['frame'])
                    all_villages_found.append(village_name)
                    logger.info(f"    ✅ Найден снимок: {result['frame']}")
        
        if not all_photos:
            logger.info(f"❌ Снимки для деревни '{query}' не найдены")
            return []
        
        result = [{
            'id': hash(query),
            'villages': list(set(all_villages_found)),
            'photos': all_photos
        }]
        
        logger.info(f"📊 ИТОГО: найдено {len(all_photos)} снимков для деревни '{query}'")
        
        return result
    
    def get_photo_details(self, photo_num: str) -> Optional[str]:
        """
        Возвращает полное описание снимка со ссылками на файлы и списком населенных пунктов.
        """
        logger.info(f"📸 ЗАПРОШЕН СНИМОК: {photo_num}")
        
        # Получаем описание из каталога АФС
        details = self.afs_catalog.get_photo_details(photo_num)
        villages = self.afs_catalog.get_villages_for_frame(photo_num)
        
        # Формируем заголовок с названием снимка
        result_text = f"📸 <b>Снимок {photo_num}</b>\n\n"
        
        # Добавляем описание (если есть)
        if details and details != f"📸 Снимок {photo_num}":
            result_text += f"{details}\n\n"
        else:
            # Если нет полного описания, добавляем базовую информацию
            parts = photo_num.split('-')
            if len(parts) >= 3:
                result_text += f"📍 Квадрат: {parts[0]}\n"
                result_text += f"🖼️ Налет: {parts[1]}\n"
                result_text += f"🎞️ Кадр: {parts[2]}\n\n"
        
        # Добавляем список населенных пунктов в кадре
        if villages:
            result_text += f"📍 <b>Населенные пункты в кадре ({len(villages)}):</b>\n"
            
            # Показываем первые 5 деревень
            for i, v in enumerate(villages[:5], 1):
                result_text += f"  {i}. {v}\n"
            
            # Если деревень больше 5, добавляем кнопку "Показать все"
            if len(villages) > 5:
                result_text += f"\n  <i>... и ещё {len(villages) - 5} населенных пунктов</i>"
                result_text += f"\n  🔽 Нажмите на снимок еще раз для просмотра всех НП"
        else:
            result_text += f"📍 <b>Населенные пункты в кадре:</b>\n"
            result_text += f"  ℹ️ Нет данных о населенных пунктах в этом кадре\n"
        
        # Получаем ссылки на файлы с Яндекс.Диска
        files = self.photo_files.get(photo_num, {})
        links = []
        
        for file_type, label in [('mbtiles', '🗺️ Locus Maps'), ('kmz', '🌍 Google Earth KMZ')]:
            for v in files.get(file_type, []):
                version = f"версия {v['version']}" if v['version'] > 0 else ""
                size = f"({v['size_mb']} МБ)"
                links.append(f"<a href='{v['download_link']}'>📥 Загрузить для {label} {version} {size}</a>")
                logger.info(f"  🔗 Найдена ссылка для {label}: версия {v['version']}, {v['size_mb']} МБ")
        
        if links:
            result_text += "\n\n📥 <b>Скачать файлы:</b>\n" + "\n".join(links)
            logger.info(f"  ✅ Добавлено {len(links)} ссылок на скачивание")
        else:
            result_text += "\n\n❌ <b>Файлы не найдены на Яндекс.Диске</b>"
            logger.warning(f"  ❌ Файлы для снимка {photo_num} не найдены")
        
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