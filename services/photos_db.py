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
                    logger.info(f"  ✅ Найдены файлы для {photo}: MBTILES={len(files['mbtiles'])}, KMZ={len(files['kmz'])}")
                else:
                    logger.warning(f"  ❌ Файлы не найдены для {photo}")
        
        logger.info("=" * 50)
        logger.info(f"✅ ЗАГРУЗКА ЗАВЕРШЕНА: найдено {found_count} снимков с файлами из {len(all_photos)}")
    
    def _log_stats(self):
        logger.info(f"📊 СТАТИСТИКА:")
        logger.info(f"   • Снимков в каталоге АФС: {len(self.afs_catalog.catalog)}")
        logger.info(f"   • Снимков с файлами на Яндекс.Диске: {len(self.photo_files)}")
    
    def search_by_village(self, query: str) -> List[Dict]:
        """Поиск снимков по названию деревни, координатам или номеру снимка"""
        if not query:
            return []
        
        query_lower = query.lower().strip()
        
        # Проверяем, является ли запрос координатами
        coords = self._parse_coordinates_input(query)
        if coords:
            lat, lon = coords
            return self.search_by_coordinates(lat, lon)
        
        # Проверяем, является ли запрос номером снимка
        if self._is_frame_number(query):
            return self.search_by_frame_name(query)
        
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
                    logger.info(f"    ✅ Найден снимок: {result['frame']} (деревня: {village_name})")
        
        if not all_photos:
            logger.info(f"❌ Снимки для деревни '{query}' не найдены")
            return []
        
        # Группируем результаты
        result = [{
            'id': hash(query),
            'villages': list(set(all_villages_found)),
            'photos': all_photos
        }]
        
        logger.info(f"📊 ИТОГО: найдено {len(all_photos)} снимков для деревни '{query}'")
        
        return result
    
    def search_by_coordinates(self, lat: float, lon: float, tolerance_km: float = 5.0) -> List[Dict]:
        """Поиск снимков по координатам"""
        logger.info(f"🔍 ПОИСК СНИМКОВ ПО КООРДИНАТАМ: {lat}, {lon} (точность ±{tolerance_km} км)")
        
        results = self.afs_catalog.search_by_coordinates(lat, lon, tolerance_km)
        
        if not results:
            logger.info(f"❌ Снимки по координатам не найдены")
            return []
        
        photos = [r['frame'] for r in results]
        all_villages = []
        for r in results:
            all_villages.extend(r.get('villages', []))
        
        result = [{
            'id': hash(f"{lat}_{lon}"),
            'villages': list(set(all_villages)) if all_villages else [f"Координаты: {lat}, {lon}"],
            'photos': photos,
            'distances': {r['frame']: r['distance_km'] for r in results}
        }]
        
        logger.info(f"📊 ИТОГО: найдено {len(photos)} снимков по координатам")
        
        return result
    
    def search_by_frame_name(self, frame_name: str) -> List[Dict]:
        """Поиск снимка по названию (номеру снимка)"""
        logger.info(f"🔍 ПОИСК СНИМКА ПО НАЗВАНИЮ: {frame_name}")
        
        results = self.afs_catalog.search_by_frame_name(frame_name)
        
        if not results:
            logger.info(f"❌ Снимок '{frame_name}' не найден")
            return []
        
        photos = [r['frame'] for r in results]
        all_villages = []
        for r in results:
            all_villages.extend(r.get('villages', []))
        
        result = [{
            'id': hash(frame_name),
            'villages': list(set(all_villages)) if all_villages else [f"Снимок: {frame_name}"],
            'photos': photos
        }]
        
        logger.info(f"📊 ИТОГО: найден {len(photos)} снимок по названию")
        
        return result
    
    def get_photo_details(self, photo_num: str) -> Optional[str]:
        """Возвращает описание снимка и ссылки на файлы"""
        logger.info(f"📸 ЗАПРОШЕН СНИМОК: {photo_num}")
        
        details = self.afs_catalog.get_photo_details(photo_num)
        villages = self.afs_catalog.get_villages_for_frame(photo_num)
        
        if not details:
            details = f"📸 Снимок {photo_num}"
            logger.info(f"  ℹ️ Описание не найдено, используется стандартное")
        else:
            logger.info(f"  ✅ Найдено описание: {details[:100]}...")
        
        # Добавляем информацию о деревнях
        if villages:
            villages_text = f"\n\n📍 <b>Населенные пункты в кадре ({len(villages)}):</b>\n" + "\n".join([f"• {v}" for v in villages[:20]])
            if len(villages) > 20:
                villages_text += f"\n... и ещё {len(villages)-20}"
            details += villages_text
            logger.info(f"  📍 Добавлено {len(villages)} населенных пунктов")
        
        files = self.photo_files.get(photo_num, {})
        links = []
        
        for file_type, label in [('mbtiles', '🗺️ Locus Maps'), ('kmz', '🌍 Google Earth KMZ')]:
            for v in files.get(file_type, []):
                version = f"версия {v['version']}" if v['version'] > 0 else ""
                size = f"({v['size_mb']} МБ)"
                links.append(f"<a href='{v['download_link']}'>📥 Загрузить для {label} {version} {size}</a>")
                logger.info(f"  🔗 Найдена ссылка для {label}: версия {v['version']}, {v['size_mb']} МБ")
        
        if links:
            details += "\n\n" + "\n".join(links)
            logger.info(f"  ✅ Добавлено {len(links)} ссылок на скачивание")
        else:
            details += "\n\n❌ Файлы не найдены на Яндекс.Диске"
            logger.warning(f"  ❌ Файлы для снимка {photo_num} не найдены")
        
        return details
    
    def get_all_villages_list(self) -> List[str]:
        """Возвращает отсортированный список всех деревень из каталога НП"""
        return sorted([v['name'] for v in self.village_db.villages])
    
    def _parse_coordinates_input(self, text: str) -> Optional[Tuple[float, float]]:
        """Парсит ввод пользователя как координаты в разных форматах"""
        text = text.strip()
        
        # Формат: 56.2345, 34.1234 или 56.2345 34.1234
        decimal_pattern = r'^(\d{1,3}\.\d{4,})\s*[,\s]\s*(\d{1,3}\.\d{4,})$'
        match = re.match(decimal_pattern, text)
        if match:
            try:
                lat = float(match.group(1))
                lon = float(match.group(2))
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    return (lat, lon)
            except:
                pass
        
        # Формат: 56°13'41" с.ш. 34°08'10" в.д.
        dms_pattern = r'(\d+)°(\d+)′([\d.]+)″\s*([сю])\.[^\d]*(\d+)°(\d+)′([\d.]+)″\s*([зв])\.[^\d]*'
        match = re.match(dms_pattern, text, re.IGNORECASE)
        if match:
            try:
                lat_deg, lat_min, lat_sec, lat_dir = match.group(1), match.group(2), match.group(3), match.group(4)
                lon_deg, lon_min, lon_sec, lon_dir = match.group(5), match.group(6), match.group(7), match.group(8)
                
                lat = float(lat_deg) + float(lat_min)/60 + float(lat_sec)/3600
                lon = float(lon_deg) + float(lon_min)/60 + float(lon_sec)/3600
                
                if lat_dir.lower() == 'ю':
                    lat = -lat
                if lon_dir.lower() == 'з':
                    lon = -lon
                
                return (lat, lon)
            except:
                pass
        
        # Формат: 56°13'41″ N 34°08'10″ E
        dms_lat_pattern = r'(\d+)°(\d+)′([\d.]+)″\s*([NS])'
        dms_lon_pattern = r'(\d+)°(\d+)′([\d.]+)″\s*([EW])'
        
        lat_match = re.search(dms_lat_pattern, text, re.IGNORECASE)
        lon_match = re.search(dms_lon_pattern, text, re.IGNORECASE)
        
        if lat_match and lon_match:
            try:
                lat_deg, lat_min, lat_sec, lat_dir = lat_match.groups()
                lon_deg, lon_min, lon_sec, lon_dir = lon_match.groups()
                
                lat = float(lat_deg) + float(lat_min)/60 + float(lat_sec)/3600
                lon = float(lon_deg) + float(lon_min)/60 + float(lon_sec)/3600
                
                if lat_dir.upper() == 'S':
                    lat = -lat
                if lon_dir.upper() == 'W':
                    lon = -lon
                
                return (lat, lon)
            except:
                pass
        
        return None
    
    def _is_frame_number(self, text: str) -> bool:
        """Проверяет, является ли текст номером снимка (Frame-XXX)"""
        pattern = r'^[A-Z]?\d+[A-Z]?\d*-\d+-\d+$'
        return bool(re.match(pattern, text.upper()))
    
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