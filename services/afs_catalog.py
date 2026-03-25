# services/afs_catalog.py
import os
import time
import logging
import re
from typing import List, Dict, Optional, Tuple

from config import AFS_CATALOG_FILE

logger = logging.getLogger(__name__)


class AFSCatalog:
    """Класс для работы с каталогом аэрофотоснимков (АФС)"""
    
    def __init__(self):
        self.catalog: List[Dict] = []
        self._load()
    
    def _load(self):
        """Загружает каталог из файла"""
        if not os.path.exists(AFS_CATALOG_FILE):
            self._create_empty()
            return
        
        try:
            with open(AFS_CATALOG_FILE, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                if not lines:
                    self._create_empty()
                    return
                
                for line in lines[1:]:  # Пропускаем заголовок
                    line = line.strip()
                    if not line:
                        continue
                    
                    parts = line.split('|')
                    if len(parts) >= 2:
                        self.catalog.append({
                            'frame': parts[0],
                            'description': parts[1] if len(parts) > 1 and parts[1] != 'None' else ''
                        })
            
            logger.info(f"✅ Загружено {len(self.catalog)} снимков в каталог АФС")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки каталога АФС: {e}")
            self._create_empty()
    
    def _create_empty(self):
        """Создает пустой каталог"""
        os.makedirs(os.path.dirname(AFS_CATALOG_FILE), exist_ok=True)
        with open(AFS_CATALOG_FILE, 'w', encoding='utf-8') as f:
            f.write("Номер_снимка|Описание\n")
        self.catalog = []
    
    def _save(self):
        """Сохраняет каталог в файл"""
        try:
            with open(AFS_CATALOG_FILE, 'w', encoding='utf-8') as f:
                f.write("Номер_снимка|Описание\n")
                for item in self.catalog:
                    description = item.get('description', '')
                    if description is None:
                        description = ''
                    description = str(description).replace('|', '\\|').replace('\n', ' ')
                    f.write(f"{item['frame']}|{description}\n")
            logger.debug(f"✅ Каталог АФС сохранен, записей: {len(self.catalog)}")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения каталога АФС: {e}")
    
    def create_from_kml_results(self, results: List[Dict], frames_without_np: List[Dict]) -> Dict:
        """Создает каталог АФС из ВСЕХ результатов обработки KML"""
        stats = {'added': 0, 'duplicates': 0, 'total': 0, 'with_np': 0, 'without_np': 0}
        
        existing_frames = {item['frame'] for item in self.catalog}
        
        all_frames = []
        for result in results:
            all_frames.append({
                'frame': result.get('photo_num', ''),
                'description': result.get('description', ''),
                'has_np': True
            })
        
        for frame_data in frames_without_np:
            all_frames.append({
                'frame': frame_data.get('frame', ''),
                'description': frame_data.get('description', ''),
                'has_np': False
            })
        
        logger.info(f"📁 СОЗДАНИЕ КАТАЛОГА АФС из {len(all_frames)} снимков")
        
        for frame_data in all_frames:
            frame = frame_data['frame']
            description = frame_data['description']
            has_np = frame_data['has_np']
            
            if not frame:
                continue
            
            if frame in existing_frames:
                stats['duplicates'] += 1
                continue
            
            self.catalog.append({
                'frame': frame,
                'description': description
            })
            stats['added'] += 1
            existing_frames.add(frame)
            
            if has_np:
                stats['with_np'] += 1
            else:
                stats['without_np'] += 1
        
        stats['total'] = len(self.catalog)
        self._save()
        
        logger.info(f"✅ Создание каталога АФС завершено: добавлено {stats['added']}, с НП: {stats['with_np']}, без НП: {stats['without_np']}")
        
        return stats
    
    def get_catalog(self) -> List[Dict]:
        """Возвращает копию каталога"""
        return self.catalog.copy()
    
    def get_statistics(self) -> Dict:
        """Возвращает расширенную статистику каталога"""
        total = len(self.catalog)
        with_description = sum(1 for item in self.catalog if item.get('description') and item['description'].strip())
        
        recent_items = []
        if self.catalog:
            recent_items = self.catalog[-5:]
        
        desc_lengths = [len(str(item.get('description', ''))) for item in self.catalog if item.get('description')]
        avg_desc_length = sum(desc_lengths) / len(desc_lengths) if desc_lengths else 0
        
        return {
            'total': total,
            'with_description': with_description,
            'without_description': total - with_description,
            'recent_items': recent_items,
            'avg_description_length': round(avg_desc_length, 0)
        }
    
    def get_photo_details(self, photo_num: str) -> Optional[str]:
        """Возвращает описание снимка из каталога АФС"""
        for item in self.catalog:
            if item['frame'] == photo_num:
                return item.get('description', '')
        return None
    
    def search_by_village_name(self, village_name: str) -> List[Dict]:
        """Поиск снимков по названию деревни в описании"""
        if not village_name or not self.catalog:
            return []
        
        village_lower = village_name.lower().strip()
        results = []
        
        logger.info(f"🔍 ПОИСК снимков для деревни: {village_name}")
        
        for item in self.catalog:
            description = item.get('description', '').lower()
            frame = item['frame']
            
            if village_lower in description:
                results.append({
                    'frame': frame,
                    'description': item.get('description', '')
                })
                logger.info(f"  ✅ Найден снимок: {frame} (описание содержит '{village_name}')")
        
        logger.info(f"📊 Найдено {len(results)} снимков для деревни '{village_name}'")
        return results
    
    def search_by_coordinates(self, lat: float, lon: float, tolerance_km: float = 5.0) -> List[Dict]:
        """Поиск снимков по координатам"""
        if not self.catalog:
            return []
        
        results = []
        tolerance_deg = tolerance_km / 111.0
        
        logger.info(f"🔍 ПОИСК снимков по координатам: {lat}, {lon} (точность ±{tolerance_km} км)")
        
        for item in self.catalog:
            description = item.get('description', '')
            frame = item['frame']
            
            coords_found = self._extract_coordinates_from_text(description)
            
            for desc_lat, desc_lon in coords_found:
                distance = self._haversine_distance(lat, lon, desc_lat, desc_lon)
                if distance <= tolerance_km:
                    results.append({
                        'frame': frame,
                        'description': description,
                        'distance_km': round(distance, 2)
                    })
                    logger.info(f"  ✅ Найден снимок: {frame} (расстояние {distance:.2f} км)")
                    break
        
        logger.info(f"📊 Найдено {len(results)} снимков по координатам")
        return results
    
    def search_by_frame_name(self, frame_name: str) -> List[Dict]:
        """Поиск снимка по названию (номеру снимка)"""
        if not frame_name or not self.catalog:
            return []
        
        frame_lower = frame_name.lower().strip()
        results = []
        
        logger.info(f"🔍 ПОИСК снимка по названию: {frame_name}")
        
        for item in self.catalog:
            frame = item['frame']
            if frame_lower in frame.lower():
                results.append({
                    'frame': frame,
                    'description': item.get('description', '')
                })
                logger.info(f"  ✅ Найден снимок: {frame}")
        
        logger.info(f"📊 Найдено {len(results)} снимков по названию '{frame_name}'")
        return results
    
    def _extract_coordinates_from_text(self, text: str) -> List[Tuple[float, float]]:
        """Извлекает координаты из текста в разных форматах"""
        coords = []
        
        # Формат: 56.2345, 34.1234 или 56.2345 34.1234
        decimal_pattern = r'(\d{1,3}\.\d{4,})\s*[,\s]\s*(\d{1,3}\.\d{4,})'
        matches = re.findall(decimal_pattern, text)
        for match in matches:
            try:
                lat = float(match[0])
                lon = float(match[1])
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    coords.append((lat, lon))
            except:
                pass
        
        # Формат: 56°13'41" с.ш. 34°08'10" в.д.
        dms_pattern = r'(\d+)°(\d+)′([\d.]+)″\s*([сю])\.[^\d]*(\d+)°(\d+)′([\d.]+)″\s*([зв])\.[^\d]*'
        matches = re.findall(dms_pattern, text, re.IGNORECASE)
        for match in matches:
            try:
                lat_deg, lat_min, lat_sec, lat_dir = match[0], match[1], match[2], match[3]
                lon_deg, lon_min, lon_sec, lon_dir = match[4], match[5], match[6], match[7]
                
                lat = float(lat_deg) + float(lat_min)/60 + float(lat_sec)/3600
                lon = float(lon_deg) + float(lon_min)/60 + float(lon_sec)/3600
                
                if lat_dir.lower() == 'ю':
                    lat = -lat
                if lon_dir.lower() == 'з':
                    lon = -lon
                
                coords.append((lat, lon))
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
                
                coords.append((lat, lon))
            except:
                pass
        
        return coords
    
    def _haversine_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """Вычисляет расстояние между двумя точками на сфере (в км)"""
        from math import radians, sin, cos, sqrt, atan2
        
        R = 6371.0
        
        lat1_rad = radians(lat1)
        lon1_rad = radians(lon1)
        lat2_rad = radians(lat2)
        lon2_rad = radians(lon2)
        
        dlat = lat2_rad - lat1_rad
        dlon = lon2_rad - lon1_rad
        
        a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        
        return R * c
    
    def clear(self) -> int:
        """Очищает каталог"""
        removed = len(self.catalog)
        self.catalog = []
        self._save()
        logger.info(f"🗑️ Каталог АФС очищен, удалено {removed} снимков")
        return removed
    
    def is_empty(self) -> bool:
        return len(self.catalog) == 0