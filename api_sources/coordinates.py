# coordinates.py
# Парсинг координат из различных источников

import re
import json
import logging
from typing import Tuple, Optional
from bs4 import BeautifulSoup

from .utils import validate_coordinates

logger = logging.getLogger(__name__)

# Расширенные границы Тверской области (буфер 0.3 градуса)
TVER_BOUNDS_EXTENDED = {
    'min_lat': 54.7,
    'max_lat': 58.8,
    'min_lon': 29.7,
    'max_lon': 38.8
}


def parse_dic_coordinates(text: str, cell=None) -> Tuple[Optional[float], Optional[float]]:
    """Парсит координаты из dic.academic.ru"""
    try:
        if cell:
            geo_span = cell.find('span', class_='geo')
            if geo_span:
                lat_span = geo_span.find('span', class_='latitude')
                lon_span = geo_span.find('span', class_='longitude')
                if lat_span and lon_span:
                    try:
                        lat = float(lat_span.get_text().strip())
                        lon = float(lon_span.get_text().strip())
                        if (TVER_BOUNDS_EXTENDED['min_lat'] <= lat <= TVER_BOUNDS_EXTENDED['max_lat'] and
                            TVER_BOUNDS_EXTENDED['min_lon'] <= lon <= TVER_BOUNDS_EXTENDED['max_lon']):
                            return lat, lon
                    except:
                        pass
        
        dms_pattern = r'(\d+)°(\d+)′([\d.]+)″.*?(\d+)°(\d+)′([\d.]+)″'
        match = re.search(dms_pattern, text)
        if match:
            lat_deg, lat_min, lat_sec = map(float, match.group(1, 2, 3))
            lon_deg, lon_min, lon_sec = map(float, match.group(4, 5, 6))
            lat = lat_deg + lat_min/60 + lat_sec/3600
            lon = lon_deg + lon_min/60 + lon_sec/3600
            if (TVER_BOUNDS_EXTENDED['min_lat'] <= lat <= TVER_BOUNDS_EXTENDED['max_lat'] and
                TVER_BOUNDS_EXTENDED['min_lon'] <= lon <= TVER_BOUNDS_EXTENDED['max_lon']):
                return lat, lon
        
        decimal_pattern = r'([0-9]+\.[0-9]+)[,\s]+([0-9]+\.[0-9]+)'
        match = re.search(decimal_pattern, text)
        if match:
            lat = float(match.group(1))
            lon = float(match.group(2))
            if (TVER_BOUNDS_EXTENDED['min_lat'] <= lat <= TVER_BOUNDS_EXTENDED['max_lat'] and
                TVER_BOUNDS_EXTENDED['min_lon'] <= lon <= TVER_BOUNDS_EXTENDED['max_lon']):
                return lat, lon
        
        numbers = re.findall(r'[\d.]+', text)
        if len(numbers) >= 2:
            lat = float(numbers[0])
            lon = float(numbers[1])
            if (TVER_BOUNDS_EXTENDED['min_lat'] <= lat <= TVER_BOUNDS_EXTENDED['max_lat'] and
                TVER_BOUNDS_EXTENDED['min_lon'] <= lon <= TVER_BOUNDS_EXTENDED['max_lon']):
                return lat, lon
        
        return None, None
    except Exception:
        return None, None


async def parse_wikipedia_coordinates(html: str, village_name: str) -> Optional[Tuple[str, str]]:
    """
    Парсит координаты из HTML страницы Wikipedia.
    Ищет:
    1. Класс coordinates с data-param (основной формат)
    2. geo span (старый формат)
    3. DMS формат в тексте
    4. Инфобокс с координатами
    """
    try:
        soup = BeautifulSoup(html, 'html.parser')
        
        # ВАРИАНТ 1: Ищем coordinates с data-param
        coord_elem = soup.find('span', class_='coordinates')
        if coord_elem:
            # Пробуем data-mw-kartographer
            maplink = coord_elem.find('a', class_='mw-kartographer-maplink')
            if maplink and maplink.get('data-mw-kartographer'):
                try:
                    data = json.loads(maplink['data-mw-kartographer'])
                    if 'lat' in data and 'lon' in data:
                        lat = float(data['lat'])
                        lon = float(data['lon'])
                        if (TVER_BOUNDS_EXTENDED['min_lat'] <= lat <= TVER_BOUNDS_EXTENDED['max_lat'] and
                            TVER_BOUNDS_EXTENDED['min_lon'] <= lon <= TVER_BOUNDS_EXTENDED['max_lon']):
                            logger.debug(f"          ✅ Wikipedia: найдены координаты через data-param: {lat:.5f}, {lon:.5f}")
                            return (str(round(lat, 5)), str(round(lon, 5)))
                except Exception as e:
                    logger.debug(f"          Ошибка парсинга data-mw-kartographer: {e}")
            
            # Пробуем geo span
            geo = coord_elem.find('span', class_='geo')
            if geo:
                lat_span = geo.find('span', class_='latitude')
                lon_span = geo.find('span', class_='longitude')
                if lat_span and lon_span:
                    try:
                        lat = float(lat_span.get_text().strip())
                        lon = float(lon_span.get_text().strip())
                        if (TVER_BOUNDS_EXTENDED['min_lat'] <= lat <= TVER_BOUNDS_EXTENDED['max_lat'] and
                            TVER_BOUNDS_EXTENDED['min_lon'] <= lon <= TVER_BOUNDS_EXTENDED['max_lon']):
                            logger.debug(f"          ✅ Wikipedia: найдены координаты через geo span: {lat:.5f}, {lon:.5f}")
                            return (str(round(lat, 5)), str(round(lon, 5)))
                    except:
                        pass
            
            # Пробуем DMS формат в coordinates
            coord_text = coord_elem.get_text()
            # Формат: 56°13′41″ с.ш. 34°08′10″ в.д.
            dms_pattern = r'(\d+)°(\d+)′([\d.]+)″\s*([сю])\.[^\d]*(\d+)°(\d+)′([\d.]+)″\s*([зв])\.[^\d]*'
            match = re.search(dms_pattern, coord_text)
            if match:
                try:
                    lat_deg, lat_min, lat_sec, lat_dir = match.group(1, 2, 3, 4)
                    lon_deg, lon_min, lon_sec, lon_dir = match.group(5, 6, 7, 8)
                    
                    lat = float(lat_deg) + float(lat_min)/60 + float(lat_sec)/3600
                    lon = float(lon_deg) + float(lon_min)/60 + float(lon_sec)/3600
                    
                    if lat_dir == 'ю':
                        lat = -lat
                    if lon_dir == 'з':
                        lon = -lon
                    
                    if (TVER_BOUNDS_EXTENDED['min_lat'] <= lat <= TVER_BOUNDS_EXTENDED['max_lat'] and
                        TVER_BOUNDS_EXTENDED['min_lon'] <= lon <= TVER_BOUNDS_EXTENDED['max_lon']):
                        logger.debug(f"          ✅ Wikipedia: найдены координаты через DMS в coordinates: {lat:.5f}, {lon:.5f}")
                        return (str(round(lat, 5)), str(round(lon, 5)))
                except:
                    pass
        
        # ВАРИАНТ 2: Ищем инфобокс с координатами
        infobox = soup.find('table', class_='infobox')
        if infobox:
            for row in infobox.find_all('tr'):
                header = row.find('th')
                if header and ('координаты' in header.get_text().lower()):
                    coord_cell = row.find('td')
                    if coord_cell:
                        # Ищем geo span в инфобоксе
                        geo_span = coord_cell.find('span', class_='geo')
                        if geo_span:
                            lat_span = geo_span.find('span', class_='latitude')
                            lon_span = geo_span.find('span', class_='longitude')
                            if lat_span and lon_span:
                                try:
                                    lat = float(lat_span.get_text().strip())
                                    lon = float(lon_span.get_text().strip())
                                    if (TVER_BOUNDS_EXTENDED['min_lat'] <= lat <= TVER_BOUNDS_EXTENDED['max_lat'] and
                                        TVER_BOUNDS_EXTENDED['min_lon'] <= lon <= TVER_BOUNDS_EXTENDED['max_lon']):
                                        logger.debug(f"          ✅ Wikipedia: найдены координаты в инфобоксе: {lat:.5f}, {lon:.5f}")
                                        return (str(round(lat, 5)), str(round(lon, 5)))
                                except:
                                    pass
                        
                        # Ищем координаты в тексте ячейки
                        coord_text = coord_cell.get_text()
                        dms_pattern = r'(\d+)°(\d+)′([\d.]+)″\s*([сю])\.[^\d]*(\d+)°(\d+)′([\d.]+)″\s*([зв])\.[^\d]*'
                        match = re.search(dms_pattern, coord_text)
                        if match:
                            try:
                                lat_deg, lat_min, lat_sec, lat_dir = match.group(1, 2, 3, 4)
                                lon_deg, lon_min, lon_sec, lon_dir = match.group(5, 6, 7, 8)
                                
                                lat = float(lat_deg) + float(lat_min)/60 + float(lat_sec)/3600
                                lon = float(lon_deg) + float(lon_min)/60 + float(lon_sec)/3600
                                
                                if lat_dir == 'ю':
                                    lat = -lat
                                if lon_dir == 'з':
                                    lon = -lon
                                
                                if (TVER_BOUNDS_EXTENDED['min_lat'] <= lat <= TVER_BOUNDS_EXTENDED['max_lat'] and
                                    TVER_BOUNDS_EXTENDED['min_lon'] <= lon <= TVER_BOUNDS_EXTENDED['max_lon']):
                                    logger.debug(f"          ✅ Wikipedia: найдены координаты через DMS в инфобоксе: {lat:.5f}, {lon:.5f}")
                                    return (str(round(lat, 5)), str(round(lon, 5)))
                            except:
                                pass
        
        # ВАРИАНТ 3: Ищем координаты в формате DMS в любом месте страницы
        text = soup.get_text()
        
        # Формат: 56°13′41″ с. ш. 34°08′10″ в. д.
        dms_pattern = r'(\d+)°(\d+)′([\d.]+)″\s*([сю])\.[\s]*ш\.\s*(\d+)°(\d+)′([\d.]+)″\s*([зв])\.[\s]*д\.'
        match = re.search(dms_pattern, text)
        if not match:
            # Альтернативный формат: 56°13′41″ N 34°08′10″ E
            dms_pattern = r'(\d+)°(\d+)′([\d.]+)″\s*([NS])\s+(\d+)°(\d+)′([\d.]+)″\s*([EW])'
            match = re.search(dms_pattern, text)
        
        if match:
            try:
                lat_deg, lat_min, lat_sec, lat_dir = match.group(1, 2, 3, 4)
                lon_deg, lon_min, lon_sec, lon_dir = match.group(5, 6, 7, 8)
                
                lat = float(lat_deg) + float(lat_min)/60 + float(lat_sec)/3600
                lon = float(lon_deg) + float(lon_min)/60 + float(lon_sec)/3600
                
                if lat_dir in ['S', 'ю']:
                    lat = -lat
                if lon_dir in ['W', 'з']:
                    lon = -lon
                
                if (TVER_BOUNDS_EXTENDED['min_lat'] <= lat <= TVER_BOUNDS_EXTENDED['max_lat'] and
                    TVER_BOUNDS_EXTENDED['min_lon'] <= lon <= TVER_BOUNDS_EXTENDED['max_lon']):
                    logger.debug(f"          ✅ Wikipedia: найдены координаты через DMS в тексте: {lat:.5f}, {lon:.5f}")
                    return (str(round(lat, 5)), str(round(lon, 5)))
            except:
                pass
        
        # ВАРИАНТ 4: Ищем десятичные координаты
        decimal_pattern = r'([0-9]{2}\.[0-9]{4,})[,\s]+([0-9]{2,3}\.[0-9]{4,})'
        match = re.search(decimal_pattern, text)
        if match:
            try:
                lat = float(match.group(1))
                lon = float(match.group(2))
                if (TVER_BOUNDS_EXTENDED['min_lat'] <= lat <= TVER_BOUNDS_EXTENDED['max_lat'] and
                    TVER_BOUNDS_EXTENDED['min_lon'] <= lon <= TVER_BOUNDS_EXTENDED['max_lon']):
                    logger.debug(f"          ✅ Wikipedia: найдены координаты через десятичные: {lat:.5f}, {lon:.5f}")
                    return (str(round(lat, 5)), str(round(lon, 5)))
            except:
                pass
        
        # ВАРИАНТ 5: Ищем координаты в формате с градусами и минутами
        deg_min_pattern = r'(\d+)°(\d+)′\s*([сю])\.[^\d]*(\d+)°(\d+)′\s*([зв])\.[^\d]*'
        match = re.search(deg_min_pattern, text)
        if match:
            try:
                lat_deg, lat_min, lat_dir = match.group(1, 2, 3)
                lon_deg, lon_min, lon_dir = match.group(4, 5, 6)
                
                lat = float(lat_deg) + float(lat_min)/60
                lon = float(lon_deg) + float(lon_min)/60
                
                if lat_dir == 'ю':
                    lat = -lat
                if lon_dir == 'з':
                    lon = -lon
                
                if (TVER_BOUNDS_EXTENDED['min_lat'] <= lat <= TVER_BOUNDS_EXTENDED['max_lat'] and
                    TVER_BOUNDS_EXTENDED['min_lon'] <= lon <= TVER_BOUNDS_EXTENDED['max_lon']):
                    logger.debug(f"          ✅ Wikipedia: найдены координаты через градусы и минуты: {lat:.5f}, {lon:.5f}")
                    return (str(round(lat, 5)), str(round(lon, 5)))
            except:
                pass
        
        return None
        
    except Exception as e:
        logger.debug(f"          ❌ Ошибка парсинга координат Wikipedia: {e}")
        return None