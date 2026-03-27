"""
Калькулятор координат для пересчета военных захоронений
из системы координат топографических карт (километровая сетка)
в географические координаты (широта/долгота) и СК-42
"""

import math
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class MapSheetParams:
    """Параметры номенклатурного листа карты"""
    zone: str           # Номенклатурная зона (O35, O36, N36 и т.д.)
    sheet: str          # Номенклатурный лист (O36-141)
    epsg: str           # Система координат (EPSG:28405, EPSG:28406, EPSG:28407)
    scale: int          # Масштаб (100000)
    offset_x: float     # Смещение по X (север) в метрах
    offset_y: float     # Смещение по Y (восток) в метрах
    dx: float           # Поправка по X для СК-42
    dy: float           # Поправка по Y для СК-42


class CoordCalculator:
    """
    Калькулятор координат для пересчета военных захоронений
    из километровой сетки топографических карт в географические координаты
    """
    
    # Параметры зон (постоянные значения для разных номенклатур)
    ZONE_PARAMS = {
        # Зона 5 (EPSG:28405)
        'O35': {'epsg': 'EPSG:28405', 'dx': 807, 'dy': 83, 'offset_x': 6500000, 'offset_y': 5500000},
        'P35': {'epsg': 'EPSG:28405', 'dx': 808, 'dy': 81, 'offset_x': 6650000, 'offset_y': 5500000},
        # Зона 6 (EPSG:28406)
        'N36': {'epsg': 'EPSG:28406', 'dx': 802, 'dy': 34, 'offset_x': 5770000, 'offset_y': 6210000},
        'O36': {'epsg': 'EPSG:28406', 'dx': 802, 'dy': 34, 'offset_x': 6200000, 'offset_y': 6500000},
        'P36': {'epsg': 'EPSG:28406', 'dx': 800, 'dy': 34, 'offset_x': 6650000, 'offset_y': 6650000},
        # Зона 7 (EPSG:28407)
        'N37': {'epsg': 'EPSG:28407', 'dx': 801, 'dy': -13, 'offset_x': 5770000, 'offset_y': 6210000},
        'O37': {'epsg': 'EPSG:28407', 'dx': 801, 'dy': -13, 'offset_x': 6210000, 'offset_y': 6650000},
        'P37': {'epsg': 'EPSG:28407', 'dx': 800, 'dy': -15, 'offset_x': 6650000, 'offset_y': 7100000},
    }
    
    # Дополнительные параметры для листов (из файла)
    SHEET_PARAMS = {
        'O36-141': {'zone': 'O36', 'offset_x': 6200000, 'offset_y': 6500000},
        'O35-21/22': {'zone': 'O35', 'offset_x': 6500000, 'offset_y': 5500000},
        'N36-017': {'zone': 'N36', 'offset_x': 5770000, 'offset_y': 6210000},
    }
    
    @classmethod
    def get_zone_params(cls, zone: str) -> Optional[Dict]:
        """Получить параметры зоны по номенклатуре"""
        return cls.ZONE_PARAMS.get(zone)
    
    @classmethod
    def get_sheet_params(cls, sheet: str) -> Optional[Dict]:
        """Получить параметры листа по номенклатурному листу"""
        return cls.SHEET_PARAMS.get(sheet)
    
    @classmethod
    def calculate_full_coords(cls, offset_x: float, offset_y: float, 
                               x_doc: float, y_doc: float) -> Tuple[float, float]:
        """
        Вычисляет полные координаты (X полный, Y полный)
        X полный = Смещение X + X как в документе
        Y полный = Смещение Y + Y как в документе
        """
        x_full = offset_x + x_doc
        y_full = offset_y + y_doc
        return x_full, y_full
    
    @classmethod
    def calculate_sk42_coords(cls, x_full: float, y_full: float, 
                               zone: str) -> Tuple[float, float]:
        """
        Вычисляет координаты в системе СК-42
        X СК-42 = X полный + Dx (поправка по X)
        Y СК-42 = Y полный + Dy (поправка по Y)
        """
        zone_params = cls.get_zone_params(zone)
        if not zone_params:
            logger.warning(f"Зона {zone} не найдена в параметрах")
            return x_full, y_full
        
        x_sk42 = x_full + zone_params['dx']
        y_sk42 = y_full + zone_params['dy']
        return x_sk42, y_sk42
    
    @classmethod
    def sk42_to_geographic(cls, x: float, y: float, zone: str) -> Tuple[float, float]:
        """
        Переводит координаты из СК-42 (зональные) в географические (широта, долгота)
        Упрощенная формула для зон 5, 6, 7
        """
        # Параметры для разных зон (центральные меридианы)
        zone_meridians = {
            'O35': 27,   # 27° восточной долготы
            'P35': 27,
            'N36': 33,   # 33° восточной долготы
            'O36': 33,
            'P36': 33,
            'N37': 39,   # 39° восточной долготы
            'O37': 39,
            'P37': 39,
        }
        
        zone_params = cls.get_zone_params(zone)
        if not zone_params:
            return 0, 0
        
        # Получаем центральный меридиан зоны
        central_meridian = zone_meridians.get(zone, 33)
        
        # Коэффициенты для пересчета метров в градусы
        # 1 градус широты ≈ 111 км (111000 м)
        # 1 градус долготы ≈ 85 км (85000 м) на широте 56°
        lat_meters_per_deg = 111000
        lon_meters_per_deg = 85000
        
        # Вычисляем широту и долготу относительно центральных точек
        # Для зоны O36: центральная точка ~ 56° с.ш., 33° в.д.
        if zone in ['O36', 'N36', 'P36']:
            base_lat = 56.0
            base_lon = 33.0
        elif zone in ['O35', 'P35']:
            base_lat = 56.0
            base_lon = 27.0
        elif zone in ['O37', 'N37', 'P37']:
            base_lat = 56.0
            base_lon = 39.0
        else:
            base_lat = 56.0
            base_lon = 33.0
        
        # Пересчет метров в градусы
        lat = base_lat + (x / lat_meters_per_deg)
        lon = base_lon + (y / lon_meters_per_deg)
        
        return lat, lon
    
    @classmethod
    def process_burial_coords(cls, zone: str, sheet: str, 
                               x_doc: float, y_doc: float,
                               offset_x: float = None, offset_y: float = None) -> Dict:
        """
        Основной метод для обработки координат захоронения
        Возвращает словарь с результатами всех этапов пересчета
        """
        result = {
            'zone': zone,
            'sheet': sheet,
            'x_doc': x_doc,
            'y_doc': y_doc,
            'success': False,
            'errors': []
        }
        
        # Получаем параметры зоны
        zone_params = cls.get_zone_params(zone)
        if not zone_params:
            result['errors'].append(f"Зона {zone} не найдена в параметрах")
            return result
        
        # Определяем смещения
        if offset_x is None:
            offset_x = zone_params['offset_x']
        if offset_y is None:
            offset_y = zone_params['offset_y']
        
        result['offset_x'] = offset_x
        result['offset_y'] = offset_y
        
        # Этап 1: Вычисление полных координат
        x_full, y_full = cls.calculate_full_coords(offset_x, offset_y, x_doc, y_doc)
        result['x_full'] = x_full
        result['y_full'] = y_full
        
        # Этап 2: Вычисление координат СК-42
        x_sk42, y_sk42 = cls.calculate_sk42_coords(x_full, y_full, zone)
        result['x_sk42'] = x_sk42
        result['y_sk42'] = y_sk42
        
        # Этап 3: Перевод в географические координаты
        lat, lon = cls.sk42_to_geographic(x_sk42, y_sk42, zone)
        result['latitude'] = lat
        result['longitude'] = lon
        
        result['success'] = True
        return result
    
    @classmethod
    def format_coordinates(cls, lat: float, lon: float, format_type: str = 'dd') -> str:
        """
        Форматирует координаты в нужный формат
        format_type: 'dd' - десятичные градусы, 'dms' - градусы/минуты/секунды
        """
        if format_type == 'dd':
            return f"{lat:.6f}°, {lon:.6f}°"
        elif format_type == 'dms':
            lat_deg = int(lat)
            lat_min = int((lat - lat_deg) * 60)
            lat_sec = ((lat - lat_deg) * 60 - lat_min) * 60
            lon_deg = int(lon)
            lon_min = int((lon - lon_deg) * 60)
            lon_sec = ((lon - lon_deg) * 60 - lon_min) * 60
            return f"{lat_deg}°{lat_min:02d}'{lat_sec:.1f}″ N, {lon_deg}°{lon_min:02d}'{lon_sec:.1f}″ E"
        return f"{lat:.6f}, {lon:.6f}"
    
    @classmethod
    def get_supported_zones(cls) -> List[str]:
        """Возвращает список поддерживаемых зон"""
        return list(cls.ZONE_PARAMS.keys())
    
    @classmethod
    def add_custom_zone(cls, zone: str, epsg: str, dx: float, dy: float,
                         offset_x: float, offset_y: float) -> bool:
        """Добавляет пользовательскую зону в параметры"""
        try:
            cls.ZONE_PARAMS[zone] = {
                'epsg': epsg,
                'dx': dx,
                'dy': dy,
                'offset_x': offset_x,
                'offset_y': offset_y
            }
            logger.info(f"Добавлена зона: {zone}")
            return True
        except Exception as e:
            logger.error(f"Ошибка добавления зоны: {e}")
            return False