# api_sources.py
# Модуль для загрузки данных из различных источников
# Основной фокус: парсинг dic.academic.ru для Бельского района

import aiohttp
import asyncio
import logging
from typing import List, Dict, Optional, Tuple
import os
import time
import re
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

# Координаты центров районов Тверской области
DISTRICTS = {
    "Ржевский": {
        "center_lat": 56.2628,
        "center_lon": 34.3289,
        "radius": 50000  # 50 км
    },
    "Бельский": {
        "center_lat": 55.8500,
        "center_lon": 33.2500,
        "radius": 40000  # 40 км
    },
    "Оленинский": {
        "center_lat": 56.2000,
        "center_lon": 33.4833,
        "radius": 45000
    },
    "Зубцовский": {
        "center_lat": 56.1667,
        "center_lon": 34.5833,
        "radius": 45000
    }
}

class APISourceManager:
    """
    Менеджер для загрузки данных из различных источников.
    В текущей версии: парсинг dic.academic.ru для Бельского района
    """
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.thread_pool = ThreadPoolExecutor(max_workers=2)
        
        # Для rate limiting
        self.request_count = 0
        self.last_request_time = 0
        self.min_request_interval = 0.5  # 500 мс между запросами
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Получает или создает HTTP сессию"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def close_session(self):
        """Закрывает HTTP сессию"""
        if self.session and not self.session.closed:
            await self.session.close()
        self.thread_pool.shutdown(wait=False)
    
    async def _rate_limit(self):
        """Соблюдение rate limiting"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()
        self.request_count += 1
    
    async def fetch_district_data(self, district: str) -> Dict[str, List[Dict]]:
        """
        Загружает данные для конкретного района из всех доступных источников
        """
        if district not in DISTRICTS:
            raise ValueError(f"Неизвестный район: {district}")
        
        logger.info(f"🌐 Загрузка данных для района: {district}")
        
        results = {
            "google_places": [],  # Оставляем для совместимости, но не используем
            "academic_ru": [],
            "total": []
        }
        
        try:
            # Для Бельского района парсим dic.academic.ru
            if district == "Бельский":
                academic_results = await self.fetch_academic_ru_villages()
                results["academic_ru"] = academic_results
                results["total"].extend(academic_results)
                logger.info(f"  ✅ Academic.ru: {len(academic_results)} записей")
            else:
                # Для других районов пока нет источников
                logger.info(f"  ⚠️ Для района {district} пока нет источников данных")
            
            logger.info(f"  ✅ Всего уникальных записей: {len(results['total'])}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке данных для района {district}: {e}")
        
        return results
    
    async def fetch_academic_ru_villages(self) -> List[Dict]:
        """
        Парсит страницу со списком бывших населённых пунктов Бельского района
        с сайта dic.academic.ru
        Использует точные селекторы на основе анализа HTML структуры
        """
        logger.info("  🔍 Парсинг сайта dic.academic.ru (Бельский район)...")
        url = "https://dic.academic.ru/dic.nsf/ruwiki/1635988"
        results = []
        
        try:
            session = await self._get_session()
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
                'Connection': 'keep-alive',
            }
            
            await self._rate_limit()
            async with session.get(url, headers=headers, timeout=30) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    # Парсим HTML в отдельном потоке
                    loop = asyncio.get_event_loop()
                    villages = await loop.run_in_executor(
                        self.thread_pool,
                        self._parse_academic_ru_html,
                        html
                    )
                    
                    results.extend(villages)
                    logger.info(f"    ✅ Найдено бывших НП: {len(results)}")
                    
                    # Для отладки сохраняем первые несколько записей
                    if results:
                        logger.debug(f"    Пример: {results[0]}")
                    
                else:
                    logger.error(f"    ❌ Ошибка загрузки страницы: HTTP {response.status}")
                    
        except asyncio.TimeoutError:
            logger.error("    ❌ Таймаут при загрузке dic.academic.ru")
        except Exception as e:
            logger.error(f"    ❌ Ошибка при парсинге dic.academic.ru: {e}")
        
        return results
    
    def _parse_academic_ru_html(self, html: str) -> List[Dict]:
        """
        Парсит HTML страницы dic.academic.ru
        Извлекает только необходимые поля:
        - название
        - координаты (из скрытого span с классом geo)
        - тип населенного пункта (удаляется сокращение)
        - источник указывается в notes
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            villages = []
            
            # Ищем таблицу с классом "standard sortable" (основная таблица с данными)
            table = soup.find('table', class_='standard sortable')
            
            if not table:
                logger.error("    ❌ Таблица с классом 'standard sortable' не найдена")
                # Пробуем найти любую таблицу для отладки
                all_tables = soup.find_all('table')
                logger.info(f"    Найдено таблиц: {len(all_tables)}")
                return []
            
            # Получаем все строки таблицы
            rows = table.find_all('tr')
            logger.info(f"    Найдено строк в таблице: {len(rows)}")
            
            if len(rows) < 2:
                logger.warning("    В таблице нет строк с данными")
                return []
            
            # Пропускаем заголовок (первую строку)
            for i, row in enumerate(rows[1:], 1):
                try:
                    cells = row.find_all('td')
                    
                    # Должно быть 7 колонок
                    if len(cells) < 7:
                        logger.debug(f"    Строка {i} содержит только {len(cells)} ячеек, пропускаем")
                        continue
                    
                    # 1. Название (колонка 0)
                    name_cell = cells[0]
                    name = name_cell.get_text().strip()
                    if not name:
                        continue
                    
                    # 2. Тип (колонка 1) - удаляем точку в конце
                    type_cell = cells[1]
                    raw_type = type_cell.get_text().strip()
                    # Убираем точку в конце, если есть
                    if raw_type.endswith('.'):
                        village_type = raw_type[:-1]
                    else:
                        village_type = raw_type
                    
                    # 3. Координаты (колонка 5)
                    coords_cell = cells[5]
                    lat, lon = self._extract_coordinates_from_cell(coords_cell)
                    
                    if lat is None or lon is None:
                        logger.debug(f"    Не удалось извлечь координаты для {name}")
                        continue
                    
                    # Формируем запись
                    village = {
                        "name": name,
                        "type": village_type,
                        "lat": str(round(lat, 6)),  # Округляем до 6 знаков
                        "lon": str(round(lon, 6)),
                        "source": "academic_ru",
                        "district": "Бельский",
                        "status": "abandoned",  # Все записи - бывшие НП
                        "notes": "<i>Источник: dic.academic.ru</i>"
                    }
                    
                    villages.append(village)
                    
                    # Логируем каждую 100-ю запись для отладки
                    if len(villages) % 100 == 0:
                        logger.info(f"      Обработано {len(villages)} записей...")
                    
                except Exception as e:
                    logger.debug(f"    Ошибка парсинга строки {i}: {e}")
                    continue
            
            logger.info(f"    Успешно обработано {len(villages)} записей")
            return villages
            
        except Exception as e:
            logger.error(f"    ❌ Критическая ошибка в _parse_academic_ru_html: {e}")
            return []
    
    def _extract_coordinates_from_cell(self, cell) -> Tuple[Optional[float], Optional[float]]:
        """
        Извлекает координаты из ячейки таблицы.
        Ищет скрытый span с классом 'geo', содержащий latitude и longitude.
        """
        try:
            # Ищем скрытый span с координатами
            geo_span = cell.find('span', class_='geo')
            
            if geo_span:
                # Ищем latitude и longitude внутри geo span
                lat_span = geo_span.find('span', class_='latitude')
                lon_span = geo_span.find('span', class_='longitude')
                
                if lat_span and lon_span:
                    lat_text = lat_span.get_text().strip()
                    lon_text = lon_span.get_text().strip()
                    
                    try:
                        lat = float(lat_text)
                        lon = float(lon_text)
                        
                        # Проверяем, что координаты в пределах Тверской области
                        if 55.0 <= lat <= 58.0 and 30.0 <= lon <= 38.0:
                            return lat, lon
                    except ValueError:
                        pass
            
            # Если не нашли через geo span, пробуем найти десятичные координаты в тексте
            cell_text = cell.get_text().strip()
            
            # Ищем паттерн "55.8324, 33.3499" или "55.8324 33.3499"
            coord_pattern = r'([0-9]+\.[0-9]+)[,\s]+([0-9]+\.[0-9]+)'
            match = re.search(coord_pattern, cell_text)
            
            if match:
                try:
                    lat = float(match.group(1))
                    lon = float(match.group(2))
                    
                    if 55.0 <= lat <= 58.0 and 30.0 <= lon <= 38.0:
                        return lat, lon
                except ValueError:
                    pass
            
            return None, None
            
        except Exception as e:
            logger.debug(f"    Ошибка извлечения координат: {e}")
            return None, None