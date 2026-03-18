# api_sources.py
# Универсальный парсер для всех районов Тверской области
# Поддерживает: общие списки, сельские поселения, бывшие НП

import aiohttp
import asyncio
import logging
from typing import List, Dict, Optional, Tuple, Any, Set
import os
import time
import re
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import json

logger = logging.getLogger(__name__)

# ========== КОНФИГУРАЦИЯ РАЙОНОВ ==========

# Сельские поселения для каждого района
SETTLEMENTS_CONFIG = {
    "Ржевский": {
        "name": "Ржевский район",
        "wiki_prefix": "Список_бывших_населённых_пунктов_на_территории_сельского_поселения_“{}”_Ржевского_района",
        "settlements": [
            "Есинка",
            "Итомля", 
            "Медведево",
            "Победа",
            "Успенское",
            "Хорошево",
            "Чертолино"
        ],
        "master_list_url": "https://ru.wikipedia.org/wiki/Ржевский_район#Населённые_пункты",
        "master_list_section": "Населённые пункты"
    },
    "Оленинский": {
        "name": "Оленинский район",
        "wiki_prefix": "Список_бывших_населённых_пунктов_на_территории_сельского_поселения_“{}”_Оленинского_района",
        "settlements": [
            "Глазковское",
            "Гришинское", 
            "Знаменское",
            "Каденское",
            "Медновское",
            "Молодотудское",
            "Оленинское"
        ],
        "master_list_url": "https://ru.wikipedia.org/wiki/Оленинский_район#Населённые_пункты",
        "master_list_section": "Населённые пункты"
    },
    "Зубцовский": {
        "name": "Зубцовский район",
        "wiki_prefix": "Список_бывших_населённых_пунктов_на_территории_сельского_поселения_“{}”_Зубцовского_района",
        "settlements": [
            "Вазузское",
            "Дорожаевское",
            "Зубцовское",
            "Княжьегорское", 
            "Погорельское",
            "Столипинское",
            "Ульяновское"
        ],
        "master_list_url": "https://ru.wikipedia.org/wiki/Зубцовский_район#Населённые_пункты",
        "master_list_section": "Населённые пункты"
    },
    "Бельский": {
        "name": "Бельский район",
        "wiki_prefix": None,  # Для Бельского используем dic.academic.ru
        "settlements": [
            "Егорьевское",
            "Верховское",
            "Кавельщинское",
            "Пригородное",
            "Демяховское",
            "Будинское"
        ],
        "master_list_url": None,
        "master_list_section": None
    }
}

# Соответствие сокращений полным названиям типов
TYPE_MAPPING = {
    'дер.': 'деревня',
    'д.': 'деревня',
    'пос.': 'посёлок',
    'п.': 'посёлок',
    'с.': 'село',
    'х.': 'хутор',
    'ур.': 'урочище',
    'ст.': 'станция',
    'разъезд': 'разъезд',
    'ж/д ст.': 'железнодорожная станция',
    'ж/д будка': 'железнодорожная будка',
    'кордон': 'кордон',
    'местечко': 'местечко',
    'сл.': 'слобода',
    'дп': 'дачный посёлок',
    'кп': 'курортный посёлок',
    'рп': 'рабочий посёлок'
}

# Базовые URL для формирования ссылок
WIKIPEDIA_BASE_URL = "https://ru.wikipedia.org/wiki/"
DIC_ACADEMIC_BASE_URL = "https://dic.academic.ru/dic.nsf/ruwiki/"

class APISourceManager:
    """
    Универсальный менеджер для загрузки данных из всех доступных источников
    """
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.thread_pool = ThreadPoolExecutor(max_workers=5)
        
        # Для rate limiting
        self.request_count = 0
        self.last_request_time = 0
        self.min_request_interval = 0.3  # 300 мс между запросами
        
        # Кэш для проверенных URL
        self.url_cache: Dict[str, bool] = {}
        
        # Стандартные заголовки
        self.default_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
            'Connection': 'keep-alive',
        }
    
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
    
    async def _check_url_exists(self, url: str) -> bool:
        """
        Проверяет существование URL через HEAD-запрос
        Использует кэширование результатов
        """
        if url in self.url_cache:
            return self.url_cache[url]
        
        try:
            session = await self._get_session()
            await self._rate_limit()
            
            async with session.head(url, headers=self.default_headers, timeout=10, allow_redirects=True) as response:
                exists = response.status == 200
                self.url_cache[url] = exists
                return exists
        except Exception:
            self.url_cache[url] = False
            return False
    
    async def fetch_district_data(self, district: str) -> Dict[str, List[Dict]]:
        """
        Загружает данные для конкретного района из всех доступных источников
        """
        if district not in SETTLEMENTS_CONFIG:
            raise ValueError(f"Неизвестный район: {district}")
        
        logger.info(f"🌐 Загрузка данных для района: {district}")
        
        results = {
            "total": [],
            "sources": {},
            "master_list": [],
            "settlements": {}
        }
        
        config = SETTLEMENTS_CONFIG[district]
        
        # ЭТАП 1: Загружаем общий список населенных пунктов (если есть)
        if config.get("master_list_url"):
            try:
                logger.info(f"  🔍 Загрузка общего списка НП...")
                master_list = await self._parse_master_list(config["master_list_url"], district)
                results["master_list"] = master_list
                results["sources"]["Общий список"] = len(master_list)
                results["total"].extend(master_list)
                logger.info(f"    ✅ Загружено: {len(master_list)} записей")
            except Exception as e:
                logger.error(f"    ❌ Ошибка загрузки общего списка: {e}")
        
        # ЭТАП 2: Для каждого сельского поселения ищем страницы с бывшими НП
        logger.info(f"  🔍 Поиск страниц для сельских поселений...")
        
        for settlement in config["settlements"]:
            try:
                # Формируем URL для Википедии
                if config.get("wiki_prefix"):
                    wiki_url = WIKIPEDIA_BASE_URL + config["wiki_prefix"].format(settlement)
                    
                    # Проверяем существование страницы
                    exists = await self._check_url_exists(wiki_url)
                    
                    if exists:
                        logger.info(f"    ✅ Найдена страница для СП {settlement}")
                        
                        # Парсим страницу
                        settlement_data = await self._parse_settlement_page(wiki_url, district, settlement)
                        results["settlements"][settlement] = len(settlement_data)
                        results["total"].extend(settlement_data)
                        logger.info(f"      Загружено: {len(settlement_data)} записей")
                    else:
                        logger.info(f"    ⏭️ Страница для СП {settlement} не найдена")
                        results["settlements"][settlement] = 0
            
            except Exception as e:
                logger.error(f"    ❌ Ошибка обработки СП {settlement}: {e}")
                results["settlements"][settlement] = 0
        
        # ЭТАП 3: Для Бельского района используем dic.academic.ru
        if district == "Бельский":
            try:
                logger.info(f"  🔍 Загрузка данных с dic.academic.ru...")
                academic_data = await self._parse_academic_source()
                results["sources"]["dic.academic.ru"] = len(academic_data)
                results["total"].extend(academic_data)
                logger.info(f"    ✅ Загружено: {len(academic_data)} записей")
            except Exception as e:
                logger.error(f"    ❌ Ошибка загрузки с dic.academic.ru: {e}")
        
        # Удаляем дубликаты с приоритетом записей с координатами
        unique_results = self._deduplicate_with_priority(results["total"])
        results["total"] = unique_results
        
        logger.info(f"  ✅ Всего уникальных записей: {len(results['total'])}")
        
        return results
    
    async def _parse_master_list(self, url: str, district: str) -> List[Dict]:
        """
        Парсит общий список населенных пунктов из статьи Википедии о районе
        """
        session = await self._get_session()
        await self._rate_limit()
        
        results = []
        
        try:
            async with session.get(url, headers=self.default_headers, timeout=45) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    loop = asyncio.get_event_loop()
                    parsed = await loop.run_in_executor(
                        self.thread_pool,
                        self._parse_master_list_html,
                        html,
                        district
                    )
                    
                    results.extend(parsed)
                else:
                    logger.error(f"    ❌ Ошибка загрузки: HTTP {response.status}")
                    
        except Exception as e:
            logger.error(f"    ❌ Ошибка: {e}")
        
        return results
    
    def _parse_master_list_html(self, html: str, district: str) -> List[Dict]:
        """
        Парсит HTML общего списка населенных пунктов
        Ожидает структуру как в статье Ржевского района
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            # Ищем таблицу с населенными пунктами
            tables = soup.find_all('table', class_=['wikitable', 'standard'])
            
            for table in tables:
                rows = table.find_all('tr')
                if len(rows) < 2:
                    continue
                
                # Определяем заголовки
                header_cells = rows[0].find_all(['th', 'td'])
                headers = [h.get_text().strip().lower() for h in header_cells]
                
                # Ищем индексы нужных колонок
                name_idx = self._find_column_index(headers, ['населённый пункт', 'название'])
                type_idx = self._find_column_index(headers, ['тип'])
                settlement_idx = self._find_column_index(headers, ['сельское поселение'])
                
                for row in rows[1:]:
                    try:
                        cells = row.find_all('td')
                        if len(cells) < max(filter(None, [name_idx, type_idx, settlement_idx])) + 1:
                            continue
                        
                        # Название
                        if name_idx is not None and name_idx < len(cells):
                            name = cells[name_idx].get_text().strip()
                        else:
                            continue
                        
                        if not name or name in ['ИТОГО', 'Всего']:
                            continue
                        
                        # Тип
                        village_type = 'деревня'
                        if type_idx is not None and type_idx < len(cells):
                            raw_type = cells[type_idx].get_text().strip()
                            village_type = self._expand_type(raw_type)
                        
                        # Сельское поселение
                        settlement = ''
                        if settlement_idx is not None and settlement_idx < len(cells):
                            settlement = cells[settlement_idx].get_text().strip()
                        
                        results.append({
                            "name": name,
                            "type": village_type,
                            "lat": "",
                            "lon": "",
                            "source": f"Общий список НП {district} района",
                            "district": district,
                            "settlement": settlement,
                            "status": "existing",
                            "notes": f"<i>Источник: Общий список НП {district} района</i>"
                        })
                        
                    except Exception as e:
                        continue
            
            return results
            
        except Exception as e:
            logger.error(f"    ❌ Ошибка в _parse_master_list_html: {e}")
            return []
    
    async def _parse_settlement_page(self, url: str, district: str, settlement: str) -> List[Dict]:
        """
        Парсит страницу с бывшими населенными пунктами конкретного СП
        """
        session = await self._get_session()
        await self._rate_limit()
        
        results = []
        
        try:
            async with session.get(url, headers=self.default_headers, timeout=45) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    loop = asyncio.get_event_loop()
                    parsed = await loop.run_in_executor(
                        self.thread_pool,
                        self._parse_settlement_page_html,
                        html,
                        district,
                        settlement,
                        url
                    )
                    
                    results.extend(parsed)
                else:
                    logger.error(f"    ❌ Ошибка загрузки: HTTP {response.status}")
                    
        except Exception as e:
            logger.error(f"    ❌ Ошибка: {e}")
        
        return results
    
    def _parse_settlement_page_html(self, html: str, district: str, settlement: str, url: str) -> List[Dict]:
        """
        Парсит HTML страницы с бывшими НП сельского поселения
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            # Ищем таблицу с данными
            tables = soup.find_all('table', class_=['wikitable', 'standard', 'sortable'])
            
            for table in tables:
                rows = table.find_all('tr')
                if len(rows) < 2:
                    continue
                
                # Определяем заголовки
                header_cells = rows[0].find_all(['th', 'td'])
                headers = [h.get_text().strip().lower() for h in header_cells]
                
                # Ищем индексы нужных колонок
                name_idx = self._find_column_index(headers, ['населённый пункт', 'название'])
                type_idx = self._find_column_index(headers, ['тип'])
                coords_idx = self._find_column_index(headers, ['координаты', 'коорд'])
                year_idx = self._find_column_index(headers, ['год', 'упразднения'])
                
                for row in rows[1:]:
                    try:
                        cells = row.find_all('td')
                        if len(cells) < max(filter(None, [name_idx, type_idx])) + 1:
                            continue
                        
                        # Название
                        if name_idx is not None and name_idx < len(cells):
                            name = cells[name_idx].get_text().strip()
                        else:
                            continue
                        
                        if not name:
                            continue
                        
                        # Тип
                        village_type = 'деревня'
                        if type_idx is not None and type_idx < len(cells):
                            raw_type = cells[type_idx].get_text().strip()
                            village_type = self._expand_type(raw_type)
                        
                        # Координаты
                        lat, lon = None, None
                        if coords_idx is not None and coords_idx < len(cells):
                            lat, lon = self._parse_coordinates_universal('', cells[coords_idx])
                        
                        # Дополнительная информация
                        notes_parts = [f"<i>Источник: {settlement} СП, {district} район</i>"]
                        
                        if year_idx is not None and year_idx < len(cells):
                            year_text = cells[year_idx].get_text().strip()
                            if year_text and year_text not in ['—', '-', '']:
                                notes_parts.append(f"упразднён в {year_text} г.")
                        
                        results.append({
                            "name": name,
                            "type": village_type,
                            "lat": str(round(lat, 5)) if lat else "",
                            "lon": str(round(lon, 5)) if lon else "",
                            "source": url,
                            "district": district,
                            "settlement": settlement,
                            "status": "abandoned",
                            "notes": "<br>".join(notes_parts)
                        })
                        
                    except Exception as e:
                        continue
            
            return results
            
        except Exception as e:
            logger.error(f"    ❌ Ошибка в _parse_settlement_page_html: {e}")
            return []
    
    async def _parse_academic_source(self) -> List[Dict]:
        """
        Парсит dic.academic.ru для Бельского района
        """
        url = "https://dic.academic.ru/dic.nsf/ruwiki/1635988"
        session = await self._get_session()
        await self._rate_limit()
        
        results = []
        
        try:
            async with session.get(url, headers=self.default_headers, timeout=45) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    loop = asyncio.get_event_loop()
                    parsed = await loop.run_in_executor(
                        self.thread_pool,
                        self._parse_academic_html,
                        html
                    )
                    
                    results.extend(parsed)
                else:
                    logger.error(f"    ❌ Ошибка загрузки: HTTP {response.status}")
                    
        except Exception as e:
            logger.error(f"    ❌ Ошибка: {e}")
        
        return results
    
    def _parse_academic_html(self, html: str) -> List[Dict]:
        """
        Парсит HTML dic.academic.ru
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            table = soup.find('table', class_='standard sortable')
            if not table:
                return []
            
            rows = table.find_all('tr')
            
            for row in rows[1:]:
                try:
                    cells = row.find_all('td')
                    if len(cells) < 6:
                        continue
                    
                    name = cells[0].get_text().strip()
                    raw_type = cells[1].get_text().strip()
                    village_type = self._expand_type(raw_type)
                    
                    lat, lon = self._parse_coordinates_universal('', cells[5])
                    
                    year = ''
                    if len(cells) > 4:
                        year = cells[4].get_text().strip()
                    
                    notes_parts = [f"<i>Источник: dic.academic.ru</i>"]
                    if year and year not in ['—', '-', '']:
                        notes_parts.append(f"упразднён в {year} г.")
                    
                    results.append({
                        "name": name,
                        "type": village_type,
                        "lat": str(round(lat, 5)) if lat else "",
                        "lon": str(round(lon, 5)) if lon else "",
                        "source": "dic.academic.ru",
                        "district": "Бельский",
                        "settlement": self._determine_settlement_from_name(name),
                        "status": "abandoned",
                        "notes": "<br>".join(notes_parts)
                    })
                    
                except Exception as e:
                    continue
            
            return results
            
        except Exception as e:
            return []
    
    def _determine_settlement_from_name(self, name: str) -> str:
        """
        Пытается определить сельское поселение по названию для Бельского района
        """
        # Здесь можно добавить логику определения СП по названию
        # Пока возвращаем пустую строку
        return ""
    
    def _find_column_index(self, headers: List[str], possible_names: List[str]) -> Optional[int]:
        """Находит индекс колонки по возможным названиям"""
        for i, header in enumerate(headers):
            for name in possible_names:
                if name in header:
                    return i
        return None
    
    def _expand_type(self, short_type: str) -> str:
        """Преобразует сокращение в полное название типа"""
        if not short_type:
            return 'деревня'
        
        clean_type = short_type.rstrip('.').lower().strip()
        
        for short, full in TYPE_MAPPING.items():
            if clean_type == short.rstrip('.'):
                return full
        
        if clean_type in TYPE_MAPPING.values():
            return clean_type
        
        return 'деревня'
    
    def _parse_coordinates_universal(self, text: str, cell=None) -> Tuple[Optional[float], Optional[float]]:
        """Универсальный парсер координат для всех форматов"""
        try:
            # Ищем в скрытых span
            if cell:
                geo_span = cell.find('span', class_='geo')
                if geo_span:
                    lat_span = geo_span.find('span', class_='latitude')
                    lon_span = geo_span.find('span', class_='longitude')
                    
                    if lat_span and lon_span:
                        try:
                            lat = float(lat_span.get_text().strip())
                            lon = float(lon_span.get_text().strip())
                            if self._validate_coordinates(lat, lon):
                                return lat, lon
                        except:
                            pass
            
            # DMS формат
            dms_pattern = r'(\d+)°(\d+)′([\d.]+)″.*?(\d+)°(\d+)′([\d.]+)″'
            match = re.search(dms_pattern, text)
            
            if match:
                lat_deg, lat_min, lat_sec = map(float, match.group(1, 2, 3))
                lon_deg, lon_min, lon_sec = map(float, match.group(4, 5, 6))
                
                lat = lat_deg + lat_min/60 + lat_sec/3600
                lon = lon_deg + lon_min/60 + lon_sec/3600
                
                if self._validate_coordinates(lat, lon):
                    return lat, lon
            
            # Десятичные
            decimal_pattern = r'([0-9]+\.[0-9]+)[,\s]+([0-9]+\.[0-9]+)'
            match = re.search(decimal_pattern, text)
            
            if match:
                lat = float(match.group(1))
                lon = float(match.group(2))
                
                if self._validate_coordinates(lat, lon):
                    return lat, lon
            
            return None, None
            
        except Exception:
            return None, None
    
    def _validate_coordinates(self, lat: float, lon: float) -> bool:
        """Проверяет координаты в пределах Тверской области"""
        return (55.0 <= lat <= 58.0 and 30.0 <= lon <= 38.0)
    
    def _deduplicate_with_priority(self, items: List[Dict]) -> List[Dict]:
        """
        Удаляет дубликаты с приоритетом записей с координатами
        """
        unique: Dict[str, Dict] = {}
        
        for item in items:
            # Ключ: название + район
            key = f"{item['name']}_{item['district']}_{item.get('settlement', '')}"
            
            if key not in unique:
                unique[key] = item
            else:
                existing = unique[key]
                
                # Приоритет: запись с координатами
                if not existing.get('lat') and item.get('lat'):
                    unique[key] = item
                elif existing.get('lat') and not item.get('lat'):
                    pass  # оставляем существующую
                else:
                    # Если обе с координатами или обе без, объединяем notes
                    existing_notes = existing.get('notes', '')
                    item_notes = item.get('notes', '')
                    
                    if item_notes and item_notes not in existing_notes:
                        existing['notes'] = existing_notes + "<br>" + item_notes
        
        return list(unique.values())

# Экспортируем конфигурацию для использования в bot.py
SOURCES_CONFIG = SETTLEMENTS_CONFIG