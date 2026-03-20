# api_sources.py
# Оптимизированная версия с групповым поиском координат

import aiohttp
import asyncio
import logging
from typing import List, Dict, Optional, Tuple, Any, Set
import os
import time
import re
import random
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote

logger = logging.getLogger(__name__)

# ========== КОНФИГУРАЦИЯ ==========

DISTRICTS = ["Ржевский", "Оленинский", "Зубцовский", "Бельский"]

DIC_ACADEMIC_BASE_URL = "https://dic.academic.ru"
DIC_ACADEMIC_SEARCH_URL = "https://dic.academic.ru/searchall.php"
DIC_ACADEMIC_ARTICLE_URL = "https://dic.academic.ru/dic.nsf/ruwiki/{}"

TYPE_MAPPING = {
    'дер.': 'деревня', 'д.': 'деревня', 'пос.': 'посёлок', 'п.': 'посёлок',
    'с.': 'село', 'х.': 'хутор', 'ур.': 'урочище', 'ст.': 'станция',
    'разъезд': 'разъезд', 'ж/д ст.': 'железнодорожная станция'
}

TYPE_SHORT = {'деревня': 'дер.', 'село': 'с.', 'посёлок': 'пос.', 'хутор': 'х.', 'урочище': 'ур.'}

LIST_KEYWORDS = [
    "список населённых пунктов", "список населенных пунктов",
    "список бывших населённых пунктов", "населённые пункты"
]

SETTLEMENTS_SECTION_KEYWORDS = [
    "населенные пункты", "населённые пункты", "список населенных пунктов",
    "деревни", "поселки", "села"
]

SERVICE_VILLAGE_WORDS = [
    'россия', 'ржев', 'тверская', 'область', 'тыс', 'чел', 'население',
    'площадь', 'км', 'район', '▼', '▲', 'статья', 'категория', 'примечания'
]

MIN_NAME_LENGTH = 2
MAX_NAME_LENGTH = 50


class APISourceManager:
    """
    Оптимизированный менеджер с групповым поиском координат
    """
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.thread_pool = ThreadPoolExecutor(max_workers=5)
        
        self.request_count = 0
        self.last_request_time = 0
        self.min_request_interval = 0.5  # Уменьшено с 1.5
        
        # Кэши
        self.page_cache: Dict[str, Tuple[str, float]] = {}
        self.search_cache: Dict[str, List[Dict]] = {}  # Кэш поиска по запросу
        self.coords_cache: Dict[str, Dict] = {}  # Кэш координат по названию
        self.processed_articles: Set[str] = set()
        
        self.cache_ttl = 7200  # 2 часа
        self.max_retries = 3
        
        self.default_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept-Language': 'ru-RU,ru;q=0.8',
            'Connection': 'keep-alive',
        }
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def close_session(self):
        if self.session and not self.session.closed:
            await self.session.close()
        self.thread_pool.shutdown(wait=False)
    
    async def _rate_limit(self):
        """Rate limiting с уменьшенной задержкой"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval - time_since_last)
        
        self.last_request_time = time.time()
        self.request_count += 1
    
    async def _fetch_page(self, url: str) -> Optional[str]:
        """Загружает страницу с кэшированием"""
        current_time = time.time()
        
        if url in self.page_cache:
            html, timestamp = self.page_cache[url]
            if current_time - timestamp < self.cache_ttl:
                return html
        
        for attempt in range(self.max_retries):
            try:
                session = await self._get_session()
                await self._rate_limit()
                
                async with session.get(url, headers=self.default_headers, timeout=60) as response:
                    if response.status == 200:
                        html = await response.text()
                        self.page_cache[url] = (html, current_time)
                        return html
                    elif response.status == 429:
                        wait = 2 ** attempt * random.uniform(0.5, 1.5)
                        await asyncio.sleep(wait)
                    else:
                        return None
            except asyncio.TimeoutError:
                if attempt == self.max_retries - 1:
                    return None
                await asyncio.sleep(2 ** attempt)
            except Exception:
                return None
        
        return None
    
    async def _search_cached(self, query: str, max_results: int = 5) -> List[Dict]:
        """
        Поиск с кэшированием результатов
        """
        cache_key = f"search_{query}"
        
        if cache_key in self.search_cache:
            return self.search_cache[cache_key][:max_results]
        
        encoded_query = quote(query)
        search_url = f"{DIC_ACADEMIC_SEARCH_URL}?SWord={encoded_query}"
        
        html = await self._fetch_page(search_url)
        if not html:
            return []
        
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(
            self.thread_pool,
            self._parse_search_results,
            html
        )
        
        # Кэшируем только первые 10 результатов
        self.search_cache[cache_key] = results[:10]
        
        return results[:max_results]
    
    def _parse_search_results(self, html: str) -> List[Dict]:
        """Парсит результаты поиска"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            for link in soup.find_all('a', href=re.compile(r'/dic\.nsf/ruwiki/\d+')):
                href = link.get('href', '')
                match = re.search(r'/dic\.nsf/ruwiki/(\d+)', href)
                if not match:
                    continue
                
                article_id = match.group(1)
                title = link.get_text().strip()
                
                # Оцениваем релевантность
                score = 0
                parent = link.find_parent()
                if parent:
                    text = parent.get_text().lower()
                    if 'деревня' in text or 'село' in text or 'посёлок' in text:
                        score += 30
                    if 'координаты' in text or 'широта' in text or 'долгота' in text:
                        score += 20
                
                results.append({
                    'id': article_id,
                    'title': title,
                    'score': score
                })
            
            # Сортируем по релевантности
            results.sort(key=lambda x: x['score'], reverse=True)
            return results
            
        except Exception as e:
            logger.error(f"Ошибка парсинга поиска: {e}")
            return []
    
    async def _get_village_coordinates(self, name: str, village_type: str, district: str) -> Optional[Tuple[str, str]]:
        """
        Получает координаты для одного НП с кэшированием
        """
        cache_key = f"{name}_{district}"
        
        if cache_key in self.coords_cache:
            cached = self.coords_cache[cache_key]
            if cached.get('lat'):
                return (cached['lat'], cached['lon'])
            return None
        
        # Пробуем найти через поиск
        type_short = TYPE_SHORT.get(village_type, 'дер.')
        
        queries = [
            f'"{name}" {type_short} {district} район',
            f'{name} {type_short} {district}',
            f'{name} Тверская область'
        ]
        
        for query in queries:
            results = await self._search_cached(query, max_results=3)
            
            for result in results:
                if result['id'] in self.processed_articles:
                    continue
                
                self.processed_articles.add(result['id'])
                
                # Загружаем страницу
                url = DIC_ACADEMIC_ARTICLE_URL.format(result['id'])
                html = await self._fetch_page(url)
                
                if html:
                    coords = await self._parse_coordinates_from_page(html, name)
                    if coords:
                        lat, lon = coords
                        self.coords_cache[cache_key] = {'lat': lat, 'lon': lon}
                        logger.info(f"    ✅ Найдены координаты для {name}: {lat}, {lon}")
                        return (lat, lon)
            
            await asyncio.sleep(0.5)  # Небольшая пауза между запросами
        
        self.coords_cache[cache_key] = {'lat': None, 'lon': None}
        return None
    
    async def _parse_coordinates_from_page(self, html: str, village_name: str) -> Optional[Tuple[str, str]]:
        """Парсит координаты со страницы"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Ищем geo span
            geo_span = soup.find('span', class_='geo')
            if geo_span:
                lat_span = geo_span.find('span', class_='latitude')
                lon_span = geo_span.find('span', class_='longitude')
                if lat_span and lon_span:
                    try:
                        lat = float(lat_span.get_text().strip())
                        lon = float(lon_span.get_text().strip())
                        if self._validate_coordinates(lat, lon):
                            return (str(round(lat, 5)), str(round(lon, 5)))
                    except:
                        pass
            
            # Ищем DMS формат
            text = soup.get_text()
            dms_pattern = r'(\d+)°(\d+)′([\d.]+)″.*?(\d+)°(\d+)′([\d.]+)″'
            match = re.search(dms_pattern, text)
            if match:
                try:
                    lat_deg, lat_min, lat_sec = map(float, match.group(1, 2, 3))
                    lon_deg, lon_min, lon_sec = map(float, match.group(4, 5, 6))
                    lat = lat_deg + lat_min/60 + lat_sec/3600
                    lon = lon_deg + lon_min/60 + lon_sec/3600
                    if self._validate_coordinates(lat, lon):
                        return (str(round(lat, 5)), str(round(lon, 5)))
                except:
                    pass
            
            # Ищем десятичные координаты
            decimal_pattern = r'([0-9]{2}\.[0-9]{4,})[,\s]+([0-9]{2,3}\.[0-9]{4,})'
            match = re.search(decimal_pattern, text)
            if match:
                try:
                    lat = float(match.group(1))
                    lon = float(match.group(2))
                    if self._validate_coordinates(lat, lon):
                        return (str(round(lat, 5)), str(round(lon, 5)))
                except:
                    pass
            
            return None
            
        except Exception as e:
            logger.debug(f"Ошибка парсинга координат: {e}")
            return None
    
    def _validate_coordinates(self, lat: float, lon: float) -> bool:
        """Проверяет, что координаты в пределах Тверской области"""
        return (55.0 <= lat <= 58.0 and 30.0 <= lon <= 38.0)
    
    async def fetch_district_data(self, district: str) -> List[Dict]:
        """
        Основной метод загрузки данных для района
        """
        self.request_count = 0
        self.start_time = time.time()
        logger.info(f"🌐 Загрузка данных для района: {district}")
        
        # Сначала пробуем найти главный список
        all_villages = []
        seen_names = set()
        
        # Поиск главной страницы района
        district_queries = [
            f"{district} район Тверская область",
            f"{district} муниципальный район",
            f"{district} район"
        ]
        
        district_page_id = None
        
        for query in district_queries:
            results = await self._search_cached(query, max_results=3)
            for result in results:
                if result['score'] > 20:
                    district_page_id = result['id']
                    logger.info(f"  ✅ Найдена страница района: ID {district_page_id}")
                    break
            if district_page_id:
                break
        
        # Ищем списки населенных пунктов
        if district_page_id:
            url = DIC_ACADEMIC_ARTICLE_URL.format(district_page_id)
            html = await self._fetch_page(url)
            
            if html:
                # Ищем ссылки на списки
                soup = BeautifulSoup(html, 'html.parser')
                for link in soup.find_all('a', href=re.compile(r'/dic\.nsf/ruwiki/\d+')):
                    text = link.get_text().lower()
                    href = link.get('href', '')
                    
                    if any(kw in text for kw in LIST_KEYWORDS):
                        match = re.search(r'/dic\.nsf/ruwiki/(\d+)', href)
                        if match:
                            list_id = match.group(1)
                            logger.info(f"  🔍 Найден список НП: ID {list_id}")
                            
                            # Парсим список
                            list_url = DIC_ACADEMIC_ARTICLE_URL.format(list_id)
                            list_html = await self._fetch_page(list_url)
                            
                            if list_html:
                                villages = await self._parse_village_list(list_html, district)
                                for v in villages:
                                    if v['name'] not in seen_names:
                                        seen_names.add(v['name'])
                                        all_villages.append(v)
                            
                            await asyncio.sleep(1)
        
        # Ищем бывшие населенные пункты
        former_queries = [
            f"бывшие населенные пункты {district} района",
            f"список бывших населенных пунктов {district} района",
            f"исчезнувшие населенные пункты {district} района"
        ]
        
        for query in former_queries:
            results = await self._search_cached(query, max_results=2)
            for result in results:
                url = DIC_ACADEMIC_ARTICLE_URL.format(result['id'])
                html = await self._fetch_page(url)
                
                if html:
                    villages = await self._parse_village_list(html, district, is_former=True)
                    for v in villages:
                        if v['name'] not in seen_names:
                            seen_names.add(v['name'])
                            all_villages.append(v)
            
            await asyncio.sleep(1)
        
        # Если не нашли списки, пробуем найти через сельские поселения
        if len(all_villages) < 50:
            logger.info("  🔍 Ищем через сельские поселения...")
            
            settlement_queries = [
                f"сельские поселения {district} района",
                f"муниципальные образования {district} района"
            ]
            
            for query in settlement_queries:
                results = await self._search_cached(query, max_results=3)
                for result in results:
                    url = DIC_ACADEMIC_ARTICLE_URL.format(result['id'])
                    html = await self._fetch_page(url)
                    
                    if html:
                        villages = await self._parse_settlements_villages(html, district)
                        for v in villages:
                            if v['name'] not in seen_names:
                                seen_names.add(v['name'])
                                all_villages.append(v)
                
                await asyncio.sleep(1)
        
        # Поиск координат для всех НП
        logger.info(f"  🔍 Поиск координат для {len(all_villages)} населенных пунктов...")
        
        villages_with_coords = 0
        
        for i, village in enumerate(all_villages):
            if i > 0 and i % 20 == 0:
                logger.info(f"    Обработано {i}/{len(all_villages)} НП, найдено координат: {villages_with_coords}")
                await asyncio.sleep(2)  # Пауза каждые 20 запросов
            
            if village.get('lat') and village.get('lon'):
                villages_with_coords += 1
                continue
            
            coords = await self._get_village_coordinates(
                village['name'], 
                village['type'], 
                district
            )
            
            if coords:
                village['lat'] = coords[0]
                village['lon'] = coords[1]
                village['has_coords'] = True
                villages_with_coords += 1
            else:
                village['lat'] = ''
                village['lon'] = ''
                village['has_coords'] = False
            
            # Небольшая задержка между запросами
            await asyncio.sleep(0.3)
        
        # Сортируем по алфавиту
        all_villages.sort(key=lambda x: x['name'])
        
        total_time = time.time() - self.start_time
        logger.info(f"  ✅ Завершено: {len(all_villages)} НП, из них с координатами: {villages_with_coords}")
        logger.info(f"  ⏱️ Время: {total_time:.1f}с, запросов: {self.request_count}")
        
        return all_villages
    
    async def _parse_village_list(self, html: str, district: str, is_former: bool = False) -> List[Dict]:
        """Парсит список населенных пунктов из таблицы"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            # Ищем таблицы
            tables = soup.find_all('table', class_=['standard', 'sortable', 'wikitable'])
            
            for table in tables:
                rows = table.find_all('tr')
                if len(rows) < 2:
                    continue
                
                # Определяем заголовки
                headers = []
                header_row = rows[0]
                for cell in header_row.find_all(['th', 'td']):
                    headers.append(cell.get_text().strip().lower())
                
                # Ищем индексы колонок
                name_idx = None
                type_idx = None
                coords_idx = None
                
                for i, h in enumerate(headers):
                    if 'название' in h or 'населённый пункт' in h or 'населенный пункт' in h:
                        name_idx = i
                    elif 'тип' in h:
                        type_idx = i
                    elif 'координат' in h:
                        coords_idx = i
                
                # Если не нашли, пробуем определить по первым строкам
                if name_idx is None and len(rows) > 1:
                    for i, cell in enumerate(rows[1].find_all('td')):
                        text = cell.get_text().strip()
                        if text and len(text) > 2 and not text.isdigit():
                            name_idx = i
                            break
                
                if name_idx is None:
                    name_idx = 0
                
                # Парсим строки
                for row in rows[1:]:
                    cells = row.find_all('td')
                    if len(cells) <= name_idx:
                        continue
                    
                    name = cells[name_idx].get_text().strip()
                    name = re.sub(r'^\d+\s*', '', name)
                    name = re.sub(r'\s+', ' ', name).strip()
                    
                    if not name or len(name) < MIN_NAME_LENGTH or len(name) > MAX_NAME_LENGTH:
                        continue
                    
                    if any(word in name.lower() for word in SERVICE_VILLAGE_WORDS):
                        continue
                    
                    # Определяем тип
                    village_type = 'деревня'
                    if type_idx is not None and type_idx < len(cells):
                        type_text = cells[type_idx].get_text().strip().lower()
                        for short, full in TYPE_MAPPING.items():
                            if short in type_text:
                                village_type = full
                                break
                    else:
                        for short, full in TYPE_MAPPING.items():
                            if short in name.lower():
                                village_type = full
                                name = name.replace(short, '').strip()
                                break
                    
                    # Парсим координаты
                    lat = ''
                    lon = ''
                    if coords_idx is not None and coords_idx < len(cells):
                        coords_text = cells[coords_idx].get_text()
                        lat, lon = self._parse_coords_from_text(coords_text)
                    
                    results.append({
                        "name": name,
                        "type": village_type,
                        "lat": lat,
                        "lon": lon,
                        "district": district,
                        "has_coords": bool(lat and lon)
                    })
            
            return results
            
        except Exception as e:
            logger.error(f"Ошибка парсинга списка: {e}")
            return []
    
    async def _parse_settlements_villages(self, html: str, district: str) -> List[Dict]:
        """Парсит НП из страниц сельских поселений"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            # Ищем раздел с населенными пунктами
            for header in soup.find_all(['h2', 'h3', 'h4']):
                header_text = header.get_text().lower()
                if any(kw in header_text for kw in SETTLEMENTS_SECTION_KEYWORDS):
                    parent = header.find_parent()
                    if parent:
                        for link in parent.find_all('a', href=re.compile(r'/dic\.nsf/ruwiki/\d+')):
                            name = link.get_text().strip()
                            name = re.sub(r'^\d+\s*', '', name)
                            name = re.sub(r'\s+', ' ', name).strip()
                            
                            if not name or len(name) < MIN_NAME_LENGTH:
                                continue
                            
                            if any(word in name.lower() for word in SERVICE_VILLAGE_WORDS):
                                continue
                            
                            # Определяем тип
                            village_type = 'деревня'
                            for short, full in TYPE_MAPPING.items():
                                if short in name.lower():
                                    village_type = full
                                    name = name.replace(short, '').strip()
                                    break
                            
                            results.append({
                                "name": name,
                                "type": village_type,
                                "lat": "",
                                "lon": "",
                                "district": district,
                                "has_coords": False
                            })
            
            # Убираем дубликаты
            unique = {}
            for v in results:
                if v['name'] not in unique:
                    unique[v['name']] = v
            
            return list(unique.values())
            
        except Exception as e:
            logger.error(f"Ошибка парсинга СП: {e}")
            return []
    
    def _parse_coords_from_text(self, text: str) -> Tuple[str, str]:
        """Парсит координаты из текста"""
        try:
            # Ищем десятичные координаты
            decimal_pattern = r'([0-9]{2}\.[0-9]{4,})[,\s]+([0-9]{2,3}\.[0-9]{4,})'
            match = re.search(decimal_pattern, text)
            if match:
                lat = float(match.group(1))
                lon = float(match.group(2))
                if self._validate_coordinates(lat, lon):
                    return (str(round(lat, 5)), str(round(lon, 5)))
            
            # Ищем DMS
            dms_pattern = r'(\d+)°(\d+)′([\d.]+)″'
            coords = re.findall(dms_pattern, text)
            if len(coords) >= 2:
                lat_deg, lat_min, lat_sec = map(float, coords[0])
                lon_deg, lon_min, lon_sec = map(float, coords[1])
                lat = lat_deg + lat_min/60 + lat_sec/3600
                lon = lon_deg + lon_min/60 + lon_sec/3600
                if self._validate_coordinates(lat, lon):
                    return (str(round(lat, 5)), str(round(lon, 5)))
            
            return ('', '')
            
        except Exception:
            return ('', '')


AVAILABLE_DISTRICTS = DISTRICTS