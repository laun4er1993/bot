# api_sources.py
# Универсальный парсер для всех районов через dic.academic.ru
# Автоматический поиск страниц районов, сельских поселений и населенных пунктов

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
from urllib.parse import quote, urljoin, urlparse

logger = logging.getLogger(__name__)

# ========== КОНФИГУРАЦИЯ ==========

# Список районов для поиска
DISTRICTS = [
    "Ржевский",
    "Оленинский", 
    "Зубцовский",
    "Бельский"
]

# Базовые URL
DIC_ACADEMIC_BASE_URL = "https://dic.academic.ru"
DIC_ACADEMIC_SEARCH_URL = "https://dic.academic.ru/searchall.php"
DIC_ACADEMIC_ARTICLE_URL = "https://dic.academic.ru/dic.nsf/ruwiki/{}"

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

# Ключевые слова для поиска ссылок на списки НП
LIST_KEYWORDS = [
    "список населённых пунктов",
    "список населенных пунктов",
    "список бывших населённых пунктов",
    "список бывших населенных пунктов",
    "населённые пункты",
    "населенные пункты",
    "бывшие населённые пункты",
    "бывшие населенные пункты"
]

# Ключевые слова для поиска сельских поселений
SETTLEMENT_KEYWORDS = [
    "сельское поселение",
    "сельские поселения",
    "список сельских поселений",
    "муниципальное образование"
]

class APISourceManager:
    """
    Универсальный менеджер для загрузки данных из dic.academic.ru
    Автоматически ищет страницы районов, сельских поселений и населенных пунктов
    """
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.thread_pool = ThreadPoolExecutor(max_workers=5)
        
        # Для rate limiting
        self.request_count = 0
        self.last_request_time = 0
        self.min_request_interval = 0.5  # 500 мс между запросами
        
        # Кэш для найденных ID
        self.article_cache: Dict[str, str] = {}  # запрос -> ID статьи
        self.settlement_cache: Dict[str, List[str]] = {}  # район -> список СП
        self.page_cache: Dict[str, str] = {}  # URL -> HTML
        
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
    
    async def _fetch_page(self, url: str) -> Optional[str]:
        """Загружает страницу с кэшированием"""
        if url in self.page_cache:
            return self.page_cache[url]
        
        try:
            session = await self._get_session()
            await self._rate_limit()
            
            async with session.get(url, headers=self.default_headers, timeout=30) as response:
                if response.status == 200:
                    html = await response.text()
                    self.page_cache[url] = html
                    return html
                else:
                    logger.debug(f"Ошибка загрузки {url}: HTTP {response.status}")
                    return None
        except Exception as e:
            logger.debug(f"Ошибка загрузки {url}: {e}")
            return None
    
    async def _search_article(self, query: str) -> Optional[str]:
        """
        Ищет статью на dic.academic.ru по запросу
        Возвращает ID статьи или None
        """
        cache_key = f"search_{query}"
        if cache_key in self.article_cache:
            return self.article_cache[cache_key]
        
        try:
            # Кодируем запрос для URL
            encoded_query = quote(query)
            search_url = f"{DIC_ACADEMIC_SEARCH_URL}?SWord={encoded_query}"
            
            html = await self._fetch_page(search_url)
            if not html:
                return None
            
            # Парсим результаты поиска
            loop = asyncio.get_event_loop()
            article_id = await loop.run_in_executor(
                self.thread_pool,
                self._parse_search_results,
                html,
                query
            )
            
            if article_id:
                self.article_cache[cache_key] = article_id
            
            return article_id
            
        except Exception as e:
            logger.error(f"Ошибка поиска статьи '{query}': {e}")
            return None
    
    def _parse_search_results(self, html: str, query: str) -> Optional[str]:
        """
        Парсит страницу результатов поиска
        Ищет наиболее релевантную статью
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Ищем ссылки на статьи Википедии
            links = soup.find_all('a', href=re.compile(r'/dic\.nsf/ruwiki/\d+'))
            
            best_match = None
            best_score = 0
            
            for link in links:
                href = link.get('href', '')
                title = link.get_text().strip().lower()
                
                # Извлекаем ID статьи
                match = re.search(r'/dic\.nsf/ruwiki/(\d+)', href)
                if not match:
                    continue
                
                article_id = match.group(1)
                
                # Оцениваем релевантность
                score = 0
                query_lower = query.lower()
                
                # Точное совпадение заголовка
                if query_lower == title:
                    score += 100
                # Заголовок содержит запрос
                elif query_lower in title:
                    score += 50
                
                # Дополнительные баллы за ключевые слова
                for keyword in LIST_KEYWORDS + SETTLEMENT_KEYWORDS:
                    if keyword in title:
                        score += 10
                
                if score > best_score:
                    best_score = score
                    best_match = article_id
            
            return best_match if best_score > 0 else None
            
        except Exception as e:
            logger.error(f"Ошибка парсинга результатов поиска: {e}")
            return None
    
    async def _get_district_page(self, district: str) -> Optional[Dict]:
        """
        Находит страницу района и извлекает из нее информацию
        """
        logger.info(f"  🔍 Поиск страницы района: {district}")
        
        # Пробуем разные варианты запроса
        queries = [
            f"{district} район",
            f"{district} район Тверская область",
            district
        ]
        
        for query in queries:
            article_id = await self._search_article(query)
            if article_id:
                # Загружаем страницу района
                article_url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
                html = await self._fetch_page(article_url)
                
                if html:
                    # Парсим страницу района
                    loop = asyncio.get_event_loop()
                    district_info = await loop.run_in_executor(
                        self.thread_pool,
                        self._parse_district_page,
                        html,
                        article_id,
                        district
                    )
                    
                    if district_info:
                        logger.info(f"    ✅ Найдена страница района (ID: {article_id})")
                        return district_info
        
        logger.info(f"    ❌ Страница района не найдена")
        return None
    
    def _parse_district_page(self, html: str, article_id: str, district: str) -> Optional[Dict]:
        """
        Парсит страницу района, извлекает:
        - ссылки на списки населенных пунктов
        - список сельских поселений
        - область из названия статьи
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Извлекаем область из названия
            title_elem = soup.find('h1')
            title = title_elem.get_text() if title_elem else ""
            
            # Ищем область в названии (обычно "Тверская область")
            region = "Тверская область"  # по умолчанию
            region_match = re.search(r'([А-Яа-я]+ская область)', title)
            if region_match:
                region = region_match.group(1)
            
            # Ищем ссылки на списки НП
            list_links = []
            
            # Ищем в разделе "См. также"
            see_also_section = soup.find('span', id=re.compile(r'См\._также|См_также', re.I))
            if see_also_section:
                # Ищем все ссылки в этом разделе
                parent = see_also_section.find_parent()
                if parent:
                    for link in parent.find_all('a', href=True):
                        href = link.get('href', '')
                        text = link.get_text().strip().lower()
                        
                        # Проверяем, что это ссылка на статью Википедии
                        match = re.search(r'/dic\.nsf/ruwiki/(\d+)', href)
                        if match and any(keyword in text for keyword in LIST_KEYWORDS):
                            list_links.append({
                                'id': match.group(1),
                                'title': link.get_text().strip(),
                                'type': 'list'
                            })
            
            # Ищем везде ссылки на списки НП
            for link in soup.find_all('a', href=re.compile(r'/dic\.nsf/ruwiki/\d+')):
                href = link.get('href', '')
                text = link.get_text().strip().lower()
                
                match = re.search(r'/dic\.nsf/ruwiki/(\d+)', href)
                if match and any(keyword in text for keyword in LIST_KEYWORDS):
                    link_id = match.group(1)
                    # Проверяем, не добавили ли уже
                    if not any(l['id'] == link_id for l in list_links):
                        list_links.append({
                            'id': link_id,
                            'title': link.get_text().strip(),
                            'type': 'list'
                        })
            
            # Ищем сельские поселения
            settlements = []
            
            # Ищем по ключевым словам
            for keyword in SETTLEMENT_KEYWORDS:
                # Ищем заголовки
                for header in soup.find_all(['h2', 'h3', 'h4']):
                    if keyword in header.get_text().lower():
                        # Ищем список после заголовка
                        parent = header.find_parent()
                        if parent:
                            # Ищем маркированные списки
                            for ul in parent.find_all('ul'):
                                for li in ul.find_all('li'):
                                    text = li.get_text().strip()
                                    if text and len(text) < 100:  # Фильтруем длинные строки
                                        settlements.append(text)
                            
                            # Ищем таблицы
                            for table in parent.find_all('table'):
                                for row in table.find_all('tr'):
                                    cells = row.find_all('td')
                                    for cell in cells:
                                        text = cell.get_text().strip()
                                        if text and len(text) < 100:
                                            settlements.append(text)
            
            # Удаляем дубликаты и пустые значения
            settlements = list(set(s for s in settlements if s and len(s) > 2))
            
            logger.info(f"    Найдено ссылок на списки: {len(list_links)}")
            logger.info(f"    Найдено сельских поселений: {len(settlements)}")
            
            return {
                'id': article_id,
                'title': title,
                'region': region,
                'list_links': list_links,
                'settlements': settlements
            }
            
        except Exception as e:
            logger.error(f"Ошибка парсинга страницы района: {e}")
            return None
    
    async def _parse_settlement_page(self, article_id: str, district: str, settlement: str, region: str) -> List[Dict]:
        """
        Парсит страницу с бывшими населенными пунктами сельского поселения
        """
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        html = await self._fetch_page(url)
        
        if not html:
            return []
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.thread_pool,
            self._parse_settlement_page_html,
            html,
            article_id,
            district,
            settlement,
            region
        )
    
    def _parse_settlement_page_html(self, html: str, article_id: str, district: str, settlement: str, region: str) -> List[Dict]:
        """
        Парсит HTML страницы с бывшими НП сельского поселения
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            # Ищем таблицу с данными (как на странице Есинки)
            tables = soup.find_all('table', class_=['standard', 'sortable'])
            
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
                        
                        if not name or name in ['ИТОГО', 'Всего']:
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
                        
                        # Формируем запись
                        results.append({
                            "name": name,
                            "type": village_type,
                            "lat": str(round(lat, 5)) if lat else "",
                            "lon": str(round(lon, 5)) if lon else "",
                            "source": f"dic.academic.ru (ID: {article_id})",
                            "district": district,
                            "settlement": settlement,
                            "region": region,
                            "status": "abandoned",
                            "notes": f"<i>Источник: dic.academic.ru, {settlement} СП</i>"
                        })
                        
                    except Exception as e:
                        continue
            
            return results
            
        except Exception as e:
            logger.error(f"Ошибка парсинга страницы СП: {e}")
            return []
    
    async def fetch_district_data(self, district: str) -> Dict[str, List[Dict]]:
        """
        Основной метод: загружает данные для конкретного района
        """
        logger.info(f"🌐 Загрузка данных для района: {district}")
        
        results = {
            "total": [],
            "sources": {},
            "settlements": {},
            "region": "Тверская область"
        }
        
        # Шаг 1: Находим страницу района
        district_info = await self._get_district_page(district)
        
        if not district_info:
            logger.warning(f"  ⚠️ Страница района не найдена")
            return results
        
        results['region'] = district_info.get('region', 'Тверская область')
        
        # Шаг 2: Парсим все найденные ссылки на списки НП
        for link in district_info.get('list_links', []):
            try:
                logger.info(f"  🔍 Парсинг списка: {link['title']}")
                
                # Определяем тип списка
                if 'бывших' in link['title'].lower():
                    # Это список бывших НП
                    data = await self._parse_settlement_page(
                        link['id'], 
                        district, 
                        "общий список", 
                        results['region']
                    )
                    
                    if data:
                        results["sources"][f"Список бывших НП"] = len(data)
                        results["total"].extend(data)
                        logger.info(f"    ✅ Найдено бывших НП: {len(data)}")
                
            except Exception as e:
                logger.error(f"    ❌ Ошибка парсинга списка: {e}")
        
        # Шаг 3: Ищем страницы для каждого сельского поселения
        settlements = district_info.get('settlements', [])
        logger.info(f"  🔍 Поиск страниц для {len(settlements)} сельских поселений...")
        
        for settlement in settlements:
            try:
                # Формируем поисковые запросы для СП
                search_queries = [
                    f"Список бывших населённых пунктов на территории сельского поселения {settlement} {district} района",
                    f"Список бывших населенных пунктов на территории сельского поселения {settlement} {district} района",
                    f"Список бывших населённых пунктов {settlement} {district} района",
                    f"Бывшие населённые пункты {settlement} СП"
                ]
                
                found = False
                for query in search_queries:
                    article_id = await self._search_article(query)
                    if article_id:
                        # Парсим страницу СП
                        data = await self._parse_settlement_page(
                            article_id,
                            district,
                            settlement,
                            results['region']
                        )
                        
                        if data:
                            results["settlements"][settlement] = len(data)
                            results["total"].extend(data)
                            logger.info(f"    ✅ СП {settlement}: {len(data)} записей")
                            found = True
                            break
                
                if not found:
                    logger.info(f"    ⏭️ СП {settlement}: страница не найдена")
                    results["settlements"][settlement] = 0
                    
            except Exception as e:
                logger.error(f"    ❌ Ошибка обработки СП {settlement}: {e}")
                results["settlements"][settlement] = 0
        
        # Удаляем дубликаты с приоритетом записей с координатами
        unique_results = self._deduplicate_with_priority(results["total"])
        results["total"] = unique_results
        
        logger.info(f"  ✅ Всего уникальных записей: {len(results['total'])}")
        
        return results
    
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
            
            # DMS формат (56°13′41.16″ с. ш. 34°08′10.32″ в. д.)
            dms_pattern = r'(\d+)°(\d+)′([\d.]+)″.*?(\d+)°(\d+)′([\d.]+)″'
            match = re.search(dms_pattern, text)
            
            if match:
                lat_deg, lat_min, lat_sec = map(float, match.group(1, 2, 3))
                lon_deg, lon_min, lon_sec = map(float, match.group(4, 5, 6))
                
                lat = lat_deg + lat_min/60 + lat_sec/3600
                lon = lon_deg + lon_min/60 + lon_sec/3600
                
                if self._validate_coordinates(lat, lon):
                    return lat, lon
            
            # Десятичные с пробелом или запятой
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
        """Проверяет координаты (примерные границы для Тверской области)"""
        return (55.0 <= lat <= 58.0 and 30.0 <= lon <= 38.0)
    
    def _deduplicate_with_priority(self, items: List[Dict]) -> List[Dict]:
        """
        Удаляет дубликаты с приоритетом записей с координатами
        """
        unique: Dict[str, Dict] = {}
        
        for item in items:
            # Ключ: название + район + сельское поселение
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
                elif existing.get('lat') and item.get('lat'):
                    # Если обе с координатами, объединяем notes
                    existing['notes'] = existing.get('notes', '') + "<br>" + item.get('notes', '')
                else:
                    # Если обе без координат, объединяем notes
                    existing['notes'] = existing.get('notes', '') + "<br>" + item.get('notes', '')
        
        return list(unique.values())

# Экспортируем список районов для использования в bot.py
AVAILABLE_DISTRICTS = DISTRICTS