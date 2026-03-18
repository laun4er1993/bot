# api_sources.py
# Универсальный парсер для всех районов через dic.academic.ru
# Версия с улучшенной обработкой ошибок 429, поиском дополнительных списков
# и быстрой проверкой дубликатов

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
import json
from urllib.parse import quote, urljoin, urlparse
from collections import defaultdict

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
    "муниципальное образование",
    "состав района",
    "муниципальное устройство",
    "административное деление"
]

# Ключевые слова для идентификации страницы района
DISTRICT_KEYWORDS = [
    "муниципальный район",
    "административная единица",
    "районный центр",
    "расположен на юге",
    "граничит с"
]

# Известные сельские поселения для фильтрации
KNOWN_SETTLEMENTS = {
    "Ржевский": [
        "Есинка", "Итомля", "Медведево", "Победа", 
        "Успенское", "Хорошево", "Чертолино", "Шолохово"
    ],
    "Оленинский": [
        "Глазковское", "Гришинское", "Знаменское", 
        "Каденское", "Медновское", "Молодотудское", "Оленинское"
    ],
    "Зубцовский": [
        "Вазузское", "Дорожаевское", "Зубцовское", 
        "Княжьегорское", "Погорельское", "Столипинское", "Ульяновское"
    ],
    "Бельский": [
        "Егорьевское", "Верховское", "Кавельщинское", 
        "Пригородное", "Демяховское", "Будинское"
    ]
}

class APISourceManager:
    """
    Универсальный менеджер для загрузки данных из dic.academic.ru
    Автоматически ищет страницы районов, сельских поселений и населенных пунктов
    """
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.thread_pool = ThreadPoolExecutor(max_workers=3)
        
        # Для rate limiting
        self.request_count = 0
        self.last_request_time = 0
        self.min_request_interval = 1.5
        
        # Кэш для найденных ID
        self.article_cache: Dict[str, str] = {}
        self.district_cache: Dict[str, Dict] = {}
        self.settlement_pages_cache: Dict[str, str] = {}
        self.page_cache: Dict[str, Tuple[str, float]] = {}
        
        # Время жизни кэша
        self.cache_ttl = 3600
        
        # Максимальное количество повторных попыток
        self.max_retries = 5
        
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
        """Соблюдение rate limiting с случайной вариацией"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        actual_interval = self.min_request_interval * random.uniform(0.8, 1.2)
        
        if time_since_last < actual_interval:
            await asyncio.sleep(actual_interval - time_since_last)
        
        self.last_request_time = time.time()
        self.request_count += 1
        
        if self.request_count % 100 == 0:
            logger.info(f"    Выполнено {self.request_count} запросов")
    
    async def _fetch_page_with_retry(self, url: str, retry_count: int = 0) -> Optional[str]:
        """
        Загружает страницу с повторными попытками при ошибках
        """
        if retry_count >= self.max_retries:
            logger.error(f"Превышено максимальное количество попыток для {url}")
            return None
        
        try:
            session = await self._get_session()
            await self._rate_limit()
            
            async with session.get(url, headers=self.default_headers, timeout=60) as response:
                if response.status == 200:
                    html = await response.text()
                    return html
                elif response.status == 429:
                    base_wait = 2 ** retry_count
                    jitter = random.uniform(0.5, 1.5)
                    wait_time = base_wait * jitter
                    
                    logger.warning(f"Ошибка 429 для {url}, повтор через {wait_time:.1f}с (попытка {retry_count + 1}/{self.max_retries})")
                    await asyncio.sleep(wait_time)
                    return await self._fetch_page_with_retry(url, retry_count + 1)
                elif response.status in [500, 502, 503, 504]:
                    wait_time = 2 ** retry_count
                    logger.warning(f"Ошибка {response.status} для {url}, повтор через {wait_time}с")
                    await asyncio.sleep(wait_time)
                    return await self._fetch_page_with_retry(url, retry_count + 1)
                else:
                    logger.debug(f"Ошибка загрузки {url}: HTTP {response.status}")
                    return None
        except asyncio.TimeoutError:
            wait_time = 2 ** retry_count
            logger.warning(f"Таймаут для {url}, повтор через {wait_time}с")
            await asyncio.sleep(wait_time)
            return await self._fetch_page_with_retry(url, retry_count + 1)
        except Exception as e:
            logger.error(f"Ошибка загрузки {url}: {e}")
            return None
    
    async def _fetch_page(self, url: str) -> Optional[str]:
        """Загружает страницу с кэшированием и повторными попытками"""
        current_time = time.time()
        
        if url in self.page_cache:
            html, timestamp = self.page_cache[url]
            if current_time - timestamp < self.cache_ttl:
                return html
        
        html = await self._fetch_page_with_retry(url)
        
        if html:
            self.page_cache[url] = (html, current_time)
        
        return html
    
    async def _search_with_pagination(self, query: str, max_pages: int = 20, unlimited: bool = False) -> List[Dict]:
        """
        Выполняет поиск с обработкой нескольких страниц результатов
        """
        all_results = []
        page = 1
        
        while True:
            if not unlimited and page > max_pages:
                break
            
            if page > 1:
                await asyncio.sleep(2.0)
            
            encoded_query = quote(query)
            search_url = f"{DIC_ACADEMIC_SEARCH_URL}?SWord={encoded_query}"
            if page > 1:
                search_url += f"&page={page}"
            
            html = await self._fetch_page(search_url)
            if not html:
                break
            
            loop = asyncio.get_event_loop()
            page_results = await loop.run_in_executor(
                self.thread_pool,
                self._parse_search_page,
                html,
                page
            )
            
            if not page_results:
                break
            
            all_results.extend(page_results)
            logger.info(f"      Страница {page}: найдено {len(page_results)} результатов")
            
            has_next = await loop.run_in_executor(
                self.thread_pool,
                self._check_next_page,
                html
            )
            
            if not has_next:
                break
            
            page += 1
        
        if page > 1:
            logger.info(f"    Всего найдено результатов: {len(all_results)} на {page-1} страницах")
        
        return all_results
    
    def _parse_search_page(self, html: str, page_num: int) -> List[Dict]:
        """
        Парсит одну страницу результатов поиска
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            for link in soup.find_all('a', href=re.compile(r'/dic\.nsf/ruwiki/\d+')):
                href = link.get('href', '')
                title_text = link.get_text().strip()
                
                match = re.search(r'/dic\.nsf/ruwiki/(\d+)', href)
                if not match:
                    continue
                    
                article_id = match.group(1)
                
                parent = link.find_parent()
                full_text = ""
                if parent:
                    description = parent.find_next('span', class_='description')
                    if description:
                        full_text = description.get_text().strip()
                    else:
                        full_text = parent.get_text().strip()
                
                position_match = re.match(r'^(\d+)', full_text)
                position = int(position_match.group(1)) if position_match else 0
                
                results.append({
                    'id': article_id,
                    'title': title_text,
                    'full_text': full_text,
                    'page': page_num,
                    'position': position
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Ошибка парсинга страницы поиска: {e}")
            return []
    
    def _check_next_page(self, html: str) -> bool:
        """
        Проверяет наличие ссылки на следующую страницу
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            next_link = soup.find('a', string=re.compile(r'далее|следующая|next', re.I))
            return next_link is not None
        except:
            return False
    
    async def _find_master_list_links(self, html: str, district: str) -> List[str]:
        """
        Ищет на странице ссылки вида "См. также: Список населённых пунктов ..."
        Возвращает список ID найденных статей
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            found_ids = []
            
            see_also_patterns = ['см. также', 'смотри также', 'см также']
            
            for pattern in see_also_patterns:
                for elem in soup.find_all(['p', 'div', 'span'], string=re.compile(pattern, re.I)):
                    parent = elem.find_parent()
                    if parent:
                        for link in parent.find_all('a', href=re.compile(r'/dic\.nsf/ruwiki/\d+')):
                            href = link.get('href', '')
                            text = link.get_text().lower()
                            
                            if 'список' in text and 'населённых пунктов' in text:
                                match = re.search(r'/dic\.nsf/ruwiki/(\d+)', href)
                                if match:
                                    article_id = match.group(1)
                                    found_ids.append(article_id)
                                    logger.info(f"      Найдена ссылка на список НП: ID {article_id} - {link.get_text()}")
            
            return list(set(found_ids))
            
        except Exception as e:
            logger.error(f"Ошибка поиска ссылок на списки: {e}")
            return []
    
    async def _parse_master_list_page(self, article_id: str, district: str) -> List[Dict]:
        """
        Парсит страницу со списком населенных пунктов
        Извлекает названия и типы
        """
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        html = await self._fetch_page(url)
        
        if not html:
            return []
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.thread_pool,
            self._parse_master_list_html,
            html,
            article_id,
            district
        )
    
    def _parse_master_list_html(self, html: str, article_id: str, district: str) -> List[Dict]:
        """
        Парсит HTML страницы со списком населенных пунктов
        Обрабатывает таблицы с данными
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            tables = soup.find_all('table', class_=['standard', 'sortable', 'wikitable'])
            
            for table in tables:
                rows = table.find_all('tr')
                if len(rows) < 2:
                    continue
                
                header_cells = rows[0].find_all(['th', 'td'])
                headers = [h.get_text().strip().lower() for h in header_cells]
                
                name_idx = self._find_column_index(headers, ['населённый пункт', 'название', 'наименование'])
                type_idx = self._find_column_index(headers, ['тип'])
                
                for row in rows[1:]:
                    try:
                        cells = row.find_all('td')
                        if len(cells) < max(filter(None, [name_idx, type_idx])) + 1:
                            continue
                        
                        if name_idx is not None and name_idx < len(cells):
                            name = cells[name_idx].get_text().strip()
                        else:
                            continue
                        
                        if not name or name in ['ИТОГО', 'Всего']:
                            continue
                        
                        village_type = 'деревня'
                        if type_idx is not None and type_idx < len(cells):
                            raw_type = cells[type_idx].get_text().strip()
                            village_type = self._expand_type(raw_type)
                        
                        results.append({
                            "name": name,
                            "type": village_type,
                            "lat": "",
                            "lon": "",
                            "district": district
                        })
                        
                    except Exception as e:
                        continue
            
            logger.info(f"      Из списка ID {article_id} получено {len(results)} записей")
            return results
            
        except Exception as e:
            logger.error(f"Ошибка парсинга страницы списка: {e}")
            return []
    
    async def _find_district_page(self, district: str) -> Optional[Dict]:
        """
        Находит страницу района, анализируя результаты поиска
        """
        cache_key = f"district_{district}"
        if cache_key in self.district_cache:
            return self.district_cache[cache_key]
        
        logger.info(f"  🔍 Поиск страницы района: {district}")
        
        queries = [
            f"{district} район",
            f"{district} район Тверская область",
            f"{district} муниципальный район",
            district
        ]
        
        all_results = []
        
        for query in queries:
            results = await self._search_with_pagination(query, max_pages=10)
            all_results.extend(results)
            await asyncio.sleep(1.0)
        
        if not all_results:
            logger.info(f"    ❌ Страница района не найдена")
            return None
        
        for result in all_results:
            score = self._score_district_relevance(result, district)
            result['score'] = score
        
        sorted_results = sorted(all_results, key=lambda x: x['score'], reverse=True)
        top_results = sorted_results[:10]
        
        for result in top_results:
            if result['score'] >= 50:
                page_url = DIC_ACADEMIC_ARTICLE_URL.format(result['id'])
                html = await self._fetch_page(page_url)
                
                if html:
                    loop = asyncio.get_event_loop()
                    is_district = await loop.run_in_executor(
                        self.thread_pool,
                        self._verify_district_page,
                        html,
                        district
                    )
                    
                    if is_district:
                        logger.info(f"    ✅ Найдена страница района (ID: {result['id']}, score: {result['score']})")
                        
                        district_info = {
                            'id': result['id'],
                            'title': result['title'],
                            'url': page_url,
                            'score': result['score']
                        }
                        
                        self.district_cache[cache_key] = district_info
                        return district_info
        
        logger.info(f"    ❌ Страница района не найдена")
        return None
    
    def _score_district_relevance(self, result: Dict, district: str) -> int:
        """
        Оценивает релевантность результата для страницы района
        """
        title_lower = result['title'].lower()
        full_text_lower = result['full_text'].lower()
        district_lower = district.lower()
        
        score = 0
        
        if f"{district_lower} район" in title_lower:
            score += 100
        elif district_lower in title_lower:
            score += 50
        
        if '(' not in result['title']:
            score += 30
        
        if result['position'] == 1:
            score += 20
        elif result['position'] <= 3:
            score += 10
        
        for keyword in DISTRICT_KEYWORDS:
            if keyword in full_text_lower:
                score += 15
        
        if "тверская область" in full_text_lower or "тверской области" in full_text_lower:
            score += 10
        
        return score
    
    def _verify_district_page(self, html: str, district: str) -> bool:
        """
        Проверяет, что страница действительно является страницей района
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            text = soup.get_text().lower()
            district_lower = district.lower()
            
            if f"{district_lower} район" not in text:
                return False
            
            expected_sections = ['география', 'история', 'население', 'состав района']
            found_sections = 0
            
            for section in expected_sections:
                if section in text:
                    found_sections += 1
            
            return found_sections >= 2
            
        except Exception as e:
            logger.error(f"Ошибка проверки страницы района: {e}")
            return False
    
    async def _extract_settlements(self, district_id: str, district: str) -> List[str]:
        """
        Извлекает список сельских поселений со страницы района
        """
        url = DIC_ACADEMIC_ARTICLE_URL.format(district_id)
        html = await self._fetch_page(url)
        
        if not html:
            return []
        
        loop = asyncio.get_event_loop()
        settlements = await loop.run_in_executor(
            self.thread_pool,
            self._parse_settlements_from_page,
            html,
            district
        )
        
        if settlements:
            logger.info(f"    Найдено сельских поселений: {len(settlements)}")
        
        return settlements
    
    def _parse_settlements_from_page(self, html: str, district: str) -> List[str]:
        """
        Парсит страницу района для извлечения списка сельских поселений
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            found_settlements = []
            
            known = KNOWN_SETTLEMENTS.get(district, [])
            text = soup.get_text()
            
            for settlement in known:
                if settlement in text:
                    found_settlements.append(settlement)
            
            if not found_settlements:
                for keyword in SETTLEMENT_KEYWORDS:
                    for header in soup.find_all(['h2', 'h3', 'h4']):
                        if keyword in header.get_text().lower():
                            parent = header.find_parent()
                            if parent:
                                for ul in parent.find_all('ul'):
                                    for li in ul.find_all('li'):
                                        text = li.get_text().strip()
                                        if text and len(text) < 50 and not re.search(r'\d', text):
                                            if any(word in text.lower() for word in ['ское', 'ское сп']):
                                                found_settlements.append(text)
            
            unique_settlements = []
            seen = set()
            for s in found_settlements:
                if s not in seen:
                    unique_settlements.append(s)
                    seen.add(s)
            
            return unique_settlements
            
        except Exception as e:
            logger.error(f"Ошибка парсинга сельских поселений: {e}")
            return []
    
    async def _find_settlement_page(self, settlement: str, district: str) -> Optional[str]:
        """
        Находит страницу с бывшими населенными пунктами для сельского поселения
        """
        cache_key = f"settlement_page_{district}_{settlement}"
        if cache_key in self.settlement_pages_cache:
            return self.settlement_pages_cache[cache_key]
        
        queries = [
            f"Список бывших населённых пунктов на территории сельского поселения {settlement} {district} района",
            f"Список бывших населенных пунктов на территории сельского поселения {settlement} {district} района",
            f"Список бывших населённых пунктов {settlement} {district} района",
            f"Бывшие населённые пункты {settlement} СП",
            f"Список бывших населённых пунктов {settlement} сельского поселения",
            f"{settlement} сельское поселение бывшие населенные пункты"
        ]
        
        all_results = []
        
        for query in queries:
            results = await self._search_with_pagination(query, max_pages=20)
            all_results.extend(results)
            await asyncio.sleep(1.0)
        
        if not all_results:
            return None
        
        for result in all_results:
            score = self._score_settlement_relevance(result, settlement, district)
            result['score'] = score
        
        best = max(all_results, key=lambda x: x['score'])
        
        if best['score'] >= 40:
            logger.info(f"      Найдена страница для СП {settlement} (ID: {best['id']}, score: {best['score']})")
            self.settlement_pages_cache[cache_key] = best['id']
            return best['id']
        
        return None
    
    def _score_settlement_relevance(self, result: Dict, settlement: str, district: str) -> int:
        """
        Оценивает релевантность результата для страницы сельского поселения
        """
        title_lower = result['title'].lower()
        full_text_lower = result['full_text'].lower()
        settlement_lower = settlement.lower()
        district_lower = district.lower()
        
        score = 0
        
        if settlement_lower in title_lower:
            score += 50
        
        if "список бывших" in title_lower:
            score += 40
        elif "бывшие населённые" in title_lower:
            score += 30
        
        if district_lower in title_lower or district_lower in full_text_lower:
            score += 20
        
        if "сельское поселение" in title_lower or "сельского поселения" in title_lower:
            score += 25
        
        if result['position'] == 1:
            score += 15
        elif result['position'] <= 3:
            score += 10
        
        return score
    
    async def _parse_settlement_page(self, article_id: str, district: str, settlement: str) -> List[Dict]:
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
            settlement
        )
    
    def _parse_settlement_page_html(self, html: str, article_id: str, district: str, settlement: str) -> List[Dict]:
        """
        Парсит HTML страницы с бывшими НП сельского поселения
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            tables = soup.find_all('table', class_=['standard', 'sortable'])
            
            for table in tables:
                rows = table.find_all('tr')
                if len(rows) < 2:
                    continue
                
                header_cells = rows[0].find_all(['th', 'td'])
                headers = [h.get_text().strip().lower() for h in header_cells]
                
                name_idx = self._find_column_index(headers, ['населённый пункт', 'название'])
                type_idx = self._find_column_index(headers, ['тип'])
                coords_idx = self._find_column_index(headers, ['координаты', 'коорд'])
                
                for row in rows[1:]:
                    try:
                        cells = row.find_all('td')
                        if len(cells) < max(filter(None, [name_idx, type_idx])) + 1:
                            continue
                        
                        if name_idx is not None and name_idx < len(cells):
                            name = cells[name_idx].get_text().strip()
                        else:
                            continue
                        
                        if not name or name in ['ИТОГО', 'Всего']:
                            continue
                        
                        village_type = 'деревня'
                        if type_idx is not None and type_idx < len(cells):
                            raw_type = cells[type_idx].get_text().strip()
                            village_type = self._expand_type(raw_type)
                        
                        lat, lon = None, None
                        if coords_idx is not None and coords_idx < len(cells):
                            lat, lon = self._parse_coordinates_universal('', cells[coords_idx])
                        
                        if not lat or not lon:
                            row_text = ' '.join([c.get_text() for c in cells])
                            lat, lon = self._parse_coordinates_universal(row_text, None)
                        
                        results.append({
                            "name": name,
                            "type": village_type,
                            "lat": str(round(lat, 5)) if lat else "",
                            "lon": str(round(lon, 5)) if lon else "",
                            "district": district
                        })
                        
                    except Exception as e:
                        continue
            
            return results
            
        except Exception as e:
            logger.error(f"Ошибка парсинга страницы СП: {e}")
            return []
    
    async def _parse_individual_village_page(self, article_id: str, district: str) -> Optional[Dict]:
        """
        Парсит отдельную страницу населенного пункта для извлечения координат
        """
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        html = await self._fetch_page(url)
        
        if not html:
            return None
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.thread_pool,
            self._parse_individual_village_html,
            html,
            article_id,
            district
        )
    
    def _parse_individual_village_html(self, html: str, article_id: str, district: str) -> Optional[Dict]:
        """
        Парсит отдельную страницу населенного пункта
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            title_elem = soup.find('h1')
            if not title_elem:
                return None
            
            full_title = title_elem.get_text().strip()
            
            name = full_title
            village_type = 'деревня'
            
            type_match = re.search(r'\(([^)]+)\)$', full_title)
            if type_match:
                possible_type = type_match.group(1).lower()
                name = full_title.replace(f'({possible_type})', '').strip()
                village_type = self._expand_type(possible_type)
            else:
                type_match = re.search(r',\s*([^,]+)$', full_title)
                if type_match:
                    possible_type = type_match.group(1).lower()
                    name = full_title.replace(f', {possible_type}', '').strip()
                    village_type = self._expand_type(possible_type)
            
            lat, lon = None, None
            
            geo_span = soup.find('span', class_='geo')
            if geo_span:
                lat_span = geo_span.find('span', class_='latitude')
                lon_span = geo_span.find('span', class_='longitude')
                
                if lat_span and lon_span:
                    try:
                        lat = float(lat_span.get_text().strip())
                        lon = float(lon_span.get_text().strip())
                    except:
                        pass
            
            if not lat or not lon:
                text = soup.get_text()
                lat, lon = self._parse_coordinates_universal(text, None)
            
            if lat and lon:
                return {
                    "name": name,
                    "type": village_type,
                    "lat": str(round(lat, 5)),
                    "lon": str(round(lon, 5)),
                    "district": district
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Ошибка парсинга отдельной страницы НП: {e}")
            return None
    
    async def fetch_district_data(self, district: str) -> List[Dict]:
        """
        Основной метод: загружает данные для конкретного района
        С быстрой проверкой дубликатов через set
        """
        logger.info(f"🌐 Загрузка данных для района: {district}")
        
        all_villages = []
        found_settlements = {}
        processed_master_lists = set()
        seen_villages = set()
        
        district_info = await self._find_district_page(district)
        
        if not district_info:
            logger.warning(f"  ⚠️ Страница района не найдена")
            return []
        
        settlements = await self._extract_settlements(district_info['id'], district)
        
        if not settlements:
            logger.warning(f"  ⚠️ Сельские поселения не найдены на странице района")
            settlements = KNOWN_SETTLEMENTS.get(district, [])
            if settlements:
                logger.info(f"    Используем известный список СП: {len(settlements)}")
        
        logger.info(f"  🔍 Поиск страниц для {len(settlements)} сельских поселений...")
        
        for settlement in settlements:
            try:
                await asyncio.sleep(1.0)
                
                article_id = await self._find_settlement_page(settlement, district)
                
                if article_id:
                    url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
                    html = await self._fetch_page(url)
                    
                    if html:
                        data = await self._parse_settlement_page(article_id, district, settlement)
                        
                        new_count = 0
                        for village in data:
                            key = f"{village['name']}_{village['district']}"
                            if key not in seen_villages:
                                seen_villages.add(key)
                                all_villages.append(village)
                                new_count += 1
                        
                        if data:
                            found_settlements[settlement] = len(data)
                            logger.info(f"    ✅ СП {settlement}: добавлено {new_count} новых записей")
                        
                        master_list_ids = await self._find_master_list_links(html, district)
                        
                        for list_id in master_list_ids:
                            if list_id not in processed_master_lists:
                                processed_master_lists.add(list_id)
                                logger.info(f"      Обрабатываем дополнительный список ID {list_id}")
                                
                                list_data = await self._parse_master_list_page(list_id, district)
                                
                                list_new = 0
                                for village in list_data:
                                    key = f"{village['name']}_{village['district']}"
                                    if key not in seen_villages:
                                        seen_villages.add(key)
                                        all_villages.append(village)
                                        list_new += 1
                                
                                logger.info(f"        Добавлено {list_new} новых записей из списка")
                    else:
                        logger.info(f"    ⏭️ СП {settlement}: страница найдена, но не загрузилась")
                else:
                    logger.info(f"    ⏭️ СП {settlement}: страница не найдена")
                    
            except Exception as e:
                logger.error(f"    ❌ Ошибка обработки СП {settlement}: {e}")
        
        if all_villages:
            logger.info(f"  🔍 Поиск координат для записей без них...")
            
            villages_without_coords = [v for v in all_villages if not v.get('lat')]
            
            if villages_without_coords:
                logger.info(f"    Найдено {len(villages_without_coords)} записей без координат")
                
                for i, village in enumerate(villages_without_coords[:100]):
                    try:
                        if i > 0:
                            await asyncio.sleep(1.0)
                        
                        query = f"{village['name']} {district} район"
                        results = await self._search_with_pagination(query, max_pages=30, unlimited=False)
                        
                        if results:
                            article_id = results[0]['id']
                            village_data = await self._parse_individual_village_page(article_id, district)
                            
                            if village_data and village_data.get('lat'):
                                for v in all_villages:
                                    if v['name'] == village['name'] and not v.get('lat'):
                                        v['lat'] = village_data['lat']
                                        v['lon'] = village_data['lon']
                                        logger.info(f"      ✅ Найдены координаты для {v['name']}")
                                        break
                        
                        if (i + 1) % 10 == 0:
                            logger.info(f"      Обработано {i+1}/{len(villages_without_coords[:100])}")
                            
                    except Exception as e:
                        continue
        
        logger.info(f"  ✅ Всего уникальных записей: {len(all_villages)}")
        logger.info(f"  ✅ С координатами: {sum(1 for v in all_villages if v.get('lat'))}")
        
        return all_villages
    
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
        """
        Универсальный парсер координат для всех форматов
        """
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
                            if self._validate_coordinates(lat, lon):
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
                
                if self._validate_coordinates(lat, lon):
                    return lat, lon
            
            decimal_pattern = r'([0-9]+\.[0-9]+)[,\s]+([0-9]+\.[0-9]+)'
            match = re.search(decimal_pattern, text)
            
            if match:
                lat = float(match.group(1))
                lon = float(match.group(2))
                
                if self._validate_coordinates(lat, lon):
                    return lat, lon
            
            numbers = re.findall(r'[\d.]+', text)
            if len(numbers) >= 2:
                lat = float(numbers[0])
                lon = float(numbers[1])
                
                if self._validate_coordinates(lat, lon):
                    return lat, lon
            
            return None, None
            
        except Exception:
            return None, None
    
    def _validate_coordinates(self, lat: float, lon: float) -> bool:
        """Проверяет координаты (примерные границы для Тверской области)"""
        return (55.0 <= lat <= 58.0 and 30.0 <= lon <= 38.0)

# Экспортируем список районов для использования в bot.py
AVAILABLE_DISTRICTS = DISTRICTS