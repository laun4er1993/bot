# api_sources.py
# Парсер для загрузки данных о населенных пунктах из dic.academic.ru и Wikipedia
# Приоритет: dic.academic.ru (основной) -> Wikipedia (дополнительный)

import aiohttp
import asyncio
import logging
from typing import List, Dict, Optional, Tuple, Set
import re
import random
import time
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote, quote_plus

logger = logging.getLogger(__name__)

# ========== КОНФИГУРАЦИЯ ==========

AVAILABLE_DISTRICTS = ["Ржевский", "Оленинский", "Зубцовский", "Бельский"]

DISTRICT_WIKI_NAMES = {
    "Ржевский": ["Ржевский муниципальный округ", "Ржевский район"],
    "Оленинский": ["Оленинский муниципальный округ", "Оленинский район"],
    "Зубцовский": ["Зубцовский муниципальный округ", "Зубцовский район"],
    "Бельский": ["Бельский муниципальный округ", "Бельский район"]
}

DIC_ACADEMIC_BASE_URL = "https://dic.academic.ru"
DIC_ACADEMIC_SEARCH_URL = "https://dic.academic.ru/searchall.php"
DIC_ACADEMIC_ARTICLE_URL = "https://dic.academic.ru/dic.nsf/ruwiki/{}"
WIKIPEDIA_BASE_URL = "https://ru.wikipedia.org"
WIKIPEDIA_SEARCH_URL = "https://ru.wikipedia.org/w/api.php"

TYPE_MAPPING = {
    'дер.': 'деревня', 'д.': 'деревня', 'пос.': 'посёлок', 'п.': 'посёлок',
    'с.': 'село', 'х.': 'хутор', 'ур.': 'урочище', 'ст.': 'станция',
    'разъезд': 'разъезд', 'кордон': 'кордон', 'сл.': 'слобода'
}

TYPE_SHORT = {
    'деревня': 'дер.', 'село': 'с.', 'посёлок': 'пос.', 'хутор': 'х.', 'урочище': 'ур.'
}

LIST_KEYWORDS = [
    "список населённых пунктов", "список населенных пунктов",
    "список бывших населённых пунктов", "бывшие населённые пункты"
]

SERVICE_VILLAGE_WORDS = [
    'россия', 'ржев', 'тверская', 'область', 'федерация',
    'тыс', 'чел', 'население', 'площадь', 'км', 'район',
    'статья', 'категория', 'примечания', 'ссылки', 'всего', 'итого'
]

MIN_NAME_LENGTH = 2
MAX_NAME_LENGTH = 50
TYPE_INDICATORS = ['дер.', 'д.', 'пос.', 'п.', 'с.', 'х.', 'ур.', 'ст.', 'разъезд']


class APISourceManager:
    """
    Менеджер для загрузки данных о населенных пунктах.
    
    Алгоритм:
    1. Находит страницу района на dic.academic.ru
    2. Собирает НП из:
       - общих списков на странице района
       - страниц бывших НП (часто содержат координаты)
       - страниц сельских поселений
    3. Для НП без координат:
       - Сначала ищет на dic.academic.ru по сохраненным ссылкам
       - Если не найдено, ищет на Wikipedia
    4. Возвращает список уникальных НП с координатами (где найдены)
    """
    
    def __init__(self):
        self.session = None
        self.thread_pool = ThreadPoolExecutor(max_workers=3)
        
        self.request_count = 0
        self.last_request_time = 0
        self.min_request_interval = 1.5
        
        # Кэши
        self.district_cache = {}
        self.former_cache = {}
        self.settlement_cache = {}
        self.page_cache = {}
        self.processed_ids = set()
        
        # Ссылки на НП
        self.dic_links = {}      # название -> ID на dic.academic.ru
        self.wiki_links = {}     # название -> URL на Wikipedia
        
        # Временный кэш координат
        self.coords_cache = {}
        
        self.cache_ttl = 3600
        self.max_retries = 3
        self.start_time = 0
        
        # Статистика сбора НП
        self.collection_stats = {
            'from_master_lists': 0,
            'from_former': 0,
            'from_settlements': 0,
            'total_unique': 0
        }
        
        # Статистика поиска координат
        self.coords_stats = {
            'from_former': 0,
            'from_dic_link': 0,
            'from_wiki': 0,
            'from_cache': 0,
            'total_without': 0,
            'found': 0,
            'remaining': 0
        }
        
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml',
            'Accept-Language': 'ru-RU,ru;q=0.8',
        }
    
    async def _get_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def close_session(self):
        if self.session and not self.session.closed:
            await self.session.close()
        self.thread_pool.shutdown(wait=False)
    
    def clear_cache(self):
        """Очищает все кэши перед новым поиском"""
        self.district_cache.clear()
        self.former_cache.clear()
        self.settlement_cache.clear()
        self.page_cache.clear()
        self.processed_ids.clear()
        self.dic_links.clear()
        self.wiki_links.clear()
        self.coords_cache.clear()
        
        self.collection_stats = {'from_master_lists': 0, 'from_former': 0, 'from_settlements': 0, 'total_unique': 0}
        self.coords_stats = {'from_former': 0, 'from_dic_link': 0, 'from_wiki': 0, 'from_cache': 0, 'total_without': 0, 'found': 0, 'remaining': 0}
        
        logger.info("🧹 Кэш очищен")
    
    async def _rate_limit(self):
        current_time = time.time()
        interval = self.min_request_interval * random.uniform(0.8, 1.2)
        
        if current_time - self.last_request_time < interval:
            await asyncio.sleep(interval - (current_time - self.last_request_time))
        
        self.last_request_time = time.time()
        self.request_count += 1
        
        if self.request_count % 50 == 0:
            elapsed = time.time() - self.start_time
            logger.info(f"    📊 Запросов: {self.request_count} за {elapsed:.1f}с")
    
    async def _fetch_page(self, url: str) -> Optional[str]:
        current_time = time.time()
        
        if url in self.page_cache:
            html, ts = self.page_cache[url]
            if current_time - ts < self.cache_ttl:
                logger.debug(f"    🔄 Кэш: {url[:80]}...")
                return html
        
        for attempt in range(self.max_retries):
            try:
                session = await self._get_session()
                await self._rate_limit()
                
                logger.debug(f"    🌐 Загрузка: {url[:80]}...")
                async with session.get(url, headers=self.headers, timeout=60) as resp:
                    if resp.status == 200:
                        html = await resp.text()
                        self.page_cache[url] = (html, current_time)
                        return html
                    elif resp.status == 429:
                        wait = 2 ** attempt
                        logger.warning(f"    ⚠️ Ошибка 429, повтор через {wait}с")
                        await asyncio.sleep(wait)
                    else:
                        logger.debug(f"    ❌ HTTP {resp.status}: {url[:80]}")
                        return None
            except asyncio.TimeoutError:
                logger.warning(f"    ⏱️ Таймаут, попытка {attempt + 1}/{self.max_retries}")
                if attempt == self.max_retries - 1:
                    return None
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"    ❌ Ошибка: {e}")
                return None
        
        return None
    
    def _is_valid_name(self, name: str) -> bool:
        if not name or len(name) < MIN_NAME_LENGTH or len(name) > MAX_NAME_LENGTH:
            return False
        name_lower = name.lower()
        for word in SERVICE_VILLAGE_WORDS:
            if word in name_lower:
                return False
        return bool(re.search(r'[а-яА-ЯёЁ]', name)) and not name.isdigit()
    
    def _expand_type(self, short: str) -> str:
        if not short:
            return 'деревня'
        clean = short.rstrip('.').lower().strip()
        return TYPE_MAPPING.get(clean, clean if clean in TYPE_MAPPING.values() else 'деревня')
    
    # ========== ПОИСК НА DIC.ACADEMIC.RU ==========
    
    async def _search_dic(self, query: str, max_pages: int = 5) -> List[Dict]:
        all_results = []
        page = 1
        
        while page <= max_pages:
            if page > 1:
                await asyncio.sleep(1.5)
            
            url = f"{DIC_ACADEMIC_SEARCH_URL}?SWord={quote(query)}"
            if page > 1:
                url += f"&page={page}"
            
            html = await self._fetch_page(url)
            if not html:
                break
            
            loop = asyncio.get_event_loop()
            results = await loop.run_in_executor(self.thread_pool, self._parse_search, html, page)
            
            if not results:
                break
            
            all_results.extend(results)
            logger.debug(f"      📄 Страница {page}: {len(results)} результатов")
            
            has_next = await loop.run_in_executor(self.thread_pool, self._has_next, html)
            if not has_next:
                break
            
            page += 1
        
        return all_results
    
    def _parse_search(self, html: str, page_num: int) -> List[Dict]:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            for link in soup.find_all('a', href=re.compile(r'/dic\.nsf/ruwiki/\d+')):
                href = link.get('href', '')
                match = re.search(r'/dic\.nsf/ruwiki/(\d+)', href)
                if not match:
                    continue
                
                pid = match.group(1)
                title = link.get_text().strip()
                
                parent = link.find_parent()
                text = ""
                if parent:
                    desc = parent.find_next('span', class_='description')
                    text = desc.get_text().strip() if desc else parent.get_text().strip()
                
                pos = re.match(r'^(\d+)', text)
                results.append({
                    'id': pid, 'title': title, 'text': text,
                    'page': page_num, 'position': int(pos.group(1)) if pos else 0
                })
            
            return results
        except:
            return []
    
    def _has_next(self, html: str) -> bool:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            return soup.find('a', string=re.compile(r'далее|следующая|next', re.I)) is not None
        except:
            return False
    
    async def _find_district_page(self, district: str) -> Optional[Dict]:
        cache_key = f"district_{district}"
        if cache_key in self.district_cache:
            logger.info(f"  🔍 Страница района в кэше: {self.district_cache[cache_key]['url']}")
            return self.district_cache[cache_key]
        
        logger.info(f"  🔍 Поиск страницы района на dic.academic.ru: {district}")
        
        queries = [f"{district} район", f"{district} район Тверская область", district]
        
        all_results = []
        for q in queries:
            logger.debug(f"    🔎 Запрос: {q}")
            results = await self._search_dic(q, max_pages=5)
            all_results.extend(results)
            await asyncio.sleep(1)
        
        if not all_results:
            logger.warning(f"    ❌ Результатов не найдено")
            return None
        
        # Оценка релевантности
        for r in all_results:
            score = 0
            title = r['title'].lower()
            text = r['text'].lower()
            d_lower = district.lower()
            
            if f"{d_lower} район" in title:
                score += 100
                logger.debug(f"      {r['title']}: +100 (название района)")
            elif d_lower in title:
                score += 50
                logger.debug(f"      {r['title']}: +50 (название в заголовке)")
            
            if r['position'] == 1:
                score += 20
                logger.debug(f"      {r['title']}: +20 (позиция 1)")
            elif r['position'] <= 3:
                score += 10
                logger.debug(f"      {r['title']}: +10 (позиция {r['position']})")
            
            if "тверская область" in text:
                score += 10
                logger.debug(f"      {r['title']}: +10 (Тверская область)")
            
            r['score'] = score
            logger.debug(f"      {r['title']}: итого {score}")
        
        sorted_results = sorted(all_results, key=lambda x: x['score'], reverse=True)
        
        for r in sorted_results[:5]:
            if r['score'] >= 50:
                url = DIC_ACADEMIC_ARTICLE_URL.format(r['id'])
                logger.info(f"    📄 Проверяем страницу: {r['title']} (score: {r['score']})")
                html = await self._fetch_page(url)
                if html and self._is_district_page(html, district):
                    info = {'id': r['id'], 'title': r['title'], 'url': url, 'score': r['score']}
                    self.district_cache[cache_key] = info
                    logger.info(f"    ✅ Найдена страница района: {r['title']} (ID: {r['id']})")
                    return info
        
        logger.warning(f"    ❌ Страница района не найдена")
        return None
    
    def _is_district_page(self, html: str, district: str) -> bool:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            text = soup.get_text().lower()
            d_lower = district.lower()
            
            if f"{d_lower} район" not in text:
                return False
            
            sections = ['география', 'история', 'население', 'состав района']
            found = sum(1 for s in sections if s in text)
            logger.debug(f"    Проверка страницы: найдено разделов: {found}/4")
            return found >= 2
        except:
            return False
    
    async def _find_former_page(self, settlement: str, district: str) -> Optional[str]:
        cache_key = f"former_{district}_{settlement}"
        if cache_key in self.former_cache:
            logger.debug(f"      🔄 Страница бывших НП в кэше")
            return self.former_cache[cache_key]
        
        queries = [
            f"Список бывших населённых пунктов {settlement} {district} района",
            f"Бывшие населённые пункты {settlement} СП",
        ]
        
        all_results = []
        for q in queries:
            results = await self._search_dic(q, max_pages=10)
            all_results.extend(results)
            await asyncio.sleep(1)
        
        if not all_results:
            return None
        
        for r in all_results:
            title = r['title'].lower()
            if "список бывших" in title and settlement.lower() in title:
                r['score'] = 150
            else:
                score = 0
                if settlement.lower() in title:
                    score += 50
                if r['position'] == 1:
                    score += 15
                r['score'] = score
        
        best = max(all_results, key=lambda x: x['score'])
        
        if best['score'] >= 50:
            logger.info(f"      ✅ Найдена страница бывших НП для {settlement}: ID {best['id']} (score: {best['score']})")
            self.former_cache[cache_key] = best['id']
            return best['id']
        
        return None
    
    async def _find_settlement_page(self, settlement: str, district: str) -> Optional[str]:
        cache_key = f"settlement_{district}_{settlement}"
        if cache_key in self.settlement_cache:
            logger.debug(f"      🔄 Страница СП в кэше")
            return self.settlement_cache[cache_key]
        
        queries = [f"Сельское поселение {settlement}", f"{settlement} СП"]
        
        all_results = []
        for q in queries:
            results = await self._search_dic(q, max_pages=10)
            all_results.extend(results)
            await asyncio.sleep(1)
        
        if not all_results:
            return None
        
        for r in all_results:
            title = r['title'].lower()
            score = 0
            if settlement.lower() in title:
                score += 50
            if "сельское поселение" in title:
                score += 40
            if r['position'] == 1:
                score += 15
            r['score'] = score
        
        best = max(all_results, key=lambda x: x['score'])
        
        if best['score'] >= 40:
            logger.info(f"      ✅ Найдена страница СП {settlement}: ID {best['id']} (score: {best['score']})")
            self.settlement_cache[cache_key] = best['id']
            return best['id']
        
        return None
    
    async def _parse_former_page(self, article_id: str, district: str, settlement: str) -> List[Dict]:
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        logger.debug(f"        📄 Парсим страницу бывших НП: {url}")
        html = await self._fetch_page(url)
        if not html:
            return []
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.thread_pool, self._parse_former_html, html, district, settlement)
    
    def _parse_former_html(self, html: str, district: str, settlement: str) -> List[Dict]:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            found_with_coords = 0
            
            for table in soup.find_all('table', class_=['standard', 'sortable']):
                rows = table.find_all('tr')
                if len(rows) < 2:
                    continue
                
                headers = [h.get_text().strip().lower() for h in rows[0].find_all(['th', 'td'])]
                name_idx = self._find_col(headers, ['населённый пункт', 'название'])
                type_idx = self._find_col(headers, ['тип'])
                coords_idx = self._find_col(headers, ['координаты', 'коорд'])
                
                if name_idx is None:
                    continue
                
                for row in rows[1:]:
                    cells = row.find_all('td')
                    if len(cells) <= name_idx:
                        continue
                    
                    name = cells[name_idx].get_text().strip()
                    if not name or name in ['ИТОГО', 'Всего', 'Итого']:
                        continue
                    
                    if not self._is_valid_name(name):
                        continue
                    
                    vtype = 'деревня'
                    if type_idx is not None and type_idx < len(cells):
                        vtype = self._expand_type(cells[type_idx].get_text().strip())
                    
                    lat = lon = None
                    if coords_idx is not None and coords_idx < len(cells):
                        lat, lon = self._parse_coords('', cells[coords_idx])
                    
                    if not lat or not lon:
                        row_text = ' '.join([c.get_text() for c in cells])
                        lat, lon = self._parse_coords(row_text, None)
                    
                    if lat and lon:
                        self.coords_cache[name] = (str(round(lat, 5)), str(round(lon, 5)))
                        found_with_coords += 1
                        logger.debug(f"          📍 Найдены координаты для {name}: {lat:.5f}, {lon:.5f}")
                    
                    results.append({
                        "name": name, "type": vtype,
                        "lat": str(round(lat, 5)) if lat else "",
                        "lon": str(round(lon, 5)) if lon else "",
                        "district": district
                    })
            
            if results:
                logger.info(f"        📊 Из страницы бывших НП получено {len(results)} записей, из них с координатами: {found_with_coords}")
            return results
            
        except Exception as e:
            logger.error(f"        ❌ Ошибка парсинга: {e}")
            return []
    
    async def _parse_settlement_page(self, article_id: str, district: str, settlement: str) -> List[Dict]:
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        logger.debug(f"        📄 Парсим страницу СП: {url}")
        html = await self._fetch_page(url)
        if not html:
            return []
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.thread_pool, self._parse_settlement_html, html, district, settlement)
    
    def _parse_settlement_html(self, html: str, district: str, settlement: str) -> List[Dict]:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            links_found = 0
            
            for table in soup.find_all('table', class_=['standard', 'sortable', 'wikitable']):
                rows = table.find_all('tr')
                if len(rows) < 2:
                    continue
                
                headers = [h.get_text().strip().lower() for h in rows[0].find_all(['th', 'td'])]
                name_idx = self._find_col(headers, ['название', 'населённый пункт'])
                type_idx = self._find_col(headers, ['тип'])
                
                if name_idx is None:
                    continue
                
                for row in rows[1:]:
                    cells = row.find_all('td')
                    if len(cells) <= name_idx:
                        continue
                    
                    name = cells[name_idx].get_text().strip()
                    name = re.sub(r'^\d+\s*', '', name).strip()
                    
                    if not name or not self._is_valid_name(name):
                        continue
                    
                    vtype = 'деревня'
                    if type_idx is not None and type_idx < len(cells):
                        vtype = self._expand_type(cells[type_idx].get_text().strip())
                    
                    link = cells[name_idx].find('a')
                    if link:
                        href = link.get('href', '')
                        match = re.search(r'(\d+)', href)
                        if match:
                            self.dic_links[name] = match.group(1)
                            links_found += 1
                            logger.debug(f"          🔗 Найдена ссылка для {name}: ID {match.group(1)}")
                    
                    results.append({
                        "name": name, "type": vtype,
                        "lat": "", "lon": "",
                        "district": district
                    })
            
            if results:
                logger.info(f"        📊 Из страницы СП получено {len(results)} записей, найдено ссылок: {links_found}")
            return results
            
        except Exception as e:
            logger.error(f"        ❌ Ошибка парсинга: {e}")
            return []
    
    async def _parse_master_list(self, article_id: str, district: str) -> List[Dict]:
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        logger.debug(f"      📄 Парсим общий список: {url}")
        html = await self._fetch_page(url)
        if not html:
            return []
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.thread_pool, self._parse_list_html, html, district)
    
    def _parse_list_html(self, html: str, district: str) -> List[Dict]:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            for table in soup.find_all('table', class_=['standard', 'sortable', 'wikitable']):
                rows = table.find_all('tr')
                if len(rows) < 2:
                    continue
                
                headers = [h.get_text().strip().lower() for h in rows[0].find_all(['th', 'td'])]
                name_idx = self._find_col(headers, ['населённый пункт', 'название', 'пункт'])
                type_idx = self._find_col(headers, ['тип'])
                
                if name_idx is None:
                    name_idx = 0
                
                for row in rows[1:]:
                    cells = row.find_all('td')
                    if len(cells) <= name_idx:
                        continue
                    
                    name = cells[name_idx].get_text().strip()
                    if not name or name in ['ИТОГО', 'Всего', 'Итого']:
                        continue
                    
                    name = re.sub(r'^\d+\s*', '', name).strip()
                    if not self._is_valid_name(name):
                        continue
                    
                    vtype = 'деревня'
                    if type_idx is not None and type_idx < len(cells):
                        vtype = self._expand_type(cells[type_idx].get_text().strip())
                    
                    results.append({
                        "name": name, "type": vtype,
                        "lat": "", "lon": "",
                        "district": district
                    })
            
            return results
        except Exception as e:
            logger.error(f"      ❌ Ошибка парсинга: {e}")
            return []
    
    async def _get_dic_coords(self, article_id: str, name: str, district: str) -> Optional[Dict]:
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        logger.debug(f"        🔍 Загружаем страницу dic.academic.ru для {name}: {url}")
        html = await self._fetch_page(url)
        if not html:
            return None
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.thread_pool, self._parse_dic_coords, html, name, district)
    
    def _parse_dic_coords(self, html: str, name: str, district: str) -> Optional[Dict]:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            geo = soup.find('span', class_='geo')
            if geo:
                lat_span = geo.find('span', class_='latitude')
                lon_span = geo.find('span', class_='longitude')
                if lat_span and lon_span:
                    try:
                        lat = float(lat_span.get_text().strip())
                        lon = float(lon_span.get_text().strip())
                        if self._validate_coords(lat, lon):
                            logger.info(f"          ✅ Найдены координаты на dic.academic.ru: {lat:.5f}, {lon:.5f}")
                            return {
                                "name": name, "type": 'деревня',
                                "lat": str(round(lat, 5)), "lon": str(round(lon, 5)),
                                "district": district
                            }
                    except:
                        pass
            
            dms = r'(\d+)°(\d+)′([\d.]+)″.*?(\d+)°(\d+)′([\d.]+)″'
            text = soup.get_text()
            match = re.search(dms, text)
            if match:
                try:
                    lat = float(match.group(1)) + float(match.group(2))/60 + float(match.group(3))/3600
                    lon = float(match.group(4)) + float(match.group(5))/60 + float(match.group(6))/3600
                    if self._validate_coords(lat, lon):
                        logger.info(f"          ✅ Найдены координаты через DMS: {lat:.5f}, {lon:.5f}")
                        return {
                            "name": name, "type": 'деревня',
                            "lat": str(round(lat, 5)), "lon": str(round(lon, 5)),
                            "district": district
                        }
                except:
                    pass
            
            logger.debug(f"          ❌ Координаты не найдены на странице dic.academic.ru")
            return None
        except Exception as e:
            logger.error(f"          ❌ Ошибка: {e}")
            return None
    
    # ========== ПОИСК НА WIKIPEDIA ==========
    
    async def _find_wiki_district_page(self, district: str) -> Optional[str]:
        logger.info(f"  🔍 Поиск страницы района на Wikipedia: {district}")
        
        for name in DISTRICT_WIKI_NAMES.get(district, [district, f"{district} район"]):
            url = f"{WIKIPEDIA_BASE_URL}/wiki/{quote_plus(name)}"
            logger.debug(f"    🔎 Пробуем: {url}")
            html = await self._fetch_page(url)
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                if not soup.find('div', class_='noarticletext'):
                    text = soup.get_text().lower()
                    if 'тверская область' in text:
                        logger.info(f"    ✅ Найдена страница района: {url}")
                        return url
            await asyncio.sleep(1)
        
        # Поиск через API
        logger.info(f"    🔎 Пробуем поиск через API Wikipedia")
        search_url = f"{WIKIPEDIA_SEARCH_URL}?action=query&list=search&srsearch={quote_plus(f'{district} район Тверская область')}&format=json"
        html = await self._fetch_page(search_url)
        if html:
            try:
                import json
                data = json.loads(html)
                for r in data.get('query', {}).get('search', [])[:3]:
                    url = f"{WIKIPEDIA_BASE_URL}/wiki/{quote_plus(r['title'])}"
                    logger.debug(f"    🔎 Проверяем: {url}")
                    page_html = await self._fetch_page(url)
                    if page_html and 'тверская область' in page_html.lower():
                        logger.info(f"    ✅ Найдена страница через API: {url}")
                        return url
                    await asyncio.sleep(1)
            except Exception as e:
                logger.error(f"    ❌ Ошибка API: {e}")
        
        logger.warning(f"    ❌ Страница района на Wikipedia не найдена")
        return None
    
    async def _extract_wiki_links(self, page_url: str) -> Dict[str, str]:
        logger.info(f"  🔍 Извлечение ссылок на НП из Wikipedia")
        
        html = await self._fetch_page(page_url)
        if not html:
            return {}
        
        soup = BeautifulSoup(html, 'html.parser')
        links = {}
        
        for table in soup.find_all('table', class_=['standard', 'wikitable', 'sortable']):
            headers = [h.get_text().strip().lower() for h in table.find_all('th')]
            col_idx = None
            for i, h in enumerate(headers):
                if 'населённый пункт' in h or 'населенный пункт' in h or 'название' in h:
                    col_idx = i
                    break
            
            if col_idx is None:
                continue
            
            for row in table.find_all('tr')[1:]:
                cells = row.find_all('td')
                if len(cells) <= col_idx:
                    continue
                
                cell = cells[col_idx]
                link = cell.find('a')
                if link and link.get('href', '').startswith('/wiki/') and ':' not in link['href']:
                    name = re.sub(r'\[\d+\]', '', link.get_text().strip())
                    if name and self._is_valid_name(name):
                        links[name] = f"{WIKIPEDIA_BASE_URL}{link['href']}"
                        logger.debug(f"      🔗 Найдена ссылка: {name}")
        
        logger.info(f"    📊 Найдено {len(links)} ссылок на НП в Wikipedia")
        return links
    
    async def _get_wiki_coords(self, url: str, name: str, district: str) -> Optional[Dict]:
        logger.debug(f"      🔍 Загружаем Wikipedia страницу: {url}")
        html = await self._fetch_page(url)
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'html.parser')
        
        geo = soup.find('span', class_='geo')
        if geo:
            lat_span = geo.find('span', class_='latitude')
            lon_span = geo.find('span', class_='longitude')
            if lat_span and lon_span:
                try:
                    lat = float(lat_span.get_text().strip())
                    lon = float(lon_span.get_text().strip())
                    if self._validate_coords(lat, lon):
                        logger.info(f"          ✅ Найдены координаты в Wikipedia: {lat:.5f}, {lon:.5f}")
                        return {
                            "name": name, "type": 'деревня',
                            "lat": str(round(lat, 5)), "lon": str(round(lon, 5)),
                            "district": district
                        }
                except:
                    pass
        
        logger.debug(f"          ❌ Координаты не найдены в Wikipedia")
        return None
    
    # ========== ОСНОВНОЙ МЕТОД ==========
    
    async def fetch_district_data(self, district: str) -> List[Dict]:
        """Основной метод: загружает данные для района"""
        self.clear_cache()
        self.start_time = time.time()
        logger.info(f"🌐 ========== ЗАГРУЗКА ДАННЫХ ДЛЯ РАЙОНА: {district} ==========")
        
        all_villages = []
        seen = set()
        processed_master_lists = set()
        
        # Шаг 1: Находим страницу района на dic.academic.ru
        logger.info(f"📌 ШАГ 1: Поиск страницы района на dic.academic.ru")
        district_info = await self._find_district_page(district)
        if not district_info:
            logger.error(f"❌ Страница района на dic.academic.ru не найдена")
            return []
        
        district_html = await self._fetch_page(district_info['url'])
        logger.info(f"✅ Страница района загружена: {district_info['url']}")
        
        # Шаг 2: Ищем общие списки на странице района
        logger.info(f"📌 ШАГ 2: Поиск общих списков населенных пунктов")
        if district_html:
            soup = BeautifulSoup(district_html, 'html.parser')
            list_count = 0
            
            for link in soup.find_all('a', href=re.compile(r'/dic\.nsf/ruwiki/\d+')):
                text = link.get_text().lower()
                if any(kw in text for kw in LIST_KEYWORDS):
                    match = re.search(r'/dic\.nsf/ruwiki/(\d+)', link.get('href', ''))
                    if match:
                        list_id = match.group(1)
                        if list_id not in processed_master_lists:
                            processed_master_lists.add(list_id)
                            logger.info(f"  🔍 Обрабатываем общий список ID {list_id}: {link.get_text()[:50]}")
                            data = await self._parse_master_list(list_id, district)
                            for v in data:
                                key = f"{v['name']}_{v['district']}"
                                if key not in seen:
                                    seen.add(key)
                                    all_villages.append(v)
                                    self.collection_stats['from_master_lists'] += 1
                            list_count += 1
            
            logger.info(f"  📊 Найдено общих списков: {list_count}, добавлено НП: {self.collection_stats['from_master_lists']}")
        
        # Шаг 3: Извлекаем список сельских поселений
        logger.info(f"📌 ШАГ 3: Извлечение сельских поселений")
        settlements = []
        if district_html:
            soup = BeautifulSoup(district_html, 'html.parser')
            for header in soup.find_all(['h2', 'h3', 'h4']):
                if 'состав района' in header.get_text().lower():
                    parent = header.find_parent()
                    if parent:
                        for ul in parent.find_all('ul'):
                            for li in ul.find_all('li'):
                                text = li.get_text().strip()
                                match = re.search(r'Сельское поселение\s+([А-Яа-я-]+)', text)
                                if match:
                                    settlements.append(match.group(1))
        
        settlements = list(set(settlements))
        logger.info(f"  📊 Найдено сельских поселений: {len(settlements)}")
        if settlements:
            logger.debug(f"    Список СП: {', '.join(settlements[:20])}")
        
        # Шаг 4: Обрабатываем каждое сельское поселение
        logger.info(f"📌 ШАГ 4: Обработка сельских поселений")
        processed_settlements = 0
        
        for settlement in settlements[:30]:
            try:
                elapsed = time.time() - self.start_time
                if elapsed > 1500:
                    logger.warning(f"  ⏱️ Превышено время выполнения ({elapsed:.1f}с), прерываем обработку СП")
                    break
                
                logger.info(f"  🔍 Обработка СП: {settlement}")
                
                # 4a: Страница с бывшими населенными пунктами
                former_id = await self._find_former_page(settlement, district)
                if former_id and former_id not in self.processed_ids:
                    self.processed_ids.add(former_id)
                    logger.info(f"    📄 Обрабатываем страницу бывших НП (ID: {former_id})")
                    data = await self._parse_former_page(former_id, district, settlement)
                    for v in data:
                        key = f"{v['name']}_{v['district']}"
                        if key not in seen:
                            seen.add(key)
                            all_villages.append(v)
                            self.collection_stats['from_former'] += 1
                        else:
                            for i, existing in enumerate(all_villages):
                                if f"{existing['name']}_{existing['district']}" == key:
                                    if not existing.get('lat') and v.get('lat'):
                                        all_villages[i] = v
                                        logger.debug(f"      🔄 Обновлены координаты для {v['name']} из списка бывших НП")
                                        break
                
                await asyncio.sleep(1.5)
                
                # 4b: Основная страница сельского поселения
                page_id = await self._find_settlement_page(settlement, district)
                if page_id and page_id not in self.processed_ids:
                    self.processed_ids.add(page_id)
                    logger.info(f"    📄 Обрабатываем страницу СП (ID: {page_id})")
                    data = await self._parse_settlement_page(page_id, district, settlement)
                    for v in data:
                        key = f"{v['name']}_{v['district']}"
                        if key not in seen:
                            seen.add(key)
                            all_villages.append(v)
                            self.collection_stats['from_settlements'] += 1
                
                processed_settlements += 1
                
            except Exception as e:
                logger.error(f"  ❌ Ошибка обработки СП {settlement}: {e}")
        
        logger.info(f"  📊 Обработано СП: {processed_settlements}/{len(settlements)}")
        
        # Статистика сбора
        self.collection_stats['total_unique'] = len(all_villages)
        logger.info(f"📊 СТАТИСТИКА СБОРА НП:")
        logger.info(f"  • Из общих списков: {self.collection_stats['from_master_lists']}")
        logger.info(f"  • Из бывших НП: {self.collection_stats['from_former']}")
        logger.info(f"  • Из СП: {self.collection_stats['from_settlements']}")
        logger.info(f"  • Всего уникальных: {self.collection_stats['total_unique']}")
        
        # Шаг 5: Поиск координат для НП без них
        logger.info(f"📌 ШАГ 5: ПОИСК КООРДИНАТ")
        
        without_coords = [v for v in all_villages if not v.get('lat') or not v['lat'].strip()]
        self.coords_stats['total_without'] = len(without_coords)
        
        logger.info(f"  📊 Статистика перед поиском координат:")
        logger.info(f"    • Всего НП: {len(all_villages)}")
        logger.info(f"    • Уже с координатами: {len(all_villages) - len(without_coords)}")
        logger.info(f"    • Без координат: {len(without_coords)}")
        
        # Получаем ссылки из Wikipedia
        wiki_page = await self._find_wiki_district_page(district)
        wiki_links = {}
        if wiki_page:
            wiki_links = await self._extract_wiki_links(wiki_page)
            logger.info(f"  📊 Получено {len(wiki_links)} ссылок из Wikipedia")
        
        # Поиск координат
        found = 0
        total_to_process = len(without_coords)
        
        for i, v in enumerate(without_coords):
            try:
                if time.time() - self.start_time > 1500:
                    logger.warning(f"  ⏱️ Превышено время выполнения, прерываем поиск координат")
                    break
                
                if i > 0 and i % 10 == 0:
                    await asyncio.sleep(1.5)
                
                name = v['name']
                coords = None
                
                # 1. Проверяем кэш координат
                if name in self.coords_cache:
                    lat, lon = self.coords_cache[name]
                    coords = {'lat': lat, 'lon': lon}
                    self.coords_stats['from_cache'] += 1
                    logger.info(f"    📍 [{i+1}/{total_to_process}] {name}: из кэша ({lat}, {lon})")
                
                # 2. Ищем на dic.academic.ru по ссылке
                elif name in self.dic_links:
                    logger.info(f"    🔍 [{i+1}/{total_to_process}] {name}: поиск на dic.academic.ru по ссылке")
                    data = await self._get_dic_coords(self.dic_links[name], name, district)
                    if data:
                        coords = data
                        self.coords_stats['from_dic_link'] += 1
                        logger.info(f"    ✅ [{i+1}/{total_to_process}] {name}: координаты найдены на dic.academic.ru")
                    else:
                        logger.debug(f"    ❌ [{i+1}/{total_to_process}] {name}: координаты не найдены на dic.academic.ru")
                
                # 3. Ищем в Wikipedia
                elif name in wiki_links:
                    logger.info(f"    🔍 [{i+1}/{total_to_process}] {name}: поиск в Wikipedia")
                    data = await self._get_wiki_coords(wiki_links[name], name, district)
                    if data:
                        coords = data
                        self.coords_stats['from_wiki'] += 1
                        logger.info(f"    ✅ [{i+1}/{total_to_process}] {name}: координаты найдены в Wikipedia")
                    else:
                        logger.debug(f"    ❌ [{i+1}/{total_to_process}] {name}: координаты не найдены в Wikipedia")
                
                if coords:
                    for idx, existing in enumerate(all_villages):
                        if existing['name'] == name:
                            all_villages[idx]['lat'] = coords['lat']
                            all_villages[idx]['lon'] = coords['lon']
                            found += 1
                            logger.info(f"    ✅ ДОБАВЛЕНЫ КООРДИНАТЫ: {name} -> {coords['lat']}, {coords['lon']}")
                            break
                
                if (i + 1) % 50 == 0:
                    logger.info(f"  📊 Прогресс: обработано {i+1}/{total_to_process} НП, найдено координат: {found}")
                
                await asyncio.sleep(0.3)
                
            except Exception as e:
                logger.error(f"  ❌ Ошибка обработки {v.get('name', 'unknown')}: {e}")
        
        self.coords_stats['found'] = found
        self.coords_stats['remaining'] = total_to_process - found
        
        logger.info(f"  ✅ Поиск координат завершен. Найдено: {found}")
        logger.info(f"  📊 ИТОГО ПО КООРДИНАТАМ:")
        logger.info(f"    • Из бывших НП: {self.coords_stats['from_former']}")
        logger.info(f"    • Из кэша: {self.coords_stats['from_cache']}")
        logger.info(f"    • Из dic.academic.ru: {self.coords_stats['from_dic_link']}")
        logger.info(f"    • Из Wikipedia: {self.coords_stats['from_wiki']}")
        logger.info(f"    • Всего найдено: {self.coords_stats['found']}")
        logger.info(f"    • Осталось без координат: {self.coords_stats['remaining']}")
        
        # Сортируем и возвращаем
        all_villages.sort(key=lambda x: x['name'])
        
        final_with_coords = sum(1 for v in all_villages if v.get('lat') and v['lat'].strip())
        total_time = time.time() - self.start_time
        
        logger.info(f"📊 ФИНАЛЬНАЯ СТАТИСТИКА:")
        logger.info(f"  • Всего НП: {len(all_villages)}")
        logger.info(f"  • С координатами: {final_with_coords}")
        logger.info(f"  • Без координат: {len(all_villages) - final_with_coords}")
        logger.info(f"  • Время выполнения: {total_time:.1f}с")
        
        return all_villages
    
    # ========== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ==========
    
    def _find_col(self, headers: List[str], names: List[str]) -> Optional[int]:
        for i, h in enumerate(headers):
            for n in names:
                if n in h:
                    return i
        return None
    
    def _parse_coords(self, text: str, cell=None) -> Tuple[Optional[float], Optional[float]]:
        try:
            if cell:
                geo = cell.find('span', class_='geo')
                if geo:
                    lat_span = geo.find('span', class_='latitude')
                    lon_span = geo.find('span', class_='longitude')
                    if lat_span and lon_span:
                        lat = float(lat_span.get_text().strip())
                        lon = float(lon_span.get_text().strip())
                        if self._validate_coords(lat, lon):
                            return lat, lon
            
            dms = r'(\d+)°(\d+)′([\d.]+)″.*?(\d+)°(\d+)′([\d.]+)″'
            match = re.search(dms, text)
            if match:
                lat = float(match.group(1)) + float(match.group(2))/60 + float(match.group(3))/3600
                lon = float(match.group(4)) + float(match.group(5))/60 + float(match.group(6))/3600
                if self._validate_coords(lat, lon):
                    return lat, lon
            
            decimal = r'([0-9]+\.[0-9]+)[,\s]+([0-9]+\.[0-9]+)'
            match = re.search(decimal, text)
            if match:
                lat = float(match.group(1))
                lon = float(match.group(2))
                if self._validate_coords(lat, lon):
                    return lat, lon
            
            return None, None
        except:
            return None, None
    
    def _validate_coords(self, lat: float, lon: float) -> bool:
        return 55.0 <= lat <= 58.0 and 30.0 <= lon <= 38.0