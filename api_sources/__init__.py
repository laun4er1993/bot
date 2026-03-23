# __init__.py
# Основной класс APISourceManager

import aiohttp
import asyncio
import logging
import time
import random
import re
import json
from typing import List, Dict, Optional, Tuple, Set, Any
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote, quote_plus

from .config import (
    AVAILABLE_DISTRICTS, DIC_ACADEMIC_SEARCH_URL, DIC_ACADEMIC_ARTICLE_URL,
    WIKIPEDIA_BASE_URL, WIKIPEDIA_SEARCH_URL, TVER_OBLAST_URL,
    LIST_KEYWORDS, SETTLEMENT_KEYWORDS, DISTRICT_KEYWORDS,
    SETTLEMENTS_SECTION_KEYWORDS, TYPE_INDICATORS, TYPE_MAPPING,
    SERVICE_SETTLEMENT_WORDS, SERVICE_VILLAGE_WORDS,
    MIN_NAME_LENGTH, MAX_NAME_LENGTH, DISTRICT_WIKI_NAMES,
    KNOWN_PERSONALITIES
)
from .utils import (
    is_valid_name, is_valid_settlement_name, expand_type,
    find_column_index, validate_coordinates
)
from .coordinates import parse_dic_coordinates, parse_wikipedia_coordinates

logger = logging.getLogger(__name__)

# Расширенные границы для учета пограничных НП (буфер 0.3 градуса)
TVER_BOUNDS_EXTENDED = {
    'min_lat': 54.7,
    'max_lat': 58.8,
    'min_lon': 29.7,
    'max_lon': 38.8
}


class APISourceManager:
    """Универсальный менеджер для загрузки данных из dic.academic.ru и Wikipedia"""
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.thread_pool = ThreadPoolExecutor(max_workers=3)
        
        # Rate limiting
        self.request_count = 0
        self.last_request_time = 0
        self.min_request_interval = 2.0
        
        # Кэши
        self.article_cache: Dict[str, str] = {}
        self.district_cache: Dict[str, Dict] = {}
        self.former_np_pages_cache: Dict[str, str] = {}
        self.settlement_pages_cache: Dict[str, str] = {}
        self.page_cache: Dict[str, Tuple[str, float]] = {}
        self.processed_article_ids: Set[str] = set()
        self.processed_former_np_ids: Set[str] = set()
        
        # Ссылки на страницы НП
        self.village_links: Dict[str, str] = {}
        
        # Кэш границ районов
        self.district_bounds_cache: Dict[str, Dict[str, float]] = {}
        
        self.cache_ttl = 3600
        self.max_retries = 5
        self.start_time = 0
        
        # Статистика
        self.coords_stats = {
            'from_former': 0,
            'from_links': 0,
            'from_wikipedia': 0,
            'total_without': 0
        }
        
        self.collection_stats = {
            'from_master_lists': 0,
            'from_former': 0,
            'from_settlements': 0,
            'from_district_page': 0,
            'total_unique': 0
        }
        
        self.max_concurrent_dic = 2
        
        self.default_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
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
    
    def clear_cache(self):
        self.article_cache.clear()
        self.district_cache.clear()
        self.former_np_pages_cache.clear()
        self.settlement_pages_cache.clear()
        self.page_cache.clear()
        self.processed_article_ids.clear()
        self.processed_former_np_ids.clear()
        self.village_links.clear()
        self.district_bounds_cache.clear()
        self.coords_stats = {'from_former': 0, 'from_links': 0, 'from_wikipedia': 0, 'total_without': 0}
        self.collection_stats = {'from_master_lists': 0, 'from_former': 0, 'from_settlements': 0, 'from_district_page': 0, 'total_unique': 0}
        logger.info("🧹 Кэш очищен")
    
    async def _rate_limit(self):
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        actual_interval = self.min_request_interval * random.uniform(0.8, 1.2)
        if time_since_last < actual_interval:
            await asyncio.sleep(actual_interval - time_since_last)
        self.last_request_time = time.time()
        self.request_count += 1
    
    async def _fetch_page_with_retry(self, url: str, retry_count: int = 0) -> Optional[str]:
        if retry_count >= self.max_retries:
            logger.error(f"Превышено количество попыток для {url}")
            return None
        
        try:
            session = await self._get_session()
            await self._rate_limit()
            
            async with session.get(url, headers=self.default_headers, timeout=60) as response:
                if response.status == 200:
                    return await response.text()
                elif response.status == 429:
                    wait_time = (2 ** retry_count) * random.uniform(5.0, 10.0)
                    logger.warning(f"Ошибка 429, повтор через {wait_time:.1f}с")
                    await asyncio.sleep(wait_time)
                    return await self._fetch_page_with_retry(url, retry_count + 1)
                elif response.status in [500, 502, 503, 504]:
                    wait_time = 2 ** retry_count
                    await asyncio.sleep(wait_time)
                    return await self._fetch_page_with_retry(url, retry_count + 1)
                return None
        except asyncio.TimeoutError:
            wait_time = 2 ** retry_count
            await asyncio.sleep(wait_time)
            return await self._fetch_page_with_retry(url, retry_count + 1)
        except Exception as e:
            logger.error(f"Ошибка загрузки {url}: {e}")
            return None
    
    async def _fetch_page(self, url: str) -> Optional[str]:
        current_time = time.time()
        if url in self.page_cache:
            html, timestamp = self.page_cache[url]
            if current_time - timestamp < self.cache_ttl:
                return html
        
        html = await self._fetch_page_with_retry(url)
        if html:
            self.page_cache[url] = (html, current_time)
        return html
    
    async def _search_with_pagination(self, query: str, max_pages: int = 10) -> List[Dict]:
        all_results = []
        page = 1
        
        while page <= max_pages:
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
            page_results = await loop.run_in_executor(self.thread_pool, self._parse_search_page, html, page)
            
            if not page_results:
                break
            
            all_results.extend(page_results)
            logger.info(f"      Страница {page}: найдено {len(page_results)} результатов")
            
            has_next = await loop.run_in_executor(self.thread_pool, self._check_next_page, html)
            if not has_next:
                break
            
            page += 1
        
        return all_results
    
    def _parse_search_page(self, html: str, page_num: int) -> List[Dict]:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            terms_list = soup.find('ul', class_='terms-list')
            if not terms_list:
                return []
            
            for item in terms_list.find_all('li', recursive=False):
                link = item.find('a', href=re.compile(r'/dic\.nsf/ruwiki/\d+'))
                if not link:
                    continue
                
                href = link.get('href', '')
                match = re.search(r'/dic\.nsf/ruwiki/(\d+)', href)
                if not match:
                    continue
                
                article_id = match.group(1)
                title = link.get_text().strip()
                
                first_p = item.find('p')
                full_text = first_p.get_text().strip() if first_p else ""
                if title in full_text:
                    full_text = full_text.replace(title, '').strip()
                
                position_match = re.match(r'^(\d+)', full_text)
                position = int(position_match.group(1)) if position_match else 0
                if position > 0:
                    full_text = re.sub(r'^\d+\s*', '', full_text).strip()
                
                results.append({
                    'id': article_id, 'title': title, 'full_text': full_text,
                    'page': page_num, 'position': position
                })
            return results
        except Exception as e:
            logger.error(f"Ошибка парсинга поиска: {e}")
            return []
    
    def _check_next_page(self, html: str) -> bool:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            return soup.find('a', string=re.compile(r'далее|следующая|next', re.I)) is not None
        except:
            return False
    
    def _generate_district_variants(self, district_name: str) -> List[str]:
        variants = [district_name.lower().strip()]
        base = district_name.lower().strip()
        
        if base.endswith('ский'):
            stem = base[:-4]
            variants.extend([f"{stem}ского", f"{stem}скому", f"{stem}ским", f"{stem}ском"])
            if base.endswith('ий'):
                variants.extend([f"{stem}его", f"{stem}ему", f"{stem}им", f"{stem}ем"])
        elif base.endswith('ой'):
            stem = base[:-2]
            variants.extend([f"{stem}ого", f"{stem}ому", f"{stem}ым", f"{stem}ом"])
        elif base.endswith('ый'):
            stem = base[:-2]
            variants.extend([f"{stem}ого", f"{stem}ому", f"{stem}ым", f"{stem}ом"])
        
        variants.extend([f"{base} район", f"{base} района", f"{base} району", f"{base} районом", f"{base} районе"])
        variants.extend([f"{base} муниципальный округ", f"{base} муниципального округа"])
        return list(set(variants))
    
    def _check_district_in_text(self, text: str, district: str) -> bool:
        if not text or not district:
            return False
        text_lower = text.lower()
        for variant in self._generate_district_variants(district):
            if variant in text_lower:
                return True
        return False
    
    def _is_valid_name(self, name: str, district: str = "") -> bool:
        if not name or len(name) < MIN_NAME_LENGTH or len(name) > MAX_NAME_LENGTH:
            return False
        
        name_lower = name.lower()
        for word in SERVICE_VILLAGE_WORDS:
            if word in name_lower:
                return False
        
        if not re.search(r'[а-яА-ЯёЁ]', name):
            return False
        if name.isdigit():
            return False
        
        if any(x in name_lower for x in ['фильм', 'сериал', 'трасса', 'дорога', 'система', 'водохранилище']):
            return False
        if re.search(r'[А-Я]\.\s*[А-Я]\.', name):
            return False
        if re.search(r'[А-Я][а-я]+\s+[А-Я]\.', name):
            return False
        
        for personality in KNOWN_PERSONALITIES:
            if personality.lower() in name_lower:
                return False
        
        other_regions = ['ростовская', 'рязанская', 'волгоградская', 'пермский', 'удмуртия', 'московская', 'владимирская', 'калужская']
        if any(region in name_lower for region in other_regions):
            return False
        
        return True
    
    def _is_valid_settlement_name(self, name: str, district: str = "") -> bool:
        if not name or len(name) < 2 or len(name) > 50:
            return False
        
        name_lower = name.lower()
        if re.match(r'^\d+\s+(мая|января|февраля|марта|апреля|июня|июля|августа|сентября|октября|ноября|декабря)', name_lower):
            return False
        
        for word in SERVICE_SETTLEMENT_WORDS:
            if word in name_lower:
                return False
        
        if not re.search(r'[а-яА-ЯёЁ]', name):
            return False
        
        return True
    
    async def _get_district_bounds(self, district: str, district_html: str = None) -> Dict[str, float]:
        if district in self.district_bounds_cache:
            return self.district_bounds_cache[district]
        
        bounds = TVER_BOUNDS_EXTENDED.copy()
        self.district_bounds_cache[district] = bounds
        return bounds
    
    def _check_coordinate_in_district(self, lat: float, lon: float, bounds: Dict[str, float]) -> bool:
        return bounds['min_lat'] <= lat <= bounds['max_lat'] and bounds['min_lon'] <= lon <= bounds['max_lon']
    
    # ========== МЕТОДЫ ДЛЯ РАБОТЫ С DIC.ACADEMIC.RU ==========
    
    async def _find_district_page(self, district: str) -> Optional[Dict]:
        cache_key = f"district_{district}"
        if cache_key in self.district_cache:
            return self.district_cache[cache_key]
        
        logger.info(f"  🔍 Поиск страницы района: {district}")
        
        queries = [f"{district} район", f"{district} район Тверская область", f"{district} муниципальный район", district]
        all_results = []
        
        for query in queries:
            results = await self._search_with_pagination(query, max_pages=10)
            all_results.extend(results)
            await asyncio.sleep(1.5)
        
        if not all_results:
            return None
        
        for result in all_results:
            score = self._score_district_relevance(result, district)
            result['score'] = score
        
        sorted_results = sorted(all_results, key=lambda x: x['score'], reverse=True)[:10]
        
        for result in sorted_results:
            if result['score'] >= 50:
                page_url = DIC_ACADEMIC_ARTICLE_URL.format(result['id'])
                html = await self._fetch_page(page_url)
                if html and self._verify_district_page(html, district):
                    logger.info(f"    ✅ Найдена страница района (ID: {result['id']})")
                    district_info = {'id': result['id'], 'title': result['title'], 'url': page_url, 'html': html}
                    self.district_cache[cache_key] = district_info
                    return district_info
        
        return None
    
    def _score_district_relevance(self, result: Dict, district: str) -> int:
        title_lower = result['title'].lower()
        full_text_lower = result['full_text'].lower()
        district_lower = district.lower()
        score = 0
        
        if f"{district_lower} район" in title_lower:
            score += 100
        elif district_lower in title_lower:
            score += 50
        
        if result['position'] == 1:
            score += 20
        elif result['position'] <= 3:
            score += 10
        
        for keyword in DISTRICT_KEYWORDS:
            if keyword in full_text_lower:
                score += 15
        
        if "тверская область" in full_text_lower:
            score += 10
        
        return score
    
    def _verify_district_page(self, html: str, district: str) -> bool:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            text = soup.get_text().lower()
            if f"{district.lower()} район" not in text:
                return False
            return True
        except:
            return False
    
    async def _extract_settlements_from_page(self, html: str, district: str) -> List[str]:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            found_settlements = []
            
            for header in soup.find_all(['h2', 'h3', 'h4']):
                header_text = header.get_text().lower()
                if 'сельские поселения' in header_text or 'состав района' in header_text:
                    current = header.find_next_sibling()
                    while current and len(found_settlements) < 20:
                        if current.name in ['ul', 'ol']:
                            for li in current.find_all('li', recursive=False):
                                link = li.find('a')
                                if link:
                                    text = link.get_text().strip()
                                    if 'сельское поселение' in text.lower():
                                        match = re.search(r'«([^»]+)»', text)
                                        settlement = match.group(1) if match else re.sub(r'^сельское\s+поселение\s*', '', text, flags=re.IGNORECASE).strip()
                                        if settlement and self._is_valid_settlement_name(settlement, district):
                                            found_settlements.append(settlement)
                        current = current.find_next_sibling()
                    if found_settlements:
                        break
            
            return sorted(list(set(found_settlements)))
        except Exception as e:
            logger.error(f"Ошибка парсинга СП: {e}")
            return []
    
    async def _find_district_former_np_page(self, district: str, district_html: str) -> Optional[str]:
        cache_key = f"district_former_{district}"
        if cache_key in self.former_np_pages_cache:
            return self.former_np_pages_cache[cache_key]
        
        queries = [
            f"Список бывших населённых пунктов {district} района",
            f"Бывшие населенные пункты {district} района",
            f"Список бывших населённых пунктов {district} муниципального округа"
        ]
        
        for query in queries:
            results = await self._search_with_pagination(query, max_pages=5)
            for result in results[:10]:
                title_lower = result['title'].lower()
                if ('список бывших' in title_lower or 'бывшие населенные' in title_lower) and self._check_district_in_text(title_lower, district):
                    page_url = DIC_ACADEMIC_ARTICLE_URL.format(result['id'])
                    html = await self._fetch_page(page_url)
                    if html and BeautifulSoup(html, 'html.parser').find_all('table', class_=['standard', 'sortable']):
                        self.former_np_pages_cache[cache_key] = result['id']
                        return result['id']
            await asyncio.sleep(1.5)
        
        return None
    
    async def _find_former_np_page(self, settlement: str, district: str) -> Optional[str]:
        cache_key = f"former_np_{district}_{settlement}"
        if cache_key in self.former_np_pages_cache:
            return self.former_np_pages_cache[cache_key]
        
        queries = [
            f"Список бывших населённых пунктов сельского поселения {settlement}",
            f"Бывшие населённые пункты {settlement}",
            f"{settlement} бывшие населенные пункты"
        ]
        
        for query in queries:
            results = await self._search_with_pagination(query, max_pages=5)
            for result in results[:15]:
                title_lower = result['title'].lower()
                if 'бывших' in title_lower and self._check_district_in_text(title_lower, district):
                    if settlement.lower() in title_lower or self._normalize_text(settlement) in self._normalize_text(title_lower):
                        self.former_np_pages_cache[cache_key] = result['id']
                        return result['id']
            await asyncio.sleep(1.5)
        
        return None
    
    def _normalize_text(self, text: str) -> str:
        if not text:
            return ""
        return re.sub(r'[„“«»"\'`\(\)\[\]\{\}]', '', text).lower().strip()
    
    async def _find_settlement_main_page(self, settlement: str, district: str) -> Optional[str]:
        cache_key = f"settlement_main_{district}_{settlement}"
        if cache_key in self.settlement_pages_cache:
            return self.settlement_pages_cache[cache_key]
        
        queries = [f"Сельское поселение {settlement}", f"{settlement} сельское поселение"]
        
        for query in queries:
            results = await self._search_with_pagination(query, max_pages=5)
            for result in results[:10]:
                title_lower = result['title'].lower()
                if 'список бывших' in title_lower:
                    continue
                
                score = 0
                if settlement.lower() in title_lower:
                    score += 50
                if 'сельское поселение' in title_lower:
                    score += 40
                if self._check_district_in_text(title_lower, district):
                    score += 20
                
                if score >= 40:
                    self.settlement_pages_cache[cache_key] = result['id']
                    return result['id']
            await asyncio.sleep(1.5)
        
        return None
    
    async def _parse_former_np_page(self, article_id: str, district: str, settlement: str) -> List[Dict]:
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        html = await self._fetch_page(url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        if not self._check_district_in_text(soup.get_text().lower(), district):
            return []
        
        results = []
        for table in soup.find_all('table', class_=['standard', 'sortable']):
            rows = table.find_all('tr')
            if len(rows) < 2:
                continue
            
            headers = [h.get_text().strip().lower() for h in rows[0].find_all(['th', 'td'])]
            name_idx = find_column_index(headers, ['населённый пункт', 'название'])
            type_idx = find_column_index(headers, ['тип'])
            coords_idx = find_column_index(headers, ['координат'])
            
            if name_idx is None:
                continue
            
            for row in rows[1:]:
                cells = row.find_all('td')
                if len(cells) <= name_idx:
                    continue
                
                name = re.sub(r'^\d+\s*', '', cells[name_idx].get_text().strip())
                name = re.sub(r'\s+', ' ', name).strip()
                
                if not name or len(name) < MIN_NAME_LENGTH or not self._is_valid_name(name, district):
                    continue
                
                village_type = 'деревня'
                if type_idx is not None and type_idx < len(cells):
                    raw_type = cells[type_idx].get_text().strip()
                    if raw_type:
                        village_type = expand_type(raw_type)
                
                lat, lon = None, None
                if coords_idx is not None and coords_idx < len(cells):
                    coord_text = cells[coords_idx].get_text().strip()
                    lat, lon = parse_dic_coordinates(coord_text, cells[coords_idx])
                    if not lat or not lon:
                        lat, lon = parse_dic_coordinates(' '.join(c.get_text() for c in cells), None)
                
                if lat and lon and self._check_coordinate_in_district(lat, lon, TVER_BOUNDS_EXTENDED):
                    results.append({
                        "name": name, "type": village_type, "district": district,
                        "lat": str(round(lat, 5)), "lon": str(round(lon, 5)), "has_coords": True
                    })
                else:
                    results.append({
                        "name": name, "type": village_type, "district": district,
                        "lat": "", "lon": "", "has_coords": False
                    })
        
        return results
    
    async def _parse_settlement_main_page(self, article_id: str, district: str, settlement: str) -> List[Dict]:
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        html = await self._fetch_page(url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        if not self._check_district_in_text(soup.get_text().lower(), district):
            return []
        
        results = []
        for table in soup.find_all('table', class_=['standard', 'sortable', 'wikitable']):
            rows = table.find_all('tr')
            if len(rows) < 2:
                continue
            
            headers = [h.get_text().strip().lower() for h in rows[0].find_all(['th', 'td'])]
            name_idx = find_column_index(headers, ['населённый пункт', 'название'])
            type_idx = find_column_index(headers, ['тип'])
            
            if name_idx is None:
                continue
            
            for row in rows[1:]:
                cells = row.find_all('td')
                if len(cells) <= name_idx:
                    continue
                
                name = re.sub(r'^\d+\s*', '', cells[name_idx].get_text().strip())
                name = re.sub(r'\s+', ' ', name).strip()
                
                if not name or len(name) < MIN_NAME_LENGTH or not self._is_valid_name(name, district):
                    continue
                
                village_type = 'деревня'
                if type_idx is not None and type_idx < len(cells):
                    raw_type = cells[type_idx].get_text().strip()
                    if raw_type:
                        village_type = expand_type(raw_type)
                
                link = cells[name_idx].find('a')
                article_id_link = None
                if link:
                    match = re.search(r'(\d+)', link.get('href', ''))
                    if match:
                        article_id_link = match.group(1)
                        self.village_links[name] = article_id_link
                
                results.append({
                    "name": name, "type": village_type, "district": district,
                    "lat": "", "lon": "", "has_coords": False, "article_id": article_id_link
                })
        
        return results
    
    async def _find_master_list_links(self, html: str, district: str) -> List[str]:
        soup = BeautifulSoup(html, 'html.parser')
        found_ids = []
        
        for link in soup.find_all('a', href=re.compile(r'/dic\.nsf/ruwiki/\d+')):
            text = link.get_text().lower()
            if any(kw in text for kw in LIST_KEYWORDS) and self._check_district_in_text(text, district):
                match = re.search(r'(\d+)', link.get('href', ''))
                if match:
                    found_ids.append(match.group(1))
        
        return list(set(found_ids))
    
    async def _parse_master_list_page(self, article_id: str, district: str) -> List[Dict]:
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        html = await self._fetch_page(url)
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        if not self._check_district_in_text(soup.get_text().lower(), district):
            return []
        
        results = []
        for table in soup.find_all('table', class_=['standard', 'sortable']):
            rows = table.find_all('tr')
            if len(rows) < 2:
                continue
            
            headers = [h.get_text().strip().lower() for h in rows[0].find_all(['th', 'td'])]
            name_idx = find_column_index(headers, ['населённый пункт', 'название'])
            type_idx = find_column_index(headers, ['тип'])
            
            if name_idx is None:
                name_idx = 0
            
            for row in rows[1:]:
                cells = row.find_all('td')
                if len(cells) <= name_idx:
                    continue
                
                name = re.sub(r'^\d+\s*', '', cells[name_idx].get_text().strip())
                name = re.sub(r'\s+', ' ', name).strip()
                
                if not name or len(name) < MIN_NAME_LENGTH or not self._is_valid_name(name, district):
                    continue
                
                village_type = 'деревня'
                if type_idx is not None and type_idx < len(cells):
                    raw_type = cells[type_idx].get_text().strip()
                    if raw_type:
                        village_type = expand_type(raw_type)
                
                results.append({"name": name, "type": village_type, "district": district, "has_coords": False})
        
        return results
    
    async def _parse_individual_village_page(self, article_id: str, district: str) -> Optional[Dict]:
        await asyncio.sleep(random.uniform(1.5, 3.0))
        
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        html = await self._fetch_page(url)
        if not html:
            return None
        
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
            village_type = expand_type(possible_type)
        
        if not self._is_valid_name(name, district):
            return None
        
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
            dms_pattern = r'(\d+)°(\d+)′([\d.]+)″.*?(\d+)°(\d+)′([\d.]+)″'
            text = soup.get_text()
            match = re.search(dms_pattern, text)
            if match:
                try:
                    lat_deg, lat_min, lat_sec = map(float, match.group(1, 2, 3))
                    lon_deg, lon_min, lon_sec = map(float, match.group(4, 5, 6))
                    lat = lat_deg + lat_min/60 + lat_sec/3600
                    lon = lon_deg + lon_min/60 + lon_sec/3600
                except:
                    pass
        
        if lat and lon and self._check_coordinate_in_district(lat, lon, TVER_BOUNDS_EXTENDED):
            return {
                "name": name, "type": village_type, "district": district,
                "lat": str(round(lat, 5)), "lon": str(round(lon, 5)), "has_coords": True
            }
        
        return None
    
    async def _get_article_info(self, article_id: str) -> Optional[Dict]:
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        html = await self._fetch_page(url)
        if not html:
            return None
        soup = BeautifulSoup(html, 'html.parser')
        title_elem = soup.find('h1')
        return {'id': article_id, 'title': title_elem.get_text().strip() if title_elem else ""}
    
    # ========== ПОИСК НА СТРАНИЦЕ РАЙОНА (ШАГ 3) ==========
    
    async def _fetch_villages_from_district_page(self, district: str, existing_villages: Dict[str, Dict]) -> Dict[str, Dict]:
        """Находит страницу района на Wikipedia и возвращает координаты для всех НП"""
        logger.info(f"  🔍 ШАГ 3: Поиск на странице района {district}...")
        
        def wiki_encode(name: str) -> str:
            return quote(name.replace(' ', '_'), safe='')
        
        possible_names = DISTRICT_WIKI_NAMES.get(district, [f"{district} район", f"{district} муниципальный округ", district])
        district_page_url = None
        
        for name in possible_names:
            direct_url = f"{WIKIPEDIA_BASE_URL}/wiki/{wiki_encode(name)}"
            html = await self._fetch_page(direct_url)
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                if not soup.find('div', class_='noarticletext'):
                    title = soup.find('h1')
                    if title and ('район' in title.get_text().lower() or 'округ' in title.get_text().lower()):
                        district_page_url = direct_url
                        logger.info(f"    🌐 Найдена страница района: {direct_url}")
                        break
            await asyncio.sleep(0.5)
        
        if not district_page_url:
            logger.warning(f"    ⚠️ Страница района {district} не найдена")
            return {}
        
        html = await self._fetch_page(district_page_url)
        if not html:
            return {}
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Поиск таблицы с населенными пунктами
        tables = soup.find_all('table', class_=['standard', 'wikitable', 'sortable'])
        
        district_villages = {}
        
        for table in tables:
            rows = table.find_all('tr')
            if len(rows) < 2:
                continue
            
            headers = [h.get_text().strip().lower() for h in rows[0].find_all(['th', 'td'])]
            name_col_idx = None
            type_col_idx = None
            
            for i, h in enumerate(headers):
                if 'населённый пункт' in h or 'населенный пункт' in h or 'название' in h:
                    name_col_idx = i
                elif 'тип' in h:
                    type_col_idx = i
            
            if name_col_idx is None:
                for row in rows[1:]:
                    cells = row.find_all('td')
                    for i, cell in enumerate(cells):
                        if cell.find('a') and len(cell.get_text().strip()) > 2:
                            name_col_idx = i
                            break
                    if name_col_idx is not None:
                        break
            
            if name_col_idx is None:
                continue
            
            for row in rows[1:]:
                cells = row.find_all('td')
                if len(cells) <= name_col_idx:
                    continue
                
                name = re.sub(r'^\d+\s*', '', cells[name_col_idx].get_text().strip())
                name = re.sub(r'\s+', ' ', name).strip()
                
                if not name or len(name) < MIN_NAME_LENGTH:
                    continue
                if name in ['ИТОГО', 'Всего', 'Итого']:
                    continue
                
                village_type = 'деревня'
                if type_col_idx is not None and type_col_idx < len(cells):
                    type_text = cells[type_col_idx].get_text().strip().lower()
                    if 'дер' in type_text:
                        village_type = 'деревня'
                    elif 'пос' in type_text:
                        village_type = 'посёлок'
                    elif 'село' in type_text:
                        village_type = 'село'
                
                link = cells[name_col_idx].find('a')
                page_url = None
                if link and link.get('href', '').startswith('/wiki/'):
                    href = link['href']
                    if 'район' not in href.lower() and 'список' not in href.lower() and ':' not in href:
                        page_url = f"{WIKIPEDIA_BASE_URL}{href}"
                
                district_villages[name] = {'type': village_type, 'url': page_url}
        
        logger.info(f"    📊 На странице района найдено {len(district_villages)} НП")
        
        # Поиск координат для НП без координат
        villages_to_search = []
        for name, data in district_villages.items():
            if name in existing_villages:
                if not existing_villages[name].get('has_coords', False):
                    villages_to_search.append({'name': name, 'type': data['type'], 'wiki_url': data['url'], 'is_new': False})
            else:
                if self._is_valid_name(name, district):
                    villages_to_search.append({'name': name, 'type': data['type'], 'wiki_url': data['url'], 'is_new': True})
        
        if not villages_to_search:
            return {}
        
        logger.info(f"    🔍 Поиск координат для {len(villages_to_search)} НП...")
        
        found_coords = {}
        semaphore = asyncio.Semaphore(5)
        
        async def fetch_coords(village):
            async with semaphore:
                name = village['name']
                wiki_url = village.get('wiki_url')
                await asyncio.sleep(random.uniform(0.3, 0.8))
                
                if wiki_url:
                    try:
                        html = await self._fetch_page(wiki_url)
                        if html:
                            coords = await parse_wikipedia_coordinates(html, name)
                            if coords:
                                lat, lon = coords
                                lat_f, lon_f = float(lat), float(lon)
                                if self._check_coordinate_in_district(lat_f, lon_f, TVER_BOUNDS_EXTENDED):
                                    logger.info(f"      ✅ Найдены координаты для {name} по ссылке: {lat}, {lon}")
                                    return name, {'name': name, 'type': village['type'], 'lat': lat, 'lon': lon, 'is_new': village['is_new']}
                    except Exception as e:
                        logger.debug(f"      Ошибка загрузки {name}: {e}")
                
                direct_url = f"{WIKIPEDIA_BASE_URL}/wiki/{wiki_encode(name)}"
                try:
                    html = await self._fetch_page(direct_url)
                    if html:
                        coords = await parse_wikipedia_coordinates(html, name)
                        if coords:
                            lat, lon = coords
                            lat_f, lon_f = float(lat), float(lon)
                            if self._check_coordinate_in_district(lat_f, lon_f, TVER_BOUNDS_EXTENDED):
                                logger.info(f"      ✅ Найдены координаты для {name} по прямому URL: {lat}, {lon}")
                                return name, {'name': name, 'type': village['type'], 'lat': lat, 'lon': lon, 'is_new': village['is_new']}
                except Exception as e:
                    logger.debug(f"      Ошибка загрузки {name}: {e}")
                
                return None, None
        
        tasks = [fetch_coords(v) for v in villages_to_search]
        results = await asyncio.gather(*tasks)
        
        for name, data in results:
            if data:
                found_coords[name] = data
        
        logger.info(f"    📊 Найдено координат для {len(found_coords)} НП со страницы района")
        return found_coords
    
    # ========== ОСНОВНОЙ МЕТОД ==========
    
    async def fetch_district_data(self, district: str) -> List[Dict]:
        self.clear_cache()
        self.start_time = time.time()
        logger.info(f"🌐 Загрузка данных для района: {district}")
        
        seen_villages: Dict[str, Dict] = {}
        processed_master_lists = set()
        
        # Шаг 1: Находим страницу района на dic.academic.ru
        district_info = await self._find_district_page(district)
        if not district_info:
            logger.warning(f"  ⚠️ Страница района не найдена")
            return []
        
        district_bounds = await self._get_district_bounds(district)
        logger.info(f"  📍 Границы района: {district_bounds}")
        
        district_html = await self._fetch_page(district_info['url'])
        
        # Шаг 2: Получаем список сельских поселений
        settlements = await self._extract_settlements_from_page(district_html, district) if district_html else []
        logger.info(f"  🔍 Найдено {len(settlements)} сельских поселений")
        
        # Шаг 2.5: Общий список бывших НП района
        district_former_id = await self._find_district_former_np_page(district, district_html)
        if district_former_id and district_former_id not in self.processed_former_np_ids:
            self.processed_former_np_ids.add(district_former_id)
            logger.info(f"  📌 Обрабатываем общий список бывших НП района")
            district_former_data = await self._parse_former_np_page(district_former_id, district, "всего района")
            former_added = 0
            former_with_coords = 0
            for village in district_former_data:
                key = f"{village['name']}_{village['district']}"
                if key not in seen_villages:
                    seen_villages[key] = village
                    self.collection_stats['from_former'] += 1
                    former_added += 1
                    if village.get('has_coords'):
                        former_with_coords += 1
                        self.coords_stats['from_former'] += 1
                        logger.info(f"      📍 Бывший НП с координатами: {village['name']} ({village['lat']}, {village['lon']})")
                else:
                    existing = seen_villages[key]
                    if not existing.get('has_coords') and village.get('has_coords'):
                        seen_villages[key] = village
                        former_added += 1
                        former_with_coords += 1
                        self.coords_stats['from_former'] += 1
                        logger.info(f"      🔄 Обновлены координаты для {village['name']} из общего списка бывших НП")
            logger.info(f"  ✅ Из общего списка бывших НП района добавлено {former_added} записей (из них с координатами: {former_with_coords})")
        
        # Шаг 3: Общие списки на странице района
        if district_html:
            master_list_ids = await self._find_master_list_links(district_html, district)
            for list_id in master_list_ids:
                if list_id not in processed_master_lists and list_id not in self.processed_article_ids:
                    processed_master_lists.add(list_id)
                    self.processed_article_ids.add(list_id)
                    list_data = await self._parse_master_list_page(list_id, district)
                    list_added = 0
                    for village in list_data:
                        key = f"{village['name']}_{village['district']}"
                        if key not in seen_villages:
                            seen_villages[key] = village
                            self.collection_stats['from_master_lists'] += 1
                            list_added += 1
                    logger.info(f"    Из списка ID {list_id} добавлено {list_added} записей")
        
        # Шаг 4: Для каждого СП
        for settlement in settlements:
            try:
                if time.time() - self.start_time > 1500:
                    logger.warning(f"  ⏱️ Превышено время, прерываем")
                    break
                
                await asyncio.sleep(1.5)
                logger.info(f"\n  📍 Обработка СП: {settlement}")
                
                # Бывшие НП для СП
                former_np_id = await self._find_former_np_page(settlement, district)
                if not former_np_id:
                    district_former_id = await self._find_district_former_np_page(district, district_html)
                    if district_former_id and district_former_id not in self.processed_former_np_ids:
                        former_np_id = district_former_id
                        logger.info(f"    📌 Используем общий список бывших НП района для СП {settlement}")
                
                if former_np_id and former_np_id not in self.processed_former_np_ids:
                    self.processed_former_np_ids.add(former_np_id)
                    logger.info(f"    ✅ Найдена страница бывших НП для СП {settlement} (ID: {former_np_id})")
                    former_np_data = await self._parse_former_np_page(former_np_id, district, settlement)
                    
                    former_new = 0
                    former_with_coords = 0
                    
                    for village in former_np_data:
                        key = f"{village['name']}_{village['district']}"
                        
                        if village.get('has_coords'):
                            former_with_coords += 1
                            logger.info(f"      📍 Бывший НП с координатами: {village['name']} ({village['lat']}, {village['lon']})")
                        
                        if key not in seen_villages:
                            seen_villages[key] = village
                            self.collection_stats['from_former'] += 1
                            former_new += 1
                            if village.get('has_coords'):
                                self.coords_stats['from_former'] += 1
                        else:
                            existing = seen_villages[key]
                            if not existing.get('has_coords') and village.get('has_coords'):
                                seen_villages[key] = village
                                former_new += 1
                                self.coords_stats['from_former'] += 1
                                logger.info(f"      🔄 Обновлены координаты для {village['name']} из бывших НП")
                    
                    if former_new > 0:
                        logger.info(f"    ✅ СП {settlement}: добавлено {former_new} записей из списка бывших НП (из них с координатами: {former_with_coords})")
                else:
                    if former_np_id:
                        logger.info(f"    ⚠️ Страница бывших НП для СП {settlement} уже обработана (ID: {former_np_id}), пропускаем")
                    else:
                        logger.info(f"    ⚠️ Страница бывших НП для СП {settlement} не найдена")
                
                # Основная страница СП
                main_page_id = await self._find_settlement_main_page(settlement, district)
                if main_page_id and main_page_id not in self.processed_article_ids:
                    self.processed_article_ids.add(main_page_id)
                    logger.info(f"    🔍 Обрабатываем основную страницу СП {settlement} (ID: {main_page_id})")
                    main_page_data = await self._parse_settlement_main_page(main_page_id, district, settlement)
                    
                    main_new = 0
                    for village in main_page_data:
                        key = f"{village['name']}_{village['district']}"
                        
                        if village.get('article_id'):
                            self.village_links[village['name']] = village['article_id']
                            logger.debug(f"      Сохранена ссылка для {village['name']}: ID {village['article_id']}")
                        
                        village_copy = village.copy()
                        village_copy.pop('article_id', None)
                        
                        if key not in seen_villages:
                            seen_villages[key] = village_copy
                            self.collection_stats['from_settlements'] += 1
                            main_new += 1
                            logger.info(f"      ➕ Добавлен НП из СП: {village['name']} ({village['type']})")
                        else:
                            existing = seen_villages[key]
                            if not existing.get('has_coords') and village_copy.get('has_coords'):
                                seen_villages[key] = village_copy
                                main_new += 1
                                logger.info(f"      🔄 Обновлены координаты для {village['name']}")
                    
                    if main_new > 0:
                        logger.info(f"    ✅ СП {settlement}: добавлено {main_new} записей из раздела 'Населенные пункты'")
                
                # Дополнительные списки на странице бывших НП
                if former_np_id:
                    former_np_url = DIC_ACADEMIC_ARTICLE_URL.format(former_np_id)
                    former_np_html = await self._fetch_page(former_np_url)
                    
                    if former_np_html:
                        additional_list_ids = await self._find_master_list_links(former_np_html, district)
                        
                        for list_id in additional_list_ids:
                            if list_id == former_np_id:
                                continue
                            
                            if list_id in self.processed_former_np_ids:
                                continue
                            
                            if list_id not in processed_master_lists and list_id not in self.processed_article_ids:
                                list_info = await self._get_article_info(list_id)
                                if list_info and self._check_district_in_text(list_info.get('title', ''), district):
                                    processed_master_lists.add(list_id)
                                    self.processed_article_ids.add(list_id)
                                    logger.info(f"      Обрабатываем дополнительный список ID {list_id}")
                                    
                                    list_data = await self._parse_master_list_page(list_id, district)
                                    
                                    list_new = 0
                                    for village in list_data:
                                        key = f"{village['name']}_{village['district']}"
                                        if key not in seen_villages:
                                            seen_villages[key] = village
                                            list_new += 1
                                        else:
                                            existing = seen_villages[key]
                                            if not existing.get('has_coords') and village.get('has_coords'):
                                                seen_villages[key] = village
                                                list_new += 1
                                    
                                    logger.info(f"        Добавлено {list_new} новых записей из дополнительного списка")
                
            except Exception as e:
                logger.error(f"    ❌ Ошибка обработки СП {settlement}: {e}")
                import traceback
                traceback.print_exc()
        
        all_villages = list(seen_villages.values())
        
        # Статистика сбора
        self.collection_stats['total_unique'] = len(all_villages)
        logger.info(f"📊 СТАТИСТИКА СБОРА НП:")
        logger.info(f"  • Из общих списков: {self.collection_stats['from_master_lists']}")
        logger.info(f"  • Из бывших НП: {self.collection_stats['from_former']}")
        logger.info(f"  • Из СП: {self.collection_stats['from_settlements']}")
        logger.info(f"  • Всего уникальных: {self.collection_stats['total_unique']}")
        
        # ========== ПОИСК КООРДИНАТ ==========
        if all_villages:
            villages_with_coords = [v for v in all_villages if v.get('has_coords')]
            villages_without_coords = [v for v in all_villages if not v.get('has_coords')]
            total_without = len(villages_without_coords)
            self.coords_stats['total_without'] = total_without
            
            logger.info(f"  🔍 ПОИСК КООРДИНАТ...")
            logger.info(f"    • Всего НП: {len(all_villages)}")
            logger.info(f"    • Уже с координатами: {len(villages_with_coords)}")
            logger.info(f"    • Без координат: {total_without}")
            logger.info(f"    • Координат из бывших НП: {self.coords_stats['from_former']}")
            
            # Шаг 1: Поиск на dic.academic.ru по ссылкам из СП
            with_links = [v for v in villages_without_coords if v['name'] in self.village_links]
            if with_links:
                logger.info(f"  📊 Поиск на dic.academic.ru по {len(with_links)} ссылкам")
                semaphore = asyncio.Semaphore(self.max_concurrent_dic)
                
                async def fetch_dic(village):
                    async with semaphore:
                        name = village['name']
                        if name in self.village_links:
                            article_id = self.village_links[name]
                            coords_data = await self._parse_individual_village_page(article_id, district)
                            if coords_data and coords_data.get('has_coords'):
                                return name, coords_data
                        return None, None
                
                tasks = [fetch_dic(v) for v in with_links]
                results = await asyncio.gather(*tasks)
                
                dic_found = 0
                for name, data in results:
                    if data:
                        for v in all_villages:
                            if v['name'] == name and not v.get('has_coords'):
                                v['lat'] = data['lat']
                                v['lon'] = data['lon']
                                v['has_coords'] = True
                                dic_found += 1
                                self.coords_stats['from_links'] += 1
                                logger.info(f"      ✅ Найдены координаты на dic.academic.ru: {name}")
                                break
                logger.info(f"  ✅ Найдено координат на dic.academic.ru: {dic_found}")
            
            # Шаг 2: ШАГ 3 - поиск на странице района (Wikipedia)
            villages_without_coords = [v for v in all_villages if not v.get('has_coords')]
            if villages_without_coords:
                logger.info(f"  📊 Осталось без координат: {len(villages_without_coords)}")
                logger.info(f"  🌐 ШАГ 3: Поиск на странице района...")
                
                current_villages_dict = {v['name']: v for v in all_villages}
                district_page_coords = await self._fetch_villages_from_district_page(district, current_villages_dict)
                
                if district_page_coords:
                    wiki_found = 0
                    for name, data in district_page_coords.items():
                        found = False
                        for v in all_villages:
                            if v['name'] == name and not v.get('has_coords'):
                                v['lat'] = data['lat']
                                v['lon'] = data['lon']
                                v['has_coords'] = True
                                wiki_found += 1
                                self.coords_stats['from_wikipedia'] += 1
                                logger.info(f"      ✅ Обновлены координаты для {name}: ({v['lat']}, {v['lon']})")
                                found = True
                                break
                        
                        if not found and data.get('is_new'):
                            if self._is_valid_name(name, district):
                                all_villages.append({
                                    "name": name, "type": data.get('type', 'деревня'),
                                    "lat": data['lat'], "lon": data['lon'],
                                    "district": district, "has_coords": True
                                })
                                wiki_found += 1
                                self.coords_stats['from_wikipedia'] += 1
                                self.collection_stats['from_district_page'] += 1
                                logger.info(f"  ➕ Добавлен новый НП: {name} ({data['lat']}, {data['lon']})")
                    
                    logger.info(f"  ✅ Найдено координат на странице района: {wiki_found}")
            
            # Финальная статистика
            final_with_coords = sum(1 for v in all_villages if v.get('has_coords'))
            remaining = [v for v in all_villages if not v.get('has_coords')]
            
            logger.info(f"    📊 ИТОГО ПО КООРДИНАТАМ:")
            logger.info(f"      • Из бывших НП: {self.coords_stats['from_former']}")
            logger.info(f"      • По ссылкам из СП: {self.coords_stats['from_links']}")
            logger.info(f"      • Со страницы района: {self.coords_stats['from_wikipedia']}")
            logger.info(f"      • Всего найдено: {self.coords_stats['from_former'] + self.coords_stats['from_links'] + self.coords_stats['from_wikipedia']}")
            logger.info(f"      • Осталось без координат: {len(remaining)}")
        
        # Очистка и сортировка
        all_villages.sort(key=lambda x: x['name'])
        for v in all_villages:
            v.pop('has_coords', None)
            v.pop('source', None)
            v.pop('article_id', None)
        
        total_time = time.time() - self.start_time
        final_with_coords = sum(1 for v in all_villages if v.get('lat') and v.get('lon'))
        
        logger.info(f"  ✅ Всего уникальных записей: {len(all_villages)}")
        logger.info(f"  ✅ С координатами: {final_with_coords}")
        logger.info(f"  ✅ Без координат: {len(all_villages) - final_with_coords}")
        logger.info(f"  ⏱️ Общее время: {total_time:.1f}с")
        
        return all_villages


__all__ = ['APISourceManager', 'AVAILABLE_DISTRICTS']