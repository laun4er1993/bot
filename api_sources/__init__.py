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
    MIN_NAME_LENGTH, MAX_NAME_LENGTH, DISTRICT_WIKI_NAMES, DISTRICT_UYEZDS,
    INVALID_SETTLEMENT_MARKERS, INVALID_VILLAGE_MARKERS, INVALID_LINK_PATTERNS,
    KNOWN_PERSONALITIES
)
from .utils import (
    is_valid_name, is_valid_settlement_name, expand_type,
    find_column_index, clean_village_name, validate_coordinates
)
from .coordinates import parse_dic_coordinates, parse_wikipedia_coordinates

logger = logging.getLogger(__name__)

# Общие границы Тверской области
TVER_BOUNDS = {
    'min_lat': 55.0,
    'max_lat': 58.5,
    'min_lon': 30.0,
    'max_lon': 38.5
}


class APISourceManager:
    """
    Универсальный менеджер для загрузки данных из dic.academic.ru и Wikipedia
    """
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.thread_pool = ThreadPoolExecutor(max_workers=3)
        
        # Для rate limiting
        self.request_count = 0
        self.last_request_time = 0
        self.min_request_interval = 2.0
        
        # Кэш для найденных ID
        self.article_cache: Dict[str, str] = {}
        self.district_cache: Dict[str, Dict] = {}
        self.former_np_pages_cache: Dict[str, str] = {}
        self.settlement_pages_cache: Dict[str, str] = {}
        self.page_cache: Dict[str, Tuple[str, float]] = {}
        self.processed_article_ids: Set[str] = set()
        
        # Словарь для хранения ссылок на отдельные страницы НП
        self.village_links: Dict[str, str] = {}
        
        # Словарь для хранения Wikipedia ссылок на НП
        self.wikipedia_links: Dict[str, str] = {}
        
        # Кэш координат из Wikipedia
        self.wikipedia_coords_cache: Dict[str, Tuple[str, str]] = {}
        
        # Кэш границ районов
        self.district_bounds_cache: Dict[str, Dict[str, float]] = {}
        
        # Время жизни кэша
        self.cache_ttl = 3600
        
        # Максимальное количество повторных попыток
        self.max_retries = 5
        
        # Статистика времени
        self.start_time = 0
        self.last_log_time = 0
        
        # Статистика поиска координат
        self.coords_stats = {
            'from_former': 0,
            'from_links': 0,
            'from_wikipedia': 0,
            'total_without': 0,
            'found': 0,
            'remaining': 0
        }
        
        # Статистика сбора НП
        self.collection_stats = {
            'from_master_lists': 0,
            'from_former': 0,
            'from_settlements': 0,
            'total_unique': 0
        }
        
        # Параллельные запросы
        self.max_concurrent_requests = 3
        self.max_concurrent_dic = 2
        
        # Стандартные заголовки
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
        self.village_links.clear()
        self.wikipedia_links.clear()
        self.wikipedia_coords_cache.clear()
        self.district_bounds_cache.clear()
        self.coords_stats = {
            'from_former': 0,
            'from_links': 0,
            'from_wikipedia': 0,
            'total_without': 0,
            'found': 0,
            'remaining': 0
        }
        self.collection_stats = {
            'from_master_lists': 0,
            'from_former': 0,
            'from_settlements': 0,
            'total_unique': 0
        }
        logger.info("🧹 Кэш очищен для нового поиска")
    
    async def _rate_limit(self):
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        actual_interval = self.min_request_interval * random.uniform(0.8, 1.2)
        
        if time_since_last < actual_interval:
            await asyncio.sleep(actual_interval - time_since_last)
        
        self.last_request_time = time.time()
        self.request_count += 1
        
        if self.request_count % 50 == 0:
            elapsed = time.time() - self.start_time
            logger.info(f"    Выполнено {self.request_count} запросов за {elapsed:.1f}с")
    
    async def _fetch_page_with_retry(self, url: str, retry_count: int = 0) -> Optional[str]:
        if retry_count >= self.max_retries:
            logger.error(f"Превышено максимальное количество попыток для {url}")
            return None
        
        try:
            session = await self._get_session()
            await self._rate_limit()
            
            async with session.get(url, headers=self.default_headers, timeout=120) as response:
                if response.status == 200:
                    html = await response.text()
                    return html
                elif response.status == 429:
                    base_wait = (2 ** retry_count) * random.uniform(1.0, 2.0)
                    wait_time = base_wait
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
            logger.warning(f"Таймаут для {url}, повтор через {wait_time}с (попытка {retry_count + 1}/{self.max_retries})")
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
    
    async def _search_with_pagination(self, query: str, max_pages: int = 10, unlimited: bool = False) -> List[Dict]:
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
        """Парсит страницу поиска dic.academic.ru"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            # Ищем список результатов
            terms_list = soup.find('ul', class_='terms-list')
            if not terms_list:
                logger.debug(f"    Не найден список результатов на странице {page_num}")
                return []
            
            for item in terms_list.find_all('li', recursive=False):
                try:
                    # Ищем ссылку
                    link = item.find('a', href=re.compile(r'/dic\.nsf/ruwiki/\d+'))
                    if not link:
                        continue
                    
                    href = link.get('href', '')
                    match = re.search(r'/dic\.nsf/ruwiki/(\d+)', href)
                    if not match:
                        continue
                    
                    article_id = match.group(1)
                    title = link.get_text().strip()
                    
                    # Ищем описание (текст после ссылки)
                    first_p = item.find('p')
                    full_text = ""
                    if first_p:
                        p_text = first_p.get_text().strip()
                        if title in p_text:
                            full_text = p_text.replace(title, '').strip()
                        else:
                            full_text = p_text
                    
                    # Определяем позицию (номер в списке)
                    position_match = re.match(r'^(\d+)', full_text)
                    position = int(position_match.group(1)) if position_match else 0
                    
                    # Убираем номер из full_text
                    if position > 0:
                        full_text = re.sub(r'^\d+\s*', '', full_text).strip()
                    
                    results.append({
                        'id': article_id,
                        'title': title,
                        'full_text': full_text,
                        'page': page_num,
                        'position': position
                    })
                    
                except Exception as e:
                    logger.debug(f"    Ошибка парсинга элемента поиска: {e}")
                    continue
            
            return results
            
        except Exception as e:
            logger.error(f"Ошибка парсинга страницы поиска: {e}")
            return []
    
    def _check_next_page(self, html: str) -> bool:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            next_link = soup.find('a', string=re.compile(r'далее|следующая|next', re.I))
            return next_link is not None
        except:
            return False
    
    def _normalize_text(self, text: str) -> str:
        """Нормализует текст: убирает кавычки, скобки, лишние пробелы"""
        if not text:
            return ""
        text = re.sub(r'[„“«»"\'`]', '', text)
        text = re.sub(r'[\(\)\[\]\{\}]', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text.lower()
    
    def _generate_district_variants(self, district_name: str) -> List[str]:
        """
        Генерирует все падежные формы названия района
        """
        variants = []
        base = district_name.lower().strip()
        variants.append(base)
        
        # Основа для склонения
        if base.endswith('ский'):
            stem = base[:-4]
            variants.append(f"{stem}ского")
            variants.append(f"{stem}скому")
            variants.append(f"{stem}ским")
            variants.append(f"{stem}ском")
            
            if base.endswith('ий'):
                variants.append(f"{stem}его")
                variants.append(f"{stem}ему")
                variants.append(f"{stem}им")
                variants.append(f"{stem}ем")
        
        elif base.endswith('ой'):
            stem = base[:-2]
            variants.append(f"{stem}ого")
            variants.append(f"{stem}ому")
            variants.append(f"{stem}ым")
            variants.append(f"{stem}ом")
        
        elif base.endswith('ый'):
            stem = base[:-2]
            variants.append(f"{stem}ого")
            variants.append(f"{stem}ому")
            variants.append(f"{stem}ым")
            variants.append(f"{stem}ом")
        
        # Добавляем вариант с "район"
        variants.append(f"{base} район")
        variants.append(f"{base} района")
        variants.append(f"{base} району")
        variants.append(f"{base} районом")
        variants.append(f"{base} районе")
        
        variants.append(f"{base} муниципальный округ")
        variants.append(f"{base} муниципального округа")
        variants.append(f"{base} муниципальному округу")
        
        return list(set(variants))
    
    def _check_district_in_text(self, text: str, district: str) -> bool:
        """
        Проверяет, есть ли упоминание района в тексте (с учётом всех падежей)
        """
        if not text or not district:
            return False
        
        text_lower = text.lower()
        district_lower = district.lower()
        
        district_variants = self._generate_district_variants(district_lower)
        
        for variant in district_variants:
            if variant in text_lower:
                logger.debug(f"      Найдено упоминание района: '{variant}'")
                return True
        
        return False
    
    def _is_valid_name(self, name: str, district: str = "") -> bool:
        """Проверяет, является ли текст валидным названием населенного пункта"""
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
        
        if any(x in name_lower for x in ['фильм', 'сериал', 'картина', 'спектакль', 'трасса', 'дорога', 'система', 'гидросистема', 'водохранилище']):
            return False
        
        if re.search(r'[А-Я]\.\s*[А-Я]\.', name):
            return False
        if re.search(r'[А-Я][а-я]+\s+[А-Я]\.', name):
            return False
        
        for personality in KNOWN_PERSONALITIES:
            if personality.lower() in name_lower:
                return False
        
        other_regions = ['ростовская', 'рязанская', 'волгоградская', 'пермский', 'удмуртия', 'московская', 'владимирская', 'калужская']
        for region in other_regions:
            if region in name_lower:
                return False
        
        if name_lower in ['список', 'категория', 'статья', 'примечания', 'ссылки', 'культура', 'искусство']:
            return False
        
        if name[0].isdigit():
            return False
        
        return True
    
    def _is_valid_settlement_name(self, name: str, district: str = "") -> bool:
        """Проверяет, является ли текст валидным названием сельского поселения"""
        if not name or len(name) < 2 or len(name) > 50:
            return False
        
        name_lower = name.lower()
        
        if re.match(r'^\d+\s+(мая|января|февраля|марта|апреля|июня|июля|августа|сентября|октября|ноября|декабря)', name_lower):
            return False
        
        if name.isdigit():
            return False
        
        for word in SERVICE_SETTLEMENT_WORDS:
            if word in name_lower:
                return False
        
        if not re.search(r'[а-яА-ЯёЁ]', name):
            return False
        
        if re.search(r'[А-Я]\.\s*[А-Я]\.', name):
            return False
        if re.search(r'[А-Я][а-я]+\s+[А-Я]\.', name):
            return False
        if re.search(r'[А-Я][а-я]+\s+[А-Я][а-я]+', name) and len(name.split()) >= 2:
            return False
        
        for personality in KNOWN_PERSONALITIES:
            if personality.lower() in name_lower:
                return False
        
        other_regions = ['ростовская', 'рязанская', 'волгоградская', 'пермский']
        for region in other_regions:
            if region in name_lower:
                return False
        
        if any(x in name_lower for x in ['водохранилище', 'культура', 'список', 'категория', 'уезд']):
            return False
        
        if 'сельское поселение' in name_lower or 'сельсовет' in name_lower:
            name_clean = re.sub(r'^сельское\s+поселение\s*', '', name_lower, flags=re.IGNORECASE)
            name_clean = re.sub(r'\s+\(.*?\)', '', name_clean).strip()
            if name_clean and len(name_clean) >= 2:
                if name_clean in ['зубцов', 'ржев', 'осташков', 'торжок', 'белый', 'нелидово', 'оленино']:
                    return False
                return True
        
        if len(name) <= 20 and re.match(r'^[А-Я][а-я]+$', name):
            if name_lower in ['зубцов', 'ржев', 'осташков', 'торжок', 'белый', 'нелидово', 'оленино']:
                return False
            return True
        
        return False
    
    # ========== ОПРЕДЕЛЕНИЕ ГРАНИЦ РАЙОНА ==========
    
    async def _get_district_bounds(self, district: str, district_html: str = None) -> Dict[str, float]:
        if district in self.district_bounds_cache:
            return self.district_bounds_cache[district]
        
        bounds = {
            'min_lat': TVER_BOUNDS['min_lat'],
            'max_lat': TVER_BOUNDS['max_lat'],
            'min_lon': TVER_BOUNDS['min_lon'],
            'max_lon': TVER_BOUNDS['max_lon']
        }
        
        self.district_bounds_cache[district] = bounds
        return bounds
    
    def _check_coordinate_in_district(self, lat: float, lon: float, district_bounds: Dict[str, float]) -> bool:
        return (district_bounds['min_lat'] <= lat <= district_bounds['max_lat'] and
                district_bounds['min_lon'] <= lon <= district_bounds['max_lon'])
    
    # ========== МЕТОДЫ ДЛЯ РАБОТЫ С DIC.ACADEMIC.RU ==========
    
    async def _find_district_page(self, district: str) -> Optional[Dict]:
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
            await asyncio.sleep(1.5)
        
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
                            'score': result['score'],
                            'html': html
                        }
                        
                        self.district_cache[cache_key] = district_info
                        return district_info
        
        logger.info(f"    ❌ Страница района не найдена")
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
    
    async def _extract_settlements_from_page(self, html: str, district: str) -> List[str]:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            found_settlements = []
            
            settlement_headers = [
                'состав района', 'сельские поселения', 'муниципальное устройство',
                'административное деление', 'список сельских поселений'
            ]
            
            for header in soup.find_all(['h2', 'h3', 'h4']):
                header_text = header.get_text().lower()
                header_text = re.sub(r'\s+', ' ', header_text).strip()
                
                is_settlement_header = False
                for keyword in settlement_headers:
                    if keyword in header_text:
                        is_settlement_header = True
                        break
                
                if not is_settlement_header:
                    continue
                
                logger.debug(f"        Найден заголовок: {header_text}")
                
                current = header.find_next_sibling()
                found_lists = []
                
                while current and len(found_lists) < 3:
                    if current.name in ['ul', 'ol']:
                        found_lists.append(current)
                        for li in current.find_all('li', recursive=False):
                            link = li.find('a')
                            if not link:
                                continue
                            
                            text = link.get_text().strip()
                            text = re.sub(r'\s+', ' ', text).strip()
                            
                            if 'сельское поселение' in text.lower():
                                match = re.search(r'«([^»]+)»', text)
                                if match:
                                    settlement = match.group(1).strip()
                                else:
                                    settlement = re.sub(r'^сельское\s+поселение\s*', '', text, flags=re.IGNORECASE)
                                    settlement = re.sub(r'\s+\(.*?\)', '', settlement).strip()
                                
                                if settlement and len(settlement) > 2:
                                    if self._is_valid_settlement_name(settlement, district):
                                        found_settlements.append(settlement)
                                        logger.debug(f"        Найдено СП: {settlement}")
                        
                        if len(found_lists) >= 2 and found_settlements:
                            break
                    
                    current = current.find_next_sibling()
                
                if found_settlements:
                    break
            
            if not found_settlements:
                for link in soup.find_all('a', href=re.compile(r'/dic\.nsf/ruwiki/\d+')):
                    link_text = link.get_text().strip()
                    if 'сельское поселение' in link_text.lower():
                        match = re.search(r'«([^»]+)»', link_text)
                        if match:
                            settlement = match.group(1).strip()
                        else:
                            settlement = re.sub(r'^сельское\s+поселение\s*', '', link_text, flags=re.IGNORECASE)
                            settlement = re.sub(r'\s+\(.*?\)', '', settlement).strip()
                        
                        if settlement and len(settlement) > 2:
                            if self._is_valid_settlement_name(settlement, district):
                                found_settlements.append(settlement)
                                logger.debug(f"        Найдено СП по прямой ссылке: {settlement}")
            
            unique_settlements = sorted(list(set(found_settlements)))
            
            logger.info(f"    Найдено сельских поселений: {len(unique_settlements)}")
            if unique_settlements:
                logger.debug(f"    Список СП: {', '.join(unique_settlements[:20])}")
            
            return unique_settlements
            
        except Exception as e:
            logger.error(f"Ошибка парсинга сельских поселений: {e}")
            return []
    
    async def _find_district_former_np_page(self, district: str, district_html: str) -> Optional[str]:
        """Находит общую страницу бывших населенных пунктов для всего района"""
        cache_key = f"district_former_{district}"
        if cache_key in self.former_np_pages_cache:
            return self.former_np_pages_cache[cache_key]
        
        soup = BeautifulSoup(district_html, 'html.parser')
        
        # Ищем в разделе "См. также"
        see_also = soup.find('div', class_='rellink boilerplate seealso')
        if see_also:
            for link in see_also.find_all('a', href=re.compile(r'/dic\.nsf/ruwiki/\d+')):
                link_text = link.get_text().lower()
                if 'список бывших населённых пунктов' in link_text and self._check_district_in_text(link_text, district):
                    match = re.search(r'/dic\.nsf/ruwiki/(\d+)', link.get('href', ''))
                    if match:
                        article_id = match.group(1)
                        logger.info(f"      Найдена общая страница бывших НП для района {district} (ID: {article_id})")
                        self.former_np_pages_cache[cache_key] = article_id
                        return article_id
        
        # Если не нашли в "См. также", ищем по запросу
        queries = [
            f"Список бывших населённых пунктов на территории {district} района Тверской области",
            f"Список бывших населенных пунктов {district} района",
            f"Бывшие населенные пункты {district} района"
        ]
        
        for query in queries:
            results = await self._search_with_pagination(query, max_pages=10)
            
            for result in results[:10]:
                title_lower = result['title'].lower()
                if ('список бывших' in title_lower or 'бывшие населенные' in title_lower) and self._check_district_in_text(title_lower, district):
                    page_url = DIC_ACADEMIC_ARTICLE_URL.format(result['id'])
                    html = await self._fetch_page(page_url)
                    if html:
                        soup_page = BeautifulSoup(html, 'html.parser')
                        tables = soup_page.find_all('table', class_=['standard', 'sortable'])
                        if tables:
                            logger.info(f"      Найдена общая страница бывших НП для района {district} (ID: {result['id']})")
                            self.former_np_pages_cache[cache_key] = result['id']
                            return result['id']
            
            await asyncio.sleep(1.5)
        
        return None
    
    async def _find_former_np_page(self, settlement: str, district: str) -> Optional[str]:
        """Находит страницу с бывшими населенными пунктами для сельского поселения"""
        cache_key = f"former_np_{district}_{settlement}"
        if cache_key in self.former_np_pages_cache:
            return self.former_np_pages_cache[cache_key]
        
        district_lower = district.lower()
        settlement_lower = settlement.lower()
        
        queries = [
            f"Список бывших населённых пунктов на территории сельского поселения {settlement} {district} район",
            f"Список бывших населенных пунктов на территории сельского поселения {settlement} {district} район",
            f"Список бывших населённых пунктов {settlement} {district} район",
            f"Бывшие населённые пункты {settlement} СП",
            f"Список бывших населённых пунктов {settlement} сельского поселения",
            f"{settlement} бывшие населенные пункты"
        ]
        
        all_results = []
        
        logger.info(f"    🔍 Поиск бывших НП для СП {settlement}...")
        
        for query in queries:
            results = await self._search_with_pagination(query, max_pages=15)
            all_results.extend(results)
            logger.info(f"      Запрос '{query[:80]}...' дал {len(results)} результатов")
            await asyncio.sleep(1.5)
        
        if not all_results:
            logger.info(f"      ❌ Поиск бывших НП для СП {settlement} не дал результатов")
            return None
        
        logger.info(f"      Всего результатов поиска: {len(all_results)}")
        
        all_results.sort(key=lambda x: x['position'] if x['position'] > 0 else 999)
        
        for i, result in enumerate(all_results[:30]):
            title_lower = result['title'].lower()
            full_text_lower = result['full_text'].lower()
            
            title_normalized = self._normalize_text(title_lower)
            settlement_normalized = self._normalize_text(settlement_lower)
            
            logger.info(f"        Результат {i+1}: ID {result['id']} - {result['title'][:100]}...")
            
            if not self._check_district_in_text(title_lower + " " + full_text_lower, district):
                logger.info(f"          ❌ Не относится к району {district}")
                continue
            
            if 'бывших' not in title_lower and 'бывшие' not in title_lower:
                logger.info(f"          ❌ Нет слова 'бывших' в заголовке")
                continue
            
            if (settlement_lower not in title_lower and 
                settlement_normalized not in title_normalized):
                logger.info(f"          ❌ Нет названия СП '{settlement}' в заголовке")
                continue
            
            logger.info(f"          ✅ Прошел проверки, загружаем страницу...")
            
            page_url = DIC_ACADEMIC_ARTICLE_URL.format(result['id'])
            html = await self._fetch_page(page_url)
            
            if not html:
                logger.info(f"          ❌ Не удалось загрузить страницу")
                continue
            
            soup = BeautifulSoup(html, 'html.parser')
            
            tables = soup.find_all('table', class_=['standard', 'sortable'])
            logger.info(f"          Найдено таблиц: {len(tables)}")
            
            for table in tables:
                headers = [h.get_text().strip().lower() for h in table.find_all('th')]
                has_coords = any('координат' in h for h in headers)
                has_year = any('год' in h and ('упраздн' in h or 'упразднения' in h) for h in headers)
                
                logger.info(f"          Таблица: колонки {headers}, has_coords={has_coords}, has_year={has_year}")
                
                if has_coords or has_year:
                    logger.info(f"      ✅ Найдена страница бывших НП для СП {settlement} (ID: {result['id']})")
                    self.former_np_pages_cache[cache_key] = result['id']
                    return result['id']
            
            logger.info(f"          ❌ Нет таблицы с координатами или годом упразднения")
        
        logger.info(f"      ❌ Страница бывших НП для СП {settlement} не найдена")
        return None
    
    async def _find_settlement_main_page(self, settlement: str, district: str) -> Optional[str]:
        cache_key = f"settlement_main_{district}_{settlement}"
        if cache_key in self.settlement_pages_cache:
            return self.settlement_pages_cache[cache_key]
        
        queries = [
            f"Сельское поселение {settlement}",
            f"{settlement} сельское поселение",
            f"{settlement} СП"
        ]
        
        all_results = []
        
        for query in queries:
            results = await self._search_with_pagination(query, max_pages=10)
            all_results.extend(results)
            await asyncio.sleep(1.5)
        
        if not all_results:
            return None
        
        for result in all_results:
            title_lower = result['title'].lower()
            if "список бывших" in title_lower:
                result['score'] = 0
            else:
                result['score'] = self._score_settlement_relevance(result, settlement, district)
        
        best = max(all_results, key=lambda x: x['score'])
        
        if best['score'] >= 40:
            page_url = DIC_ACADEMIC_ARTICLE_URL.format(best['id'])
            html = await self._fetch_page(page_url)
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                title_elem = soup.find('h1')
                title_text = title_elem.get_text().lower() if title_elem else ""
                
                if "район" in title_text and "сельское поселение" not in title_text:
                    logger.debug(f"      Пропускаем страницу района: {best['id']} - {title_text}")
                    return None
                
                has_settlements = False
                for header in soup.find_all(['h2', 'h3', 'h4']):
                    if any(kw in header.get_text().lower() for kw in SETTLEMENTS_SECTION_KEYWORDS):
                        has_settlements = True
                        break
                if not has_settlements:
                    tables = soup.find_all('table', class_=['standard', 'sortable', 'wikitable'])
                    if not tables:
                        logger.debug(f"      Страница ID {best['id']} не содержит НП, пропускаем")
                        return None
            logger.info(f"      Найдена основная страница СП {settlement} (ID: {best['id']}, score: {best['score']})")
            self.settlement_pages_cache[cache_key] = best['id']
            return best['id']
        
        return None
    
    def _score_settlement_relevance(self, result: Dict, settlement: str, district: str) -> int:
        title_lower = result['title'].lower()
        full_text_lower = result['full_text'].lower()
        settlement_lower = settlement.lower()
        
        score = 0
        
        if settlement_lower in title_lower:
            score += 50
        
        if "сельское поселение" in title_lower:
            score += 40
        elif "сельское поселение" in full_text_lower:
            score += 20
        
        if self._check_district_in_text(title_lower + " " + full_text_lower, district):
            score += 20
        
        if result['position'] == 1:
            score += 15
        elif result['position'] <= 3:
            score += 10
        
        return score
    
    async def _parse_former_np_page(self, article_id: str, district: str, settlement: str) -> List[Dict]:
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        html = await self._fetch_page(url)
        
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        page_text = soup.get_text().lower()
        
        if not self._check_district_in_text(page_text, district):
            logger.debug(f"      Страница ID {article_id} не относится к району {district}, пропускаем")
            return []
        
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(
            self.thread_pool,
            self._parse_former_np_html,
            html,
            article_id,
            district,
            settlement
        )
        
        if results:
            logger.info(f"      Из списка бывших НП ID {article_id} получено {len(results)} записей")
            for i, v in enumerate(results[:5]):
                coords_info = f" (коорд: {v['lat']}, {v['lon']})" if v['has_coords'] else " (без коорд)"
                logger.info(f"        {i+1}. {v['name']} ({v['type']}){coords_info}")
        
        return results
    
    def _parse_former_np_html(self, html: str, article_id: str, district: str, settlement: str) -> List[Dict]:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            tables = soup.find_all('table', class_=['standard', 'sortable'])
            logger.info(f"        Найдено таблиц для парсинга: {len(tables)}")
            
            for table_idx, table in enumerate(tables):
                rows = table.find_all('tr')
                if len(rows) < 2:
                    continue
                
                header_cells = rows[0].find_all(['th', 'td'])
                headers = [h.get_text().strip().lower() for h in header_cells]
                
                name_idx = None
                type_idx = None
                coords_idx = None
                
                for i, header in enumerate(headers):
                    header_clean = header.replace('\n', ' ').strip()
                    if 'населённый пункт' in header_clean or 'название' in header_clean:
                        name_idx = i
                    elif 'тип' in header_clean:
                        type_idx = i
                    elif 'координат' in header_clean:
                        coords_idx = i
                
                logger.info(f"        Таблица {table_idx}: name_idx={name_idx}, type_idx={type_idx}, coords_idx={coords_idx}")
                
                if name_idx is None:
                    logger.warning(f"        Таблица {table_idx}: не найдена колонка с названиями")
                    continue
                
                coords_found_in_table = 0
                
                for row_idx, row in enumerate(rows[1:], 1):
                    try:
                        cells = row.find_all('td')
                        if len(cells) <= name_idx:
                            continue
                        
                        name_cell = cells[name_idx]
                        name = name_cell.get_text().strip()
                        
                        name = re.sub(r'^\d+\s*', '', name)
                        name = re.sub(r'\s+', ' ', name).strip()
                        
                        if not name or len(name) < MIN_NAME_LENGTH:
                            continue
                        
                        if name in ['ИТОГО', 'Всего', 'Итого', 'ИТОГО:', 'Всего:']:
                            continue
                        
                        if not self._is_valid_name(name, district):
                            continue
                        
                        village_type = 'деревня'
                        if type_idx is not None and type_idx < len(cells):
                            raw_type = cells[type_idx].get_text().strip()
                            if raw_type:
                                village_type = expand_type(raw_type)
                        
                        lat, lon = None, None
                        if coords_idx is not None and coords_idx < len(cells):
                            coord_text = cells[coords_idx].get_text().strip()
                            logger.debug(f"          Координаты для {name}: {coord_text}")
                            lat, lon = parse_dic_coordinates(coord_text, cells[coords_idx])
                            
                            if not lat or not lon:
                                row_text = ' '.join([c.get_text() for c in cells])
                                lat, lon = parse_dic_coordinates(row_text, None)
                        
                        if lat and lon and validate_coordinates(lat, lon):
                            lat_rounded = round(lat, 5)
                            lon_rounded = round(lon, 5)
                            logger.info(f"          ✅ {name}: координаты {lat_rounded}, {lon_rounded}")
                            results.append({
                                "name": name,
                                "type": village_type,
                                "lat": str(lat_rounded),
                                "lon": str(lon_rounded),
                                "district": district,
                                "has_coords": True,
                                "source": "former"
                            })
                            coords_found_in_table += 1
                            self.coords_stats['from_former'] += 1
                        else:
                            logger.debug(f"          ⚠️ {name}: координаты не найдены")
                            results.append({
                                "name": name,
                                "type": village_type,
                                "lat": "",
                                "lon": "",
                                "district": district,
                                "has_coords": False,
                                "source": "former"
                            })
                        
                    except Exception as e:
                        logger.debug(f"        Ошибка парсинга строки {row_idx} в таблице {table_idx}: {e}")
                        continue
                
                if coords_found_in_table > 0:
                    logger.info(f"        Таблица {table_idx}: найдено координат для {coords_found_in_table} населенных пунктов")
            
            return results
            
        except Exception as e:
            logger.error(f"Ошибка парсинга страницы бывших НП: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    async def _parse_settlement_main_page(self, article_id: str, district: str, settlement: str) -> List[Dict]:
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        html = await self._fetch_page(url)
        
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        page_text = soup.get_text().lower()
        
        if not self._check_district_in_text(page_text, district):
            logger.debug(f"      Страница ID {article_id} не относится к району {district}, пропускаем")
            return []
        
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(
            self.thread_pool,
            self._parse_settlements_section,
            html,
            article_id,
            district,
            settlement
        )
        
        if results:
            logger.info(f"      Из раздела 'Населенные пункты' СП {settlement} получено {len(results)} записей")
        else:
            alt_results = await loop.run_in_executor(
                self.thread_pool,
                self._parse_settlements_alternative,
                html,
                article_id,
                district,
                settlement
            )
            if alt_results:
                logger.info(f"      Из альтернативного парсинга СП {settlement} получено {len(alt_results)} записей")
                results = alt_results
        
        return results
    
    def _parse_settlements_section(self, html: str, article_id: str, district: str, settlement: str) -> List[Dict]:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            links_found = 0
            
            section_headers = []
            for header in soup.find_all(['h2', 'h3', 'h4']):
                header_text = header.get_text().lower()
                for keyword in SETTLEMENTS_SECTION_KEYWORDS:
                    if keyword in header_text:
                        section_headers.append(header)
                        logger.info(f"        Найден заголовок: {header_text}")
                        break
            
            if not section_headers:
                for elem in soup.find_all(['p', 'div', 'span']):
                    elem_text = elem.get_text().lower()
                    for keyword in SETTLEMENTS_SECTION_KEYWORDS:
                        if keyword in elem_text and len(elem_text) < 100:
                            parent = elem.find_parent()
                            if parent:
                                section_headers.append(elem)
                                logger.info(f"        Найден текстовый маркер: {elem_text[:50]}")
                                break
            
            all_tables = soup.find_all('table', class_=['standard', 'sortable', 'wikitable', 'simple', 'collapsible', 'collapsed'])
            
            tables_to_parse = []
            if section_headers:
                for header in section_headers:
                    parent = header.find_parent()
                    if parent:
                        nearby_tables = parent.find_all('table', class_=['standard', 'sortable', 'wikitable', 'simple', 'collapsible', 'collapsed'])
                        tables_to_parse.extend(nearby_tables)
            
            if not tables_to_parse:
                tables_to_parse = all_tables
            
            unique_tables = []
            seen = set()
            for table in tables_to_parse:
                table_id = id(table)
                if table_id not in seen:
                    seen.add(table_id)
                    unique_tables.append(table)
            
            for table in unique_tables:
                rows = table.find_all('tr')
                if len(rows) < 2:
                    continue
                
                header_row = rows[0]
                header_cells = header_row.find_all(['th', 'td'])
                
                type_idx = None
                name_idx = None
                
                for i, cell in enumerate(header_cells):
                    cell_text = cell.get_text().strip().lower()
                    if 'тип' in cell_text:
                        type_idx = i
                    elif 'название' in cell_text or 'населённый пункт' in cell_text or 'населенный пункт' in cell_text:
                        name_idx = i
                
                if type_idx is None or name_idx is None:
                    if len(rows) > 1:
                        sample_row = rows[1]
                        sample_cells = sample_row.find_all('td')
                        for i, cell in enumerate(sample_cells):
                            cell_text = cell.get_text().strip()
                            if any(indicator in cell_text for indicator in TYPE_INDICATORS):
                                type_idx = i
                                if i + 1 < len(sample_cells):
                                    name_idx = i + 1
                                break
                
                if name_idx is None:
                    if len(header_cells) >= 2:
                        name_idx = 1
                    else:
                        name_idx = 0
                
                if type_idx is None:
                    if name_idx > 0:
                        type_idx = name_idx - 1
                    else:
                        type_idx = 0
                
                for row in rows[1:]:
                    try:
                        cells = row.find_all('td')
                        if len(cells) <= max(type_idx, name_idx):
                            continue
                        
                        type_cell = cells[type_idx]
                        raw_type = type_cell.get_text().strip()
                        village_type = expand_type(raw_type)
                        
                        name_cell = cells[name_idx]
                        name = name_cell.get_text().strip()
                        
                        name = re.sub(r'^\d+\s*', '', name)
                        name = re.sub(r'\s+', ' ', name).strip()
                        
                        if not name or len(name) < MIN_NAME_LENGTH or len(name) > MAX_NAME_LENGTH:
                            continue
                        
                        if not self._is_valid_name(name, district):
                            continue
                        
                        link = name_cell.find('a')
                        article_id_from_link = None
                        if link:
                            href = link.get('href', '')
                            match = re.search(r'(\d+)', href)
                            if match:
                                article_id_from_link = match.group(1)
                                self.village_links[name] = article_id_from_link
                                links_found += 1
                                logger.info(f"        🔗 Найдена ссылка для {name}: ID {article_id_from_link}")
                        
                        results.append({
                            "name": name,
                            "type": village_type,
                            "lat": "",
                            "lon": "",
                            "district": district,
                            "has_coords": False,
                            "article_id": article_id_from_link
                        })
                        
                    except Exception as e:
                        continue
                
                if links_found == 0:
                    for row in rows:
                        cells = row.find_all('td')
                        for i, cell in enumerate(cells):
                            link = cell.find('a')
                            if link:
                                name = link.get_text().strip()
                                name = re.sub(r'^\d+\s*', '', name)
                                name = re.sub(r'\s+', ' ', name).strip()
                                
                                if not name or len(name) < MIN_NAME_LENGTH or len(name) > MAX_NAME_LENGTH:
                                    continue
                                
                                if not self._is_valid_name(name, district):
                                    continue
                                
                                type_text = 'деревня'
                                if i > 0:
                                    prev_cell = cells[i-1]
                                    prev_text = prev_cell.get_text().strip()
                                    if any(ind in prev_text for ind in TYPE_INDICATORS):
                                        type_text = expand_type(prev_text)
                                
                                href = link.get('href', '')
                                match = re.search(r'(\d+)', href)
                                if match:
                                    article_id_from_link = match.group(1)
                                    self.village_links[name] = article_id_from_link
                                    links_found += 1
                                    logger.info(f"        🔗 Найдена ссылка (альт) для {name}: ID {article_id_from_link}")
                                    
                                    results.append({
                                        "name": name,
                                        "type": type_text,
                                        "lat": "",
                                        "lon": "",
                                        "district": district,
                                        "has_coords": False,
                                        "article_id": article_id_from_link
                                    })
            
            logger.info(f"        Всего найдено ссылок: {links_found}")
            return results
            
        except Exception as e:
            logger.error(f"Ошибка парсинга раздела 'Населенные пункты': {e}")
            return []
    
    def _parse_settlements_alternative(self, html: str, article_id: str, district: str, settlement: str) -> List[Dict]:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            seen_names = set()
            links_found = 0
            
            for link in soup.find_all('a', href=re.compile(r'/dic\.nsf/ruwiki/\d+')):
                href = link.get('href', '')
                if article_id in href:
                    continue
                
                name = link.get_text().strip()
                name = re.sub(r'^\d+\s*', '', name)
                name = re.sub(r'\s+', ' ', name).strip()
                
                if not name or len(name) < MIN_NAME_LENGTH or len(name) > MAX_NAME_LENGTH:
                    continue
                if name in seen_names:
                    continue
                
                if not self._is_valid_name(name, district):
                    continue
                
                if re.search(r'[А-Я]\.\s*[А-Я]\.', name):
                    continue
                if re.search(r'[А-Я][а-я]+\s+[А-Я]\.', name):
                    continue
                if re.search(r'[А-Я][а-я]+\s+[А-Я][а-я]+', name) and len(name.split()) >= 2:
                    continue
                
                other_regions = ['ростовская', 'рязанская', 'волгоградская', 'пермский', 'удмуртия', 'владимирская', 'калужская']
                if any(region in name.lower() for region in other_regions):
                    continue
                
                if any(word in name.lower() for word in ['список', 'категория', 'фильм', 'сериал', 'статья', 'трасса', 'дорога', 'система', 'водохранилище']):
                    continue
                
                village_type = 'деревня'
                parent = link.find_parent('td')
                if parent:
                    row = parent.find_parent('tr')
                    if row:
                        for cell in row.find_all('td'):
                            cell_text = cell.get_text().strip().lower()
                            if cell_text in ['дер.', 'д.', 'пос.', 'п.', 'с.', 'х.', 'ур.']:
                                village_type = expand_type(cell_text)
                                break
                
                match = re.search(r'(\d+)', href)
                if match:
                    link_id = match.group(1)
                    seen_names.add(name)
                    self.village_links[name] = link_id
                    links_found += 1
                    logger.info(f"        🔗 Найдена ссылка (альт) для {name}: ID {link_id}")
                    results.append({
                        "name": name,
                        "type": village_type,
                        "lat": "",
                        "lon": "",
                        "district": district,
                        "has_coords": False,
                        "article_id": link_id
                    })
            
            logger.info(f"        Всего найдено ссылок (альт): {links_found}")
            return results
            
        except Exception as e:
            logger.error(f"Ошибка альтернативного парсинга: {e}")
            return []
    
    async def _find_master_list_links(self, html: str, district: str) -> List[str]:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            found_ids = []
            
            for link in soup.find_all('a', href=re.compile(r'/dic\.nsf/ruwiki/\d+')):
                href = link.get('href', '')
                text = link.get_text().lower().strip()
                surrounding = ''
                
                parent = link.find_parent(['p', 'div', 'li'])
                if parent:
                    surrounding = parent.get_text().lower()
                
                full_context = text + ' ' + surrounding
                
                for keyword in LIST_KEYWORDS:
                    if keyword in full_context:
                        if self._check_district_in_text(full_context, district):
                            match = re.search(r'/dic\.nsf/ruwiki/(\d+)', href)
                            if match:
                                article_id = match.group(1)
                                found_ids.append(article_id)
                                logger.info(f"      Найдена ссылка на список НП: ID {article_id} - {link.get_text()}")
                                break
            
            see_also_patterns = ['см. также', 'смотри также', 'см также']
            
            for pattern in see_also_patterns:
                for elem in soup.find_all(['p', 'div', 'span', 'li'], string=re.compile(pattern, re.I)):
                    parent = elem.find_parent()
                    if parent:
                        for link in parent.find_all('a', href=re.compile(r'/dic\.nsf/ruwiki/\d+')):
                            href = link.get('href', '')
                            text = link.get_text().lower()
                            
                            for keyword in LIST_KEYWORDS:
                                if keyword in text:
                                    if self._check_district_in_text(text, district):
                                        match = re.search(r'/dic\.nsf/ruwiki/(\d+)', href)
                                        if match:
                                            article_id = match.group(1)
                                            if article_id not in found_ids:
                                                found_ids.append(article_id)
                                                logger.info(f"      Найдена ссылка на список НП в 'См. также': ID {article_id} - {link.get_text()}")
                                            break
            
            return list(set(found_ids))
            
        except Exception as e:
            logger.error(f"Ошибка поиска ссылок на списки: {e}")
            return []
    
    async def _parse_master_list_page(self, article_id: str, district: str) -> List[Dict]:
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        html = await self._fetch_page(url)
        
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        page_text = soup.get_text().lower()
        
        if not self._check_district_in_text(page_text, district):
            logger.debug(f"      Страница ID {article_id} не относится к району {district}, пропускаем")
            return []
        
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(
            self.thread_pool,
            self._parse_master_list_html,
            html,
            article_id,
            district
        )
        
        if results:
            logger.info(f"      Из списка ID {article_id} получено {len(results)} записей")
            if len(results) > 0:
                sample = results[:min(5, len(results))]
                logger.info(f"        Примеры: {[(v['name'], v['type']) for v in sample]}")
        else:
            logger.warning(f"      Из списка ID {article_id} не получено записей")
        
        return results
    
    def _parse_master_list_html(self, html: str, article_id: str, district: str) -> List[Dict]:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            tables = soup.find_all('table', class_=['standard', 'sortable', 'wikitable', 'simple', 'collapsible', 'collapsed'])
            
            for table in tables:
                rows = table.find_all('tr')
                if len(rows) < 2:
                    continue
                
                header_cells = rows[0].find_all(['th', 'td'])
                headers = [h.get_text().strip().lower() for h in header_cells]
                
                if len(headers) < 2 and len(rows) > 2:
                    header_cells = rows[1].find_all(['th', 'td'])
                    headers = [h.get_text().strip().lower() for h in header_cells]
                    start_row = 2
                else:
                    start_row = 1
                
                name_idx = find_column_index(headers, [
                    'населённый пункт', 'название', 'наименование', 
                    'населенный пункт', 'пункт', 'нп'
                ])
                
                type_idx = find_column_index(headers, [
                    'тип', 'тип нп', 'категория'
                ])
                
                if name_idx is None:
                    name_idx = 0
                
                for row in rows[start_row:]:
                    try:
                        cells = row.find_all('td')
                        if len(cells) <= name_idx:
                            continue
                        
                        name = cells[name_idx].get_text().strip()
                        
                        if not name or name in ['ИТОГО', 'Всего', 'Итого']:
                            continue
                        
                        name = re.sub(r'^\d+\s*', '', name)
                        name = re.sub(r'\s+', ' ', name).strip()
                        
                        if not self._is_valid_name(name, district):
                            continue
                        
                        village_type = 'деревня'
                        if type_idx is not None and type_idx < len(cells):
                            raw_type = cells[type_idx].get_text().strip()
                            village_type = expand_type(raw_type)
                        else:
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
                        
                    except Exception as e:
                        continue
            
            return results
            
        except Exception as e:
            logger.error(f"Ошибка парсинга страницы списка: {e}")
            return []
    
    async def _parse_individual_village_page(self, article_id: str, district: str) -> Optional[Dict]:
        await asyncio.sleep(random.uniform(1.5, 3.0))
        
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
                village_type = expand_type(possible_type)
            else:
                type_match = re.search(r',\s*([^,]+)$', full_title)
                if type_match:
                    possible_type = type_match.group(1).lower()
                    name = full_title.replace(f', {possible_type}', '').strip()
                    village_type = expand_type(possible_type)
            
            if not self._is_valid_name(name, district):
                logger.debug(f"        ❌ Невалидное название: {name}")
                return None
            
            lat, lon = None, None
            source = None
            
            geo_span = soup.find('span', class_='geo')
            if geo_span:
                lat_span = geo_span.find('span', class_='latitude')
                lon_span = geo_span.find('span', class_='longitude')
                
                if lat_span and lon_span:
                    try:
                        lat = float(lat_span.get_text().strip())
                        lon = float(lon_span.get_text().strip())
                        source = "скрытый geo span"
                        logger.info(f"        ✅ Найдены координаты через geo span: {lat:.5f}, {lon:.5f}")
                    except ValueError as e:
                        logger.debug(f"        ❌ Ошибка парсинга geo span: {e}")
            
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
                        source = "DMS формат"
                        logger.info(f"        ✅ Найдены координаты через DMS: {lat:.5f}, {lon:.5f}")
                    except ValueError as e:
                        logger.debug(f"        ❌ Ошибка парсинга DMS: {e}")
            
            if not lat or not lon:
                decimal_pattern = r'([0-9]+\.[0-9]+)[,\s]+([0-9]+\.[0-9]+)'
                text = soup.get_text()
                match = re.search(decimal_pattern, text)
                if match:
                    try:
                        lat_candidate = float(match.group(1))
                        lon_candidate = float(match.group(2))
                        if validate_coordinates(lat_candidate, lon_candidate):
                            lat = lat_candidate
                            lon = lon_candidate
                            source = "десятичные в тексте"
                            logger.info(f"        ✅ Найдены координаты через десятичные: {lat:.5f}, {lon:.5f}")
                    except ValueError as e:
                        logger.debug(f"        ❌ Ошибка парсинга десятичных: {e}")
            
            if not lat or not lon:
                geo_dms = soup.find('span', class_='geo-dms')
                if geo_dms:
                    dms_text = geo_dms.get_text()
                    match = re.search(dms_pattern, dms_text)
                    if match:
                        try:
                            lat_deg, lat_min, lat_sec = map(float, match.group(1, 2, 3))
                            lon_deg, lon_min, lon_sec = map(float, match.group(4, 5, 6))
                            
                            lat = lat_deg + lat_min/60 + lat_sec/3600
                            lon = lon_deg + lon_min/60 + lon_sec/3600
                            source = "geo-dms span"
                            logger.info(f"        ✅ Найдены координаты через geo-dms: {lat:.5f}, {lon:.5f}")
                        except ValueError as e:
                            logger.debug(f"        ❌ Ошибка парсинга geo-dms: {e}")
            
            if lat and lon:
                if not validate_coordinates(lat, lon):
                    logger.debug(f"        ❌ Координаты вне Тверской области: {lat}, {lon}")
                    return None
                logger.info(f"        ✅ ИТОГО: координаты для {name}: {lat:.5f}, {lon:.5f} (из {source})")
                return {
                    "name": name,
                    "type": village_type,
                    "lat": str(round(lat, 5)),
                    "lon": str(round(lon, 5)),
                    "district": district,
                    "has_coords": True
                }
            else:
                logger.debug(f"        ❌ Координаты не найдены для {name}")
                return None
            
        except Exception as e:
            logger.error(f"Ошибка парсинга отдельной страницы НП: {e}")
            return None
    
    # ========== МЕТОДЫ ДЛЯ РАБОТЫ С WIKIPEDIA ==========
    
    async def _find_district_in_tver_region(self, district: str) -> Optional[str]:
        logger.info(f"  🔍 Поиск страницы района на странице Тверской области: {district}")
        
        html = await self._fetch_page(TVER_OBLAST_URL)
        if not html:
            logger.warning(f"    ❌ Не удалось загрузить страницу Тверской области")
            return None
        
        soup = BeautifulSoup(html, 'html.parser')
        tables = soup.find_all('table', class_=['standard', 'sortable', 'wikitable', 'collapsible', 'collapsed'])
        
        district_lower = district.lower()
        logger.debug(f"    Найдено таблиц: {len(tables)}")
        
        for table in tables:
            headers = [h.get_text().strip().lower() for h in table.find_all('th')]
            name_col_idx = None
            for i, h in enumerate(headers):
                if 'название' in h or 'населённый пункт' in h:
                    name_col_idx = i
                    break
            
            if name_col_idx is None:
                continue
            
            for row in table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) <= name_col_idx:
                    continue
                
                cell_text = cells[name_col_idx].get_text().strip().lower()
                if self._check_district_in_text(cell_text, district):
                    link = cells[name_col_idx].find('a')
                    if link and link.get('href', '').startswith('/wiki/'):
                        page_url = f"{WIKIPEDIA_BASE_URL}{link['href']}"
                        logger.info(f"    ✅ Найдена страница района на странице Тверской области: {page_url}")
                        return page_url
            
            for link in table.find_all('a', href=re.compile(r'^/wiki/')):
                link_text = link.get_text().strip().lower()
                if self._check_district_in_text(link_text, district):
                    page_url = f"{WIKIPEDIA_BASE_URL}{link['href']}"
                    logger.info(f"    ✅ Найдена страница района на странице Тверской области: {page_url}")
                    return page_url
        
        logger.warning(f"    ❌ Страница района не найдена на странице Тверской области")
        return None
    
    async def _find_wikipedia_district_page(self, district: str) -> Optional[str]:
        logger.info(f"  🔍 Поиск страницы района на Wikipedia: {district}")
        
        tver_page_url = await self._find_district_in_tver_region(district)
        if tver_page_url:
            return tver_page_url
        
        possible_names = DISTRICT_WIKI_NAMES.get(district, [
            f"{district} муниципальный округ",
            f"{district} район",
            f"{district}"
        ])
        
        for name in possible_names:
            encoded_name = quote_plus(name)
            url = f"{WIKIPEDIA_BASE_URL}/wiki/{encoded_name}"
            
            logger.debug(f"    🔎 Пробуем: {url}")
            html = await self._fetch_page(url)
            
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                no_article = soup.find('div', class_='noarticletext')
                
                if not no_article:
                    title = soup.find('h1')
                    title_text = title.get_text().strip().lower() if title else ""
                    
                    if district == "Ржевский" and ("ржев" in title_text and "район" not in title_text and "округ" not in title_text):
                        logger.debug(f"    ⚠️ Пропускаем страницу города: {url}")
                        continue
                    
                    tables = soup.find_all('table', class_=['standard', 'wikitable', 'sortable', 'collapsible', 'collapsed'])
                    lists = soup.find_all(['ul', 'ol'])
                    
                    has_village_links = False
                    
                    for table in tables:
                        headers = [h.get_text().strip().lower() for h in table.find_all('th')]
                        for h in headers:
                            if 'населённый пункт' in h or 'населенный пункт' in h or 'название' in h:
                                has_village_links = True
                                logger.debug(f"      Найдена таблица с НП в {url}")
                                break
                        if has_village_links:
                            break
                    
                    if not has_village_links:
                        for lst in lists:
                            links = lst.find_all('a', href=re.compile(r'^/wiki/'))
                            if len(links) > 10:
                                has_village_links = True
                                logger.debug(f"      Найден список с {len(links)} ссылками в {url}")
                                break
                    
                    if has_village_links:
                        logger.info(f"    ✅ Найдена страница района на Wikipedia: {url}")
                        return url
                    
                    logger.debug(f"    ⚠️ Страница существует, но нет списка НП: {url}")
            
            await asyncio.sleep(1)
        
        logger.info(f"    🔎 Пробуем поиск через API Wikipedia")
        region = "Тверская область"
        
        search_queries = [
            f"{district} муниципальный округ {region}",
            f"{district} район {region}",
            f"{district} муниципальный округ",
            f"{district} район"
        ]
        
        for search_query in search_queries:
            search_url = f"{WIKIPEDIA_SEARCH_URL}?action=query&list=search&srsearch={quote_plus(search_query)}&format=json&utf8=1"
            
            html = await self._fetch_page(search_url)
            if html:
                try:
                    data = json.loads(html)
                    if 'query' in data and 'search' in data['query']:
                        for result in data['query']['search'][:15]:
                            title = result['title']
                            page_url = f"{WIKIPEDIA_BASE_URL}/wiki/{quote_plus(title)}"
                            
                            logger.debug(f"    🔎 Проверяем через API: {page_url}")
                            page_html = await self._fetch_page(page_url)
                            if page_html:
                                soup = BeautifulSoup(page_html, 'html.parser')
                                
                                tables = soup.find_all('table', class_=['standard', 'wikitable', 'sortable', 'collapsible', 'collapsed'])
                                lists = soup.find_all(['ul', 'ol'])
                                
                                has_village_links = False
                                
                                for table in tables:
                                    headers = [h.get_text().strip().lower() for h in table.find_all('th')]
                                    for h in headers:
                                        if 'населённый пункт' in h or 'населенный пункт' in h:
                                            has_village_links = True
                                            break
                                    if has_village_links:
                                        break
                                
                                if not has_village_links:
                                    for lst in lists:
                                        links = lst.find_all('a', href=re.compile(r'^/wiki/'))
                                        if len(links) > 10:
                                            has_village_links = True
                                            break
                                
                                if has_village_links:
                                    logger.info(f"    ✅ Найдена страница района через API: {page_url}")
                                    return page_url
                            
                            await asyncio.sleep(0.5)
                except Exception as e:
                    logger.error(f"    ❌ Ошибка API: {e}")
            
            await asyncio.sleep(1)
        
        logger.warning(f"    ❌ Страница района на Wikipedia не найдена")
        return None
    
    async def _extract_wikipedia_village_links(self, page_url: str, district: str) -> Dict[str, str]:
        logger.info(f"  🔍 Извлечение ссылок на НП из Wikipedia")
        
        html = await self._fetch_page(page_url)
        if not html:
            logger.warning(f"    ❌ Не удалось загрузить страницу: {page_url}")
            return {}
        
        soup = BeautifulSoup(html, 'html.parser')
        links = {}
        
        tables = soup.find_all('table', class_=['standard', 'wikitable', 'sortable', 'collapsible', 'collapsed'])
        logger.info(f"    Найдено таблиц: {len(tables)}")
        
        for table in tables:
            headers = [h.get_text().strip().lower() for h in table.find_all('th')]
            
            name_col_idx = None
            for i, h in enumerate(headers):
                if 'населённый пункт' in h or 'населенный пункт' in h or 'название' in h:
                    name_col_idx = i
                    logger.debug(f"      Найдена колонка '{h}' на позиции {i}")
                    break
            
            if name_col_idx is None:
                for row in table.find_all('tr'):
                    cells = row.find_all('td')
                    for i, cell in enumerate(cells):
                        if cell.find('a') and len(cell.get_text().strip()) > 2:
                            name_col_idx = i
                            logger.debug(f"      Определена колонка с названиями по первой ссылке: {i}")
                            break
                    if name_col_idx is not None:
                        break
            
            if name_col_idx is None:
                continue
            
            for row in table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) <= name_col_idx:
                    continue
                
                name_cell = cells[name_col_idx]
                link = name_cell.find('a')
                
                if link and link.get('href', '').startswith('/wiki/') and ':' not in link['href']:
                    name = link.get_text().strip()
                    name = re.sub(r'\[\d+\]', '', name).strip()
                    name = re.sub(r'^\d+\s*', '', name).strip()
                    
                    if name and self._is_valid_name(name, district):
                        full_url = f"{WIKIPEDIA_BASE_URL}{link['href']}"
                        links[name] = full_url
                        logger.debug(f"      🔗 Найдена ссылка из таблицы: {name}")
        
        if not links:
            logger.info(f"    Таблицы не дали результатов, ищем в списках...")
            for lst in soup.find_all(['ul', 'ol']):
                for link in lst.find_all('a', href=re.compile(r'^/wiki/')):
                    href = link.get('href', '')
                    if ':' in href or '#' in href:
                        continue
                    
                    name = link.get_text().strip()
                    name = re.sub(r'\[\d+\]', '', name).strip()
                    name = re.sub(r'^\d+\s*', '', name).strip()
                    
                    if name and self._is_valid_name(name, district):
                        full_url = f"{WIKIPEDIA_BASE_URL}{href}"
                        links[name] = full_url
                        logger.debug(f"      🔗 Найдена ссылка из списка: {name}")
        
        logger.info(f"    📊 Найдено {len(links)} ссылок на НП в Wikipedia")
        return links
    
    async def _parse_wikipedia_coordinates(self, html: str, village_name: str) -> Optional[Tuple[str, str]]:
        return await parse_wikipedia_coordinates(html, village_name)
    
    async def _get_wikipedia_coordinates(self, wiki_url: str, village_name: str, district: str) -> Optional[Dict]:
        try:
            logger.debug(f"      🔍 Загружаем Wikipedia страницу: {wiki_url}")
            html = await self._fetch_page(wiki_url)
            
            if not html:
                return None
            
            soup = BeautifulSoup(html, 'html.parser')
            no_article = soup.find('div', class_='noarticletext')
            if no_article:
                logger.debug(f"      ❌ Wikipedia: страница для {village_name} не найдена")
                return None
            
            coords = await self._parse_wikipedia_coordinates(html, village_name)
            
            if coords:
                lat, lon = coords
                if validate_coordinates(float(lat), float(lon)):
                    logger.info(f"      ✅ Wikipedia: найдены координаты для {village_name}: {lat}, {lon}")
                    return {
                        "name": village_name,
                        "type": 'деревня',
                        "lat": lat,
                        "lon": lon,
                        "district": district,
                        "has_coords": True
                    }
            
            return None
            
        except Exception as e:
            logger.debug(f"      ❌ Ошибка получения координат из Wikipedia для {village_name}: {e}")
            return None
    
    async def _get_article_info(self, article_id: str) -> Optional[Dict]:
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        html = await self._fetch_page(url)
        if not html:
            return None
        soup = BeautifulSoup(html, 'html.parser')
        title_elem = soup.find('h1')
        title = title_elem.get_text().strip() if title_elem else ""
        return {'id': article_id, 'title': title}
    
    # ========== ПАРАЛЛЕЛЬНЫЙ ПОИСК КООРДИНАТ НА WIKIPEDIA ==========
    
    async def _fetch_wikipedia_coordinates_batch(self, villages: List[Dict], district: str, district_bounds: Dict[str, float]) -> Dict[str, Dict]:
        """
        Параллельный поиск координат на Wikipedia для списка деревень
        """
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)
        
        async def fetch_one(village):
            async with semaphore:
                name = village['name']
                await asyncio.sleep(random.uniform(0.5, 1.5))
                
                logger.info(f"      🔍 Поиск координат для {name} на Wikipedia...")
                
                # ВАРИАНТ 1: Прямой URL
                encoded_name = quote_plus(name)
                direct_url = f"{WIKIPEDIA_BASE_URL}/wiki/{encoded_name}"
                
                html = await self._fetch_page(direct_url)
                if html:
                    soup = BeautifulSoup(html, 'html.parser')
                    no_article = soup.find('div', class_='noarticletext')
                    if not no_article:
                        title = soup.find('h1')
                        if title and 'список' not in title.get_text().lower():
                            coords = await self._parse_wikipedia_coordinates(html, name)
                            if coords:
                                lat, lon = coords
                                lat_f = float(lat)
                                lon_f = float(lon)
                                if validate_coordinates(lat_f, lon_f):
                                    logger.info(f"      ✅ Wikipedia: найдены координаты для {name}: {lat}, {lon}")
                                    return name, {
                                        "name": name,
                                        "type": village['type'],
                                        "lat": lat,
                                        "lon": lon,
                                        "district": district,
                                        "has_coords": True
                                    }
                                else:
                                    logger.debug(f"      ⚠️ Координаты {name} вне Тверской области: {lat}, {lon}")
                
                # ВАРИАНТ 2: Поиск через API
                try:
                    search_url = f"{WIKIPEDIA_SEARCH_URL}?action=query&list=search&srsearch={quote_plus(name)}&format=json&utf8=1"
                    search_html = await self._fetch_page(search_url)
                    
                    if search_html:
                        data = json.loads(search_html)
                        if 'query' in data and 'search' in data['query']:
                            for result in data['query']['search'][:5]:
                                title = result['title']
                                
                                # Проверяем, что это не список
                                if 'список' in title.lower() and name.lower() not in title.lower():
                                    continue
                                
                                # Проверяем, что это не район и не город
                                if 'район' in title.lower() or 'город' in title.lower():
                                    continue
                                
                                # Проверяем, что название похоже на искомое
                                if name.lower() not in title.lower() and title.lower() not in name.lower():
                                    if len(name) > 5 and name.lower() not in title.lower():
                                        continue
                                
                                page_url = f"{WIKIPEDIA_BASE_URL}/wiki/{quote_plus(title)}"
                                logger.debug(f"      🔍 Проверяем страницу: {title}")
                                
                                page_html = await self._fetch_page(page_url)
                                if page_html:
                                    coords = await self._parse_wikipedia_coordinates(page_html, name)
                                    if coords:
                                        lat, lon = coords
                                        lat_f = float(lat)
                                        lon_f = float(lon)
                                        if validate_coordinates(lat_f, lon_f):
                                            logger.info(f"      ✅ Wikipedia: найдены координаты для {name} через поиск: {lat}, {lon} (страница: {title})")
                                            return name, {
                                                "name": name,
                                                "type": village['type'],
                                                "lat": lat,
                                                "lon": lon,
                                                "district": district,
                                                "has_coords": True
                                            }
                                
                                await asyncio.sleep(0.3)
                except Exception as e:
                    logger.debug(f"      ❌ Ошибка поиска Wikipedia для {name}: {e}")
                
                logger.debug(f"      ❌ Wikipedia: координаты не найдены для {name}")
                return None, None
        
        tasks = [fetch_one(v) for v in villages]
        results = await asyncio.gather(*tasks)
        
        found = {}
        for name, data in results:
            if data:
                found[name] = data
        
        return found
    
    # ========== ОСНОВНОЙ МЕТОД ==========
    
    async def fetch_district_data(self, district: str) -> List[Dict]:
        self.clear_cache()
        self.start_time = time.time()
        logger.info(f"🌐 Загрузка данных для района: {district}")
        
        all_villages = []
        processed_master_lists = set()
        seen_villages: Dict[str, Dict] = {}
        
        # Шаг 1: Находим страницу района на dic.academic.ru
        district_info = await self._find_district_page(district)
        
        if not district_info:
            logger.warning(f"  ⚠️ Страница района на dic.academic.ru не найдена")
            return []
        
        district_bounds = await self._get_district_bounds(district, district_info.get('html'))
        logger.info(f"  📍 Границы района {district}: {district_bounds}")
        
        # Шаг 2: Получаем список сельских поселений
        district_html = await self._fetch_page(district_info['url'])
        settlements = []
        
        if district_html:
            settlements = await self._extract_settlements_from_page(district_html, district)
        
        if settlements:
            logger.info(f"  🔍 Найдено {len(settlements)} сельских поселений")
            logger.info(f"  📋 Список СП: {', '.join(settlements[:10])}")
        else:
            logger.warning(f"  ⚠️ Сельские поселения не найдены")
        
        # Шаг 2.5: Ищем общий список бывших НП для района
        district_former_id = await self._find_district_former_np_page(district, district_html)
        if district_former_id and district_former_id not in self.processed_article_ids:
            self.processed_article_ids.add(district_former_id)
            district_former_data = await self._parse_former_np_page(district_former_id, district, "всего района")
            for village in district_former_data:
                key = f"{village['name']}_{village['district']}"
                if key not in seen_villages:
                    seen_villages[key] = village
                    self.collection_stats['from_former'] += 1
                else:
                    existing = seen_villages[key]
                    if not existing.get('has_coords') and village.get('has_coords'):
                        seen_villages[key] = village
            logger.info(f"  ✅ Из общего списка бывших НП района добавлено {len(district_former_data)} записей")
        
        # Шаг 3: Ищем общие списки на странице района
        if district_html:
            master_list_ids = await self._find_master_list_links(district_html, district)
            for list_id in master_list_ids:
                if list_id not in processed_master_lists and list_id not in self.processed_article_ids:
                    processed_master_lists.add(list_id)
                    self.processed_article_ids.add(list_id)
                    logger.info(f"  🔍 Обрабатываем общий список ID {list_id}")
                    
                    list_data = await self._parse_master_list_page(list_id, district)
                    for village in list_data:
                        key = f"{village['name']}_{village['district']}"
                        if key not in seen_villages:
                            seen_villages[key] = village
                            self.collection_stats['from_master_lists'] += 1
                        else:
                            existing = seen_villages[key]
                            if not existing.get('has_coords') and village.get('has_coords'):
                                seen_villages[key] = village
                    
                    logger.info(f"    Из общего списка добавлено {len(list_data)} записей")
        
        # Шаг 4: Для каждого СП ищем страницы на dic.academic.ru
        for settlement in settlements:
            try:
                elapsed = time.time() - self.start_time
                if elapsed > 1500:
                    logger.warning(f"  ⏱️ Время выполнения {elapsed:.1f}с, прерываем обработку СП")
                    break
                
                await asyncio.sleep(1.5)
                
                logger.info(f"\n  📍 Обработка СП: {settlement}")
                
                # Страница с бывшими НП (для конкретного СП)
                former_np_id = await self._find_former_np_page(settlement, district)
                
                if former_np_id and former_np_id not in self.processed_article_ids:
                    self.processed_article_ids.add(former_np_id)
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
                        else:
                            existing = seen_villages[key]
                            if not existing.get('has_coords') and village.get('has_coords'):
                                seen_villages[key] = village
                                former_new += 1
                                logger.info(f"      🔄 Обновлены координаты для {village['name']} из бывших НП")
                    
                    if former_new > 0:
                        logger.info(f"    ✅ СП {settlement}: добавлено {former_new} записей из списка бывших НП (из них с координатами: {former_with_coords})")
                else:
                    logger.info(f"    ⚠️ Страница бывших НП для СП {settlement} не найдена")
                
                # Основная страница СП
                main_page_id = await self._find_settlement_main_page(settlement, district)
                
                if main_page_id and main_page_id not in self.processed_article_ids:
                    self.processed_article_ids.add(main_page_id)
                    main_page_data = await self._parse_settlement_main_page(main_page_id, district, settlement)
                    
                    main_new = 0
                    for village in main_page_data:
                        key = f"{village['name']}_{village['district']}"
                        
                        if village.get('article_id'):
                            self.village_links[village['name']] = village['article_id']
                        
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
        
        # Статистика сбора НП
        self.collection_stats['total_unique'] = len(all_villages)
        logger.info(f"📊 СТАТИСТИКА СБОРА НП:")
        logger.info(f"  • Из общих списков: {self.collection_stats['from_master_lists']}")
        logger.info(f"  • Из бывших НП: {self.collection_stats['from_former']}")
        logger.info(f"  • Из СП: {self.collection_stats['from_settlements']}")
        logger.info(f"  • Всего уникальных: {self.collection_stats['total_unique']}")
        
        # ========== ПОИСК КООРДИНАТ ==========
        if all_villages:
            logger.info(f"  🔍 ПОИСК КООРДИНАТ...")
            
            villages_with_coords = [v for v in all_villages if v.get('has_coords')]
            villages_without_coords = [v for v in all_villages if not v.get('has_coords')]
            
            total_without = len(villages_without_coords)
            self.coords_stats['total_without'] = total_without
            
            logger.info(f"  📊 Статистика перед поиском координат:")
            logger.info(f"    • Всего НП: {len(all_villages)}")
            logger.info(f"    • Уже с координатами: {len(villages_with_coords)}")
            logger.info(f"    • Без координат: {total_without}")
            logger.info(f"    • Сохраненных ссылок на dic.academic.ru: {len(self.village_links)}")
            logger.info(f"    • Координат из бывших НП: {self.coords_stats['from_former']}")
            
            # ========== 1. СНАЧАЛА ИЩЕМ КООРДИНАТЫ НА DIC.ACADEMIC.RU ==========
            with_links = [v for v in villages_without_coords if v['name'] in self.village_links]
            
            if with_links:
                logger.info(f"  📊 Поиск координат на dic.academic.ru по {len(with_links)} ссылкам из СП")
                
                semaphore_dic = asyncio.Semaphore(self.max_concurrent_dic)
                
                async def fetch_dic(village):
                    async with semaphore_dic:
                        name = village['name']
                        if name in self.village_links:
                            article_id = self.village_links[name]
                            logger.info(f"    📍 {name}: поиск на dic.academic.ru (ID {article_id})")
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
                                logger.info(f"      ✅ Найдены координаты на dic.academic.ru: {name} ({v['lat']}, {v['lon']})")
                                break
                logger.info(f"  ✅ Найдено координат на dic.academic.ru по ссылкам из СП: {dic_found}")
            
            # ========== 2. ЗАТЕМ ИЩЕМ НА WIKIPEDIA ДЛЯ ОСТАВШИХСЯ ==========
            villages_without_coords = [v for v in all_villages if not v.get('has_coords')]
            if villages_without_coords:
                logger.info(f"  📊 Осталось без координат после dic.academic.ru: {len(villages_without_coords)}")
                logger.info(f"  🌐 Поиск координат на Wikipedia...")
                
                wiki_coords = await self._fetch_wikipedia_coordinates_batch(villages_without_coords, district, district_bounds)
                logger.info(f"  ✅ Найдено координат на Wikipedia: {len(wiki_coords)}")
                
                for name, data in wiki_coords.items():
                    for v in all_villages:
                        if v['name'] == name and not v.get('has_coords'):
                            v['lat'] = data['lat']
                            v['lon'] = data['lon']
                            v['has_coords'] = True
                            self.coords_stats['from_wikipedia'] += 1
                            logger.info(f"      ✅ Найдены координаты на Wikipedia: {name} ({v['lat']}, {v['lon']})")
                            break
                
                for name, data in wiki_coords.items():
                    if not any(v['name'] == name for v in all_villages):
                        all_villages.append({
                            "name": name,
                            "type": "деревня",
                            "lat": data['lat'],
                            "lon": data['lon'],
                            "district": district,
                            "has_coords": True
                        })
                        logger.info(f"  ➕ Добавлен новый НП из Wikipedia: {name}")
            
            # ========== 3. ФИНАЛЬНАЯ СТАТИСТИКА ==========
            final_with_coords = sum(1 for v in all_villages if v.get('has_coords'))
            remaining = [v for v in all_villages if not v.get('has_coords')]
            
            logger.info(f"    📊 ИТОГО ПО КООРДИНАТАМ:")
            logger.info(f"      • Было без координат: {total_without}")
            logger.info(f"      • Из бывших НП (dic.academic.ru): {self.coords_stats['from_former']}")
            logger.info(f"      • По ссылкам из СП (dic.academic.ru): {self.coords_stats['from_links']}")
            logger.info(f"      • Из Wikipedia: {self.coords_stats['from_wikipedia']}")
            logger.info(f"      • Всего найдено: {self.coords_stats['from_former'] + self.coords_stats['from_links'] + self.coords_stats['from_wikipedia']}")
            logger.info(f"      • Осталось без координат: {len(remaining)}")
            
            if remaining:
                logger.info(f"  📊 Осталось без координат: {len(remaining)}")
                if len(remaining) <= 30:
                    logger.info(f"     Список: {', '.join([v['name'] for v in remaining])}")
                else:
                    logger.info(f"     Первые 20: {', '.join([v['name'] for v in remaining[:20]])}")
        
        final_with_coords = sum(1 for v in all_villages if v.get('has_coords'))
        all_villages.sort(key=lambda x: x['name'])
        
        for v in all_villages:
            if 'has_coords' in v:
                del v['has_coords']
        
        total_time = time.time() - self.start_time
        logger.info(f"  ✅ Всего уникальных записей: {len(all_villages)}")
        logger.info(f"  ✅ С координатами: {final_with_coords}")
        logger.info(f"  ✅ Без координат: {len(all_villages) - final_with_coords}")
        logger.info(f"  ✅ Сортировка: по алфавиту")
        logger.info(f"  ⏱️ Общее время обработки: {total_time:.1f} секунд")
        
        return all_villages


# Экспортируемые объекты
__all__ = ['APISourceManager', 'AVAILABLE_DISTRICTS']