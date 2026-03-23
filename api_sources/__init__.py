# __init__.py
# Основной класс APISourceManager

import aiohttp
import asyncio
import logging
import time
import random
import re
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
from .dic_parser import DicParser
from .wikipedia_parser import WikipediaParser
from .district_parser import DistrictPageParser
from .former_np_parser import FormerNPParser

logger = logging.getLogger(__name__)

# Общие границы Тверской области (расширенные)
TVER_BOUNDS = {
    'min_lat': 55.0,
    'max_lat': 58.5,
    'min_lon': 30.0,
    'max_lon': 38.5
}

# Расширенные границы для учета пограничных НП (буфер 0.3 градуса)
TVER_BOUNDS_EXTENDED = {
    'min_lat': 54.7,
    'max_lat': 58.8,
    'min_lon': 29.7,
    'max_lon': 38.8
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
        self.processed_former_np_ids: Set[str] = set()
        
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
            'from_district_page': 0,
            'total_without': 0,
            'found': 0,
            'remaining': 0
        }
        
        # Список НП без координат для логирования
        self.villages_without_coords_list: List[str] = []
        
        # Статистика сбора НП
        self.collection_stats = {
            'from_master_lists': 0,
            'from_former': 0,
            'from_settlements': 0,
            'from_district_page': 0,
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
        
        # Инициализация парсеров
        self.dic_parser = DicParser(self)
        self.wikipedia_parser = WikipediaParser(self)
        self.district_parser = DistrictPageParser(self)
        self.former_np_parser = FormerNPParser(self)
    
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
        self.wikipedia_links.clear()
        self.wikipedia_coords_cache.clear()
        self.district_bounds_cache.clear()
        self.coords_stats = {
            'from_former': 0,
            'from_links': 0,
            'from_district_page': 0,
            'total_without': 0,
            'found': 0,
            'remaining': 0
        }
        self.villages_without_coords_list = []
        self.collection_stats = {
            'from_master_lists': 0,
            'from_former': 0,
            'from_settlements': 0,
            'from_district_page': 0,
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
            
            async with session.get(url, headers=self.default_headers, timeout=60) as response:
                if response.status == 200:
                    html = await response.text()
                    return html
                elif response.status == 429:
                    base_wait = (2 ** retry_count) * random.uniform(3.0, 6.0)
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
        return await self.dic_parser.search_with_pagination(query, max_pages, unlimited)
    
    def _parse_search_page(self, html: str, page_num: int) -> List[Dict]:
        return self.dic_parser.parse_search_page(html, page_num)
    
    def _check_next_page(self, html: str) -> bool:
        return self.dic_parser.check_next_page(html)
    
    def _normalize_text(self, text: str) -> str:
        return self.dic_parser.normalize_text(text)
    
    def _generate_district_variants(self, district_name: str) -> List[str]:
        return self.dic_parser.generate_district_variants(district_name)
    
    def _check_district_in_text(self, text: str, district: str) -> bool:
        return self.dic_parser.check_district_in_text(text, district)
    
    def _is_valid_name(self, name: str, district: str = "") -> bool:
        return is_valid_name(name, district)
    
    def _is_valid_settlement_name(self, name: str, district: str = "") -> bool:
        return is_valid_settlement_name(name, district)
    
    async def _get_district_bounds(self, district: str, district_html: str = None) -> Dict[str, float]:
        if district in self.district_bounds_cache:
            return self.district_bounds_cache[district]
        
        bounds = {
            'min_lat': TVER_BOUNDS_EXTENDED['min_lat'],
            'max_lat': TVER_BOUNDS_EXTENDED['max_lat'],
            'min_lon': TVER_BOUNDS_EXTENDED['min_lon'],
            'max_lon': TVER_BOUNDS_EXTENDED['max_lon']
        }
        
        self.district_bounds_cache[district] = bounds
        return bounds
    
    def _check_coordinate_in_district(self, lat: float, lon: float, district_bounds: Dict[str, float]) -> bool:
        return (district_bounds['min_lat'] <= lat <= district_bounds['max_lat'] and
                district_bounds['min_lon'] <= lon <= district_bounds['max_lon'])
    
    # ========== МЕТОДЫ ДЛЯ РАБОТЫ С DIC.ACADEMIC.RU ==========
    
    async def _find_district_page(self, district: str) -> Optional[Dict]:
        return await self.dic_parser.find_district_page(district)
    
    def _score_district_relevance(self, result: Dict, district: str) -> int:
        return self.dic_parser.score_district_relevance(result, district)
    
    def _verify_district_page(self, html: str, district: str) -> bool:
        return self.dic_parser.verify_district_page(html, district)
    
    async def _extract_settlements_from_page(self, html: str, district: str) -> List[str]:
        return await self.dic_parser.extract_settlements_from_page(html, district)
    
    async def _find_settlement_main_page(self, settlement: str, district: str) -> Optional[str]:
        return await self.dic_parser.find_settlement_main_page(settlement, district)
    
    def _score_settlement_relevance(self, result: Dict, settlement: str, district: str) -> int:
        return self.dic_parser.score_settlement_relevance(result, settlement, district)
    
    async def _parse_settlement_main_page(self, article_id: str, district: str, settlement: str) -> List[Dict]:
        return await self.dic_parser.parse_settlement_main_page(article_id, district, settlement)
    
    def _parse_settlements_section(self, html: str, article_id: str, district: str, settlement: str) -> List[Dict]:
        return self.dic_parser.parse_settlements_section(html, article_id, district, settlement)
    
    def _parse_settlements_alternative(self, html: str, article_id: str, district: str, settlement: str) -> List[Dict]:
        return self.dic_parser.parse_settlements_alternative(html, article_id, district, settlement)
    
    async def _find_master_list_links(self, html: str, district: str) -> List[str]:
        return await self.dic_parser.find_master_list_links(html, district)
    
    async def _parse_master_list_page(self, article_id: str, district: str) -> List[Dict]:
        return await self.dic_parser.parse_master_list_page(article_id, district)
    
    def _parse_master_list_html(self, html: str, article_id: str, district: str) -> List[Dict]:
        return self.dic_parser.parse_master_list_html(html, article_id, district)
    
    async def _parse_individual_village_page(self, article_id: str, district: str) -> Optional[Dict]:
        return await self.dic_parser.parse_individual_village_page(article_id, district)
    
    def _parse_individual_village_html(self, html: str, article_id: str, district: str) -> Optional[Dict]:
        return self.dic_parser.parse_individual_village_html(html, article_id, district)
    
    async def _get_article_info(self, article_id: str) -> Optional[Dict]:
        return await self.dic_parser.get_article_info(article_id)
    
    # ========== МЕТОДЫ ДЛЯ РАБОТЫ С WIKIPEDIA ==========
    
    async def _find_district_in_tver_region(self, district: str) -> Optional[str]:
        return await self.wikipedia_parser.find_district_in_tver_region(district)
    
    async def _find_wikipedia_district_page(self, district: str) -> Optional[str]:
        return await self.wikipedia_parser.find_wikipedia_district_page(district)
    
    async def _extract_wikipedia_village_links(self, page_url: str, district: str) -> Dict[str, str]:
        return await self.wikipedia_parser.extract_wikipedia_village_links(page_url, district)
    
    async def _parse_wikipedia_coordinates(self, html: str, village_name: str) -> Optional[Tuple[str, str]]:
        return await parse_wikipedia_coordinates(html, village_name)
    
    async def _get_wikipedia_coordinates(self, wiki_url: str, village_name: str, district: str) -> Optional[Dict]:
        return await self.wikipedia_parser.get_wikipedia_coordinates(wiki_url, village_name, district)
    
    # ========== ПОИСК НА СТРАНИЦЕ РАЙОНА (ШАГ 3) - ОСНОВНОЙ МЕТОД ДЛЯ КООРДИНАТ ==========
    
    async def _fetch_villages_from_district_page(self, district: str, existing_villages: Dict[str, Dict]) -> Dict[str, Dict]:
        return await self.district_parser.fetch_villages_from_district_page(district, existing_villages)
    
    # ========== ПОИСК БЫВШИХ НП ==========
    
    async def _find_district_former_np_page(self, district: str, district_html: str) -> Optional[str]:
        return await self.former_np_parser.find_district_former_np_page(district, district_html)
    
    async def _find_former_np_page(self, settlement: str, district: str) -> Optional[str]:
        return await self.former_np_parser.find_former_np_page(settlement, district)
    
    async def _parse_former_np_page(self, article_id: str, district: str, settlement: str) -> List[Dict]:
        return await self.former_np_parser.parse_former_np_page(article_id, district, settlement)
    
    def _parse_former_np_html(self, html: str, article_id: str, district: str, settlement: str) -> List[Dict]:
        return self.former_np_parser.parse_former_np_html(html, article_id, district, settlement)
    
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
        if district_former_id and district_former_id not in self.processed_former_np_ids:
            self.processed_former_np_ids.add(district_former_id)
            logger.info(f"  📌 Обрабатываем общий список бывших НП района (ID: {district_former_id})")
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
                
                # Если не нашли страницу для конкретного СП, используем общий список района
                if not former_np_id:
                    district_former_id = await self._find_district_former_np_page(district, district_html)
                    if district_former_id and district_former_id not in self.processed_former_np_ids:
                        former_np_id = district_former_id
                        logger.info(f"    📌 Используем общий список бывших НП района (ID: {district_former_id}) для СП {settlement}")
                
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
                        else:
                            existing = seen_villages[key]
                            if not existing.get('has_coords') and village.get('has_coords'):
                                seen_villages[key] = village
                                former_new += 1
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
                                logger.info(f"      ⏭️ Пропускаем ID {list_id} (это страница бывших НП текущего СП)")
                                continue
                            
                            if list_id in self.processed_former_np_ids:
                                logger.info(f"      ⏭️ Пропускаем ID {list_id} (уже обработана как страница бывших НП)")
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
            
            # ========== 2. ПРОПУСКАЕМ ПОИСК НА WIKIPEDIA ДЛЯ ВСЕХ НП БЕЗ КООРДИНАТ ==========
            # Вместо этого сразу переходим к ШАГУ 3 - поиску на странице района
            
            villages_without_coords = [v for v in all_villages if not v.get('has_coords')]
            if villages_without_coords:
                logger.info(f"  📊 Осталось без координат после dic.academic.ru: {len(villages_without_coords)}")
                logger.info(f"  🌐 ПРОПУСКАЕМ массовый поиск на Wikipedia, переходим к ШАГУ 3 (поиск на странице района)...")
                
                # ========== 3. ШАГ 3: ПОИСК НА СТРАНИЦЕ РАЙОНА (ДЛЯ ВСЕХ НП БЕЗ КООРДИНАТ) ==========
                current_villages_dict = {v['name']: v for v in all_villages}
                
                district_page_coords = await self._fetch_villages_from_district_page(district, current_villages_dict)
                
                if district_page_coords:
                    logger.info(f"  🌐 Найдено {len(district_page_coords)} координат со страницы района")
                    
                    for name, data in district_page_coords.items():
                        found_in_existing = False
                        for v in all_villages:
                            if v['name'] == name and not v.get('has_coords'):
                                v['lat'] = data['lat']
                                v['lon'] = data['lon']
                                v['has_coords'] = True
                                self.coords_stats['from_district_page'] += 1
                                logger.info(f"      ✅ Обновлены координаты для {name} со страницы района: ({v['lat']}, {v['lon']})")
                                found_in_existing = True
                                break
                        
                        if not found_in_existing:
                            if self._is_valid_name(name, district):
                                all_villages.append({
                                    "name": name,
                                    "type": data.get('type', 'деревня'),
                                    "lat": data['lat'],
                                    "lon": data['lon'],
                                    "district": district,
                                    "has_coords": True
                                })
                                self.coords_stats['from_district_page'] += 1
                                self.collection_stats['from_district_page'] += 1
                                logger.info(f"  ➕ Добавлен новый НП со страницы района: {name} ({data['lat']}, {data['lon']})")
            
            # ========== 4. ФИНАЛЬНАЯ СТАТИСТИКА ==========
            final_with_coords = sum(1 for v in all_villages if v.get('has_coords'))
            remaining = [v for v in all_villages if not v.get('has_coords')]
            
            logger.info(f"    📊 ИТОГО ПО КООРДИНАТАМ:")
            logger.info(f"      • Было без координат: {total_without}")
            logger.info(f"      • Из бывших НП (dic.academic.ru): {self.coords_stats['from_former']}")
            logger.info(f"      • По ссылкам из СП (dic.academic.ru): {self.coords_stats['from_links']}")
            logger.info(f"      • Со страницы района (Wikipedia): {self.coords_stats['from_district_page']}")
            logger.info(f"      • Всего найдено: {self.coords_stats['from_former'] + self.coords_stats['from_links'] + self.coords_stats['from_district_page']}")
            logger.info(f"      • Осталось без координат: {len(remaining)}")
            
            if remaining:
                logger.info(f"  📊 Осталось без координат: {len(remaining)}")
                if len(remaining) <= 50:
                    logger.info(f"     📝 Список НП без координат: {', '.join([v['name'] for v in remaining])}")
                else:
                    logger.info(f"     📝 Первые 50 НП без координат: {', '.join([v['name'] for v in remaining[:50]])}")
        
        final_with_coords = sum(1 for v in all_villages if v.get('has_coords'))
        all_villages.sort(key=lambda x: x['name'])
        
        for v in all_villages:
            if 'has_coords' in v:
                del v['has_coords']
            if 'source' in v:
                del v['source']
            if 'article_id' in v:
                del v['article_id']
        
        total_time = time.time() - self.start_time
        logger.info(f"  ✅ Всего уникальных записей: {len(all_villages)}")
        logger.info(f"  ✅ С координатами: {final_with_coords}")
        logger.info(f"  ✅ Без координат: {len(all_villages) - final_with_coords}")
        logger.info(f"  ✅ Сортировка: по алфавиту")
        logger.info(f"  ⏱️ Общее время обработки: {total_time:.1f} секунд")
        
        return all_villages


# Экспортируемые объекты
__all__ = ['APISourceManager', 'AVAILABLE_DISTRICTS']