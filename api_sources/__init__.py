# __init__.py
# Основной класс APISourceManager

import aiohttp
import asyncio
import logging
import time
import random
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from urllib.parse import quote
from concurrent.futures import ThreadPoolExecutor

from .config import (
    AVAILABLE_DISTRICTS, DIC_ACADEMIC_SEARCH_URL, DIC_ACADEMIC_ARTICLE_URL,
    LIST_KEYWORDS
)
from .dic_parser import DicParser
from .wikipedia_parser import WikipediaParser
from .coordinates import parse_wikipedia_coordinates
from .utils import clean_village_name, is_valid_name, expand_type

logger = logging.getLogger(__name__)


class APISourceManager:
    """
    Универсальный менеджер для загрузки данных из dic.academic.ru и Wikipedia
    """
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.thread_pool = ThreadPoolExecutor(max_workers=3)
        
        self.request_count = 0
        self.last_request_time = 0
        self.min_request_interval = 1.5
        
        self.page_cache = {}
        self.cache_ttl = 3600
        self.max_retries = 5
        
        self.start_time = 0
        self.processed_article_ids = set()
        
        # Словари для ссылок
        self.village_links = {}      # dic.academic.ru
        self.wikipedia_links = {}    # Wikipedia
        
        # Кэш координат
        self.wikipedia_coords_cache = {}
        
        # Статистика
        self.coords_stats = {
            'from_former': 0, 'from_links': 0, 'from_wikipedia': 0,
            'from_uyezd': 0, 'total_without': 0, 'remaining': 0
        }
        self.collection_stats = {
            'from_master_lists': 0, 'from_former': 0,
            'from_settlements': 0, 'from_uyezd': 0, 'total_unique': 0
        }
        
        self.default_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
            'Accept-Language': 'ru-RU,ru;q=0.8',
            'Connection': 'keep-alive',
        }
        
        # Инициализация парсеров (будет завершена после создания сессии)
        self.dic_parser = None
        self.wiki_parser = None
    
    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
            # Инициализируем парсеры с сессией
            self.dic_parser = DicParser(
                self.session, self.thread_pool, 
                self._search_with_pagination, self._fetch_page
            )
            self.wiki_parser = WikipediaParser(
                self.session, self.thread_pool, self._fetch_page
            )
        return self.session
    
    async def close_session(self):
        if self.session and not self.session.closed:
            await self.session.close()
        self.thread_pool.shutdown(wait=False)
    
    def clear_cache(self):
        """Очищает все кэши перед новым поиском"""
        self.page_cache.clear()
        self.processed_article_ids.clear()
        self.village_links.clear()
        self.wikipedia_links.clear()
        self.wikipedia_coords_cache.clear()
        self.coords_stats = {'from_former': 0, 'from_links': 0, 'from_wikipedia': 0,
                            'from_uyezd': 0, 'total_without': 0, 'remaining': 0}
        self.collection_stats = {'from_master_lists': 0, 'from_former': 0,
                                'from_settlements': 0, 'from_uyezd': 0, 'total_unique': 0}
        
        if self.dic_parser:
            self.dic_parser.district_cache.clear()
            self.dic_parser.former_np_pages_cache.clear()
            self.dic_parser.settlement_pages_cache.clear()
            self.dic_parser.processed_article_ids.clear()
            self.dic_parser.village_links.clear()
            self.dic_parser.coords_stats = {'from_former': 0, 'from_links': 0}
            self.dic_parser.collection_stats = {'from_master_lists': 0, 'from_former': 0, 'from_settlements': 0}
        
        logger.info("🧹 Кэш очищен для нового поиска")
    
    async def _rate_limit(self):
        current_time = time.time()
        interval = self.min_request_interval * random.uniform(0.8, 1.2)
        if current_time - self.last_request_time < interval:
            await asyncio.sleep(interval - (current_time - self.last_request_time))
        self.last_request_time = time.time()
        self.request_count += 1
        if self.request_count % 50 == 0:
            logger.info(f"    Выполнено {self.request_count} запросов за {time.time() - self.start_time:.1f}с")
    
    async def _fetch_page_with_retry(self, url: str, retry: int = 0) -> Optional[str]:
        if retry >= self.max_retries:
            return None
        try:
            session = await self._get_session()
            await self._rate_limit()
            async with session.get(url, headers=self.default_headers, timeout=120) as resp:
                if resp.status == 200:
                    return await resp.text()
                elif resp.status == 429:
                    await asyncio.sleep(2 ** retry * random.uniform(0.5, 1.5))
                    return await self._fetch_page_with_retry(url, retry + 1)
                elif resp.status in [500, 502, 503, 504]:
                    await asyncio.sleep(2 ** retry)
                    return await self._fetch_page_with_retry(url, retry + 1)
        except asyncio.TimeoutError:
            await asyncio.sleep(2 ** retry)
            return await self._fetch_page_with_retry(url, retry + 1)
        except Exception:
            return None
        return None
    
    async def _fetch_page(self, url: str) -> Optional[str]:
        current_time = time.time()
        if url in self.page_cache:
            html, ts = self.page_cache[url]
            if current_time - ts < self.cache_ttl:
                return html
        html = await self._fetch_page_with_retry(url)
        if html:
            self.page_cache[url] = (html, current_time)
        return html
    
    async def _search_with_pagination(self, query: str, max_pages: int = 10) -> List[Dict]:
        """Поиск на dic.academic.ru с пагинацией"""
        all_results = []
        page = 1
        while page <= max_pages:
            if page > 1:
                await asyncio.sleep(2.0)
            url = f"{DIC_ACADEMIC_SEARCH_URL}?SWord={quote(query)}"
            if page > 1:
                url += f"&page={page}"
            html = await self._fetch_page(url)
            if not html:
                break
            loop = asyncio.get_event_loop()
            results = await loop.run_in_executor(self.thread_pool, self._parse_search_page, html, page)
            if not results:
                break
            all_results.extend(results)
            has_next = await loop.run_in_executor(self.thread_pool, self._has_next_page, html)
            if not has_next:
                break
            page += 1
        return all_results
    
    def _parse_search_page(self, html: str, page_num: int) -> List[Dict]:
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
                    'id': pid, 'title': title, 'full_text': text,
                    'page': page_num, 'position': int(pos.group(1)) if pos else 0
                })
            return results
        except:
            return []
    
    def _has_next_page(self, html: str) -> bool:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            return soup.find('a', string=re.compile(r'далее|следующая|next', re.I)) is not None
        except:
            return False
    
    async def _parse_master_list_page(self, article_id: str, district: str) -> List[Dict]:
        """Парсит страницу со списком населенных пунктов"""
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        html = await self._fetch_page(url)
        if not html:
            return []
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.thread_pool, self._parse_master_list_html, html, district)
    
    def _parse_master_list_html(self, html: str, district: str) -> List[Dict]:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            for table in soup.find_all('table', class_=['standard', 'sortable', 'wikitable', 'simple', 'collapsible', 'collapsed']):
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
                    name = clean_village_name(cells[name_idx].get_text().strip())
                    if not name or not is_valid_name(name):
                        continue
                    vtype = 'деревня'
                    if type_idx is not None and type_idx < len(cells):
                        vtype = expand_type(cells[type_idx].get_text().strip())
                    results.append({
                        "name": name, "type": vtype,
                        "lat": "", "lon": "",
                        "district": district, "has_coords": False
                    })
            return results
        except:
            return []
    
    def _find_col(self, headers: List[str], names: List[str]) -> Optional[int]:
        for i, h in enumerate(headers):
            for n in names:
                if n in h:
                    return i
        return None
    
    async def _find_master_list_links(self, html: str, district: str) -> List[str]:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            found = []
            for link in soup.find_all('a', href=re.compile(r'/dic\.nsf/ruwiki/\d+')):
                text = link.get_text().lower()
                if any(kw in text for kw in LIST_KEYWORDS):
                    match = re.search(r'/dic\.nsf/ruwiki/(\d+)', link.get('href', ''))
                    if match:
                        found.append(match.group(1))
            return list(set(found))
        except:
            return []
    
    async def _get_article_info(self, article_id: str) -> Optional[Dict]:
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        html = await self._fetch_page(url)
        if not html:
            return None
        soup = BeautifulSoup(html, 'html.parser')
        title = soup.find('h1')
        return {'id': article_id, 'title': title.get_text().strip() if title else ""}
    
    async def _get_wikipedia_coordinates(self, url: str, name: str, district: str) -> Optional[Dict]:
        """Загружает страницу НП на Wikipedia и извлекает координаты"""
        html = await self._fetch_page(url)
        if not html:
            return None
        soup = BeautifulSoup(html, 'html.parser')
        if soup.find('div', class_='noarticletext'):
            return None
        coords = await parse_wikipedia_coordinates(html, name)
        if coords:
            lat, lon = coords
            logger.info(f"      ✅ Wikipedia: найдены координаты для {name}: {lat}, {lon}")
            return {"name": name, "type": 'деревня', "lat": lat, "lon": lon, "district": district, "has_coords": True}
        return None
    
    async def fetch_district_data(self, district: str) -> List[Dict]:
        """Основной метод: загружает данные для конкретного района"""
        self.clear_cache()
        self.start_time = time.time()
        logger.info(f"🌐 Загрузка данных для района: {district}")
        
        # Инициализируем сессию
        await self._get_session()
        
        all_villages = []
        seen = set()
        processed_master_lists = set()
        
        # Шаг 1: Находим страницу района на dic.academic.ru
        district_info = await self.dic_parser.find_district_page(district)
        if not district_info:
            logger.warning(f"  ⚠️ Страница района на dic.academic.ru не найдена")
            return []
        
        district_html = await self._fetch_page(district_info['url'])
        
        # Шаг 2: Получаем список сельских поселений
        settlements = await self.dic_parser.extract_settlements(district_html, district) if district_html else []
        if settlements:
            logger.info(f"  🔍 Найдено {len(settlements)} сельских поселений")
        
        # Шаг 3: Ищем общие списки
        if district_html:
            master_ids = await self._find_master_list_links(district_html, district)
            for mid in master_ids:
                if mid not in processed_master_lists and mid not in self.processed_article_ids:
                    processed_master_lists.add(mid)
                    self.processed_article_ids.add(mid)
                    logger.info(f"  🔍 Обрабатываем общий список ID {mid}")
                    data = await self._parse_master_list_page(mid, district)
                    for v in data:
                        key = f"{v['name']}_{v['district']}"
                        if key not in seen:
                            seen.add(key)
                            all_villages.append(v)
                            self.collection_stats['from_master_lists'] += 1
                    logger.info(f"    Из общего списка добавлено {len(data)} записей")
        
        # Шаг 4: Для каждого СП ищем страницы
        for settlement in settlements:
            try:
                if time.time() - self.start_time > 1500:
                    break
                
                await asyncio.sleep(1.5)
                
                # Бывшие НП
                former_id = await self.dic_parser.find_former_np_page(settlement, district)
                if former_id and former_id not in self.processed_article_ids:
                    self.processed_article_ids.add(former_id)
                    data = await self.dic_parser.parse_former_np_page(former_id, district, settlement)
                    for v in data:
                        key = f"{v['name']}_{v['district']}"
                        if key not in seen:
                            seen.add(key)
                            all_villages.append(v)
                            self.collection_stats['from_former'] += 1
                        else:
                            for i, ex in enumerate(all_villages):
                                if f"{ex['name']}_{ex['district']}" == key and not ex.get('has_coords') and v.get('has_coords'):
                                    all_villages[i] = v
                                    break
                
                # Основная страница СП
                main_id = await self.dic_parser.find_settlement_main_page(settlement, district)
                if main_id and main_id not in self.processed_article_ids:
                    self.processed_article_ids.add(main_id)
                    data = await self.dic_parser.parse_settlement_main_page(main_id, district, settlement)
                    for v in data:
                        key = f"{v['name']}_{v['district']}"
                        if v.get('article_id'):
                            self.village_links[v['name']] = v['article_id']
                        v.pop('article_id', None)
                        if key not in seen:
                            seen.add(key)
                            all_villages.append(v)
                            self.collection_stats['from_settlements'] += 1
                        else:
                            for i, ex in enumerate(all_villages):
                                if f"{ex['name']}_{ex['district']}" == key and not ex.get('has_coords') and v.get('has_coords'):
                                    all_villages[i] = v
                                    break
                
                # Дополнительные списки на странице бывших НП
                if former_id:
                    former_url = DIC_ACADEMIC_ARTICLE_URL.format(former_id)
                    former_html = await self._fetch_page(former_url)
                    if former_html:
                        add_ids = await self._find_master_list_links(former_html, district)
                        for aid in add_ids:
                            if aid not in processed_master_lists and aid not in self.processed_article_ids:
                                info = await self._get_article_info(aid)
                                if info and district.lower() in info.get('title', '').lower():
                                    processed_master_lists.add(aid)
                                    self.processed_article_ids.add(aid)
                                    data = await self._parse_master_list_page(aid, district)
                                    for v in data:
                                        key = f"{v['name']}_{v['district']}"
                                        if key not in seen:
                                            seen.add(key)
                                            all_villages.append(v)
                                            self.collection_stats['from_settlements'] += 1
                                    
            except Exception as e:
                logger.error(f"    ❌ Ошибка обработки СП {settlement}: {e}")
        
        all_villages = list(all_villages)
        self.collection_stats['total_unique'] = len(all_villages)
        logger.info(f"📊 СТАТИСТИКА СБОРА НП: {self.collection_stats}")
        
        # Шаг 5: Поиск координат
        without_coords = [v for v in all_villages if not v.get('has_coords')]
        self.coords_stats['total_without'] = len(without_coords)
        
        logger.info(f"  🔍 ПОИСК КООРДИНАТ...")
        logger.info(f"    Всего НП: {len(all_villages)}, с координатами: {len(all_villages) - len(without_coords)}, без: {len(without_coords)}")
        
        # Wikipedia страница района
        wiki_page = await self.wiki_parser.find_wikipedia_district_page(district)
        wiki_links = {}
        if wiki_page:
            wiki_links = await self.wiki_parser.extract_village_links_from_page(wiki_page)
            logger.info(f"  📊 Получено {len(wiki_links)} ссылок из Wikipedia")
        
        # Уезды
        uyezd_page = await self.wiki_parser.find_uyezd_page(district)
        uyezd_links = {}
        if uyezd_page:
            uyezd_links = await self.wiki_parser.extract_village_links_from_page(uyezd_page)
            logger.info(f"  📊 Получено {len(uyezd_links)} ссылок из уезда")
            self.collection_stats['from_uyezd'] = len(uyezd_links)
        
        # Объединяем ссылки
        all_wiki_links = {**wiki_links, **uyezd_links}
        
        # Поиск координат
        found = 0
        for i, v in enumerate(without_coords):
            if time.time() - self.start_time > 1500:
                break
            if i > 0 and i % 5 == 0:
                await asyncio.sleep(2.0)
            
            name = v['name']
            coords_data = None
            
            if name in self.wikipedia_coords_cache:
                lat, lon = self.wikipedia_coords_cache[name]
                coords_data = {"name": name, "type": v['type'], "lat": lat, "lon": lon, "district": district, "has_coords": True}
                self.coords_stats['from_wikipedia'] += 1
            
            elif name in all_wiki_links:
                logger.info(f"    🔍 [{i+1}/{len(without_coords)}] {name}: поиск в Wikipedia")
                data = await self._get_wikipedia_coordinates(all_wiki_links[name], name, district)
                if data:
                    coords_data = data
                    self.wikipedia_coords_cache[name] = (data['lat'], data['lon'])
                    self.coords_stats['from_wikipedia'] += 1
            
            if coords_data:
                for idx, ex in enumerate(all_villages):
                    if ex['name'] == name and not ex.get('has_coords'):
                        all_villages[idx]['lat'] = coords_data['lat']
                        all_villages[idx]['lon'] = coords_data['lon']
                        all_villages[idx]['has_coords'] = True
                        found += 1
                        logger.info(f"    ✅ ДОБАВЛЕНЫ КООРДИНАТЫ: {name} -> {coords_data['lat']}, {coords_data['lon']}")
                        break
            
            if (i + 1) % 50 == 0:
                logger.info(f"      Обработано {i+1}/{len(without_coords)} записей, найдено {found}")
            
            await asyncio.sleep(0.5)
        
        self.coords_stats['remaining'] = len(without_coords) - found
        logger.info(f"  ✅ Поиск координат завершен. Найдено: {found}")
        logger.info(f"  📊 ИТОГО ПО КООРДИНАТАМ: {self.coords_stats}")
        
        final_with_coords = sum(1 for v in all_villages if v.get('has_coords'))
        all_villages.sort(key=lambda x: x['name'])
        
        for v in all_villages:
            v.pop('has_coords', None)
        
        logger.info(f"  ✅ Всего: {len(all_villages)}, с координатами: {final_with_coords}, без: {len(all_villages) - final_with_coords}")
        logger.info(f"  ⏱️ Время: {time.time() - self.start_time:.1f}с")
        
        return all_villages