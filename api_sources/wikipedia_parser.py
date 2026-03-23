# wikipedia_parser.py
# Парсер Wikipedia

import asyncio
import logging
import random
import re
import json
from typing import List, Dict, Optional, Tuple, Set, Any
from bs4 import BeautifulSoup
from urllib.parse import quote, quote_plus

from .config import (
    WIKIPEDIA_BASE_URL, WIKIPEDIA_SEARCH_URL, TVER_OBLAST_URL,
    SETTLEMENTS_SECTION_KEYWORDS, TYPE_INDICATORS, TYPE_MAPPING,
    MIN_NAME_LENGTH, MAX_NAME_LENGTH, SERVICE_VILLAGE_WORDS,
    KNOWN_PERSONALITIES, DISTRICT_WIKI_NAMES
)
from .utils import is_valid_name, expand_type, find_column_index
from .coordinates import parse_dic_coordinates, parse_wikipedia_coordinates

logger = logging.getLogger(__name__)


class WikipediaParser:
    """Парсер для Wikipedia"""
    
    def __init__(self, manager):
        self.manager = manager
    
    async def find_district_in_tver_region(self, district: str) -> Optional[str]:
        logger.info(f"  🔍 Поиск страницы района на странице Тверской области: {district}")
        
        html = await self.manager._fetch_page(TVER_OBLAST_URL)
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
                if self.manager._check_district_in_text(cell_text, district):
                    link = cells[name_col_idx].find('a')
                    if link and link.get('href', '').startswith('/wiki/'):
                        page_url = f"{WIKIPEDIA_BASE_URL}{link['href']}"
                        logger.info(f"    ✅ Найдена страница района на странице Тверской области: {page_url}")
                        return page_url
            
            for link in table.find_all('a', href=re.compile(r'^/wiki/')):
                link_text = link.get_text().strip().lower()
                if self.manager._check_district_in_text(link_text, district):
                    page_url = f"{WIKIPEDIA_BASE_URL}{link['href']}"
                    logger.info(f"    ✅ Найдена страница района на странице Тверской области: {page_url}")
                    return page_url
        
        logger.warning(f"    ❌ Страница района не найдена на странице Тверской области")
        return None
    
    async def find_wikipedia_district_page(self, district: str) -> Optional[str]:
        logger.info(f"  🔍 Поиск страницы района на Wikipedia: {district}")
        
        tver_page_url = await self.find_district_in_tver_region(district)
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
            html = await self.manager._fetch_page(url)
            
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
            
            html = await self.manager._fetch_page(search_url)
            if html:
                try:
                    data = json.loads(html)
                    if 'query' in data and 'search' in data['query']:
                        for result in data['query']['search'][:15]:
                            title = result['title']
                            page_url = f"{WIKIPEDIA_BASE_URL}/wiki/{quote_plus(title)}"
                            
                            logger.debug(f"    🔎 Проверяем через API: {page_url}")
                            page_html = await self.manager._fetch_page(page_url)
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
    
    async def extract_wikipedia_village_links(self, page_url: str, district: str) -> Dict[str, str]:
        logger.info(f"  🔍 Извлечение ссылок на НП из Wikipedia")
        
        html = await self.manager._fetch_page(page_url)
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
                    
                    if name and is_valid_name(name, district):
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
                    
                    if name and is_valid_name(name, district):
                        full_url = f"{WIKIPEDIA_BASE_URL}{href}"
                        links[name] = full_url
                        logger.debug(f"      🔗 Найдена ссылка из списка: {name}")
        
        logger.info(f"    📊 Найдено {len(links)} ссылок на НП в Wikipedia")
        return links
    
    async def get_wikipedia_coordinates(self, wiki_url: str, village_name: str, district: str) -> Optional[Dict]:
        try:
            logger.debug(f"      🔍 Загружаем Wikipedia страницу: {wiki_url}")
            html = await self.manager._fetch_page(wiki_url)
            
            if not html:
                return None
            
            soup = BeautifulSoup(html, 'html.parser')
            no_article = soup.find('div', class_='noarticletext')
            if no_article:
                logger.debug(f"      ❌ Wikipedia: страница для {village_name} не найдена")
                return None
            
            coords = await parse_wikipedia_coordinates(html, village_name)
            
            if coords:
                lat, lon = coords
                if self.manager._check_coordinate_in_district(float(lat), float(lon), self.manager._get_district_bounds(district)):
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
    
    async def fetch_wikipedia_coordinates_batch(self, villages: List[Dict], district: str, district_bounds: Dict[str, float]) -> Dict[str, Dict]:
        """
        Параллельный поиск координат на Wikipedia для списка деревень
        """
        semaphore = asyncio.Semaphore(3)
        
        total = len(villages)
        processed = 0
        found_count = 0
        
        village_links_cache = {}
        found_villages = set()
        
        def wiki_encode(name: str) -> str:
            return quote(name.replace(' ', '_'), safe='')
        
        async def fetch_one(village):
            nonlocal processed, found_count
            async with semaphore:
                name = village['name']
                
                if name in found_villages:
                    return None, None
                
                await asyncio.sleep(random.uniform(0.8, 1.5))
                
                processed += 1
                if processed % 20 == 0 or processed == total:
                    logger.info(f"      📊 Прогресс Wikipedia: {processed}/{total} (найдено: {found_count})")
                
                logger.debug(f"      🔍 Поиск координат для {name} на Wikipedia...")
                
                # Прямой URL
                direct_url = f"{WIKIPEDIA_BASE_URL}/wiki/{wiki_encode(name)}"
                
                try:
                    html = await self.manager._fetch_page(direct_url)
                    if html:
                        soup = BeautifulSoup(html, 'html.parser')
                        no_article = soup.find('div', class_='noarticletext')
                        if not no_article:
                            title = soup.find('h1')
                            if title and 'список' not in title.get_text().lower():
                                coords = await parse_wikipedia_coordinates(html, name)
                                if coords:
                                    lat, lon = coords
                                    lat_f = float(lat)
                                    lon_f = float(lon)
                                    if self.manager._check_coordinate_in_district(lat_f, lon_f, district_bounds):
                                        found_count += 1
                                        found_villages.add(name)
                                        logger.info(f"      ✅ Wikipedia: найдены координаты для {name}: {lat}, {lon}")
                                        return name, {
                                            "name": name,
                                            "type": village.get('type', 'деревня'),
                                            "lat": lat,
                                            "lon": lon,
                                            "district": district,
                                            "has_coords": True
                                        }
                except Exception as e:
                    logger.debug(f"      ❌ Ошибка загрузки {name}: {e}")
                
                # Поиск через API
                try:
                    search_url = f"{WIKIPEDIA_SEARCH_URL}?action=query&list=search&srsearch={quote_plus(name)}&format=json&utf8=1"
                    search_html = await self.manager._fetch_page(search_url)
                    
                    if search_html:
                        data = json.loads(search_html)
                        if 'query' in data and 'search' in data['query']:
                            for result in data['query']['search'][:3]:
                                title = result['title']
                                
                                if 'список' in title.lower():
                                    continue
                                if 'район' in title.lower() or 'округ' in title.lower():
                                    continue
                                
                                if name.lower() not in title.lower() and title.lower() not in name.lower():
                                    if len(name) > 5 and name.lower() not in title.lower():
                                        continue
                                
                                page_url = f"{WIKIPEDIA_BASE_URL}/wiki/{wiki_encode(title)}"
                                logger.debug(f"      🔍 Проверяем страницу: {title}")
                                
                                try:
                                    page_html = await self.manager._fetch_page(page_url)
                                    if page_html:
                                        coords = await parse_wikipedia_coordinates(page_html, name)
                                        if coords:
                                            lat, lon = coords
                                            lat_f = float(lat)
                                            lon_f = float(lon)
                                            if self.manager._check_coordinate_in_district(lat_f, lon_f, district_bounds):
                                                found_count += 1
                                                found_villages.add(name)
                                                logger.info(f"      ✅ Wikipedia: найдены координаты для {name} через поиск: {lat}, {lon} (страница: {title})")
                                                return name, {
                                                    "name": name,
                                                    "type": village.get('type', 'деревня'),
                                                    "lat": lat,
                                                    "lon": lon,
                                                    "district": district,
                                                    "has_coords": True
                                                }
                                except Exception as e:
                                    logger.debug(f"      Ошибка при загрузке {title}: {e}")
                                
                                await asyncio.sleep(0.5)
                except Exception as e:
                    logger.debug(f"      ❌ Ошибка поиска Wikipedia для {name}: {e}")
                
                if name not in found_villages:
                    self.manager.villages_without_coords_list.append(name)
                
                logger.debug(f"      ❌ Wikipedia: координаты не найдены для {name}")
                return None, None
        
        tasks = [fetch_one(v) for v in villages]
        results = await asyncio.gather(*tasks)
        
        found = {}
        for name, data in results:
            if data:
                found[name] = data
        
        logger.info(f"  📊 ИТОГО по Wikipedia: обработано {processed}, найдено {found_count}")
        return found