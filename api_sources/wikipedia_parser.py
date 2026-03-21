# wikipedia_parser.py
# Парсер для Wikipedia (районы, уезды)

import asyncio
import logging
import re
import json
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from urllib.parse import quote_plus

from .config import (
    WIKIPEDIA_BASE_URL, WIKIPEDIA_SEARCH_URL, TVER_OBLAST_URL,
    DISTRICT_WIKI_NAMES, DISTRICT_UYEZDS
)
from .utils import is_valid_name, clean_village_name

logger = logging.getLogger(__name__)


class WikipediaParser:
    """Парсер для Wikipedia"""
    
    def __init__(self, session, thread_pool, fetch_func):
        self.session = session
        self.thread_pool = thread_pool
        self._fetch_page = fetch_func
        self.page_cache = {}
        self.cache_ttl = 3600
    
    async def find_district_in_tver_region(self, district: str) -> Optional[str]:
        """Находит страницу района в таблице на странице Тверской области"""
        logger.info(f"  🔍 Поиск страницы района на странице Тверской области: {district}")
        
        html = await self._fetch_page(TVER_OBLAST_URL)
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'html.parser')
        tables = soup.find_all('table', class_=['standard', 'sortable', 'wikitable', 'collapsible', 'collapsed'])
        
        district_lower = district.lower()
        
        for table in tables:
            # Ищем колонку с названиями
            headers = [h.get_text().strip().lower() for h in table.find_all('th')]
            name_col = None
            for i, h in enumerate(headers):
                if 'название' in h or 'населённый пункт' in h:
                    name_col = i
                    break
            
            if name_col is None:
                continue
            
            for row in table.find_all('tr'):
                cells = row.find_all('td')
                if len(cells) <= name_col:
                    continue
                
                cell_text = cells[name_col].get_text().strip().lower()
                if district_lower in cell_text:
                    link = cells[name_col].find('a')
                    if link and link.get('href', '').startswith('/wiki/'):
                        url = f"{WIKIPEDIA_BASE_URL}{link['href']}"
                        logger.info(f"    ✅ Найдена страница района: {url}")
                        return url
        
        return None
    
    async def find_wikipedia_district_page(self, district: str) -> Optional[str]:
        """Находит страницу района на Wikipedia"""
        logger.info(f"  🔍 Поиск страницы района на Wikipedia: {district}")
        
        # Вариант 1: страница Тверской области
        url = await self.find_district_in_tver_region(district)
        if url:
            return url
        
        # Вариант 2: прямые названия
        possible_names = DISTRICT_WIKI_NAMES.get(district, [f"{district} район", f"{district}"])
        
        for name in possible_names:
            encoded = quote_plus(name)
            url = f"{WIKIPEDIA_BASE_URL}/wiki/{encoded}"
            html = await self._fetch_page(url)
            
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                if not soup.find('div', class_='noarticletext'):
                    tables = soup.find_all('table', class_=['standard', 'wikitable', 'sortable', 'collapsible', 'collapsed'])
                    lists = soup.find_all(['ul', 'ol'])
                    
                    has_links = False
                    for table in tables:
                        headers = [h.get_text().strip().lower() for h in table.find_all('th')]
                        if any('населённый пункт' in h or 'населенный пункт' in h for h in headers):
                            has_links = True
                            break
                    
                    if not has_links:
                        for lst in lists:
                            links = lst.find_all('a', href=re.compile(r'^/wiki/'))
                            if len(links) > 10:
                                has_links = True
                                break
                    
                    if has_links:
                        logger.info(f"    ✅ Найдена страница района: {url}")
                        return url
            
            await asyncio.sleep(1)
        
        # Вариант 3: API поиск
        logger.info(f"    🔎 Пробуем поиск через API Wikipedia")
        region = "Тверская область"
        
        for query in [f"{district} муниципальный округ {region}", f"{district} район {region}"]:
            search_url = f"{WIKIPEDIA_SEARCH_URL}?action=query&list=search&srsearch={quote_plus(query)}&format=json"
            html = await self._fetch_page(search_url)
            if html:
                try:
                    data = json.loads(html)
                    for result in data.get('query', {}).get('search', [])[:10]:
                        title = result['title']
                        page_url = f"{WIKIPEDIA_BASE_URL}/wiki/{quote_plus(title)}"
                        page_html = await self._fetch_page(page_url)
                        if page_html and 'тверская область' in page_html.lower():
                            tables = BeautifulSoup(page_html, 'html.parser').find_all('table', class_=['standard', 'wikitable', 'sortable'])
                            if tables:
                                logger.info(f"    ✅ Найдена страница района через API: {page_url}")
                                return page_url
                        await asyncio.sleep(0.5)
                except:
                    pass
            await asyncio.sleep(1)
        
        return None
    
    async def find_uyezd_page(self, district: str) -> Optional[str]:
        """Находит страницу уезда на Wikipedia для района"""
        logger.info(f"  🔍 Поиск страницы уезда для района: {district}")
        
        district_page = await self.find_wikipedia_district_page(district)
        if not district_page:
            return None
        
        html = await self._fetch_page(district_page)
        if not html:
            return None
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Ищем ссылку на уезд в разделе истории
        for header in soup.find_all(['h2', 'h3']):
            if 'история' in header.get_text().lower():
                parent = header.find_parent()
                if parent:
                    for link in parent.find_all('a', href=re.compile(r'/wiki/.+уезд')):
                        link_text = link.get_text().lower()
                        if 'уезд' in link_text:
                            uyezd_candidates = DISTRICT_UYEZDS.get(district, [])
                            for candidate in uyezd_candidates:
                                if candidate.lower() in link_text or district.lower() in link_text:
                                    href = link.get('href')
                                    if href.startswith('/wiki/'):
                                        url = f"{WIKIPEDIA_BASE_URL}{href}"
                                        logger.info(f"    ✅ Найдена страница уезда: {url}")
                                        return url
        
        # Поиск по прямым названиям
        for uyezd_name in DISTRICT_UYEZDS.get(district, [f"{district} уезд"]):
            encoded = quote_plus(uyezd_name)
            url = f"{WIKIPEDIA_BASE_URL}/wiki/{encoded}"
            html = await self._fetch_page(url)
            if html and not BeautifulSoup(html, 'html.parser').find('div', class_='noarticletext'):
                logger.info(f"    ✅ Найдена страница уезда: {url}")
                return url
            await asyncio.sleep(1)
        
        return None
    
    async def extract_village_links_from_page(self, page_url: str) -> Dict[str, str]:
        """Извлекает из страницы ссылки на статьи населенных пунктов"""
        logger.info(f"  🔍 Извлечение ссылок на НП из Wikipedia")
        
        html = await self._fetch_page(page_url)
        if not html:
            return {}
        
        soup = BeautifulSoup(html, 'html.parser')
        links = {}
        
        # Поиск в таблицах
        for table in soup.find_all('table', class_=['standard', 'wikitable', 'sortable', 'collapsible', 'collapsed']):
            rows = table.find_all('tr')
            if len(rows) < 2:
                continue
            
            headers = [h.get_text().strip().lower() for h in rows[0].find_all('th')]
            name_col = None
            for i, h in enumerate(headers):
                if 'населённый пункт' in h or 'населенный пункт' in h or 'название' in h:
                    name_col = i
                    break
            
            if name_col is None:
                continue
            
            for row in rows[1:]:
                cells = row.find_all('td')
                if len(cells) <= name_col:
                    continue
                
                cell = cells[name_col]
                link = cell.find('a')
                if link and link.get('href', '').startswith('/wiki/') and ':' not in link['href']:
                    name = clean_village_name(link.get_text().strip())
                    if name and is_valid_name(name):
                        links[name] = f"{WIKIPEDIA_BASE_URL}{link['href']}"
        
        logger.info(f"    📊 Найдено {len(links)} ссылок на НП")
        return links