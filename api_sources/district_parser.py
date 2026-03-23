# district_parser.py
# Парсер страницы района на Wikipedia

import asyncio
import logging
import random
import re
import json
from typing import List, Dict, Optional, Tuple, Set, Any
from bs4 import BeautifulSoup
from urllib.parse import quote, quote_plus

from .config import (
    WIKIPEDIA_BASE_URL, WIKIPEDIA_SEARCH_URL,
    SETTLEMENTS_SECTION_KEYWORDS, TYPE_INDICATORS,
    MIN_NAME_LENGTH, MAX_NAME_LENGTH, DISTRICT_WIKI_NAMES
)
from .utils import is_valid_name, expand_type, validate_coordinates
from .coordinates import parse_dic_coordinates, parse_wikipedia_coordinates

logger = logging.getLogger(__name__)


class DistrictPageParser:
    """Парсер страницы района на Wikipedia (ШАГ 3)"""
    
    def __init__(self, manager):
        self.manager = manager
    
    async def fetch_villages_from_district_page(self, district: str, existing_villages: Dict[str, Dict]) -> Dict[str, Dict]:
        """
        Находит страницу района на Wikipedia, извлекает все населенные пункты,
        сравнивает с существующим списком и возвращает новые НП с координатами
        """
        logger.info(f"  🔍 ШАГ 3: Поиск населенных пунктов на странице района {district}...")
        
        district_page_url = None
        
        # Функция для правильного кодирования URL для Wikipedia
        def wiki_encode(name: str) -> str:
            return quote(name.replace(' ', '_'), safe='')
        
        # Получаем возможные названия страницы района из config
        possible_names = DISTRICT_WIKI_NAMES.get(district, [f"{district} район", f"{district} муниципальный округ", district])
        
        # Пробуем все возможные названия страницы района
        for name in possible_names:
            direct_url = f"{WIKIPEDIA_BASE_URL}/wiki/{wiki_encode(name)}"
            logger.debug(f"    🔎 Пробуем: {direct_url}")
            html = await self.manager._fetch_page(direct_url)
            if html:
                soup = BeautifulSoup(html, 'html.parser')
                no_article = soup.find('div', class_='noarticletext')
                if not no_article:
                    title = soup.find('h1')
                    if title:
                        title_text = title.get_text().lower()
                        if ('район' in title_text or 'округ' in title_text) and 'город' not in title_text:
                            district_page_url = direct_url
                            logger.info(f"    🌐 Найдена страница района/округа: {direct_url}")
                            break
            await asyncio.sleep(0.5)
        
        # Если не нашли, пробуем поиск через API
        if not district_page_url:
            logger.info(f"    🔎 Пробуем поиск через API Wikipedia")
            try:
                search_queries = [
                    f"{district} район",
                    f"{district} муниципальный округ",
                    f"{district} район Тверской области",
                    f"{district} муниципальный округ Тверской области"
                ]
                
                for sq in search_queries[:2]:
                    search_url = f"{WIKIPEDIA_SEARCH_URL}?action=query&list=search&srsearch={quote_plus(sq)}&format=json&utf8=1"
                    search_html = await self.manager._fetch_page(search_url)
                    if search_html:
                        data = json.loads(search_html)
                        if 'query' in data and 'search' in data['query']:
                            for result in data['query']['search'][:10]:
                                title = result['title']
                                if 'район' in title.lower() or 'округ' in title.lower():
                                    page_url = f"{WIKIPEDIA_BASE_URL}/wiki/{wiki_encode(title)}"
                                    logger.debug(f"    🔎 Проверяем через API: {page_url}")
                                    html = await self.manager._fetch_page(page_url)
                                    if html:
                                        soup = BeautifulSoup(html, 'html.parser')
                                        no_article = soup.find('div', class_='noarticletext')
                                        if not no_article:
                                            title_elem = soup.find('h1')
                                            if title_elem and ('район' in title_elem.get_text().lower() or 'округ' in title_elem.get_text().lower()):
                                                district_page_url = page_url
                                                logger.info(f"    🌐 Найдена страница района/округа через API: {page_url}")
                                                break
                                    await asyncio.sleep(0.3)
                    await asyncio.sleep(0.5)
            except Exception as e:
                logger.debug(f"    Ошибка API поиска: {e}")
        
        if not district_page_url:
            logger.warning(f"    ⚠️ Страница района {district} не найдена")
            return {}
        
        # Загружаем страницу района
        html = await self.manager._fetch_page(district_page_url)
        if not html:
            logger.warning(f"    ⚠️ Не удалось загрузить страницу {district_page_url}")
            return {}
        
        soup = BeautifulSoup(html, 'html.parser')
        
        # Ищем раздел "Населённые пункты"
        settlements_section = None
        section_keywords = ['населённые пункты', 'населенные пункты', 'список населенных пунктов', 'список населённых пунктов']
        
        for header in soup.find_all(['h2', 'h3']):
            header_text = header.get_text().lower()
            for keyword in section_keywords:
                if keyword in header_text:
                    settlements_section = header
                    logger.debug(f"    Найден раздел: {header_text[:50]}")
                    break
            if settlements_section:
                break
        
        # Ищем таблицу после заголовка
        tables = []
        if settlements_section:
            current = settlements_section.find_next_sibling()
            while current:
                if current.name == 'table':
                    tables.append(current)
                    break
                current = current.find_next_sibling()
        
        # Если не нашли таблицу после заголовка, ищем все таблицы на странице
        if not tables:
            tables = soup.find_all('table', class_=['standard', 'wikitable', 'sortable', 'collapsible'])
        
        district_villages = {}
        
        for table in tables:
            rows = table.find_all('tr')
            if len(rows) < 2:
                continue
            
            # Определяем колонки по заголовкам
            headers = []
            header_row = rows[0]
            header_cells = header_row.find_all(['th', 'td'])
            for cell in header_cells:
                headers.append(cell.get_text().strip().lower())
            
            # Ищем колонку с названиями НП
            name_col_idx = None
            type_col_idx = None
            
            for i, header in enumerate(headers):
                if 'населённый пункт' in header or 'населенный пункт' in header or 'название' in header:
                    name_col_idx = i
                elif 'тип' in header:
                    type_col_idx = i
            
            # Если не нашли по заголовкам, пробуем по первой строке
            if name_col_idx is None and len(rows) > 1:
                sample_row = rows[1]
                sample_cells = sample_row.find_all('td')
                for i, cell in enumerate(sample_cells):
                    link = cell.find('a')
                    if link and link.get('href', '').startswith('/wiki/'):
                        name_col_idx = i
                        break
            
            if name_col_idx is None:
                continue
            
            # Парсим строки таблицы
            for row in rows[1:]:
                cells = row.find_all('td')
                if len(cells) <= name_col_idx:
                    continue
                
                name_cell = cells[name_col_idx]
                name = name_cell.get_text().strip()
                name = re.sub(r'^\d+\s*', '', name)
                name = re.sub(r'\s+', ' ', name).strip()
                
                if not name or len(name) < MIN_NAME_LENGTH:
                    continue
                
                if name in ['ИТОГО', 'Всего', 'Итого', 'ИТОГО:', 'Всего:']:
                    continue
                
                # Определяем тип
                village_type = 'деревня'
                if type_col_idx is not None and type_col_idx < len(cells):
                    type_text = cells[type_col_idx].get_text().strip().lower()
                    if 'дер' in type_text:
                        village_type = 'деревня'
                    elif 'пос' in type_text:
                        village_type = 'посёлок'
                    elif 'с' in type_text and 'село' in type_text:
                        village_type = 'село'
                    elif 'станция' in type_text:
                        village_type = 'станция'
                    elif 'хутор' in type_text:
                        village_type = 'хутор'
                
                # Ищем ссылку на страницу НП
                link = name_cell.find('a')
                page_url = None
                if link and link.get('href', '').startswith('/wiki/'):
                    href = link['href']
                    if 'район' not in href.lower() and 'список' not in href.lower() and ':' not in href:
                        page_url = f"{WIKIPEDIA_BASE_URL}{href}"
                
                district_villages[name] = {
                    'type': village_type,
                    'url': page_url,
                    'has_link': page_url is not None
                }
        
        # Если таблицы не дали результатов, ищем в списках
        if not district_villages:
            logger.info(f"    🔍 Таблицы не дали результатов, ищем в списках...")
            for lst in soup.find_all(['ul', 'ol']):
                for link in lst.find_all('a', href=re.compile(r'^/wiki/')):
                    href = link.get('href', '')
                    if ':' in href or '#' in href:
                        continue
                    
                    name = link.get_text().strip()
                    name = re.sub(r'\[\d+\]', '', name)
                    name = re.sub(r'^\d+\s*', '', name)
                    name = re.sub(r'\s+', ' ', name).strip()
                    
                    if not name or len(name) < MIN_NAME_LENGTH:
                        continue
                    
                    if name in district_villages:
                        continue
                    
                    if 'район' in name.lower() or 'округ' in name.lower():
                        continue
                    
                    page_url = f"{WIKIPEDIA_BASE_URL}{href}"
                    district_villages[name] = {
                        'type': 'деревня',
                        'url': page_url,
                        'has_link': True
                    }
        
        logger.info(f"    📊 На странице района найдено {len(district_villages)} населенных пунктов")
        
        # Находим новые НП и НП без координат
        new_villages = {}
        updated_villages = {}
        
        for name, data in district_villages.items():
            if name not in existing_villages:
                if is_valid_name(name, district):
                    new_villages[name] = data
                    logger.info(f"      🆕 Найден новый НП на странице района: {name} ({data['type']})")
            else:
                existing = existing_villages[name]
                if not existing.get('has_coords', False) and existing.get('lat', '') == '':
                    updated_villages[name] = data
                    logger.info(f"      🔄 Обновление НП без координат: {name}")
        
        if new_villages:
            logger.info(f"    ✅ Найдено {len(new_villages)} новых НП на странице района")
        
        if updated_villages:
            logger.info(f"    🔄 Найдено {len(updated_villages)} существующих НП без координат")
        
        # Ищем координаты
        all_villages_to_search = []
        
        for name, data in new_villages.items():
            all_villages_to_search.append({
                'name': name,
                'type': data['type'],
                'lat': '',
                'lon': '',
                'district': district,
                'has_coords': False,
                'is_new': True,
                'wiki_url': data.get('url')
            })
        
        for name, data in updated_villages.items():
            all_villages_to_search.append({
                'name': name,
                'type': existing_villages[name].get('type', 'деревня'),
                'lat': '',
                'lon': '',
                'district': district,
                'has_coords': False,
                'is_new': False,
                'wiki_url': data.get('url')
            })
        
        if not all_villages_to_search:
            logger.info(f"    ℹ️ Нет новых НП и НП без координат для обработки")
            return {}
        
        logger.info(f"    🔍 Поиск координат для {len(all_villages_to_search)} НП...")
        
        found_coords = {}
        semaphore = asyncio.Semaphore(5)
        
        def wiki_encode(name: str) -> str:
            return quote(name.replace(' ', '_'), safe='')
        
        async def fetch_coords(village):
            async with semaphore:
                name = village['name']
                wiki_url = village.get('wiki_url')
                
                await asyncio.sleep(random.uniform(0.3, 0.8))
                
                # По ссылке со страницы района
                if wiki_url:
                    try:
                        html = await self.manager._fetch_page(wiki_url)
                        if html:
                            coords = await parse_wikipedia_coordinates(html, name)
                            if coords:
                                lat, lon = coords
                                lat_f = float(lat)
                                lon_f = float(lon)
                                if validate_coordinates(lat_f, lon_f):
                                    logger.info(f"      ✅ Найдены координаты для {name} по ссылке: {lat}, {lon}")
                                    return name, {
                                        'name': name,
                                        'type': village['type'],
                                        'lat': lat,
                                        'lon': lon,
                                        'district': district,
                                        'has_coords': True,
                                        'is_new': village['is_new']
                                    }
                    except Exception as e:
                        logger.debug(f"      ❌ Ошибка загрузки страницы {name}: {e}")
                
                # Прямой URL
                direct_url = f"{WIKIPEDIA_BASE_URL}/wiki/{wiki_encode(name)}"
                
                try:
                    html = await self.manager._fetch_page(direct_url)
                    if html:
                        coords = await parse_wikipedia_coordinates(html, name)
                        if coords:
                            lat, lon = coords
                            lat_f = float(lat)
                            lon_f = float(lon)
                            if validate_coordinates(lat_f, lon_f):
                                logger.info(f"      ✅ Найдены координаты для {name} по прямому URL: {lat}, {lon}")
                                return name, {
                                    'name': name,
                                    'type': village['type'],
                                    'lat': lat,
                                    'lon': lon,
                                    'district': district,
                                    'has_coords': True,
                                    'is_new': village['is_new']
                                }
                except Exception as e:
                    logger.debug(f"      ❌ Ошибка загрузки {name}: {e}")
                
                logger.debug(f"      ❌ Координаты не найдены для {name}")
                return None, None
        
        tasks = [fetch_coords(v) for v in all_villages_to_search]
        results = await asyncio.gather(*tasks)
        
        for name, data in results:
            if data:
                found_coords[name] = data
        
        logger.info(f"    📊 Найдено координат для {len(found_coords)} НП со страницы района")
        
        return found_coords