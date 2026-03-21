# dic_parser.py
# Парсер для dic.academic.ru

import asyncio
import logging
import re
from typing import List, Dict, Optional
from bs4 import BeautifulSoup

from .config import (
    DIC_ACADEMIC_ARTICLE_URL, LIST_KEYWORDS, SETTLEMENTS_SECTION_KEYWORDS,
    SETTLEMENT_KEYWORDS, TYPE_INDICATORS, DISTRICT_KEYWORDS
)
from .utils import (
    is_valid_name, is_valid_settlement_name, expand_type,
    find_column_index, clean_village_name, extract_settlement_from_text
)
from .coordinates import parse_dic_coordinates

logger = logging.getLogger(__name__)


class DicParser:
    """Парсер для dic.academic.ru"""
    
    def __init__(self, session, thread_pool, search_func, fetch_func):
        self.session = session
        self.thread_pool = thread_pool
        self._search_with_pagination = search_func
        self._fetch_page = fetch_func
        
        # Кэши
        self.district_cache = {}
        self.former_np_pages_cache = {}
        self.settlement_pages_cache = {}
        self.processed_article_ids = set()
        self.village_links = {}  # название НП -> ID статьи
        self.coords_stats = {'from_former': 0, 'from_links': 0}
        self.collection_stats = {'from_master_lists': 0, 'from_former': 0, 'from_settlements': 0}
    
    async def find_district_page(self, district: str) -> Optional[Dict]:
        """Находит страницу района на dic.academic.ru"""
        cache_key = f"district_{district}"
        if cache_key in self.district_cache:
            return self.district_cache[cache_key]
        
        logger.info(f"  🔍 Поиск страницы района: {district}")
        
        # Расширенные запросы как в рабочей версии
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
        
        # Оценка релевантности
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
                    if await self._verify_district_page(html, district):
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
        """Оценивает релевантность результата для страницы района"""
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
    
    async def _verify_district_page(self, html: str, district: str) -> bool:
        """Проверяет, что страница действительно является страницей района"""
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
    
    async def extract_settlements(self, html: str, district: str) -> List[str]:
        """Извлекает список сельских поселений со страницы района"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            found_settlements = []
            
            for header in soup.find_all(['h2', 'h3', 'h4']):
                if 'состав района' in header.get_text().lower():
                    parent = header.find_parent()
                    if parent:
                        for ul in parent.find_all('ul'):
                            for li in ul.find_all('li'):
                                link = li.find('a')
                                if link:
                                    text = link.get_text().strip()
                                    match = re.search(r'Сельское поселение\s+([А-Яа-я-]+)', text)
                                    if match:
                                        settlement = match.group(1).strip()
                                        if self._is_valid_settlement_name(settlement):
                                            found_settlements.append(settlement)
                                    else:
                                        if self._is_valid_settlement_name(text):
                                            found_settlements.append(text)
            
            unique_settlements = sorted(list(set(found_settlements)))
            logger.info(f"    Найдено сельских поселений: {len(unique_settlements)}")
            return unique_settlements
            
        except Exception as e:
            logger.error(f"Ошибка парсинга сельских поселений: {e}")
            return []
    
    def _is_valid_settlement_name(self, name: str) -> bool:
        """Проверяет, является ли текст валидным названием сельского поселения"""
        if not name or len(name) < 2 or len(name) > 30:
            return False
        
        name_lower = name.lower()
        from .config import SERVICE_SETTLEMENT_WORDS
        for word in SERVICE_SETTLEMENT_WORDS:
            if word in name_lower:
                return False
        
        if not re.search(r'[а-яА-ЯёЁ]', name):
            return False
        
        if name.isdigit():
            return False
        
        return True
    
    async def find_former_np_page(self, settlement: str, district: str) -> Optional[str]:
        """Находит страницу с бывшими населенными пунктами"""
        cache_key = f"former_np_{district}_{settlement}"
        if cache_key in self.former_np_pages_cache:
            return self.former_np_pages_cache[cache_key]
        
        queries = [
            f"Список бывших населённых пунктов на территории сельского поселения {settlement} {district} района",
            f"Список бывших населенных пунктов на территории сельского поселения {settlement} {district} района",
            f"Список бывших населённых пунктов {settlement} {district} района",
            f"Бывшие населённые пункты {settlement} СП",
            f"Список бывших населённых пунктов {settlement} сельского поселения"
        ]
        
        all_results = []
        
        for query in queries:
            results = await self._search_with_pagination(query, max_pages=15)
            all_results.extend(results)
            await asyncio.sleep(1.5)
        
        if not all_results:
            return None
        
        for result in all_results:
            title_lower = result['title'].lower()
            full_text_lower = result['full_text'].lower()
            
            # Проверяем, что страница относится к нужному району
            district_lower = district.lower()
            if district_lower not in full_text_lower and district_lower not in title_lower:
                result['score'] = 0
                continue
            
            if "список бывших" in title_lower and settlement.lower() in title_lower:
                result['score'] = 150
            else:
                result['score'] = self._score_settlement_relevance(result, settlement, district)
            
            # Дополнительный бонус, если в тексте есть район
            if district_lower in full_text_lower:
                result['score'] += 20
        
        # Фильтруем результаты, оставляем только те, где есть упоминание района
        filtered_results = [r for r in all_results if r['score'] >= 50 and district.lower() in (r['full_text'].lower() + r['title'].lower())]
        
        if not filtered_results:
            return None
        
        best = max(filtered_results, key=lambda x: x['score'])
        
        if best['score'] >= 50:
            logger.info(f"      Найдена страница бывших НП для СП {settlement} (ID: {best['id']}, score: {best['score']})")
            self.former_np_pages_cache[cache_key] = best['id']
            return best['id']
        
        return None
    
    async def find_settlement_main_page(self, settlement: str, district: str) -> Optional[str]:
        """Находит основную страницу сельского поселения"""
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
            full_text_lower = result['full_text'].lower()
            
            # Проверяем, что страница относится к нужному району
            district_lower = district.lower()
            if district_lower not in full_text_lower and district_lower not in title_lower:
                result['score'] = 0
                continue
            
            if "список бывших" in title_lower:
                result['score'] = 0
            else:
                result['score'] = self._score_settlement_relevance(result, settlement, district)
            
            # Дополнительный бонус, если в тексте есть район
            if district_lower in full_text_lower:
                result['score'] += 20
        
        # Фильтруем результаты, оставляем только те, где есть упоминание района
        filtered_results = [r for r in all_results if r['score'] >= 40 and district.lower() in (r['full_text'].lower() + r['title'].lower())]
        
        if not filtered_results:
            return None
        
        best = max(filtered_results, key=lambda x: x['score'])
        
        if best['score'] >= 40:
            logger.info(f"      Найдена основная страница СП {settlement} (ID: {best['id']}, score: {best['score']})")
            self.settlement_pages_cache[cache_key] = best['id']
            return best['id']
        
        return None
    
    def _score_settlement_relevance(self, result: Dict, settlement: str, district: str) -> int:
        """Оценивает релевантность результата для страницы сельского поселения"""
        title_lower = result['title'].lower()
        full_text_lower = result['full_text'].lower()
        settlement_lower = settlement.lower()
        district_lower = district.lower()
        
        score = 0
        
        if settlement_lower in title_lower:
            score += 50
        
        if "сельское поселение" in title_lower:
            score += 40
        
        if district_lower in title_lower or district_lower in full_text_lower:
            score += 20
        
        if result['position'] == 1:
            score += 15
        elif result['position'] <= 3:
            score += 10
        
        return score
    
    async def parse_former_np_page(self, article_id: str, district: str, settlement: str) -> List[Dict]:
        """Парсит страницу с бывшими населенными пунктами"""
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        html = await self._fetch_page(url)
        
        if not html:
            return []
        
        # Проверяем, что страница относится к нужному району
        soup = BeautifulSoup(html, 'html.parser')
        page_text = soup.get_text().lower()
        district_lower = district.lower()
        
        if district_lower not in page_text:
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
        
        return results
    
    def _parse_former_np_html(self, html: str, article_id: str, district: str, settlement: str) -> List[Dict]:
        """Парсит HTML страницы с бывшими НП (с координатами)"""
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
                
                name_idx = find_column_index(headers, ['населённый пункт', 'название'])
                type_idx = find_column_index(headers, ['тип'])
                coords_idx = find_column_index(headers, ['координаты', 'коорд'])
                
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
                        
                        if not is_valid_name(name):
                            continue
                        
                        village_type = 'деревня'
                        if type_idx is not None and type_idx < len(cells):
                            raw_type = cells[type_idx].get_text().strip()
                            village_type = expand_type(raw_type)
                        
                        lat, lon = None, None
                        if coords_idx is not None and coords_idx < len(cells):
                            lat, lon = parse_dic_coordinates('', cells[coords_idx])
                        
                        if not lat or not lon:
                            row_text = ' '.join([c.get_text() for c in cells])
                            lat, lon = parse_dic_coordinates(row_text, None)
                        
                        if lat and lon:
                            self.coords_stats['from_former'] += 1
                        
                        results.append({
                            "name": name,
                            "type": village_type,
                            "lat": str(round(lat, 5)) if lat else "",
                            "lon": str(round(lon, 5)) if lon else "",
                            "district": district,
                            "has_coords": bool(lat)
                        })
                        
                    except Exception as e:
                        continue
            
            return results
            
        except Exception as e:
            logger.error(f"Ошибка парсинга страницы бывших НП: {e}")
            return []
    
    async def parse_settlement_main_page(self, article_id: str, district: str, settlement: str) -> List[Dict]:
        """Парсит основную страницу сельского поселения"""
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        html = await self._fetch_page(url)
        
        if not html:
            return []
        
        # Проверяем, что страница относится к нужному району
        soup = BeautifulSoup(html, 'html.parser')
        page_text = soup.get_text().lower()
        district_lower = district.lower()
        
        if district_lower not in page_text:
            logger.debug(f"      Страница ID {article_id} не относится к району {district}, пропускаем")
            return []
        
        loop = asyncio.get_event_loop()
        results = await loop.run_in_executor(
            self.thread_pool,
            self._parse_settlement_section,
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
    
    def _parse_settlement_section(self, html: str, article_id: str, district: str, settlement: str) -> List[Dict]:
        """Парсит раздел "Населенные пункты" на странице сельского поселения"""
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
                        name = clean_village_name(name_cell.get_text().strip())
                        
                        if not name or len(name) < 2:
                            continue
                        
                        if not is_valid_name(name):
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
                                name = clean_village_name(link.get_text().strip())
                                
                                if not name or len(name) < 2:
                                    continue
                                
                                if not is_valid_name(name):
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
        """Альтернативный метод парсинга"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            seen_names = set()
            links_found = 0
            
            for link in soup.find_all('a', href=re.compile(r'/dic\.nsf/ruwiki/\d+')):
                href = link.get('href', '')
                if article_id in href:
                    continue
                
                name = clean_village_name(link.get_text().strip())
                
                if not name or len(name) < 2 or name in seen_names:
                    continue
                
                if not is_valid_name(name):
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
    
    async def parse_individual_village_page(self, article_id: str, district: str) -> Optional[Dict]:
        """Парсит отдельную страницу населенного пункта для извлечения координат (dic.academic.ru)"""
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
        """Парсит отдельную страницу населенного пункта (dic.academic.ru)"""
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
            
            if not is_valid_name(name):
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
                        from .utils import validate_coordinates
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