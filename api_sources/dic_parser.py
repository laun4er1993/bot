# dic_parser.py
# Парсер dic.academic.ru

import asyncio
import logging
import time
import random
import re
from typing import List, Dict, Optional, Tuple, Set, Any
from bs4 import BeautifulSoup
from urllib.parse import quote

from .config import (
    DIC_ACADEMIC_SEARCH_URL, DIC_ACADEMIC_ARTICLE_URL,
    LIST_KEYWORDS, SETTLEMENT_KEYWORDS, DISTRICT_KEYWORDS,
    SETTLEMENTS_SECTION_KEYWORDS, TYPE_INDICATORS, TYPE_MAPPING,
    SERVICE_SETTLEMENT_WORDS, SERVICE_VILLAGE_WORDS,
    MIN_NAME_LENGTH, MAX_NAME_LENGTH
)
from .utils import (
    is_valid_name, is_valid_settlement_name, expand_type,
    find_column_index, clean_village_name, validate_coordinates
)

logger = logging.getLogger(__name__)


class DicParser:
    """Парсер для dic.academic.ru"""
    
    def __init__(self, manager):
        self.manager = manager
    
    async def search_with_pagination(self, query: str, max_pages: int = 10, unlimited: bool = False) -> List[Dict]:
        """Поиск с пагинацией"""
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
            
            html = await self.manager._fetch_page(search_url)
            if not html:
                break
            
            page_results = self.parse_search_page(html, page)
            
            if not page_results:
                break
            
            all_results.extend(page_results)
            logger.info(f"      Страница {page}: найдено {len(page_results)} результатов")
            
            has_next = self.check_next_page(html)
            
            if not has_next:
                break
            
            page += 1
        
        if page > 1:
            logger.info(f"    Всего найдено результатов: {len(all_results)} на {page-1} страницах")
        
        return all_results
    
    def parse_search_page(self, html: str, page_num: int) -> List[Dict]:
        """Парсит страницу поиска dic.academic.ru"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            terms_list = soup.find('ul', class_='terms-list')
            if not terms_list:
                logger.debug(f"    Не найден список результатов на странице {page_num}")
                return []
            
            for item in terms_list.find_all('li', recursive=False):
                try:
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
                    full_text = ""
                    if first_p:
                        p_text = first_p.get_text().strip()
                        if title in p_text:
                            full_text = p_text.replace(title, '').strip()
                        else:
                            full_text = p_text
                    
                    position_match = re.match(r'^(\d+)', full_text)
                    position = int(position_match.group(1)) if position_match else 0
                    
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
    
    def check_next_page(self, html: str) -> bool:
        try:
            soup = BeautifulSoup(html, 'html.parser')
            next_link = soup.find('a', string=re.compile(r'далее|следующая|next', re.I))
            return next_link is not None
        except:
            return False
    
    def normalize_text(self, text: str) -> str:
        """Нормализует текст: убирает кавычки, скобки, лишние пробелы"""
        if not text:
            return ""
        text = re.sub(r'[„“«»"\'`]', '', text)
        text = re.sub(r'[\(\)\[\]\{\}]', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text.lower()
    
    def generate_district_variants(self, district_name: str) -> List[str]:
        """Генерирует все падежные формы названия района"""
        variants = []
        base = district_name.lower().strip()
        variants.append(base)
        
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
        
        variants.append(f"{base} район")
        variants.append(f"{base} района")
        variants.append(f"{base} району")
        variants.append(f"{base} районом")
        variants.append(f"{base} районе")
        
        variants.append(f"{base} муниципальный округ")
        variants.append(f"{base} муниципального округа")
        variants.append(f"{base} муниципальному округу")
        
        return list(set(variants))
    
    def check_district_in_text(self, text: str, district: str) -> bool:
        """Проверяет, есть ли упоминание района в тексте (с учётом всех падежей)"""
        if not text or not district:
            return False
        
        text_lower = text.lower()
        district_lower = district.lower()
        
        district_variants = self.generate_district_variants(district_lower)
        
        for variant in district_variants:
            if variant in text_lower:
                logger.debug(f"      Найдено упоминание района: '{variant}'")
                return True
        
        return False
    
    async def find_district_page(self, district: str) -> Optional[Dict]:
        """Находит страницу района на dic.academic.ru"""
        cache_key = f"district_{district}"
        if cache_key in self.manager.district_cache:
            return self.manager.district_cache[cache_key]
        
        logger.info(f"  🔍 Поиск страницы района: {district}")
        
        queries = [
            f"{district} район",
            f"{district} район Тверская область",
            f"{district} муниципальный район",
            district
        ]
        
        all_results = []
        
        for query in queries:
            results = await self.search_with_pagination(query, max_pages=10)
            all_results.extend(results)
            await asyncio.sleep(1.5)
        
        if not all_results:
            logger.info(f"    ❌ Страница района не найдена")
            return None
        
        for result in all_results:
            score = self.score_district_relevance(result, district)
            result['score'] = score
        
        sorted_results = sorted(all_results, key=lambda x: x['score'], reverse=True)
        top_results = sorted_results[:10]
        
        for result in top_results:
            if result['score'] >= 50:
                page_url = DIC_ACADEMIC_ARTICLE_URL.format(result['id'])
                html = await self.manager._fetch_page(page_url)
                
                if html:
                    is_district = self.verify_district_page(html, district)
                    
                    if is_district:
                        logger.info(f"    ✅ Найдена страница района (ID: {result['id']}, score: {result['score']})")
                        
                        district_info = {
                            'id': result['id'],
                            'title': result['title'],
                            'url': page_url,
                            'score': result['score'],
                            'html': html
                        }
                        
                        self.manager.district_cache[cache_key] = district_info
                        return district_info
        
        logger.info(f"    ❌ Страница района не найдена")
        return None
    
    def score_district_relevance(self, result: Dict, district: str) -> int:
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
    
    def verify_district_page(self, html: str, district: str) -> bool:
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
    
    async def extract_settlements_from_page(self, html: str, district: str) -> List[str]:
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
                                    if is_valid_settlement_name(settlement, district):
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
                            if is_valid_settlement_name(settlement, district):
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
    
    async def find_settlement_main_page(self, settlement: str, district: str) -> Optional[str]:
        cache_key = f"settlement_main_{district}_{settlement}"
        if cache_key in self.manager.settlement_pages_cache:
            return self.manager.settlement_pages_cache[cache_key]
        
        queries = [
            f"Сельское поселение {settlement}",
            f"{settlement} сельское поселение",
            f"{settlement} СП"
        ]
        
        all_results = []
        
        for query in queries:
            results = await self.search_with_pagination(query, max_pages=10)
            all_results.extend(results)
            await asyncio.sleep(1.5)
        
        if not all_results:
            return None
        
        for result in all_results:
            title_lower = result['title'].lower()
            if "список бывших" in title_lower:
                result['score'] = 0
            else:
                result['score'] = self.score_settlement_relevance(result, settlement, district)
        
        best = max(all_results, key=lambda x: x['score'])
        
        if best['score'] >= 40:
            page_url = DIC_ACADEMIC_ARTICLE_URL.format(best['id'])
            html = await self.manager._fetch_page(page_url)
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
            self.manager.settlement_pages_cache[cache_key] = best['id']
            return best['id']
        
        return None
    
    def score_settlement_relevance(self, result: Dict, settlement: str, district: str) -> int:
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
        
        if self.check_district_in_text(title_lower + " " + full_text_lower, district):
            score += 20
        
        if result['position'] == 1:
            score += 15
        elif result['position'] <= 3:
            score += 10
        
        return score
    
    async def parse_settlement_main_page(self, article_id: str, district: str, settlement: str) -> List[Dict]:
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        html = await self.manager._fetch_page(url)
        
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        page_text = soup.get_text().lower()
        
        if not self.check_district_in_text(page_text, district):
            logger.debug(f"      Страница ID {article_id} не относится к району {district}, пропускаем")
            return []
        
        results = self.parse_settlements_section(html, article_id, district, settlement)
        
        if results:
            logger.info(f"      Из раздела 'Населенные пункты' СП {settlement} получено {len(results)} записей")
        else:
            alt_results = self.parse_settlements_alternative(html, article_id, district, settlement)
            if alt_results:
                logger.info(f"      Из альтернативного парсинга СП {settlement} получено {len(alt_results)} записей")
                results = alt_results
        
        return results
    
    def parse_settlements_section(self, html: str, article_id: str, district: str, settlement: str) -> List[Dict]:
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
                
                logger.info(f"        Таблица: name_idx={name_idx}, type_idx={type_idx}")
                
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
                        
                        if not is_valid_name(name, district):
                            continue
                        
                        link = name_cell.find('a')
                        article_id_from_link = None
                        if link:
                            href = link.get('href', '')
                            match = re.search(r'(\d+)', href)
                            if match:
                                article_id_from_link = match.group(1)
                                self.manager.village_links[name] = article_id_from_link
                                links_found += 1
                                logger.info(f"        🔗 Найдена ссылка для {name}: ID {article_id_from_link}")
                        
                        if not article_id_from_link:
                            for cell in cells:
                                link = cell.find('a')
                                if link and link.get_text().strip() == name:
                                    href = link.get('href', '')
                                    match = re.search(r'(\d+)', href)
                                    if match:
                                        article_id_from_link = match.group(1)
                                        self.manager.village_links[name] = article_id_from_link
                                        links_found += 1
                                        logger.info(f"        🔗 Найдена ссылка для {name} (альт): ID {article_id_from_link}")
                                        break
                        
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
                                
                                if not is_valid_name(name, district):
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
                                    self.manager.village_links[name] = article_id_from_link
                                    links_found += 1
                                    logger.info(f"        🔗 Найдена ссылка (альт2) для {name}: ID {article_id_from_link}")
                                    
                                    results.append({
                                        "name": name,
                                        "type": type_text,
                                        "lat": "",
                                        "lon": "",
                                        "district": district,
                                        "has_coords": False,
                                        "article_id": article_id_from_link
                                    })
            
            logger.info(f"        Всего найдено ссылок: {links_found}, НП: {len(results)}")
            return results
            
        except Exception as e:
            logger.error(f"Ошибка парсинга раздела 'Населенные пункты': {e}")
            return []
    
    def parse_settlements_alternative(self, html: str, article_id: str, district: str, settlement: str) -> List[Dict]:
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
                
                if not is_valid_name(name, district):
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
                    self.manager.village_links[name] = link_id
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
    
    async def find_master_list_links(self, html: str, district: str) -> List[str]:
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
                        if self.check_district_in_text(full_context, district):
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
                                    if self.check_district_in_text(text, district):
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
    
    async def parse_master_list_page(self, article_id: str, district: str) -> List[Dict]:
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        html = await self.manager._fetch_page(url)
        
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        page_text = soup.get_text().lower()
        
        if not self.check_district_in_text(page_text, district):
            logger.debug(f"      Страница ID {article_id} не относится к району {district}, пропускаем")
            return []
        
        results = self.parse_master_list_html(html, article_id, district)
        
        if results:
            logger.info(f"      Из списка ID {article_id} получено {len(results)} записей")
            if len(results) > 0:
                sample = results[:min(5, len(results))]
                logger.info(f"        Примеры: {[(v['name'], v['type']) for v in sample]}")
        else:
            logger.warning(f"      Из списка ID {article_id} не получено записей")
        
        return results
    
    def parse_master_list_html(self, html: str, article_id: str, district: str) -> List[Dict]:
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
                        
                        if not is_valid_name(name, district):
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
    
    async def parse_individual_village_page(self, article_id: str, district: str) -> Optional[Dict]:
        await asyncio.sleep(random.uniform(1.5, 3.0))
        
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        html = await self.manager._fetch_page(url)
        
        if not html:
            return None
        
        return self.parse_individual_village_html(html, article_id, district)
    
    def parse_individual_village_html(self, html: str, article_id: str, district: str) -> Optional[Dict]:
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
            
            if not is_valid_name(name, district):
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
                        if self.manager._check_coordinate_in_district(lat_candidate, lon_candidate, self.manager._get_district_bounds(district)):
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
                if not self.manager._check_coordinate_in_district(lat, lon, self.manager._get_district_bounds(district)):
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
    
    async def get_article_info(self, article_id: str) -> Optional[Dict]:
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        html = await self.manager._fetch_page(url)
        if not html:
            return None
        soup = BeautifulSoup(html, 'html.parser')
        title_elem = soup.find('h1')
        title = title_elem.get_text().strip() if title_elem else ""
        return {'id': article_id, 'title': title}