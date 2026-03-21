# dic_parser.py
# Парсер для dic.academic.ru

import asyncio
import logging
import re
from typing import List, Dict, Optional
from bs4 import BeautifulSoup

from .config import (
    DIC_ACADEMIC_BASE_URL, DIC_ACADEMIC_SEARCH_URL, DIC_ACADEMIC_ARTICLE_URL,
    LIST_KEYWORDS, SETTLEMENTS_SECTION_KEYWORDS, SETTLEMENT_KEYWORDS,
    TYPE_INDICATORS, DISTRICT_KEYWORDS
)
from .utils import (
    is_valid_name, is_valid_settlement_name, expand_type,
    find_column_index, clean_village_name, extract_settlement_from_text
)
from .coordinates import parse_dic_coordinates

logger = logging.getLogger(__name__)


class DicParser:
    """Парсер для dic.academic.ru"""
    
    def __init__(self, session, thread_pool, search_func):
        self.session = session
        self.thread_pool = thread_pool
        self._search_with_pagination = search_func
        
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
            return None
        
        for result in all_results:
            score = self._score_district_relevance(result, district)
            result['score'] = score
        
        sorted_results = sorted(all_results, key=lambda x: x['score'], reverse=True)
        
        for result in sorted_results[:10]:
            if result['score'] >= 50:
                page_url = DIC_ACADEMIC_ARTICLE_URL.format(result['id'])
                html = await self._fetch_page(page_url)
                if html and await self._verify_district_page(html, district):
                    info = {'id': result['id'], 'title': result['title'], 'url': page_url, 'score': result['score']}
                    self.district_cache[cache_key] = info
                    logger.info(f"    ✅ Найдена страница района (ID: {result['id']}, score: {result['score']})")
                    return info
        
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
            
            sections = ['география', 'история', 'население', 'состав района']
            found = sum(1 for s in sections if s in text)
            return found >= 2
        except:
            return False
    
    async def extract_settlements(self, html: str, district: str) -> List[str]:
        """Извлекает список сельских поселений со страницы района"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            found_settlements = []
            
            for header in soup.find_all(['h2', 'h3', 'h4']):
                header_text = header.get_text().lower()
                if any(kw in header_text for kw in SETTLEMENT_KEYWORDS):
                    parent = header.find_parent()
                    if parent:
                        for ul in parent.find_all(['ul', 'ol']):
                            for li in ul.find_all('li'):
                                text = li.get_text().strip()
                                text = re.sub(r'\[[0-9]+\]', '', text).strip()
                                
                                settlement = extract_settlement_from_text(text)
                                if settlement and is_valid_settlement_name(settlement):
                                    if not re.match(r'^\d+\s+(мая|января)', settlement, re.IGNORECASE):
                                        found_settlements.append(settlement)
            
            unique = sorted(list(set(found_settlements)))
            valid = []
            for s in unique:
                if len(s) >= 3 and not any(w in s.lower() for w in ['список', 'статья']):
                    valid.append(s)
            
            logger.info(f"    Найдено сельских поселений: {len(valid)}")
            return valid
        except Exception as e:
            logger.error(f"Ошибка парсинга сельских поселений: {e}")
            return []
    
    async def find_former_np_page(self, settlement: str, district: str) -> Optional[str]:
        """Находит страницу с бывшими населенными пунктами"""
        cache_key = f"former_np_{district}_{settlement}"
        if cache_key in self.former_np_pages_cache:
            return self.former_np_pages_cache[cache_key]
        
        queries = [
            f"Список бывших населённых пунктов на территории сельского поселения {settlement} {district} района",
            f"Список бывших населенных пунктов на территории сельского поселения {settlement} {district} района",
            f"Список бывших населённых пунктов {settlement} {district} района",
        ]
        
        all_results = []
        for q in queries:
            results = await self._search_with_pagination(q, max_pages=15)
            all_results.extend(results)
            await asyncio.sleep(1.5)
        
        if not all_results:
            return None
        
        for r in all_results:
            title = r['title'].lower()
            text = r['full_text'].lower()
            d_lower = district.lower()
            
            if d_lower not in text and d_lower not in title:
                r['score'] = 0
                continue
            
            if "список бывших" in title and settlement.lower() in title:
                r['score'] = 150
            else:
                r['score'] = self._score_settlement_relevance(r, settlement, district)
            
            if d_lower in text:
                r['score'] += 20
        
        filtered = [r for r in all_results if r['score'] >= 50 and district.lower() in (r['full_text'].lower() + r['title'].lower())]
        if not filtered:
            return None
        
        best = max(filtered, key=lambda x: x['score'])
        
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
        
        queries = [f"Сельское поселение {settlement}", f"{settlement} сельское поселение", f"{settlement} СП"]
        
        all_results = []
        for q in queries:
            results = await self._search_with_pagination(q, max_pages=10)
            all_results.extend(results)
            await asyncio.sleep(1.5)
        
        if not all_results:
            return None
        
        for r in all_results:
            title = r['title'].lower()
            text = r['full_text'].lower()
            d_lower = district.lower()
            
            if d_lower not in text and d_lower not in title:
                r['score'] = 0
                continue
            
            if "список бывших" in title:
                r['score'] = 0
            else:
                r['score'] = self._score_settlement_relevance(r, settlement, district)
            
            if d_lower in text:
                r['score'] += 20
        
        filtered = [r for r in all_results if r['score'] >= 40 and district.lower() in (r['full_text'].lower() + r['title'].lower())]
        if not filtered:
            return None
        
        best = max(filtered, key=lambda x: x['score'])
        
        if best['score'] >= 40:
            logger.info(f"      Найдена основная страница СП {settlement} (ID: {best['id']}, score: {best['score']})")
            self.settlement_pages_cache[cache_key] = best['id']
            return best['id']
        
        return None
    
    def _score_settlement_relevance(self, result: Dict, settlement: str, district: str) -> int:
        """Оценивает релевантность результата для страницы сельского поселения"""
        title = result['title'].lower()
        text = result['full_text'].lower()
        settlement_lower = settlement.lower()
        district_lower = district.lower()
        
        score = 0
        if settlement_lower in title:
            score += 50
        if "сельское поселение" in title:
            score += 40
        if district_lower in title or district_lower in text:
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
        
        soup = BeautifulSoup(html, 'html.parser')
        if district.lower() not in soup.get_text().lower():
            logger.debug(f"      Страница ID {article_id} не относится к району {district}, пропускаем")
            return []
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.thread_pool,
            self._parse_former_np_html,
            html,
            article_id,
            district,
            settlement
        )
    
    def _parse_former_np_html(self, html: str, article_id: str, district: str, settlement: str) -> List[Dict]:
        """Парсит HTML страницы с бывшими НП"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            for table in soup.find_all('table', class_=['standard', 'sortable']):
                rows = table.find_all('tr')
                if len(rows) < 2:
                    continue
                
                headers = [h.get_text().strip().lower() for h in rows[0].find_all(['th', 'td'])]
                name_idx = find_column_index(headers, ['населённый пункт', 'название'])
                type_idx = find_column_index(headers, ['тип'])
                coords_idx = find_column_index(headers, ['координаты', 'коорд'])
                
                for row in rows[1:]:
                    try:
                        cells = row.find_all('td')
                        if len(cells) < max(name_idx or 0, type_idx or 0) + 1:
                            continue
                        
                        name = cells[name_idx].get_text().strip() if name_idx is not None else None
                        if not name or name in ['ИТОГО', 'Всего'] or not is_valid_name(name):
                            continue
                        
                        village_type = 'деревня'
                        if type_idx is not None and type_idx < len(cells):
                            village_type = expand_type(cells[type_idx].get_text().strip())
                        
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
                    except:
                        continue
            
            if results:
                logger.info(f"      Из списка бывших НП ID {article_id} получено {len(results)} записей")
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
        
        soup = BeautifulSoup(html, 'html.parser')
        if district.lower() not in soup.get_text().lower():
            logger.debug(f"      Страница ID {article_id} не относится к району {district}, пропускаем")
            return []
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.thread_pool,
            self._parse_settlement_section,
            html,
            article_id,
            district,
            settlement
        )
    
    def _parse_settlement_section(self, html: str, article_id: str, district: str, settlement: str) -> List[Dict]:
        """Парсит раздел "Населенные пункты" на странице сельского поселения"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            links_found = 0
            
            # Поиск таблиц
            tables = soup.find_all('table', class_=['standard', 'sortable', 'wikitable', 'simple', 'collapsible', 'collapsed'])
            
            for table in tables:
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
                        sample = rows[1].find_all('td')
                        for i, cell in enumerate(sample):
                            if any(ind in cell.get_text() for ind in TYPE_INDICATORS):
                                type_idx = i
                                if i + 1 < len(sample):
                                    name_idx = i + 1
                                break
                
                if name_idx is None:
                    name_idx = 1 if len(header_cells) >= 2 else 0
                if type_idx is None:
                    type_idx = name_idx - 1 if name_idx > 0 else 0
                
                for row in rows[1:]:
                    cells = row.find_all('td')
                    if len(cells) <= max(type_idx, name_idx):
                        continue
                    
                    raw_type = cells[type_idx].get_text().strip()
                    village_type = expand_type(raw_type)
                    
                    name = clean_village_name(cells[name_idx].get_text().strip())
                    if not name or len(name) < 2 or not is_valid_name(name):
                        continue
                    
                    link = cells[name_idx].find('a')
                    if link:
                        href = link.get('href', '')
                        match = re.search(r'(\d+)', href)
                        if match:
                            self.village_links[name] = match.group(1)
                            links_found += 1
                            logger.info(f"        🔗 Найдена ссылка для {name}: ID {match.group(1)}")
                    
                    results.append({
                        "name": name,
                        "type": village_type,
                        "lat": "",
                        "lon": "",
                        "district": district,
                        "has_coords": False,
                        "article_id": self.village_links.get(name)
                    })
            
            logger.info(f"        Всего найдено ссылок: {links_found}")
            return results
        except Exception as e:
            logger.error(f"Ошибка парсинга: {e}")
            return []
    
    async def parse_individual_village_page(self, article_id: str, district: str) -> Optional[Dict]:
        """Парсит отдельную страницу населенного пункта для извлечения координат"""
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
        """Парсит отдельную страницу населенного пункта"""
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
                village_type = expand_type(type_match.group(1))
                name = full_title.replace(f'({type_match.group(1)})', '').strip()
            else:
                type_match = re.search(r',\s*([^,]+)$', full_title)
                if type_match:
                    village_type = expand_type(type_match.group(1))
                    name = full_title.replace(f', {type_match.group(1)}', '').strip()
            
            if not is_valid_name(name):
                return None
            
            lat, lon = None, None
            geo = soup.find('span', class_='geo')
            if geo:
                lat_span = geo.find('span', class_='latitude')
                lon_span = geo.find('span', class_='longitude')
                if lat_span and lon_span:
                    try:
                        lat = float(lat_span.get_text().strip())
                        lon = float(lon_span.get_text().strip())
                    except:
                        pass
            
            if not lat or not lon:
                dms = r'(\d+)°(\d+)′([\d.]+)″.*?(\d+)°(\d+)′([\d.]+)″'
                match = re.search(dms, soup.get_text())
                if match:
                    try:
                        lat = float(match.group(1)) + float(match.group(2))/60 + float(match.group(3))/3600
                        lon = float(match.group(4)) + float(match.group(5))/60 + float(match.group(6))/3600
                    except:
                        pass
            
            if lat and lon:
                return {
                    "name": name,
                    "type": village_type,
                    "lat": str(round(lat, 5)),
                    "lon": str(round(lon, 5)),
                    "district": district,
                    "has_coords": True
                }
            
            return None
        except Exception as e:
            logger.error(f"Ошибка парсинга: {e}")
            return None
    
    async def _fetch_page(self, url: str) -> Optional[str]:
        """Загружает страницу (делегирует внешнему методу)"""
        # Этот метод будет переопределен при создании DicParser
        raise NotImplementedError("Метод _fetch_page должен быть переопределен")