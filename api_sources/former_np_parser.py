# former_np_parser.py
# Парсер бывших населенных пунктов

import asyncio
import logging
import re
from typing import List, Dict, Optional, Tuple, Any
from bs4 import BeautifulSoup

from .config import DIC_ACADEMIC_ARTICLE_URL, MIN_NAME_LENGTH
from .utils import is_valid_name, expand_type, find_column_index
from .coordinates import parse_dic_coordinates

logger = logging.getLogger(__name__)


class FormerNPParser:
    """Парсер для бывших населенных пунктов"""
    
    def __init__(self, manager):
        self.manager = manager
    
    async def find_district_former_np_page(self, district: str, district_html: str) -> Optional[str]:
        """Находит общую страницу бывших населенных пунктов для всего района"""
        cache_key = f"district_former_{district}"
        if cache_key in self.manager.former_np_pages_cache:
            return self.manager.former_np_pages_cache[cache_key]
        
        soup = BeautifulSoup(district_html, 'html.parser')
        
        # Ищем в разделе "См. также" или в тексте страницы
        see_also = soup.find('div', class_='rellink boilerplate seealso')
        if see_also:
            for link in see_also.find_all('a', href=re.compile(r'/dic\.nsf/ruwiki/\d+')):
                link_text = link.get_text().lower()
                if 'список бывших населённых пунктов' in link_text and self.manager._check_district_in_text(link_text, district):
                    match = re.search(r'/dic\.nsf/ruwiki/(\d+)', link.get('href', ''))
                    if match:
                        article_id = match.group(1)
                        logger.info(f"      Найдена общая страница бывших НП для района {district} (ID: {article_id})")
                        self.manager.former_np_pages_cache[cache_key] = article_id
                        return article_id
        
        # Также ищем в тексте ссылку на список бывших НП
        for link in soup.find_all('a', href=re.compile(r'/dic\.nsf/ruwiki/\d+')):
            link_text = link.get_text().lower()
            parent_text = link.find_parent().get_text().lower() if link.find_parent() else ""
            full_text = link_text + " " + parent_text
            
            if 'список бывших населённых пунктов' in full_text and self.manager._check_district_in_text(full_text, district):
                match = re.search(r'/dic\.nsf/ruwiki/(\d+)', link.get('href', ''))
                if match:
                    article_id = match.group(1)
                    logger.info(f"      Найдена общая страница бывших НП для района {district} (ID: {article_id})")
                    self.manager.former_np_pages_cache[cache_key] = article_id
                    return article_id
        
        # Расширенные запросы для поиска общего списка района
        queries = [
            f"Список бывших населённых пунктов на территории {district} района Тверской области",
            f"Список бывших населенных пунктов {district} района",
            f"Бывшие населенные пункты {district} района",
            f"Список бывших населённых пунктов {district} муниципального округа",
            f"Список бывших населенных пунктов {district} муниципального округа",
            f"Бывшие населенные пункты {district} муниципального округа"
        ]
        
        for query in queries:
            results = await self.manager._search_with_pagination(query, max_pages=10)
            
            for result in results[:15]:
                title_lower = result['title'].lower()
                if ('список бывших' in title_lower or 'бывшие населенные' in title_lower) and self.manager._check_district_in_text(title_lower, district):
                    page_url = DIC_ACADEMIC_ARTICLE_URL.format(result['id'])
                    html = await self.manager._fetch_page(page_url)
                    if html:
                        soup_page = BeautifulSoup(html, 'html.parser')
                        tables = soup_page.find_all('table', class_=['standard', 'sortable'])
                        if tables:
                            logger.info(f"      Найдена общая страница бывших НП для района {district} (ID: {result['id']})")
                            self.manager.former_np_pages_cache[cache_key] = result['id']
                            return result['id']
            
            await asyncio.sleep(1.5)
        
        return None
    
    async def find_former_np_page(self, settlement: str, district: str) -> Optional[str]:
        """Находит страницу с бывшими населенными пунктами для сельского поселения"""
        cache_key = f"former_np_{district}_{settlement}"
        if cache_key in self.manager.former_np_pages_cache:
            return self.manager.former_np_pages_cache[cache_key]
        
        district_lower = district.lower()
        settlement_lower = settlement.lower()
        
        # Расширенные запросы для поиска страницы бывших НП
        queries = [
            f"Список бывших населённых пунктов на территории сельского поселения {settlement} {district} район",
            f"Список бывших населенных пунктов на территории сельского поселения {settlement} {district} район",
            f"Список бывших населённых пунктов {settlement} {district} район",
            f"Бывшие населённые пункты {settlement} СП",
            f"Список бывших населённых пунктов {settlement} сельского поселения",
            f"{settlement} бывшие населенные пункты",
            f"Бывшие населенные пункты {district} района"
        ]
        
        all_results = []
        
        logger.info(f"    🔍 Поиск бывших НП для СП {settlement}...")
        
        for query in queries:
            results = await self.manager._search_with_pagination(query, max_pages=15)
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
            
            title_normalized = self.manager._normalize_text(title_lower)
            settlement_normalized = self.manager._normalize_text(settlement_lower)
            
            logger.info(f"        Результат {i+1}: ID {result['id']} - {result['title'][:100]}...")
            
            # Проверяем принадлежность к району
            if not self.manager._check_district_in_text(title_lower + " " + full_text_lower, district):
                logger.info(f"          ❌ Не относится к району {district}")
                continue
            
            # Проверяем наличие слова "бывших"
            if 'бывших' not in title_lower and 'бывшие' not in title_lower:
                logger.info(f"          ❌ Нет слова 'бывших' в заголовке")
                continue
            
            # Проверяем наличие названия СП или района
            has_settlement = (settlement_lower in title_lower or settlement_normalized in title_normalized)
            has_district = self.manager._check_district_in_text(title_lower, district)
            
            # Если нет названия СП, но это общий список района - помечаем как районный список
            is_district_list = not has_settlement and 'района' in title_lower and has_district
            
            if not has_settlement and not is_district_list:
                logger.info(f"          ❌ Нет названия СП '{settlement}' в заголовке и это не общий список района")
                continue
            
            if is_district_list:
                logger.info(f"          ℹ️ Найден общий список бывших НП района")
            
            logger.info(f"          ✅ Прошел проверки, загружаем страницу...")
            
            page_url = DIC_ACADEMIC_ARTICLE_URL.format(result['id'])
            html = await self.manager._fetch_page(page_url)
            
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
                    self.manager.former_np_pages_cache[cache_key] = result['id']
                    return result['id']
            
            logger.info(f"          ❌ Нет таблицы с координатами или годом упразднения")
        
        logger.info(f"      ❌ Страница бывших НП для СП {settlement} не найдена")
        return None
    
    async def parse_former_np_page(self, article_id: str, district: str, settlement: str) -> List[Dict]:
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        html = await self.manager._fetch_page(url)
        
        if not html:
            return []
        
        soup = BeautifulSoup(html, 'html.parser')
        page_text = soup.get_text().lower()
        
        if not self.manager._check_district_in_text(page_text, district):
            logger.debug(f"      Страница ID {article_id} не относится к району {district}, пропускаем")
            return []
        
        results = self.parse_former_np_html(html, article_id, district, settlement)
        
        if results:
            logger.info(f"      Из списка бывших НП ID {article_id} получено {len(results)} записей")
            for i, v in enumerate(results[:5]):
                coords_info = f" (коорд: {v['lat']}, {v['lon']})" if v['has_coords'] else " (без коорд)"
                logger.info(f"        {i+1}. {v['name']} ({v['type']}){coords_info}")
        
        return results
    
    def parse_former_np_html(self, html: str, article_id: str, district: str, settlement: str) -> List[Dict]:
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
                        
                        if not is_valid_name(name, district):
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
                        
                        if lat and lon and self.manager._check_coordinate_in_district(lat, lon, self.manager._get_district_bounds(district)):
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
                            self.manager.coords_stats['from_former'] += 1
                        else:
                            logger.debug(f"          ⚠️ {name}: координаты не найдены или вне области")
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