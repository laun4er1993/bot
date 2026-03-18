# api_sources.py
# Универсальный парсер для всех районов через dic.academic.ru
# Автоматический поиск страниц районов, сельских поселений и населенных пунктов

import aiohttp
import asyncio
import logging
from typing import List, Dict, Optional, Tuple, Any, Set
import os
import time
import re
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import json
from urllib.parse import quote, urljoin, urlparse
from collections import defaultdict

logger = logging.getLogger(__name__)

# ========== КОНФИГУРАЦИЯ ==========

# Список районов для поиска
DISTRICTS = [
    "Ржевский",
    "Оленинский", 
    "Зубцовский",
    "Бельский"
]

# Базовые URL
DIC_ACADEMIC_BASE_URL = "https://dic.academic.ru"
DIC_ACADEMIC_SEARCH_URL = "https://dic.academic.ru/searchall.php"
DIC_ACADEMIC_ARTICLE_URL = "https://dic.academic.ru/dic.nsf/ruwiki/{}"

# Соответствие сокращений полным названиям типов
TYPE_MAPPING = {
    'дер.': 'деревня',
    'д.': 'деревня',
    'пос.': 'посёлок',
    'п.': 'посёлок',
    'с.': 'село',
    'х.': 'хутор',
    'ур.': 'урочище',
    'ст.': 'станция',
    'разъезд': 'разъезд',
    'ж/д ст.': 'железнодорожная станция',
    'ж/д будка': 'железнодорожная будка',
    'кордон': 'кордон',
    'местечко': 'местечко',
    'сл.': 'слобода',
    'дп': 'дачный посёлок',
    'кп': 'курортный посёлок',
    'рп': 'рабочий посёлок'
}

# Ключевые слова для поиска ссылок на списки НП
LIST_KEYWORDS = [
    "список населённых пунктов",
    "список населенных пунктов",
    "список бывших населённых пунктов",
    "список бывших населенных пунктов",
    "населённые пункты",
    "населенные пункты",
    "бывшие населённые пункты",
    "бывшие населенные пункты"
]

# Ключевые слова для поиска сельских поселений
SETTLEMENT_KEYWORDS = [
    "сельское поселение",
    "сельские поселения",
    "список сельских поселений",
    "муниципальное образование",
    "состав района",
    "муниципальное устройство",
    "административное деление"
]

# Ключевые слова для идентификации страницы района
DISTRICT_KEYWORDS = [
    "муниципальный район",
    "административная единица",
    "районный центр",
    "расположен на юге",
    "граничит с"
]

class APISourceManager:
    """
    Универсальный менеджер для загрузки данных из dic.academic.ru
    Автоматически ищет страницы районов, сельских поселений и населенных пунктов
    """
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.thread_pool = ThreadPoolExecutor(max_workers=5)
        
        # Для rate limiting
        self.request_count = 0
        self.last_request_time = 0
        self.min_request_interval = 0.5  # 500 мс между запросами
        
        # Кэш для найденных ID
        self.article_cache: Dict[str, str] = {}  # запрос -> ID статьи
        self.district_cache: Dict[str, Dict] = {}  # район -> информация о районе
        self.settlement_pages_cache: Dict[str, str] = {}  # название СП -> ID страницы
        self.page_cache: Dict[str, Tuple[str, float]] = {}  # URL -> (HTML, timestamp)
        
        # Время жизни кэша (в секундах)
        self.cache_ttl = 3600  # 1 час
        
        # Стандартные заголовки
        self.default_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
            'Connection': 'keep-alive',
        }
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """Получает или создает HTTP сессию"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def close_session(self):
        """Закрывает HTTP сессию"""
        if self.session and not self.session.closed:
            await self.session.close()
        self.thread_pool.shutdown(wait=False)
    
    async def _rate_limit(self):
        """Соблюдение rate limiting"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        if time_since_last < self.min_request_interval:
            await asyncio.sleep(self.min_request_interval - time_since_last)
        self.last_request_time = time.time()
        self.request_count += 1
    
    async def _fetch_page(self, url: str) -> Optional[str]:
        """Загружает страницу с кэшированием"""
        current_time = time.time()
        
        # Проверяем кэш
        if url in self.page_cache:
            html, timestamp = self.page_cache[url]
            if current_time - timestamp < self.cache_ttl:
                return html
        
        try:
            session = await self._get_session()
            await self._rate_limit()
            
            async with session.get(url, headers=self.default_headers, timeout=30) as response:
                if response.status == 200:
                    html = await response.text()
                    self.page_cache[url] = (html, current_time)
                    return html
                else:
                    logger.debug(f"Ошибка загрузки {url}: HTTP {response.status}")
                    return None
        except Exception as e:
            logger.debug(f"Ошибка загрузки {url}: {e}")
            return None
    
    async def _search_with_pagination(self, query: str, max_pages: int = 3) -> List[Dict]:
        """
        Выполняет поиск с обработкой нескольких страниц результатов
        Возвращает список найденных статей
        """
        all_results = []
        page = 1
        
        while page <= max_pages:
            # Формируем URL с учетом пагинации
            encoded_query = quote(query)
            search_url = f"{DIC_ACADEMIC_SEARCH_URL}?SWord={encoded_query}"
            if page > 1:
                search_url += f"&page={page}"
            
            html = await self._fetch_page(search_url)
            if not html:
                break
            
            # Парсим результаты страницы
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
            
            # Проверяем, есть ли следующая страница
            has_next = await loop.run_in_executor(
                self.thread_pool,
                self._check_next_page,
                html
            )
            
            if not has_next:
                break
            
            page += 1
        
        return all_results
    
    def _parse_search_page(self, html: str, page_num: int) -> List[Dict]:
        """
        Парсит одну страницу результатов поиска
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            # Ищем все ссылки на статьи Википедии
            for link in soup.find_all('a', href=re.compile(r'/dic\.nsf/ruwiki/\d+')):
                href = link.get('href', '')
                title_text = link.get_text().strip()
                
                # Извлекаем ID
                match = re.search(r'/dic\.nsf/ruwiki/(\d+)', href)
                if not match:
                    continue
                    
                article_id = match.group(1)
                
                # Получаем полный текст результата
                parent = link.find_parent()
                full_text = ""
                if parent:
                    # Ищем описание (обычно после ссылки)
                    description = parent.find_next('span', class_='description')
                    if description:
                        full_text = description.get_text().strip()
                    else:
                        full_text = parent.get_text().strip()
                
                # Определяем позицию на странице (по номеру в начале)
                position_match = re.match(r'^(\d+)', full_text)
                position = int(position_match.group(1)) if position_match else 0
                
                results.append({
                    'id': article_id,
                    'title': title_text,
                    'full_text': full_text,
                    'page': page_num,
                    'position': position
                })
            
            return results
            
        except Exception as e:
            logger.error(f"Ошибка парсинга страницы поиска: {e}")
            return []
    
    def _check_next_page(self, html: str) -> bool:
        """
        Проверяет наличие ссылки на следующую страницу
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            # Ищем ссылку "далее" или "следующая"
            next_link = soup.find('a', string=re.compile(r'далее|следующая|next', re.I))
            return next_link is not None
        except:
            return False
    
    async def _find_district_page(self, district: str) -> Optional[Dict]:
        """
        Находит страницу района, анализируя результаты поиска
        """
        cache_key = f"district_{district}"
        if cache_key in self.district_cache:
            return self.district_cache[cache_key]
        
        logger.info(f"  🔍 Поиск страницы района: {district}")
        
        # Варианты поисковых запросов
        queries = [
            f"{district} район",
            f"{district} район Тверская область",
            f"{district} муниципальный район",
            district
        ]
        
        all_results = []
        
        # Собираем результаты по всем запросам
        for query in queries:
            results = await self._search_with_pagination(query, max_pages=2)
            all_results.extend(results)
        
        if not all_results:
            logger.info(f"    ❌ Страница района не найдена")
            return None
        
        # Оцениваем релевантность каждого результата
        for result in all_results:
            score = self._score_district_relevance(result, district)
            result['score'] = score
        
        # Сортируем по убыванию релевантности
        sorted_results = sorted(all_results, key=lambda x: x['score'], reverse=True)
        
        # Берем топ-3 наиболее релевантных
        top_results = sorted_results[:3]
        
        for result in top_results:
            if result['score'] >= 50:  # Порог релевантности
                # Загружаем страницу для проверки
                page_url = DIC_ACADEMIC_ARTICLE_URL.format(result['id'])
                html = await self._fetch_page(page_url)
                
                if html:
                    # Проверяем, что это действительно страница района
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
                            'score': result['score']
                        }
                        
                        self.district_cache[cache_key] = district_info
                        return district_info
        
        logger.info(f"    ❌ Страница района не найдена")
        return None
    
    def _score_district_relevance(self, result: Dict, district: str) -> int:
        """
        Оценивает релевантность результата для страницы района
        """
        title_lower = result['title'].lower()
        full_text_lower = result['full_text'].lower()
        district_lower = district.lower()
        
        score = 0
        
        # 1. Название содержит точное название района
        if f"{district_lower} район" in title_lower:
            score += 100
        elif district_lower in title_lower:
            score += 50
        
        # 2. В названии нет скобок (это не страница конкретного НП)
        if '(' not in result['title']:
            score += 30
        
        # 3. Приоритет первым результатам
        if result['position'] == 1:
            score += 20
        elif result['position'] <= 3:
            score += 10
        
        # 4. Ключевые слова в описании
        for keyword in DISTRICT_KEYWORDS:
            if keyword in full_text_lower:
                score += 15
        
        # 5. Упоминание области
        if "тверская область" in full_text_lower or "тверской области" in full_text_lower:
            score += 10
        
        # 6. Длина описания (чем подробнее, тем лучше)
        if len(full_text_lower) > 100:
            score += 5
        
        return score
    
    def _verify_district_page(self, html: str, district: str) -> bool:
        """
        Проверяет, что страница действительно является страницей района
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Получаем текст страницы
            text = soup.get_text().lower()
            
            # Проверяем наличие ключевых признаков страницы района
            district_lower = district.lower()
            
            # Должно быть упоминание района
            if f"{district_lower} район" not in text:
                return False
            
            # Должны быть характерные разделы
            expected_sections = ['география', 'история', 'население', 'состав района']
            found_sections = 0
            
            for section in expected_sections:
                if section in text:
                    found_sections += 1
            
            # Если нашли хотя бы 2 раздела из списка - это страница района
            return found_sections >= 2
            
        except Exception as e:
            logger.error(f"Ошибка проверки страницы района: {e}")
            return False
    
    async def _extract_settlements(self, district_id: str, district: str) -> List[str]:
        """
        Извлекает список сельских поселений со страницы района
        """
        cache_key = f"settlements_{district}"
        if cache_key in self.settlement_pages_cache:
            # Возвращаем как есть, это не ID, а список
            pass
        
        url = DIC_ACADEMIC_ARTICLE_URL.format(district_id)
        html = await self._fetch_page(url)
        
        if not html:
            return []
        
        loop = asyncio.get_event_loop()
        settlements = await loop.run_in_executor(
            self.thread_pool,
            self._parse_settlements_from_page,
            html
        )
        
        if settlements:
            logger.info(f"    Найдено сельских поселений: {len(settlements)}")
        
        return settlements
    
    def _parse_settlements_from_page(self, html: str) -> List[str]:
        """
        Парсит страницу района для извлечения списка сельских поселений
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            settlements = []
            
            # Ищем разделы с сельскими поселениями
            for keyword in SETTLEMENT_KEYWORDS:
                # Ищем заголовки
                for header in soup.find_all(['h2', 'h3', 'h4']):
                    if keyword in header.get_text().lower():
                        # Ищем списки после заголовка
                        parent = header.find_parent()
                        if parent:
                            # Ищем маркированные списки
                            for ul in parent.find_all('ul'):
                                for li in ul.find_all('li'):
                                    text = li.get_text().strip()
                                    # Фильтруем: длина от 2 до 50 символов, не содержит цифр
                                    if (text and 2 <= len(text) <= 50 and 
                                        not re.search(r'\d', text) and
                                        not text.startswith('см. также')):
                                        settlements.append(text)
                            
                            # Ищем таблицы
                            for table in parent.find_all('table'):
                                for row in table.find_all('tr'):
                                    cells = row.find_all('td')
                                    for cell in cells:
                                        text = cell.get_text().strip()
                                        if (text and 2 <= len(text) <= 50 and
                                            not re.search(r'\d', text) and
                                            not text.startswith('см. также')):
                                            settlements.append(text)
            
            # Удаляем дубликаты и пустые значения
            unique_settlements = []
            seen = set()
            
            for s in settlements:
                # Нормализуем название (убираем лишние пробелы, приводим к правильному регистру)
                s_clean = ' '.join(s.split())
                if s_clean and s_clean not in seen and len(s_clean) > 2:
                    # Проверяем, что это похоже на название СП
                    if any(keyword in s_clean.lower() for keyword in ['ское', 'ское сп', 'поселение']):
                        unique_settlements.append(s_clean)
                        seen.add(s_clean)
                    elif not any(word in s_clean.lower() for word in ['всего', 'итого', 'страница']):
                        unique_settlements.append(s_clean)
                        seen.add(s_clean)
            
            return unique_settlements
            
        except Exception as e:
            logger.error(f"Ошибка парсинга сельских поселений: {e}")
            return []
    
    async def _find_settlement_page(self, settlement: str, district: str) -> Optional[str]:
        """
        Находит страницу с бывшими населенными пунктами для сельского поселения
        """
        cache_key = f"settlement_page_{district}_{settlement}"
        if cache_key in self.settlement_pages_cache:
            return self.settlement_pages_cache[cache_key]
        
        # Варианты поисковых запросов для СП
        queries = [
            f"Список бывших населённых пунктов на территории сельского поселения {settlement} {district} района",
            f"Список бывших населенных пунктов на территории сельского поселения {settlement} {district} района",
            f"Список бывших населённых пунктов {settlement} {district} района",
            f"Бывшие населённые пункты {settlement} СП",
            f"Список бывших населённых пунктов {settlement} сельского поселения",
            f"{settlement} сельское поселение бывшие населенные пункты"
        ]
        
        all_results = []
        
        for query in queries:
            results = await self._search_with_pagination(query, max_pages=2)
            all_results.extend(results)
        
        if not all_results:
            return None
        
        # Оцениваем релевантность
        for result in all_results:
            score = self._score_settlement_relevance(result, settlement, district)
            result['score'] = score
        
        # Берем лучший результат
        best = max(all_results, key=lambda x: x['score'])
        
        if best['score'] >= 40:
            logger.info(f"      Найдена страница для СП {settlement} (ID: {best['id']}, score: {best['score']})")
            self.settlement_pages_cache[cache_key] = best['id']
            return best['id']
        
        return None
    
    def _score_settlement_relevance(self, result: Dict, settlement: str, district: str) -> int:
        """
        Оценивает релевантность результата для страницы сельского поселения
        """
        title_lower = result['title'].lower()
        full_text_lower = result['full_text'].lower()
        settlement_lower = settlement.lower()
        district_lower = district.lower()
        
        score = 0
        
        # 1. Название содержит название СП
        if settlement_lower in title_lower:
            score += 50
        
        # 2. В названии есть ключевые слова
        if "список бывших" in title_lower:
            score += 40
        elif "бывшие населённые" in title_lower:
            score += 30
        
        # 3. Упоминание района
        if district_lower in title_lower or district_lower in full_text_lower:
            score += 20
        
        # 4. Упоминание сельского поселения
        if "сельское поселение" in title_lower or "сельского поселения" in title_lower:
            score += 25
        
        # 5. Приоритет первым результатам
        if result['position'] == 1:
            score += 15
        elif result['position'] <= 3:
            score += 10
        
        return score
    
    async def _parse_settlement_page(self, article_id: str, district: str, settlement: str) -> List[Dict]:
        """
        Парсит страницу с бывшими населенными пунктами сельского поселения
        """
        url = DIC_ACADEMIC_ARTICLE_URL.format(article_id)
        html = await self._fetch_page(url)
        
        if not html:
            return []
        
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self.thread_pool,
            self._parse_settlement_page_html,
            html,
            article_id,
            district,
            settlement
        )
    
    def _parse_settlement_page_html(self, html: str, article_id: str, district: str, settlement: str) -> List[Dict]:
        """
        Парсит HTML страницы с бывшими НП сельского поселения
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            # Ищем таблицу с данными
            tables = soup.find_all('table', class_=['standard', 'sortable'])
            
            for table in tables:
                rows = table.find_all('tr')
                if len(rows) < 2:
                    continue
                
                # Определяем заголовки
                header_cells = rows[0].find_all(['th', 'td'])
                headers = [h.get_text().strip().lower() for h in header_cells]
                
                # Ищем индексы нужных колонок
                name_idx = self._find_column_index(headers, ['населённый пункт', 'название'])
                type_idx = self._find_column_index(headers, ['тип'])
                coords_idx = self._find_column_index(headers, ['координаты', 'коорд'])
                
                for row in rows[1:]:
                    try:
                        cells = row.find_all('td')
                        if len(cells) < max(filter(None, [name_idx, type_idx])) + 1:
                            continue
                        
                        # Название
                        if name_idx is not None and name_idx < len(cells):
                            name = cells[name_idx].get_text().strip()
                        else:
                            continue
                        
                        if not name or name in ['ИТОГО', 'Всего']:
                            continue
                        
                        # Тип
                        village_type = 'деревня'
                        if type_idx is not None and type_idx < len(cells):
                            raw_type = cells[type_idx].get_text().strip()
                            village_type = self._expand_type(raw_type)
                        
                        # Координаты
                        lat, lon = None, None
                        if coords_idx is not None and coords_idx < len(cells):
                            lat, lon = self._parse_coordinates_universal('', cells[coords_idx])
                        
                        # Формируем запись
                        results.append({
                            "name": name,
                            "type": village_type,
                            "lat": str(round(lat, 5)) if lat else "",
                            "lon": str(round(lon, 5)) if lon else "",
                            "source": f"dic.academic.ru (ID: {article_id})",
                            "district": district,
                            "settlement": settlement,
                            "status": "abandoned",
                            "notes": f"<i>Источник: dic.academic.ru, {settlement} СП</i>"
                        })
                        
                    except Exception as e:
                        continue
            
            return results
            
        except Exception as e:
            logger.error(f"Ошибка парсинга страницы СП: {e}")
            return []
    
    async def fetch_district_data(self, district: str) -> Dict[str, List[Dict]]:
        """
        Основной метод: загружает данные для конкретного района
        """
        logger.info(f"🌐 Загрузка данных для района: {district}")
        
        results = {
            "total": [],
            "sources": {},
            "settlements": {},
            "region": "Тверская область"
        }
        
        # Шаг 1: Находим страницу района
        district_info = await self._find_district_page(district)
        
        if not district_info:
            logger.warning(f"  ⚠️ Страница района не найдена")
            return results
        
        # Шаг 2: Извлекаем список сельских поселений
        settlements = await self._extract_settlements(district_info['id'], district)
        
        if not settlements:
            logger.warning(f"  ⚠️ Сельские поселения не найдены на странице района")
            return results
        
        # Шаг 3: Для каждого сельского поселения ищем страницу с бывшими НП
        logger.info(f"  🔍 Поиск страниц для {len(settlements)} сельских поселений...")
        
        for settlement in settlements:
            try:
                # Ищем страницу СП
                article_id = await self._find_settlement_page(settlement, district)
                
                if article_id:
                    # Парсим страницу СП
                    data = await self._parse_settlement_page(article_id, district, settlement)
                    
                    if data:
                        results["settlements"][settlement] = len(data)
                        results["total"].extend(data)
                        logger.info(f"    ✅ СП {settlement}: {len(data)} записей")
                    else:
                        logger.info(f"    ⏭️ СП {settlement}: страница найдена, но нет данных")
                        results["settlements"][settlement] = 0
                else:
                    logger.info(f"    ⏭️ СП {settlement}: страница не найдена")
                    results["settlements"][settlement] = 0
                    
            except Exception as e:
                logger.error(f"    ❌ Ошибка обработки СП {settlement}: {e}")
                results["settlements"][settlement] = 0
        
        # Удаляем дубликаты с приоритетом записей с координатами
        unique_results = self._deduplicate_with_priority(results["total"])
        results["total"] = unique_results
        
        logger.info(f"  ✅ Всего уникальных записей: {len(results['total'])}")
        
        return results
    
    def _find_column_index(self, headers: List[str], possible_names: List[str]) -> Optional[int]:
        """Находит индекс колонки по возможным названиям"""
        for i, header in enumerate(headers):
            for name in possible_names:
                if name in header:
                    return i
        return None
    
    def _expand_type(self, short_type: str) -> str:
        """Преобразует сокращение в полное название типа"""
        if not short_type:
            return 'деревня'
        
        clean_type = short_type.rstrip('.').lower().strip()
        
        for short, full in TYPE_MAPPING.items():
            if clean_type == short.rstrip('.'):
                return full
        
        if clean_type in TYPE_MAPPING.values():
            return clean_type
        
        return 'деревня'
    
    def _parse_coordinates_universal(self, text: str, cell=None) -> Tuple[Optional[float], Optional[float]]:
        """Универсальный парсер координат для всех форматов"""
        try:
            # Ищем в скрытых span
            if cell:
                geo_span = cell.find('span', class_='geo')
                if geo_span:
                    lat_span = geo_span.find('span', class_='latitude')
                    lon_span = geo_span.find('span', class_='longitude')
                    
                    if lat_span and lon_span:
                        try:
                            lat = float(lat_span.get_text().strip())
                            lon = float(lon_span.get_text().strip())
                            if self._validate_coordinates(lat, lon):
                                return lat, lon
                        except:
                            pass
            
            # DMS формат (56°13′41.16″ с. ш. 34°08′10.32″ в. д.)
            dms_pattern = r'(\d+)°(\d+)′([\d.]+)″.*?(\d+)°(\d+)′([\d.]+)″'
            match = re.search(dms_pattern, text)
            
            if match:
                lat_deg, lat_min, lat_sec = map(float, match.group(1, 2, 3))
                lon_deg, lon_min, lon_sec = map(float, match.group(4, 5, 6))
                
                lat = lat_deg + lat_min/60 + lat_sec/3600
                lon = lon_deg + lon_min/60 + lon_sec/3600
                
                if self._validate_coordinates(lat, lon):
                    return lat, lon
            
            # Десятичные с пробелом или запятой
            decimal_pattern = r'([0-9]+\.[0-9]+)[,\s]+([0-9]+\.[0-9]+)'
            match = re.search(decimal_pattern, text)
            
            if match:
                lat = float(match.group(1))
                lon = float(match.group(2))
                
                if self._validate_coordinates(lat, lon):
                    return lat, lon
            
            return None, None
            
        except Exception:
            return None, None
    
    def _validate_coordinates(self, lat: float, lon: float) -> bool:
        """Проверяет координаты (примерные границы для Тверской области)"""
        return (55.0 <= lat <= 58.0 and 30.0 <= lon <= 38.0)
    
    def _deduplicate_with_priority(self, items: List[Dict]) -> List[Dict]:
        """
        Удаляет дубликаты с приоритетом записей с координатами
        """
        unique: Dict[str, Dict] = {}
        
        for item in items:
            # Ключ: название + район + сельское поселение
            key = f"{item['name']}_{item['district']}_{item.get('settlement', '')}"
            
            if key not in unique:
                unique[key] = item
            else:
                existing = unique[key]
                
                # Приоритет: запись с координатами
                if not existing.get('lat') and item.get('lat'):
                    unique[key] = item
                elif existing.get('lat') and not item.get('lat'):
                    pass  # оставляем существующую
                elif existing.get('lat') and item.get('lat'):
                    # Если обе с координатами, объединяем notes
                    existing['notes'] = existing.get('notes', '') + "<br>" + item.get('notes', '')
                else:
                    # Если обе без координат, объединяем notes
                    existing['notes'] = existing.get('notes', '') + "<br>" + item.get('notes', '')
        
        return list(unique.values())

# Экспортируем список районов для использования в bot.py
AVAILABLE_DISTRICTS = DISTRICTS