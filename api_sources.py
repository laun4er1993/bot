# api_sources.py
# Универсальный парсер для всех районов Тверской области
# Поддерживает: Википедию, РУВИКИ, ГАРАНТ, НашиПредки, dic.academic.ru и другие источники

import aiohttp
import asyncio
import logging
from typing import List, Dict, Optional, Tuple, Any
import os
import time
import re
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import json

logger = logging.getLogger(__name__)

# Конфигурация районов и их источников
SOURCES_CONFIG = {
    "Ржевский": [
        {
            "name": "Википедия (бывшие НП)",
            "url": "https://ru.wikipedia.org/wiki/Проект:Населённые_пункты_России/Списки/Бывшие_НП_на_территории_Ржевского_района_Тверской_области",
            "parser_type": "wikipedia",
            "status": "abandoned"
        },
        {
            "name": "РУВИКИ (бывшие НП)",
            "url": "https://ru.ruwiki.ru/wiki/Список_бывших_населённых_пунктов_на_территории_Ржевского_района_Тверской_области",
            "parser_type": "wikipedia",
            "status": "abandoned"
        },
        {
            "name": "Список населенных пунктов",
            "url": "https://ru.wikipedia.org/wiki/Список_населённых_пунктов_Ржевского_района_Тверской_области",
            "parser_type": "wikipedia_settlements",
            "status": "existing"
        }
    ],
    "Оленинский": [
        {
            "name": "Википедия (бывшие НП)",
            "url": "https://ru.m.wikipedia.org/wiki/Проект:Россия/Списки/Список_бывших_населённых_пунктов_на_территории_Оленинского_района_Тверской_области",
            "parser_type": "wikipedia",
            "status": "abandoned"
        },
        {
            "name": "Руниверсалис (все НП)",
            "url": "https://xn--h1ajim.xn--p1ai/Шаблон:Населённые_пункты_Оленинского_района",
            "parser_type": "runiversalis",
            "status": "existing"
        }
    ],
    "Зубцовский": [
        {
            "name": "ГАРАНТ (официальный перечень)",
            "url": "https://base.garant.ru/404445454/89300effb84a59912210b23abe10a68f/",
            "parser_type": "garant",
            "status": "existing"
        },
        {
            "name": "НашиПредки (база данных)",
            "url": "https://nashipredki.com/russia/tverskaya-oblast/zubtsovskiy-rayon",
            "parser_type": "nashipredki",
            "status": "historical"
        }
    ],
    "Бельский": [
        {
            "name": "dic.academic.ru",
            "url": "https://dic.academic.ru/dic.nsf/ruwiki/1635988",
            "parser_type": "academic",
            "status": "abandoned"
        }
    ]
}

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

class APISourceManager:
    """
    Универсальный менеджер для загрузки данных из всех доступных источников
    """
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.thread_pool = ThreadPoolExecutor(max_workers=3)
        
        # Для rate limiting
        self.request_count = 0
        self.last_request_time = 0
        self.min_request_interval = 0.5  # 500 мс между запросами
        
        # Стандартные заголовки для всех запросов
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
    
    async def fetch_district_data(self, district: str) -> Dict[str, List[Dict]]:
        """
        Загружает данные для конкретного района из всех доступных источников
        """
        if district not in SOURCES_CONFIG:
            raise ValueError(f"Неизвестный район: {district}")
        
        logger.info(f"🌐 Загрузка данных для района: {district}")
        
        results = {
            "total": [],
            "sources": {}
        }
        
        sources = SOURCES_CONFIG[district]
        
        for source in sources:
            try:
                logger.info(f"  🔍 Источник: {source['name']}")
                
                # Выбираем парсер в зависимости от типа источника
                if source['parser_type'] == "wikipedia":
                    parsed_data = await self._parse_wikipedia_source(source)
                elif source['parser_type'] == "wikipedia_settlements":
                    parsed_data = await self._parse_wikipedia_settlements(source)
                elif source['parser_type'] == "runiversalis":
                    parsed_data = await self._parse_runiversalis(source)
                elif source['parser_type'] == "garant":
                    parsed_data = await self._parse_garant(source)
                elif source['parser_type'] == "nashipredki":
                    parsed_data = await self._parse_nashipredki(source)
                elif source['parser_type'] == "academic":
                    parsed_data = await self._parse_academic(source)
                else:
                    logger.warning(f"    ⚠️ Неизвестный тип парсера: {source['parser_type']}")
                    continue
                
                results["sources"][source['name']] = len(parsed_data)
                results["total"].extend(parsed_data)
                logger.info(f"    ✅ Найдено записей: {len(parsed_data)}")
                
            except Exception as e:
                logger.error(f"    ❌ Ошибка при парсинге {source['name']}: {e}")
                results["sources"][source['name']] = 0
        
        # Удаляем дубликаты
        unique_results = self._deduplicate_results(results["total"])
        results["total"] = unique_results
        
        logger.info(f"  ✅ Всего уникальных записей: {len(results['total'])}")
        
        # Статистика по источникам
        logger.info(f"  📊 Статистика по источникам:")
        for source_name, count in results["sources"].items():
            logger.info(f"    • {source_name}: {count}")
        
        return results
    
    async def _parse_wikipedia_source(self, source: Dict) -> List[Dict]:
        """
        Универсальный парсер для страниц Википедии/РУВИКИ с таблицами
        """
        session = await self._get_session()
        await self._rate_limit()
        
        results = []
        
        try:
            async with session.get(source['url'], headers=self.default_headers, timeout=45) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    loop = asyncio.get_event_loop()
                    parsed = await loop.run_in_executor(
                        self.thread_pool,
                        self._parse_wikipedia_table_html,
                        html,
                        source
                    )
                    
                    results.extend(parsed)
                else:
                    logger.error(f"    ❌ Ошибка загрузки: HTTP {response.status}")
                    
        except asyncio.TimeoutError:
            logger.error(f"    ❌ Таймаут при загрузке {source['url']}")
        except Exception as e:
            logger.error(f"    ❌ Ошибка: {e}")
        
        return results
    
    def _parse_wikipedia_table_html(self, html: str, source: Dict) -> List[Dict]:
        """
        Парсит HTML таблицы Википедии
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            # Ищем все таблицы
            tables = soup.find_all('table', class_=['wikitable', 'standard', 'sortable'])
            
            if not tables:
                logger.warning(f"    Не найдено таблиц на странице")
                return []
            
            for table in tables:
                rows = table.find_all('tr')
                
                if len(rows) < 2:
                    continue
                
                # Определяем заголовки для понимания структуры
                header_cells = rows[0].find_all(['th', 'td'])
                headers = [h.get_text().strip().lower() for h in header_cells]
                
                # Ищем индексы нужных колонок
                name_idx = self._find_column_index(headers, ['населённый пункт', 'название', 'наименование'])
                type_idx = self._find_column_index(headers, ['тип'])
                coords_idx = self._find_column_index(headers, ['координаты', 'координаты', 'коорд'])
                year_idx = self._find_column_index(headers, ['год', 'упразднения', 'год упразднения'])
                note_idx = self._find_column_index(headers, ['примечание', 'примечания'])
                
                for row in rows[1:]:
                    try:
                        cells = row.find_all('td')
                        if len(cells) < max(filter(None, [name_idx, type_idx, coords_idx])) + 1:
                            continue
                        
                        # Название
                        if name_idx is not None and name_idx < len(cells):
                            name = cells[name_idx].get_text().strip()
                        else:
                            continue
                        
                        if not name or name in ['ИТОГО', 'Всего']:
                            continue
                        
                        # Тип
                        village_type = 'деревня'  # по умолчанию
                        if type_idx is not None and type_idx < len(cells):
                            raw_type = cells[type_idx].get_text().strip()
                            village_type = self._expand_type(raw_type)
                        
                        # Координаты
                        lat, lon = None, None
                        if coords_idx is not None and coords_idx < len(cells):
                            coords_text = cells[coords_idx].get_text().strip()
                            lat, lon = self._parse_coordinates_universal(coords_text, cells[coords_idx])
                        
                        # Дополнительная информация
                        notes_parts = [f"<i>Источник: {source['name']}</i>"]
                        
                        if year_idx is not None and year_idx < len(cells):
                            year_text = cells[year_idx].get_text().strip()
                            if year_text and year_text not in ['—', '-', '']:
                                notes_parts.append(f"год: {year_text}")
                        
                        if note_idx is not None and note_idx < len(cells):
                            note_text = cells[note_idx].get_text().strip()
                            if note_text and note_text not in ['—', '-', '']:
                                notes_parts.append(note_text)
                        
                        village = {
                            "name": name,
                            "type": village_type,
                            "lat": str(round(lat, 5)) if lat else "",
                            "lon": str(round(lon, 5)) if lon else "",
                            "source": source['name'],
                            "district": source['url'].split('/')[-1].split('_')[0] if '_' in source['url'] else "Ржевский",
                            "status": source.get('status', 'existing'),
                            "notes": "<br>".join(notes_parts)
                        }
                        
                        results.append(village)
                        
                    except Exception as e:
                        logger.debug(f"    Ошибка парсинга строки: {e}")
                        continue
            
            return results
            
        except Exception as e:
            logger.error(f"    ❌ Ошибка в _parse_wikipedia_table_html: {e}")
            return []
    
    async def _parse_wikipedia_settlements(self, source: Dict) -> List[Dict]:
        """
        Парсер для списков населенных пунктов (обычно с населением)
        """
        session = await self._get_session()
        await self._rate_limit()
        
        results = []
        
        try:
            async with session.get(source['url'], headers=self.default_headers, timeout=45) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    loop = asyncio.get_event_loop()
                    parsed = await loop.run_in_executor(
                        self.thread_pool,
                        self._parse_settlements_list_html,
                        html,
                        source
                    )
                    
                    results.extend(parsed)
                else:
                    logger.error(f"    ❌ Ошибка загрузки: HTTP {response.status}")
                    
        except Exception as e:
            logger.error(f"    ❌ Ошибка: {e}")
        
        return results
    
    def _parse_settlements_list_html(self, html: str, source: Dict) -> List[Dict]:
        """
        Парсит списки населенных пунктов (часто с населением)
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            # Ищем таблицы с населенными пунктами
            tables = soup.find_all('table', class_=['wikitable', 'standard'])
            
            for table in tables:
                rows = table.find_all('tr')
                if len(rows) < 2:
                    continue
                
                for row in rows[1:]:
                    try:
                        cells = row.find_all('td')
                        if len(cells) < 2:
                            continue
                        
                        # Первая ячейка обычно содержит название
                        name_cell = cells[0]
                        name = name_cell.get_text().strip()
                        
                        if not name or name in ['Итого', 'Всего']:
                            continue
                        
                        # Пытаемся найти тип (обычно во второй ячейке или в скобках)
                        village_type = 'деревня'
                        
                        # Ищем тип в тексте
                        full_text = ' '.join([c.get_text().strip() for c in cells])
                        
                        # Проверяем наличие ключевых слов
                        type_keywords = ['деревня', 'село', 'посёлок', 'хутор', 'урочище']
                        for kw in type_keywords:
                            if kw in full_text.lower():
                                village_type = kw
                                break
                        
                        # Ищем население
                        population = None
                        for cell in cells:
                            text = cell.get_text().strip()
                            if text.replace(' ', '').replace(',', '').replace('.', '').isdigit():
                                population = text
                                break
                        
                        notes_parts = [f"<i>Источник: {source['name']}</i>"]
                        if population:
                            notes_parts.append(f"население: {population} чел.")
                        
                        results.append({
                            "name": name,
                            "type": village_type,
                            "lat": "",
                            "lon": "",
                            "source": source['name'],
                            "district": "Ржевский",
                            "status": source.get('status', 'existing'),
                            "notes": "<br>".join(notes_parts)
                        })
                        
                    except Exception as e:
                        continue
            
            return results
            
        except Exception as e:
            return []
    
    async def _parse_runiversalis(self, source: Dict) -> List[Dict]:
        """
        Парсер для Руниверсалис (простые списки)
        """
        session = await self._get_session()
        await self._rate_limit()
        
        results = []
        
        try:
            async with session.get(source['url'], headers=self.default_headers, timeout=45) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    loop = asyncio.get_event_loop()
                    parsed = await loop.run_in_executor(
                        self.thread_pool,
                        self._parse_runiversalis_html,
                        html,
                        source
                    )
                    
                    results.extend(parsed)
                else:
                    logger.error(f"    ❌ Ошибка загрузки: HTTP {response.status}")
                    
        except Exception as e:
            logger.error(f"    ❌ Ошибка: {e}")
        
        return results
    
    def _parse_runiversalis_html(self, html: str, source: Dict) -> List[Dict]:
        """
        Парсит простые списки Руниверсалис
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            # Ищем списки
            lists = soup.find_all(['ul', 'ol'])
            
            for lst in lists:
                items = lst.find_all('li')
                
                for item in items:
                    text = item.get_text().strip()
                    if not text or len(text) < 3:
                        continue
                    
                    # Пытаемся разделить тип и название
                    name = text
                    village_type = 'деревня'
                    
                    # Часто формат: "Название (тип)" или "тип Название"
                    type_patterns = [
                        (r'^([а-я]+)\s+(.+)$', 1, 2),  # "деревня Название"
                        (r'^(.+)\s+\(([ая]+)\)$', 1, 2),  # "Название (деревня)"
                        (r'^(.+),\s+([ая]+)$', 1, 2),  # "Название, деревня"
                    ]
                    
                    for pattern, name_group, type_group in type_patterns:
                        match = re.search(pattern, text, re.IGNORECASE)
                        if match:
                            possible_type = match.group(type_group).lower()
                            if possible_type in TYPE_MAPPING.values() or possible_type in ['деревня', 'село', 'посёлок']:
                                name = match.group(name_group).strip()
                                village_type = possible_type
                                break
                    
                    results.append({
                        "name": name,
                        "type": village_type,
                        "lat": "",
                        "lon": "",
                        "source": source['name'],
                        "district": "Оленинский",
                        "status": source.get('status', 'existing'),
                        "notes": f"<i>Источник: {source['name']}</i>"
                    })
            
            return results
            
        except Exception as e:
            return []
    
    async def _parse_garant(self, source: Dict) -> List[Dict]:
        """
        Парсер для сайта ГАРАНТ (официальные перечни)
        """
        session = await self._get_session()
        await self._rate_limit()
        
        results = []
        
        try:
            async with session.get(source['url'], headers=self.default_headers, timeout=45) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    loop = asyncio.get_event_loop()
                    parsed = await loop.run_in_executor(
                        self.thread_pool,
                        self._parse_garant_html,
                        html,
                        source
                    )
                    
                    results.extend(parsed)
                else:
                    logger.error(f"    ❌ Ошибка загрузки: HTTP {response.status}")
                    
        except Exception as e:
            logger.error(f"    ❌ Ошибка: {e}")
        
        return results
    
    def _parse_garant_html(self, html: str, source: Dict) -> List[Dict]:
        """
        Парсит официальные перечни ГАРАНТ
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            # Ищем нумерованные списки
            lists = soup.find_all(['ol', 'ul'])
            
            for lst in lists:
                items = lst.find_all('li')
                
                for item in items:
                    text = item.get_text().strip()
                    if not text or len(text) < 3:
                        continue
                    
                    # Убираем номер в начале
                    text = re.sub(r'^\d+[\.\)]\s*', '', text)
                    
                    # Определяем тип (часто в скобках)
                    village_type = 'деревня'
                    name = text
                    
                    type_match = re.search(r'\(([^)]+)\)', text)
                    if type_match:
                        possible_type = type_match.group(1).lower()
                        if possible_type in ['деревня', 'село', 'посёлок', 'хутор']:
                            village_type = possible_type
                            name = text.replace(f'({possible_type})', '').strip()
                    
                    results.append({
                        "name": name,
                        "type": village_type,
                        "lat": "",
                        "lon": "",
                        "source": source['name'],
                        "district": "Зубцовский",
                        "status": source.get('status', 'existing'),
                        "notes": f"<i>Источник: {source['name']}</i>"
                    })
            
            return results
            
        except Exception as e:
            return []
    
    async def _parse_nashipredki(self, source: Dict) -> List[Dict]:
        """
        Парсер для сайта НашиПредки
        """
        session = await self._get_session()
        await self._rate_limit()
        
        results = []
        
        try:
            async with session.get(source['url'], headers=self.default_headers, timeout=45) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    loop = asyncio.get_event_loop()
                    parsed = await loop.run_in_executor(
                        self.thread_pool,
                        self._parse_nashipredki_html,
                        html,
                        source
                    )
                    
                    results.extend(parsed)
                else:
                    logger.error(f"    ❌ Ошибка загрузки: HTTP {response.status}")
                    
        except Exception as e:
            logger.error(f"    ❌ Ошибка: {e}")
        
        return results
    
    def _parse_nashipredki_html(self, html: str, source: Dict) -> List[Dict]:
        """
        Парсит базу данных НашиПредки
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            # Ищем таблицы с населенными пунктами
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                if len(rows) < 2:
                    continue
                
                for row in rows[1:]:
                    try:
                        cells = row.find_all('td')
                        if len(cells) < 2:
                            continue
                        
                        # Первая ячейка - название
                        name = cells[0].get_text().strip()
                        
                        # Вторая ячейка - тип или дополнительная информация
                        type_text = cells[1].get_text().strip().lower() if len(cells) > 1 else ''
                        
                        village_type = 'деревня'
                        for short, full in TYPE_MAPPING.items():
                            if short in type_text or full in type_text:
                                village_type = full
                                break
                        
                        # Ищем координаты (редко, но бывает)
                        lat, lon = None, None
                        for cell in cells:
                            coords = self._parse_coordinates_universal(cell.get_text(), cell)
                            if coords[0] and coords[1]:
                                lat, lon = coords
                                break
                        
                        # Дополнительная информация
                        notes_parts = [f"<i>Источник: {source['name']}</i>"]
                        
                        if len(cells) > 2:
                            extra = cells[2].get_text().strip()
                            if extra and extra not in name:
                                notes_parts.append(extra)
                        
                        results.append({
                            "name": name,
                            "type": village_type,
                            "lat": str(round(lat, 5)) if lat else "",
                            "lon": str(round(lon, 5)) if lon else "",
                            "source": source['name'],
                            "district": "Зубцовский",
                            "status": source.get('status', 'historical'),
                            "notes": "<br>".join(notes_parts)
                        })
                        
                    except Exception as e:
                        continue
            
            return results
            
        except Exception as e:
            return []
    
    async def _parse_academic(self, source: Dict) -> List[Dict]:
        """
        Парсер для dic.academic.ru (унифицированная версия)
        """
        session = await self._get_session()
        await self._rate_limit()
        
        results = []
        
        try:
            async with session.get(source['url'], headers=self.default_headers, timeout=45) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    loop = asyncio.get_event_loop()
                    parsed = await loop.run_in_executor(
                        self.thread_pool,
                        self._parse_academic_html,
                        html,
                        source
                    )
                    
                    results.extend(parsed)
                else:
                    logger.error(f"    ❌ Ошибка загрузки: HTTP {response.status}")
                    
        except Exception as e:
            logger.error(f"    ❌ Ошибка: {e}")
        
        return results
    
    def _parse_academic_html(self, html: str, source: Dict) -> List[Dict]:
        """
        Парсит HTML dic.academic.ru
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            results = []
            
            # Ищем таблицу с данными
            table = soup.find('table', class_='standard sortable')
            
            if not table:
                return []
            
            rows = table.find_all('tr')
            
            for row in rows[1:]:
                try:
                    cells = row.find_all('td')
                    if len(cells) < 6:
                        continue
                    
                    # Название
                    name = cells[0].get_text().strip()
                    
                    # Тип
                    raw_type = cells[1].get_text().strip()
                    village_type = self._expand_type(raw_type)
                    
                    # Координаты
                    lat, lon = self._parse_coordinates_universal('', cells[5])
                    
                    # Год упразднения
                    year = ''
                    if len(cells) > 4:
                        year = cells[4].get_text().strip()
                    
                    notes_parts = [f"<i>Источник: {source['name']}</i>"]
                    if year and year not in ['—', '-', '']:
                        notes_parts.append(f"упразднён в {year} г.")
                    
                    results.append({
                        "name": name,
                        "type": village_type,
                        "lat": str(round(lat, 5)) if lat else "",
                        "lon": str(round(lon, 5)) if lon else "",
                        "source": source['name'],
                        "district": "Бельский",
                        "status": source.get('status', 'abandoned'),
                        "notes": "<br>".join(notes_parts)
                    })
                    
                except Exception as e:
                    continue
            
            return results
            
        except Exception as e:
            return []
    
    def _find_column_index(self, headers: List[str], possible_names: List[str]) -> Optional[int]:
        """
        Находит индекс колонки по возможным названиям
        """
        for i, header in enumerate(headers):
            for name in possible_names:
                if name in header:
                    return i
        return None
    
    def _expand_type(self, short_type: str) -> str:
        """
        Преобразует сокращение в полное название типа
        """
        if not short_type:
            return 'деревня'
        
        # Убираем точку в конце
        clean_type = short_type.rstrip('.').lower().strip()
        
        # Проверяем по словарю
        for short, full in TYPE_MAPPING.items():
            if clean_type == short.rstrip('.'):
                return full
        
        # Если это уже полное название
        if clean_type in TYPE_MAPPING.values():
            return clean_type
        
        # По умолчанию
        return 'деревня'
    
    def _parse_coordinates_universal(self, text: str, cell=None) -> Tuple[Optional[float], Optional[float]]:
        """
        Универсальный парсер координат для всех форматов
        Приоритет: сначала ищем в скрытых span, потом в тексте
        """
        try:
            # 1. Ищем в скрытых span (как на dic.academic.ru)
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
            
            # 2. Формат DMS (градусы, минуты, секунды)
            # Пример: 56°20′54.6″ с. ш. 34°10′02.99″ в. д.
            dms_pattern = r'(\d+)°(\d+)′([\d.]+)″.*?(\d+)°(\d+)′([\d.]+)″'
            match = re.search(dms_pattern, text)
            
            if match:
                lat_deg, lat_min, lat_sec = map(float, match.group(1, 2, 3))
                lon_deg, lon_min, lon_sec = map(float, match.group(4, 5, 6))
                
                lat = lat_deg + lat_min/60 + lat_sec/3600
                lon = lon_deg + lon_min/60 + lon_sec/3600
                
                if self._validate_coordinates(lat, lon):
                    return lat, lon
            
            # 3. Десятичные с пробелом или запятой
            # Пример: 55.8324 33.3499 или 55.8324,33.3499
            decimal_pattern = r'([0-9]+\.[0-9]+)[,\s]+([0-9]+\.[0-9]+)'
            match = re.search(decimal_pattern, text)
            
            if match:
                lat = float(match.group(1))
                lon = float(match.group(2))
                
                if self._validate_coordinates(lat, lon):
                    return lat, lon
            
            # 4. Просто два числа через пробел
            numbers = re.findall(r'[\d.]+', text)
            if len(numbers) >= 2:
                lat = float(numbers[0])
                lon = float(numbers[1])
                
                if self._validate_coordinates(lat, lon):
                    return lat, lon
            
            return None, None
            
        except Exception as e:
            return None, None
    
    def _validate_coordinates(self, lat: float, lon: float) -> bool:
        """
        Проверяет, что координаты находятся в пределах Тверской области
        """
        # Примерные границы Тверской области
        return (55.0 <= lat <= 58.0 and 30.0 <= lon <= 38.0)
    
    def _deduplicate_results(self, results: List[Dict]) -> List[Dict]:
        """
        Удаляет дубликаты, объединяя информацию из разных источников
        """
        unique = {}
        
        for item in results:
            # Ключ: название + округленные координаты (если есть)
            lat = round(float(item.get('lat', 0)), 4) if item.get('lat') else 0
            lon = round(float(item.get('lon', 0)), 4) if item.get('lon') else 0
            key = f"{item['name']}_{lat}_{lon}"
            
            if key not in unique:
                unique[key] = item
            else:
                # Объединяем информацию
                existing = unique[key]
                
                # Если у существующей нет координат, а у новой есть - обновляем
                if not existing.get('lat') and item.get('lat'):
                    existing['lat'] = item['lat']
                    existing['lon'] = item['lon']
                
                # Объединяем notes
                if item.get('notes') and item['notes'] not in existing.get('notes', ''):
                    existing['notes'] = existing.get('notes', '') + "<br>" + item['notes']
        
        return list(unique.values())