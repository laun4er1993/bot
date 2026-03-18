# api_sources.py
# Полная замена всех API источников на Google Places API + парсинг академических источников

import aiohttp
import asyncio
import logging
from typing import List, Dict, Optional, Any, Tuple
import os
import json
import time
import re
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor

logger = logging.getLogger(__name__)

# Токен из переменных окружения (нужно добавить в .env или переменные среды)
GOOGLE_PLACES_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")

# Координаты центров районов Тверской области
DISTRICTS = {
    "Ржевский": {
        "center_lat": 56.2628,
        "center_lon": 34.3289,
        "radius": 50000  # 50 км
    },
    "Бельский": {
        "center_lat": 55.8500,
        "center_lon": 33.2500,
        "radius": 40000  # 40 км
    },
    "Оленинский": {
        "center_lat": 56.2000,
        "center_lon": 33.4833,
        "radius": 45000
    },
    "Зубцовский": {
        "center_lat": 56.1667,
        "center_lon": 34.5833,
        "radius": 45000
    }
}

# Типы мест для разных категорий населенных пунктов
VILLAGE_TYPES = [
    "locality",           # Населенный пункт
    "administrative_area_level_3",  # Сельское поселение
    "sublocality",        # Часть населенного пункта
    "neighborhood",        # Район/микрорайон
    "political"           # Политическая/административная единица
]

# Типы для исторических/заброшенных мест
HISTORICAL_TYPES = [
    "establishment",      # Учреждение/объект
    "point_of_interest",  # Точка интереса
    "premise",            # Здание/сооружение
    "natural_feature",    # Природный объект
    "park"                # Парк/лес
]

class APISourceManager:
    """
    Менеджер для загрузки данных из различных источников:
    - Google Places API (основной)
    - Парсинг академических сайтов (dic.academic.ru)
    """
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.api_key = GOOGLE_PLACES_API_KEY
        self.thread_pool = ThreadPoolExecutor(max_workers=2)
        
        if not self.api_key:
            logger.warning("⚠️ GOOGLE_PLACES_API_KEY не найден в переменных окружения")
        
        # Для отслеживания квот и пагинации
        self.request_count = 0
        self.last_request_time = 0
        self.min_request_interval = 0.1  # 100 мс между запросами
    
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
        Загружает данные для конкретного района из всех источников
        Возвращает словарь с результатами по каждому источнику
        """
        if district not in DISTRICTS:
            raise ValueError(f"Неизвестный район: {district}")
        
        logger.info(f"🌐 Загрузка данных для района: {district}")
        
        results = {
            "google_places": [],
            "academic_ru": [],
            "total": []
        }
        
        try:
            # 1. Загружаем из Google Places API
            if self.api_key:
                google_results = await self._fetch_google_places_for_district(district)
                results["google_places"] = google_results
                results["total"].extend(google_results)
                logger.info(f"  ✅ Google Places: {len(google_results)} записей")
            
            # 2. Загружаем из академических источников
            if district == "Бельский":
                academic_results = await self.fetch_academic_ru_villages()
                results["academic_ru"] = academic_results
                results["total"].extend(academic_results)
                logger.info(f"  ✅ Academic.ru: {len(academic_results)} записей")
            elif district == "Ржевский":
                # Можно добавить другие источники для Ржевского района
                pass
            
            # Дедупликация общих результатов
            results["total"] = self._deduplicate_results(results["total"])
            logger.info(f"  ✅ Всего уникальных записей: {len(results['total'])}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке данных для района {district}: {e}")
        
        return results
    
    async def _fetch_google_places_for_district(self, district: str) -> List[Dict]:
        """Загружает данные из Google Places для конкретного района"""
        if not self.api_key:
            return []
        
        district_data = DISTRICTS[district]
        results = []
        
        try:
            # Поиск населенных пунктов
            villages = await self._text_search(
                query=f"населенные пункты {district} район Тверская область",
                location_bias=f"circle:{district_data['radius']}@{district_data['center_lat']},{district_data['center_lon']}",
                included_type="locality",
                source=f"google_places_{district}",
                max_results=60
            )
            results.extend(villages)
            
            # Дополнительный поиск по конкретным типам
            for place_type in ["sublocality", "neighborhood"]:
                type_results = await self._nearby_search(
                    location=f"{district_data['center_lat']},{district_data['center_lon']}",
                    radius=district_data['radius'],
                    included_type=place_type,
                    source=f"google_places_{district}_{place_type}"
                )
                results.extend(type_results)
            
        except Exception as e:
            logger.error(f"  ❌ Ошибка Google Places для {district}: {e}")
        
        return results
    
    async def fetch_academic_ru_villages(self) -> List[Dict]:
        """
        Парсит страницу со списком бывших населённых пунктов Бельского района
        с сайта dic.academic.ru
        """
        logger.info("  🔍 Парсинг сайта dic.academic.ru (Бельский район)...")
        url = "https://dic.academic.ru/dic.nsf/ruwiki/1635988"
        results = []
        
        try:
            session = await self._get_session()
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            await self._rate_limit()
            async with session.get(url, headers=headers, timeout=30) as response:
                if response.status == 200:
                    html = await response.text()
                    
                    # Парсим HTML в отдельном потоке
                    loop = asyncio.get_event_loop()
                    villages = await loop.run_in_executor(
                        self.thread_pool,
                        self._parse_academic_ru_html,
                        html
                    )
                    
                    results.extend(villages)
                    logger.info(f"    ✅ Найдено бывших НП: {len(results)}")
                else:
                    logger.error(f"    ❌ Ошибка загрузки страницы: HTTP {response.status}")
                    
        except asyncio.TimeoutError:
            logger.error("    ❌ Таймаут при загрузке dic.academic.ru")
        except Exception as e:
            logger.error(f"    ❌ Ошибка при парсинге dic.academic.ru: {e}")
        
        return results
    
    def _parse_academic_ru_html(self, html: str) -> List[Dict]:
        """
        Синхронный метод для парсинга HTML с BeautifulSoup
        """
        try:
            soup = BeautifulSoup(html, 'html.parser')
            villages = []
            
            # Ищем все таблицы на странице
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                if len(rows) < 2:  # Должен быть хотя бы заголовок и строка с данными
                    continue
                
                for row in rows[1:]:  # Пропускаем заголовок
                    try:
                        cells = row.find_all('td')
                        if len(cells) < 5:
                            continue
                        
                        # Извлекаем название
                        name_cell = cells[0]
                        name = name_cell.get_text().strip()
                        if not name:
                            continue
                        
                        # Извлекаем тип
                        type_cell = cells[1]
                        raw_type = type_cell.get_text().strip()
                        village_type = self._determine_type_from_abbr(raw_type)
                        
                        # Извлекаем координаты из последней ячейки
                        coords_cell = cells[-1]
                        coords_text = coords_cell.get_text().strip()
                        
                        lat, lon = self._parse_coordinates(coords_text)
                        
                        if lat and lon:
                            # Извлекаем год упразднения, если есть
                            year = None
                            if len(cells) >= 5:
                                year_cell = cells[3]  # Год упразднения
                                year_text = year_cell.get_text().strip()
                                if year_text and year_text.replace('?', '').strip().isdigit():
                                    year = year_text.replace('?', '').strip()
                            
                            # Формируем заметки
                            notes_parts = ["<i>Источник: dic.academic.ru</i>"]
                            if year:
                                notes_parts.append(f"Упразднён в {year} г.")
                            
                            villages.append({
                                "name": name,
                                "type": village_type,
                                "lat": str(lat),
                                "lon": str(lon),
                                "source": "academic_ru",
                                "district": "Бельский",
                                "status": "abandoned",
                                "notes": "<br>".join(notes_parts)  # Используем <br> для переноса строк
                            })
                            
                    except Exception as e:
                        logger.debug(f"    Ошибка парсинга строки: {e}")
                        continue
            
            return villages
            
        except Exception as e:
            logger.error(f"    ❌ Ошибка в _parse_academic_ru_html: {e}")
            return []
    
    def _parse_coordinates(self, coord_text: str) -> Tuple[Optional[float], Optional[float]]:
        """
        Парсит координаты из разных форматов:
        - 55°49′56.64″ с. ш. 33°20′59.64″ в. д.
        - 55.8324 33.3499
        - 55.8324,33.3499
        """
        try:
            coord_text = coord_text.strip().replace(',', ' ')
            
            # Формат: 55°49′56.64″ с. ш. 33°20′59.64″ в. д.
            pattern_dms = r'(\d+)°(\d+)′([\d.]+)″.*?(\d+)°(\d+)′([\d.]+)″'
            match = re.search(pattern_dms, coord_text)
            
            if match:
                lat_deg, lat_min, lat_sec = map(float, match.group(1, 2, 3))
                lon_deg, lon_min, lon_sec = map(float, match.group(4, 5, 6))
                
                lat = lat_deg + lat_min/60 + lat_sec/3600
                lon = lon_deg + lon_min/60 + lon_sec/3600
                return round(lat, 6), round(lon, 6)
            
            # Формат: 55.8324 33.3499
            numbers = re.findall(r'[\d.]+', coord_text)
            if len(numbers) >= 2:
                lat = float(numbers[0])
                lon = float(numbers[1])
                # Проверяем разумные пределы для Тверской области
                if 55.0 <= lat <= 58.0 and 30.0 <= lon <= 38.0:
                    return round(lat, 6), round(lon, 6)
            
        except Exception:
            pass
        
        return None, None
    
    def _determine_type_from_abbr(self, abbr: str) -> str:
        """Определяет тип НП по сокращению"""
        mapping = {
            'дер.': 'деревня',
            'д.': 'деревня',
            'пос.': 'посёлок',
            'с.': 'село',
            'х.': 'хутор',
            'п.': 'посёлок',
            'ур.': 'урочище'
        }
        
        for abbr_key, full_type in mapping.items():
            if abbr.lower().startswith(abbr_key):
                return full_type
        
        return 'деревня'
    
    async def _nearby_search(self, location: str, radius: int, 
                             included_type: str, source: str,
                             max_results: int = 60) -> List[Dict]:
        """Nearby Search через Google Places API (New)"""
        if not self.api_key:
            return []
        
        session = await self._get_session()
        results = []
        
        url = "https://places.googleapis.com/v1/places:searchNearby"
        
        lat, lon = map(float, location.split(','))
        
        payload = {
            "locationRestriction": {
                "circle": {
                    "center": {"latitude": lat, "longitude": lon},
                    "radius": radius
                }
            },
            "includedTypes": [included_type],
            "maxResultCount": min(20, max_results),
            "languageCode": "ru"
        }
        
        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.api_key,
            "X-Goog-FieldMask": "places.id,places.displayName,places.types,places.location,places.formattedAddress,places.primaryType"
        }
        
        try:
            await self._rate_limit()
            
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    for place in data.get("places", []):
                        village = self._convert_place_to_village(place, source)
                        if village:
                            results.append(village)
                    
                elif response.status == 403:
                    logger.error("    ❌ Ошибка 403: Доступ запрещен. Проверьте API ключ и ограничения.")
                elif response.status == 429:
                    logger.error("    ❌ Ошибка 429: Превышен лимит запросов.")
                else:
                    error_text = await response.text()
                    logger.error(f"    ❌ Ошибка Nearby Search: {response.status}")
                    
        except Exception as e:
            logger.error(f"    ❌ Исключение в Nearby Search: {e}")
        
        return results
    
    async def _text_search(self, query: str, location_bias: str, 
                          included_type: str, source: str,
                          max_results: int = 60) -> List[Dict]:
        """Text Search через Google Places API (New)"""
        if not self.api_key:
            return []
        
        session = await self._get_session()
        results = []
        
        url = "https://places.googleapis.com/v1/places:searchText"
        
        bias_parts = location_bias.replace("circle:", "").split("@")
        radius = int(bias_parts[0])
        lat, lng = map(float, bias_parts[1].split(","))
        
        payload = {
            "textQuery": query,
            "locationBias": {
                "circle": {
                    "center": {"latitude": lat, "longitude": lng},
                    "radius": radius
                }
            },
            "includedType": included_type,
            "maxResultCount": min(20, max_results),
            "languageCode": "ru"
        }
        
        headers = {
            "Content-Type": "application/json",
            "X-Goog-Api-Key": self.api_key,
            "X-Goog-FieldMask": "places.id,places.displayName,places.types,places.location,places.formattedAddress,places.primaryType"
        }
        
        try:
            await self._rate_limit()
            
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    for place in data.get("places", []):
                        village = self._convert_place_to_village(place, source)
                        if village:
                            results.append(village)
                    
                else:
                    error_text = await response.text()
                    logger.error(f"    ❌ Ошибка Text Search: {response.status}")
                    
        except Exception as e:
            logger.error(f"    ❌ Исключение в Text Search: {e}")
        
        return results
    
    def _convert_place_to_village(self, place: Dict, source: str) -> Optional[Dict]:
        """Конвертирует ответ Google Places в формат каталога"""
        try:
            display_name = place.get("displayName", {})
            name = display_name.get("text", "").strip()
            if not name:
                return None
            
            location = place.get("location", {})
            lat = location.get("latitude")
            lng = location.get("longitude")
            
            if lat is None or lng is None:
                return None
            
            types = place.get("types", [])
            village_type, status = self._determine_type_and_status(types)
            
            address = place.get("formattedAddress", "")
            
            return {
                "name": name,
                "type": village_type,
                "lat": str(lat),
                "lon": str(lng),
                "source": source,
                "district": self._extract_district_from_source(source),
                "status": status,
                "notes": f"<i>Источник: Google Places</i><br>{address if address else ''}"
            }
            
        except Exception as e:
            logger.debug(f"    Ошибка конвертации места: {e}")
            return None
    
    def _determine_type_and_status(self, types: List[str]) -> Tuple[str, str]:
        """Определяет тип НП и статус по типам Google Places"""
        type_mapping = {
            "locality": ("деревня", "existing"),
            "administrative_area_level_3": ("деревня", "existing"),
            "sublocality": ("деревня", "existing"),
            "neighborhood": ("деревня", "existing"),
            "political": ("деревня", "existing"),
            "establishment": ("урочище", "abandoned"),
            "point_of_interest": ("объект", "historical"),
            "premise": ("усадьба", "historical"),
            "natural_feature": ("урочище", "natural"),
            "park": ("урочище", "natural")
        }
        
        for place_type in types:
            if place_type in type_mapping:
                return type_mapping[place_type]
        
        return ("деревня", "existing")
    
    def _extract_district_from_source(self, source: str) -> str:
        """Извлекает район из названия источника"""
        for district in DISTRICTS.keys():
            if district.lower() in source.lower():
                return district
        return "Ржевский"  # По умолчанию
    
    def _deduplicate_results(self, results: List[Dict]) -> List[Dict]:
        """Удаляет дубликаты по названию и координатам"""
        unique = {}
        
        for item in results:
            try:
                lat = round(float(item.get('lat', 0)), 4) if item.get('lat') else 0
                lon = round(float(item.get('lon', 0)), 4) if item.get('lon') else 0
                key = f"{item['name']}_{lat}_{lon}"
                
                if key not in unique:
                    unique[key] = item
                else:
                    # Если есть дубликат, выбираем запись с более точной информацией
                    existing = unique[key]
                    
                    # Приоритет: записи с координатами и заметками
                    existing_score = len(existing.get('notes', '')) + (10 if existing.get('lat') else 0)
                    new_score = len(item.get('notes', '')) + (10 if item.get('lat') else 0)
                    
                    if new_score > existing_score:
                        unique[key] = item
                        
            except Exception:
                continue
        
        return list(unique.values())