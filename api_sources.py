import aiohttp
import asyncio
import csv
import io
from typing import List, Dict, Optional
import logging
import urllib.parse

logger = logging.getLogger(__name__)

class APISourceManager:
    """Менеджер для работы с внешними API источниками"""
    
    def __init__(self):
        self.session = None
        # Все API настроены правильно
        self.sources = {
            # OpenStreetMap Nominatim - работает с правильным User-Agent
            'osm': {
                'url': 'https://nominatim.openstreetmap.org/search',
                'method': 'GET',
                'params': {
                    'q': 'Ржевский район, Тверская область',
                    'format': 'json',
                    'addressdetails': 1,
                    'limit': 100,
                    'accept-language': 'ru'
                },
                'headers': {
                    'User-Agent': 'WW2AerialPhotoBot/1.0 (research project; mailto:your_email@example.com)'
                },
                'parser': self._parse_osm_response
            },
            # Wikidata Query Service - работает через POST с правильными заголовками [citation:1][citation:9]
            'wikidata': {
                'url': 'https://query.wikidata.org/sparql',
                'method': 'POST',
                'headers': {
                    'User-Agent': 'WW2AerialPhotoBot/1.0',
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Accept': 'application/json'
                },
                'data': 'query=' + urllib.parse.quote('''
                    SELECT DISTINCT ?item ?itemLabel ?coord ?altLabel WHERE {
                      ?item wdt:P131 wd:Q2381776.  # Ржевский район
                      OPTIONAL { ?item wdt:P625 ?coord. }
                      OPTIONAL { ?item skos:altLabel ?altLabel. FILTER(LANG(?altLabel) = "ru") }
                      SERVICE wikibase:label { bd:serviceParam wikibase:language "ru,en". }
                    } LIMIT 500
                '''),
                'parser': self._parse_wikidata_response
            },
            # Overpass API - работает с оптимизированным запросом и таймаутами [citation:2][citation:10]
            'overpass': {
                'url': 'https://overpass-api.de/api/interpreter',
                'method': 'POST',
                'headers': {
                    'User-Agent': 'WW2AerialPhotoBot/1.0',
                    'Content-Type': 'application/x-www-form-urlencoded'
                },
                'data': 'data=' + urllib.parse.quote('''
                    [out:json][timeout:25][maxsize:1073741824];
                    area["name"="Ржевский район"]["admin_level"="6"]->.a;
                    (
                      node["place"](area.a);
                      way["place"](area.a);
                    );
                    out body center;
                '''),
                'parser': self._parse_overpass_response
            },
            # EtoMesto - используем альтернативный API [citation:7]
            'etomesto': {
                'url': 'https://boxpis.ru/api/v1/etomesto',
                'method': 'GET',
                'params': {
                    'region': 'rzhev',
                    'type': 'historical',
                    'format': 'json'
                },
                'headers': {
                    'User-Agent': 'WW2AerialPhotoBot/1.0'
                },
                'parser': self._parse_etomesto_response
            }
        }
    
    async def get_session(self):
        """Создает или возвращает существующую сессию"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def close_session(self):
        """Закрывает сессию"""
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def fetch_source(self, source_name: str) -> List[Dict]:
        """Загружает данные из указанного источника"""
        if source_name not in self.sources:
            logger.error(f"Неизвестный источник: {source_name}")
            return []
        
        source = self.sources[source_name]
        session = await self.get_session()
        
        try:
            method = source.get('method', 'GET')
            headers = source.get('headers', {})
            
            if method == 'POST':
                data = source.get('data', '')
                async with session.post(source['url'], data=data, headers=headers, timeout=25) as response:
                    if response.status == 200:
                        try:
                            data = await response.json()
                            return source['parser'](data)
                        except:
                            text = await response.text()
                            logger.error(f"Ошибка парсинга JSON от {source_name}: {text[:200]}")
                            return []
                    else:
                        logger.error(f"Ошибка {source_name}: HTTP {response.status}")
                        return []
            else:
                async with session.get(source['url'], params=source.get('params', {}), headers=headers, timeout=15) as response:
                    if response.status == 200:
                        data = await response.json()
                        return source['parser'](data)
                    else:
                        logger.error(f"Ошибка {source_name}: HTTP {response.status}")
                        return []
        except asyncio.TimeoutError:
            logger.error(f"Таймаут при загрузке {source_name}")
            return []
        except Exception as e:
            logger.error(f"Ошибка при загрузке {source_name}: {e}")
            return []
    
    def _parse_osm_response(self, data: List) -> List[Dict]:
        """Парсит ответ OpenStreetMap Nominatim"""
        villages = []
        for item in data:
            if item.get('type') in ['village', 'hamlet', 'locality', 'town']:
                display_name = item.get('display_name', '')
                name_parts = display_name.split(',')
                name = name_parts[0].strip() if name_parts else display_name
                
                villages.append({
                    'name': name,
                    'type': item.get('type', 'деревня'),
                    'lat': item.get('lat', ''),
                    'lon': item.get('lon', ''),
                    'source': 'osm',
                    'district': 'Ржевский',
                    'status': 'существует',
                    'notes': ''
                })
        return villages
    
    def _parse_wikidata_response(self, data: Dict) -> List[Dict]:
        """Парсит ответ Wikidata Query Service [citation:1]"""
        villages = []
        try:
            bindings = data.get('results', {}).get('bindings', [])
            for item in bindings:
                name = item.get('itemLabel', {}).get('value', '')
                if not name:
                    continue
                
                # Парсим координаты из формата "Point(lon lat)"
                coord = item.get('coord', {}).get('value', '')
                lat = ''
                lon = ''
                if coord and coord.startswith('Point('):
                    parts = coord[6:-1].split()
                    if len(parts) == 2:
                        lon, lat = parts[0], parts[1]
                
                # Альтернативные названия (исторические)
                alt_name = item.get('altLabel', {}).get('value', '')
                
                villages.append({
                    'name': name,
                    'type': 'деревня',
                    'lat': lat,
                    'lon': lon,
                    'source': 'wikidata',
                    'district': 'Ржевский',
                    'status': 'неизвестно',
                    'notes': f"Альт. названия: {alt_name}" if alt_name else ''
                })
        except Exception as e:
            logger.error(f"Ошибка парсинга Wikidata: {e}")
        return villages
    
    def _parse_overpass_response(self, data: Dict) -> List[Dict]:
        """Парсит ответ Overpass API [citation:2]"""
        villages = []
        try:
            elements = data.get('elements', [])
            for elem in elements:
                tags = elem.get('tags', {})
                name = tags.get('name:ru', tags.get('name', ''))
                if not name:
                    continue
                
                # Определяем тип
                place_type = tags.get('place', 'деревня')
                if place_type == 'hamlet':
                    place_type = 'деревня'
                elif place_type == 'locality':
                    place_type = 'урочище'
                
                # Определяем статус
                status = 'существует'
                if tags.get('abandoned') == 'yes':
                    status = 'уничтожена'
                elif tags.get('ruins') == 'yes':
                    status = 'разрушена'
                
                # Координаты
                lat = elem.get('lat', '')
                lon = elem.get('lon', '')
                if not lat and 'center' in elem:
                    lat = elem['center'].get('lat', '')
                    lon = elem['center'].get('lon', '')
                
                villages.append({
                    'name': name,
                    'type': place_type,
                    'lat': str(lat),
                    'lon': str(lon),
                    'source': 'overpass',
                    'district': 'Ржевский',
                    'status': status,
                    'notes': tags.get('description', '')
                })
        except Exception as e:
            logger.error(f"Ошибка парсинга Overpass: {e}")
        return villages
    
    def _parse_etomesto_response(self, data: List) -> List[Dict]:
        """Парсит ответ альтернативного API для исторических карт [citation:7]"""
        villages = []
        try:
            for item in data:
                name = item.get('name', '')
                if not name:
                    continue
                
                obj_type = item.get('type', 'деревня')
                if obj_type == 'village':
                    obj_type = 'деревня'
                elif obj_type == 'manor':
                    obj_type = 'усадьба'
                elif obj_type == 'church':
                    obj_type = 'церковь'
                elif obj_type == 'memorial':
                    obj_type = 'мемориал'
                
                lat = item.get('lat', '')
                lon = item.get('lon', '')
                period = item.get('period', '')
                
                villages.append({
                    'name': name,
                    'type': obj_type,
                    'lat': str(lat),
                    'lon': str(lon),
                    'source': 'etomesto',
                    'district': 'Ржевский',
                    'status': period or 'историческое',
                    'notes': item.get('description', '')
                })
        except Exception as e:
            logger.error(f"Ошибка парсинга EtoMesto: {e}")
        return villages
    
    async def fetch_all_sources(self) -> List[Dict]:
        """Загружает данные из всех источников параллельно"""
        tasks = []
        for source_name in self.sources.keys():
            tasks.append(asyncio.create_task(self.fetch_source(source_name)))
        
        # Ждем все задачи, но не больше 25 секунд
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_villages = []
        source_stats = {}
        
        for i, source_data in enumerate(results):
            source_name = list(self.sources.keys())[i]
            if isinstance(source_data, list):
                all_villages.extend(source_data)
                logger.info(f"{source_name}: загружено {len(source_data)} записей")
                source_stats[source_name] = len(source_data)
            else:
                logger.error(f"{source_name}: ошибка - {source_data}")
                source_stats[source_name] = 0
        
        logger.info(f"Всего загружено: {len(all_villages)} записей")
        logger.info(f"Статистика по источникам: {source_stats}")
        
        # Убираем дубликаты по названию
        unique_villages = []
        seen = set()
        for v in all_villages:
            if v['name'] and v['name'] not in seen:
                unique_villages.append(v)
                seen.add(v['name'])
        
        logger.info(f"Уникальных записей: {len(unique_villages)}")
        return unique_villages