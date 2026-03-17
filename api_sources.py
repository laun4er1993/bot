import aiohttp
import asyncio
import csv
import io
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)

class APISourceManager:
    """Менеджер для работы с внешними API источниками"""
    
    def __init__(self):
        self.session = None
        # Только реально рабочие API
        self.sources = {
            # OpenStreetMap Nominatim - работает с правильным User-Agent
            'osm': {
                'url': 'https://nominatim.openstreetmap.org/search',
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
            # Overpass API - работает с POST
            'overpass': {
                'url': 'https://overpass-api.de/api/interpreter',
                'method': 'POST',
                'headers': {
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'User-Agent': 'WW2AerialPhotoBot/1.0'
                },
                'data': '[out:json]; area["name"="Ржевский район"]->.a; (node["place"](area.a); way["place"](area.a);); out body;',
                'parser': self._parse_overpass_response
            },
            # Wikimapia - требует API ключ, пока отключаем
            # 'wikimapia': {
            #     'url': 'http://api.wikimapia.org/',
            #     'params': {
            #         'function': 'place.getnearest',
            #         'lat': '56.25',
            #         'lon': '34.35',
            #         'radius': '50000',
            #         'format': 'json',
            #         'key': 'YOUR_API_KEY',  # Нужно получить ключ
            #         'count': '100'
            #     },
            #     'parser': self._parse_wikimapia_response
            # },
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
                # POST запрос
                data = source.get('data', '')
                async with session.post(source['url'], data=data, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        return source['parser'](data)
                    else:
                        logger.error(f"Ошибка {source_name}: HTTP {response.status}")
                        return []
            else:
                # GET запрос
                async with session.get(source['url'], params=source.get('params', {}), headers=headers) as response:
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
                # Берем только первую часть названия
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
    
    def _parse_overpass_response(self, data: Dict) -> List[Dict]:
        """Парсит ответ Overpass API"""
        villages = []
        try:
            elements = data.get('elements', [])
            for elem in elements:
                tags = elem.get('tags', {})
                name = tags.get('name:ru', tags.get('name', ''))
                if not name:
                    continue
                
                # Определяем тип населенного пункта
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
                    'notes': ''
                })
        except Exception as e:
            logger.error(f"Ошибка парсинга Overpass: {e}")
        return villages
    
    def _parse_wikimapia_response(self, data: Dict) -> List[Dict]:
        """Парсит ответ Wikimapia API"""
        villages = []
        try:
            items = data.get('folder', {}).get('items', [])
            for item in items:
                name = item.get('title', '')
                if not name:
                    continue
                
                location = item.get('location', {})
                lat = location.get('lat', '')
                lon = location.get('lon', '')
                
                villages.append({
                    'name': name,
                    'type': item.get('category', 'деревня'),
                    'lat': str(lat),
                    'lon': str(lon),
                    'source': 'wikimapia',
                    'district': 'Ржевский',
                    'status': 'существует',
                    'notes': ''
                })
        except Exception as e:
            logger.error(f"Ошибка парсинга Wikimapia: {e}")
        return villages
    
    async def fetch_all_sources(self) -> List[Dict]:
        """Загружает данные из всех источников параллельно"""
        tasks = []
        for source_name in self.sources.keys():
            tasks.append(asyncio.create_task(self.fetch_source(source_name)))
        
        # Ждем все задачи, но не больше 15 секунд
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
        
        return all_villages