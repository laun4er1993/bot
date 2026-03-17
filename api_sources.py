# api_sources.py
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
        self.sources = {
            'agkgn': {
                'url': 'https://agkgn.ru/api/v1/search',
                'params': {'district': 'Ржевский', 'format': 'json'},
                'parser': self._parse_agkgn_response
            },
            'wikidata': {
                'url': 'https://www.wikidata.org/w/api.php',
                'params': {
                    'action': 'query',
                    'list': 'search',
                    'srsearch': 'населенные пункты Ржевского района',
                    'format': 'json'
                },
                'parser': self._parse_wikidata_response
            },
            'osm': {
                'url': 'https://nominatim.openstreetmap.org/search',
                'params': {
                    'q': 'Ржевский район',
                    'format': 'json',
                    'addressdetails': 1,
                    'limit': 100
                },
                'parser': self._parse_osm_response
            },
            'historical': {
                'url': 'https://boxpis.ru/api/v1/snm1859',
                'params': {'uezd': 'Ржевский'},
                'parser': self._parse_historical_response
            },
            'etomesto': {
                'url': 'https://etomesto.ru/api/v1/places',
                'params': {'region': 'rzhev', 'type': 'historical'},
                'parser': self._parse_etomesto_response
            }
        }
    
    async def get_session(self):
        """Создает или возвращает существующую сессию"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(
                headers={'User-Agent': 'WW2AerialPhotoBot/1.0 (research project)'}
            )
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
            async with session.get(source['url'], params=source['params']) as response:
                if response.status == 200:
                    data = await response.json()
                    return source['parser'](data)
                else:
                    logger.error(f"Ошибка {source_name}: HTTP {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Ошибка при загрузке {source_name}: {e}")
            return []
    
    def _parse_agkgn_response(self, data: Dict) -> List[Dict]:
        """Парсит ответ АГКГН API"""
        villages = []
        for item in data.get('features', []):
            props = item.get('properties', {})
            coords = item.get('geometry', {}).get('coordinates', [])
            villages.append({
                'name': props.get('name', ''),
                'type': props.get('type', 'деревня'),
                'lat': str(coords[1]) if coords else '',
                'lon': str(coords[0]) if coords else '',
                'source': 'agkgn',
                'district': 'Ржевский',
                'status': 'существует',
                'notes': props.get('notes', '')
            })
        return villages
    
    def _parse_wikidata_response(self, data: Dict) -> List[Dict]:
        """Парсит ответ Wikidata API"""
        villages = []
        for item in data.get('query', {}).get('search', []):
            villages.append({
                'name': item.get('title', ''),
                'type': 'деревня',
                'lat': '',
                'lon': '',
                'source': 'wikidata',
                'district': 'Ржевский',
                'status': 'неизвестно',
                'notes': f"Wikidata ID: {item.get('pageid', '')}"
            })
        return villages
    
    def _parse_osm_response(self, data: List) -> List[Dict]:
        """Парсит ответ OpenStreetMap Nominatim"""
        villages = []
        for item in data:
            if item.get('type') in ['village', 'hamlet', 'locality']:
                villages.append({
                    'name': item.get('display_name', '').split(',')[0],
                    'type': item.get('type', 'деревня'),
                    'lat': item.get('lat', ''),
                    'lon': item.get('lon', ''),
                    'source': 'osm',
                    'district': 'Ржевский',
                    'status': 'существует',
                    'notes': f"OSM ID: {item.get('osm_id', '')}"
                })
        return villages
    
    def _parse_historical_response(self, data: List) -> List[Dict]:
        """Парсит ответ API исторических данных 1859 года"""
        villages = []
        for item in data:
            if item.get('uezd') == 'Ржевский':
                villages.append({
                    'name': item.get('name', ''),
                    'type': item.get('type', 'деревня'),
                    'lat': item.get('lat', ''),
                    'lon': item.get('lon', ''),
                    'source': 'historical',
                    'district': 'Ржевский',
                    'status': item.get('status', 'неизвестно'),
                    'notes': item.get('notes', '')
                })
        return villages
    
    def _parse_etomesto_response(self, data: List) -> List[Dict]:
        """Парсит ответ etomesto.ru API"""
        villages = []
        for item in data:
            villages.append({
                'name': item.get('name', ''),
                'type': item.get('type', 'деревня'),
                'lat': item.get('lat', ''),
                'lon': item.get('lon', ''),
                'source': 'etomesto',
                'district': 'Ржевский',
                'status': item.get('status', 'историческое'),
                'notes': item.get('description', '')
            })
        return villages
    
    async def fetch_all_sources(self) -> List[Dict]:
        """Загружает данные из всех источников параллельно"""
        tasks = []
        for source_name in self.sources.keys():
            tasks.append(self.fetch_source(source_name))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_villages = []
        for source_data in results:
            if isinstance(source_data, list):
                all_villages.extend(source_data)
            else:
                logger.error(f"Ошибка при загрузке: {source_data}")
        
        # Убираем дубликаты по названию
        unique_villages = []
        seen = set()
        for v in all_villages:
            if v['name'] and v['name'] not in seen:
                unique_villages.append(v)
                seen.add(v['name'])
        
        return unique_villages