import aiohttp
import asyncio
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

class APISourceManager:
    """Менеджер для работы с Photon API (единственный источник)"""
    
    def __init__(self):
        self.session = None
        # Только Photon - легкий и быстрый
        self.sources = {
            'photon': {
                'url': 'https://photon.komoot.io/api',
                'method': 'GET',
                'params': {
                    'q': 'Ржевский район',
                    'limit': 100,
                    'lang': 'ru'
                },
                'headers': {
                    'User-Agent': 'WW2AerialPhotoBot/1.0'
                },
                'parser': self._parse_photon_response
            }
        }
    
    async def get_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session
    
    async def close_session(self):
        if self.session and not self.session.closed:
            await self.session.close()
    
    async def fetch_source(self, source_name: str) -> List[Dict]:
        if source_name not in self.sources:
            logger.error(f"Неизвестный источник: {source_name}")
            return []
        
        source = self.sources[source_name]
        session = await self.get_session()
        
        try:
            async with session.get(
                source['url'], 
                params=source.get('params', {}), 
                headers=source.get('headers', {}),
                timeout=10
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return source['parser'](data)
                else:
                    logger.error(f"Ошибка Photon: HTTP {response.status}")
                    return []
        except asyncio.TimeoutError:
            logger.error("Таймаут Photon API")
            return []
        except Exception as e:
            logger.error(f"Ошибка Photon: {e}")
            return []
    
    def _parse_photon_response(self, data: Dict) -> List[Dict]:
        """Парсит ответ Photon API"""
        villages = []
        try:
            features = data.get('features', [])
            for item in features:
                props = item.get('properties', {})
                coords = item.get('geometry', {}).get('coordinates', [])
                
                name = props.get('name', '')
                if not name:
                    continue
                
                # Определяем тип
                osm_type = props.get('osm_type', '')
                osm_key = props.get('osm_key', '')
                
                obj_type = 'деревня'
                if osm_key == 'place':
                    if osm_type == 'city':
                        obj_type = 'город'
                    elif osm_type == 'town':
                        obj_type = 'поселок'
                    elif osm_type == 'village':
                        obj_type = 'деревня'
                    elif osm_type == 'hamlet':
                        obj_type = 'деревня'
                    elif osm_type == 'locality':
                        obj_type = 'урочище'
                
                # Город/район для контекста
                city = props.get('city', '')
                district = props.get('district', '')
                county = props.get('county', '')
                
                # Формируем заметку
                notes = []
                if city:
                    notes.append(f"г. {city}")
                if district:
                    notes.append(f"р-н {district}")
                if county:
                    notes.append(f"обл. {county}")
                
                villages.append({
                    'name': name,
                    'type': obj_type,
                    'lat': str(coords[1]) if len(coords) > 1 else '',
                    'lon': str(coords[0]) if coords else '',
                    'source': 'photon',
                    'district': 'Ржевский',
                    'status': 'существует',
                    'notes': ', '.join(notes) if notes else ''
                })
            
            logger.info(f"✅ Photon: найдено {len(villages)} записей")
        except Exception as e:
            logger.error(f"Ошибка парсинга Photon: {e}")
        return villages
    
    async def fetch_all_sources(self) -> List[Dict]:
        """Загружает данные только из Photon"""
        result = await self.fetch_source('photon')
        logger.info(f"Всего загружено: {len(result)} записей из Photon")
        return result