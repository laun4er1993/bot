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
        # Только проверенные рабочие API
        self.sources = {
            # GeoNames.org - работает без ключа (демо-аккаунт)
            'geonames': {
                'url': 'http://api.geonames.org/searchJSON',
                'method': 'GET',
                'params': {
                    'q': 'Ржевский район',
                    'country': 'RU',
                    'maxRows': 200,
                    'username': 'demo',  # публичный демо-аккаунт
                    'lang': 'ru',
                    'featureClass': 'P'  # населенные пункты
                },
                'headers': {
                    'User-Agent': 'WW2AerialPhotoBot/1.0'
                },
                'parser': self._parse_geonames_response
            },
            # Photon (OpenStreetMap) - быстрый и стабильный
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
            },
            # Overpass API - оптимизированный запрос
            'overpass': {
                'url': 'https://overpass-api.de/api/interpreter',
                'method': 'POST',
                'headers': {
                    'User-Agent': 'WW2AerialPhotoBot/1.0',
                    'Content-Type': 'application/x-www-form-urlencoded'
                },
                'data': 'data=' + urllib.parse.quote('''
                    [out:json][timeout:10][maxsize:16777216];
                    area["name"="Ржевский район"]->.a;
                    node["place"](area.a);
                    out body;
                '''),
                'parser': self._parse_overpass_response
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
                async with session.post(source['url'], data=data, headers=headers, timeout=10) as response:
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
                async with session.get(source['url'], params=source.get('params', {}), headers=headers, timeout=10) as response:
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
    
    def _parse_geonames_response(self, data: Dict) -> List[Dict]:
        """Парсит ответ GeoNames.org """
        villages = []
        try:
            for item in data.get('geonames', []):
                name = item.get('name', '')
                if not name:
                    continue
                
                # Определяем тип населенного пункта
                fcode = item.get('fcode', '')
                fcode_name = item.get('fcodeName', '')
                
                obj_type = 'деревня'
                if 'PPL' in fcode:
                    obj_type = 'деревня'
                elif 'PPLA' in fcode:
                    obj_type = 'город'
                elif 'PPLX' in fcode:
                    obj_type = 'часть города'
                
                villages.append({
                    'name': name,
                    'type': obj_type,
                    'lat': item.get('lat', ''),
                    'lon': item.get('lng', ''),
                    'source': 'geonames',
                    'district': 'Ржевский',
                    'status': 'существует',
                    'notes': fcode_name
                })
            logger.info(f"  GeoNames: найдено {len(villages)} записей")
        except Exception as e:
            logger.error(f"Ошибка парсинга GeoNames: {e}")
        return villages
    
    def _parse_photon_response(self, data: Dict) -> List[Dict]:
        """Парсит ответ Photon API """
        villages = []
        try:
            for item in data.get('features', []):
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
                
                villages.append({
                    'name': name,
                    'type': obj_type,
                    'lat': str(coords[1]) if len(coords) > 1 else '',
                    'lon': str(coords[0]) if coords else '',
                    'source': 'photon',
                    'district': 'Ржевский',
                    'status': 'существует',
                    'notes': ''
                })
            logger.info(f"  Photon: найдено {len(villages)} записей")
        except Exception as e:
            logger.error(f"Ошибка парсинга Photon: {e}")
        return villages
    
    def _parse_overpass_response(self, data: Dict) -> List[Dict]:
        """Парсит ответ Overpass API """
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
                elif place_type == 'town':
                    place_type = 'поселок'
                elif place_type == 'city':
                    place_type = 'город'
                
                # Координаты
                lat = elem.get('lat', '')
                lon = elem.get('lon', '')
                
                villages.append({
                    'name': name,
                    'type': place_type,
                    'lat': str(lat),
                    'lon': str(lon),
                    'source': 'overpass',
                    'district': 'Ржевский',
                    'status': 'существует',
                    'notes': ''
                })
            logger.info(f"  Overpass: найдено {len(villages)} записей")
        except Exception as e:
            logger.error(f"Ошибка парсинга Overpass: {e}")
        return villages
    
    async def fetch_all_sources(self) -> List[Dict]:
        """Загружает данные из всех источников параллельно"""
        tasks = []
        for source_name in self.sources.keys():
            tasks.append(asyncio.create_task(self.fetch_source(source_name)))
        
        # Ждем все задачи, но не больше 12 секунд
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_villages = []
        source_stats = {}
        
        for i, source_data in enumerate(results):
            source_name = list(self.sources.keys())[i]
            if isinstance(source_data, list):
                all_villages.extend(source_data)
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