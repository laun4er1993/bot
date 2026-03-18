import aiohttp
import asyncio
from typing import List, Dict
import logging
import urllib.parse

logger = logging.getLogger(__name__)

class APISourceManager:
    """Менеджер для работы с Photon API (исправленная версия)"""
    
    def __init__(self):
        self.session = None
        self.sources = {
            'photon': {
                'url': 'https://photon.komoot.io/api/',  # Добавлен слеш в конце
                'method': 'GET',
                'params': {
                    'q': 'Ржевский район',  # Поисковый запрос
                    'limit': 100,             # Максимум результатов
                    'lang': 'ru',              # Язык результатов
                    'osm_tag': 'place'         # Фильтр только для населенных пунктов
                },
                'headers': {
                    'User-Agent': 'WW2AerialPhotoBot/1.0 (research project)',
                    'Accept': 'application/json'
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
            # Логируем полный URL для отладки
            params = source.get('params', {})
            url = source['url']
            logger.info(f"Запрос к Photon: {url} с параметрами {params}")
            
            async with session.get(
                url, 
                params=params,
                headers=source.get('headers', {}),
                timeout=10
            ) as response:
                logger.info(f"Photon ответ: HTTP {response.status}")
                
                if response.status == 200:
                    data = await response.json()
                    return source['parser'](data)
                else:
                    # Пробуем получить текст ошибки
                    try:
                        error_text = await response.text()
                        logger.error(f"Ошибка Photon: {error_text[:200]}")
                    except:
                        pass
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
            logger.info(f"Photon вернул {len(features)} объектов")
            
            for item in features:
                props = item.get('properties', {})
                coords = item.get('geometry', {}).get('coordinates', [])
                
                name = props.get('name', '')
                if not name:
                    continue
                
                # Проверяем, что это действительно населенный пункт
                osm_key = props.get('osm_key', '')
                if osm_key != 'place':
                    continue  # Пропускаем не населенные пункты
                
                # Определяем тип
                osm_type = props.get('osm_type', '')
                osm_value = props.get('osm_value', '')
                
                obj_type = 'деревня'
                if osm_value == 'city':
                    obj_type = 'город'
                elif osm_value == 'town':
                    obj_type = 'поселок'
                elif osm_value == 'village':
                    obj_type = 'деревня'
                elif osm_value == 'hamlet':
                    obj_type = 'деревня'
                elif osm_value == 'locality':
                    obj_type = 'урочище'
                
                # Получаем географическую информацию
                city = props.get('city', '')
                state = props.get('state', '')
                country = props.get('country', '')
                
                # Формируем заметку
                notes = []
                if city:
                    notes.append(f"г. {city}")
                if state:
                    notes.append(state)
                if country:
                    notes.append(country)
                
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
            
            logger.info(f"✅ Photon: найдено {len(villages)} населенных пунктов")
        except Exception as e:
            logger.error(f"Ошибка парсинга Photon: {e}")
        return villages
    
    async def fetch_all_sources(self) -> List[Dict]:
        """Загружает данные только из Photon"""
        result = await self.fetch_source('photon')
        logger.info(f"Всего загружено: {len(result)} записей из Photon")
        return result