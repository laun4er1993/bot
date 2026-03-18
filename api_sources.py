# Добавьте этот метод в класс APISourceManager в файле api_sources.py

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
        
        # Добавляем заголовки, чтобы имитировать браузер
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        await self._rate_limit()
        async with session.get(url, headers=headers) as response:
            if response.status == 200:
                html = await response.text()
                
                # Здесь нужен парсинг HTML
                # Поскольку структура страницы сложная, используем BeautifulSoup
                # Но в асинхронном коде лучше вынести в отдельный поток
                loop = asyncio.get_event_loop()
                villages = await loop.run_in_executor(
                    None, 
                    self._parse_academic_ru_html, 
                    html
                )
                
                # Добавляем источник для каждой записи
                for village in villages:
                    village['source'] = 'academic_ru'
                    if not village.get('district'):
                        village['district'] = 'Бельский'  # Указываем район
                    results.append(village)
                
                logger.info(f"    ✅ Найдено бывших НП: {len(results)}")
            else:
                logger.error(f"    ❌ Ошибка загрузки страницы: {response.status}")
    
    except Exception as e:
        logger.error(f"    ❌ Ошибка при парсинге dic.academic.ru: {e}")
    
    return results

def _parse_academic_ru_html(self, html: str) -> List[Dict]:
    """
    Синхронный метод для парсинга HTML с BeautifulSoup
    Вызывается в отдельном потоке, чтобы не блокировать асинхронный код
    """
    from bs4 import BeautifulSoup
    import re
    
    soup = BeautifulSoup(html, 'html.parser')
    villages = []
    
    # Ищем все таблицы на странице
    tables = soup.find_all('table')
    
    for table in tables:
        rows = table.find_all('tr')
        if not rows:
            continue
        
        # Пропускаем заголовок
        for row in rows[1:]:
            cells = row.find_all('td')
            if len(cells) < 5:  # Ожидаем минимум 5 колонок
                continue
            
            try:
                # Извлекаем название (первая колонка)
                name_cell = cells[0]
                name = name_cell.get_text().strip()
                
                # Извлекаем тип (вторая колонка)
                type_cell = cells[1]
                raw_type = type_cell.get_text().strip()
                
                # Определяем тип по сокращениям
                village_type = self._determine_type_from_abbr(raw_type)
                
                # Извлекаем координаты (последняя колонка)
                coords_cell = cells[-1]
                coords_text = coords_cell.get_text().strip()
                
                # Парсим координаты
                lat, lon = self._parse_coordinates(coords_text)
                
                if lat and lon and name:
                    # Определяем статус (бывший)
                    status = "abandoned"  # Бывший/упраздненный
                    
                    # Извлекаем год упразднения, если есть
                    year = None
                    if len(cells) >= 5:
                        year_cell = cells[3]  # Предполагаем, что год в 4-й колонке
                        year_text = year_cell.get_text().strip()
                        if year_text and year_text.isdigit():
                            year = year_text
                    
                    notes = f"Бывший НП Бельского района"
                    if year:
                        notes += f", упразднён в {year} г."
                    
                    villages.append({
                        "name": name,
                        "type": village_type,
                        "lat": str(lat),
                        "lon": str(lon),
                        "district": "Бельский",
                        "status": status,
                        "notes": notes
                    })
                    
            except Exception as e:
                logger.error(f"    Ошибка парсинга строки: {e}")
                continue
    
    return villages

def _parse_coordinates(self, coord_text: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Парсит координаты из разных форматов:
    - 55°49′56.64″ с. ш. 33°20′59.64″ в. д.
    - 55.8324 33.3499
    """
    import re
    
    coord_text = coord_text.strip()
    
    # Формат: 55°49′56.64″ с. ш. 33°20′59.64″ в. д.
    pattern_dms = r'(\d+)°(\d+)′([\d.]+)″.*?(\d+)°(\d+)′([\d.]+)″'
    match = re.search(pattern_dms, coord_text)
    
    if match:
        # Конвертируем DMS в десятичные градусы
        lat_deg, lat_min, lat_sec = map(float, match.group(1, 2, 3))
        lon_deg, lon_min, lon_sec = map(float, match.group(4, 5, 6))
        
        lat = lat_deg + lat_min/60 + lat_sec/3600
        lon = lon_deg + lon_min/60 + lon_sec/3600
        return lat, lon
    
    # Формат: 55.8324 33.3499
    pattern_dec = r'([\d.]+)\s+([\d.]+)'
    match = re.search(pattern_dec, coord_text)
    
    if match:
        lat = float(match.group(1))
        lon = float(match.group(2))
        return lat, lon
    
    return None, None

def _determine_type_from_abbr(self, abbr: str) -> str:
    """Определяет тип НП по сокращению"""
    mapping = {
        'дер.': 'деревня',
        'д.': 'деревня',
        'пос.': 'посёлок',
        'с.': 'село',
        'х.': 'хутор',
        'п.': 'посёлок'
    }
    
    for abbr_key, full_type in mapping.items():
        if abbr.startswith(abbr_key):
            return full_type
    
    return 'деревня'  # По умолчанию