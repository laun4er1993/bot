import os
import re
import math
from typing import List, Optional, Tuple
from aiogram import types, F
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from states.states import SearchStates
from keyboards.inline import photos_keyboard, search_result_keyboard, back_keyboard
from config import logger, KML_DIR
from shapely.geometry import Point


def parse_coordinates(text: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Парсит координаты из текста.
    Поддерживаемые форматы:
    1. Десятичные градусы: 56.2345 34.1234
    2. Градусы, минуты, секунды (DMS): 56°13'41″ N 34°08'10″ E
    3. Градусы и десятичные минуты (DDM): N 56° 19.938', E 034° 20.525'
    4. Градусы и десятичные минуты без пробелов: 56°19.938'N 34°20.525'E
    """
    if not text:
        return None, None
    
    text = text.strip()
    original_text = text
    
    # Заменяем запятые на точки для десятичных чисел
    text = text.replace(',', '.')
    
    # ========== ФОРМАТ 1: ДЕСЯТИЧНЫЕ ГРАДУСЫ ==========
    decimal_pattern = r'(-?\d+\.\d+)[,\s]+(-?\d+\.\d+)'
    match = re.search(decimal_pattern, text)
    if match:
        try:
            lat = float(match.group(1))
            lon = float(match.group(2))
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                logger.info(f"  📍 Распознаны десятичные координаты: {lat}, {lon}")
                return lat, lon
        except:
            pass
    
    # ========== ФОРМАТ 2: ГРАДУСЫ И ДЕСЯТИЧНЫЕ МИНУТЫ (DDM) ==========
    # Формат: N 56° 19.938', E 034° 20.525'
    # Формат: 56°19.938'N 34°20.525'E
    # Формат: 56° 19.938' N, 34° 20.525' E
    
    # Ищем широту
    lat_pattern = r'([NS]?)\s*(\d+)°\s*([\d.]+)\'?\s*([NS]?)'
    lon_pattern = r'([EW]?)\s*(\d+)°\s*([\d.]+)\'?\s*([EW]?)'
    
    # Ищем широту в тексте
    lat_match = re.search(lat_pattern, text, re.IGNORECASE)
    # Ищем долготу в тексте (после широты или отдельно)
    lon_match = re.search(lon_pattern, text, re.IGNORECASE)
    
    if lat_match and lon_match:
        try:
            # Широта
            lat_dir1 = lat_match.group(1).upper() if lat_match.group(1) else ''
            lat_deg = float(lat_match.group(2))
            lat_min = float(lat_match.group(3))
            lat_dir2 = lat_match.group(4).upper() if lat_match.group(4) else ''
            
            # Долгота
            lon_dir1 = lon_match.group(1).upper() if lon_match.group(1) else ''
            lon_deg = float(lon_match.group(2))
            lon_min = float(lon_match.group(3))
            lon_dir2 = lon_match.group(4).upper() if lon_match.group(4) else ''
            
            # Определяем направление широты
            lat_dir = lat_dir1 or lat_dir2
            if lat_dir == 'S':
                lat_sign = -1
            else:
                lat_sign = 1
            
            # Определяем направление долготы
            lon_dir = lon_dir1 or lon_dir2
            if lon_dir == 'W':
                lon_sign = -1
            else:
                lon_sign = 1
            
            # Вычисляем десятичные градусы
            lat = lat_sign * (lat_deg + lat_min / 60)
            lon = lon_sign * (lon_deg + lon_min / 60)
            
            # Проверка корректности
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                logger.info(f"  📍 Распознаны DDM координаты: {lat} ({lat_deg}°{lat_min}'), {lon} ({lon_deg}°{lon_min}')")
                return lat, lon
        except Exception as e:
            logger.debug(f"  Ошибка парсинга DDM: {e}")
    
    # ========== ФОРМАТ 3: ГРАДУСЫ, МИНУТЫ, СЕКУНДЫ (DMS) ==========
    dms_pattern = r'(\d+)°(\d+)′([\d.]+)″?\s*([NS]?)\s+(\d+)°(\d+)′([\d.]+)″?\s*([EW]?)'
    match = re.search(dms_pattern, text, re.IGNORECASE)
    if match:
        try:
            lat_deg = int(match.group(1))
            lat_min = int(match.group(2))
            lat_sec = float(match.group(3))
            lat_dir = match.group(4).upper() if match.group(4) else ''
            lon_deg = int(match.group(5))
            lon_min = int(match.group(6))
            lon_sec = float(match.group(7))
            lon_dir = match.group(8).upper() if match.group(8) else ''
            
            lat = lat_deg + lat_min/60 + lat_sec/3600
            lon = lon_deg + lon_min/60 + lon_sec/3600
            
            if lat_dir == 'S':
                lat = -lat
            if lon_dir == 'W':
                lon = -lon
            
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                logger.info(f"  📍 Распознаны DMS координаты: {lat}, {lon}")
                return lat, lon
        except Exception as e:
            logger.debug(f"  Ошибка парсинга DMS: {e}")
    
    # ========== ФОРМАТ 4: ПРОСТО ЧИСЛА С РАЗДЕЛИТЕЛЯМИ ==========
    numbers = re.findall(r'-?\d+\.?\d*', text)
    if len(numbers) >= 2:
        try:
            lat = float(numbers[0])
            lon = float(numbers[1])
            if -90 <= lat <= 90 and -180 <= lon <= 180:
                logger.info(f"  📍 Распознаны координаты из чисел: {lat}, {lon}")
                return lat, lon
        except:
            pass
    
    logger.warning(f"  ❌ Не удалось распознать координаты: {original_text}")
    return None, None


def find_photos_by_coordinates(lat: float, lon: float, kml_catalog, village_db, db) -> List[str]:
    """
    Находит снимки, в полигоны которых попадает заданная точка.
    Использует кэш полигонов из kml_processor.
    """
    from services.kml_processor import KMLProcessor
    from config import KML_MARGIN_M
    
    photos_covering_point = []
    point = Point(lon, lat)
    
    for item in kml_catalog.catalog:
        photo_num = item['frame']
        kml_path = os.path.join(KML_DIR, item['file_name'])
        
        if not os.path.exists(kml_path):
            logger.debug(f"  Файл KML не найден: {kml_path}")
            continue
        
        # Временный процессор для проверки
        temp_processor = KMLProcessor(village_db, db)
        data = temp_processor.process_kml_file(kml_path, KML_MARGIN_M)
        
        # Проверяем, есть ли этот снимок в результатах
        for result in data['results']:
            if result['photo_num'] == photo_num:
                # Проверяем, попадает ли точка в полигон
                if temp_processor.polygon_cache and photo_num in temp_processor.polygon_cache:
                    polygon = temp_processor.polygon_cache[photo_num]
                    if polygon.contains(point) or polygon.intersects(point):
                        photos_covering_point.append(photo_num)
                        logger.info(f"  ✅ Точка попадает в снимок: {photo_num}")
                        break
    
    return list(dict.fromkeys(photos_covering_point))


def register_search_handlers(dp, db, village_db):
    
    @dp.message(F.text == "🔍 ПОИСК")
    async def menu_search(message: types.Message, state: FSMContext):
        await message.answer(
            "🔍 <b>Режим поиска аэрофотоснимков</b>\n\n"
            "Введите название деревни, координаты или номер снимка:\n\n"
            "📌 <b>Примеры:</b>\n"
            "• <b>По названию деревни:</b> Горбово, Полунино\n"
            "• <b>По координатам (десятичные):</b> 56.2345 34.1234\n"
            "• <b>По координатам (градусы, минуты, секунды):</b> 56°13'41″ N 34°08'10″ E\n"
            "• <b>По координатам (градусы и десятичные минуты):</b> N 56° 19.938', E 034° 20.525'\n"
            "• <b>По номеру снимка:</b> N56E34-266-016 или 266-016\n\n"
            "💡 <i>Можно вводить как полное название, так и его часть</i>",
            parse_mode="HTML"
        )
        await state.set_state(SearchStates.waiting_for_village)
    
    @dp.message(F.text == "📋 СПИСОК ДЕРЕВЕНЬ")
    async def menu_villages(message: types.Message):
        villages = village_db.villages
        
        if not villages:
            await message.answer(
                "📭 Список деревень пуст. Добавьте населенные пункты через ⚙️ НАСТРОЙКИ → ЗАГРУЗКА НП",
                reply_markup=back_keyboard()
            )
            return
        
        villages_sorted = sorted(villages, key=lambda x: x['name'])
        chunks = [villages_sorted[i:i+25] for i in range(0, len(villages_sorted), 25)]
        
        for i, chunk in enumerate(chunks):
            text = f"📋 <b>Все населенные пункты ({len(villages_sorted)} шт.):</b>\n\n" if i == 0 else ""
            for v in chunk:
                name = v['name']
                village_type = v.get('type', 'деревня')
                lat = v.get('lat', '')
                lon = v.get('lon', '')
                district = v.get('district', '')
                
                if lat and lon:
                    coords = f"📍 {lat}, {lon}"
                else:
                    coords = "📍 координаты не указаны"
                
                text += f"• <b>{name}</b> ({village_type})\n"
                text += f"  {coords}\n"
                text += f"  🏠 Район: {district}\n\n"
            
            await message.answer(text, parse_mode="HTML")
        
        await message.answer(
            "💡 Чтобы найти снимки, нажмите 🔍 ПОИСК и введите название деревни, координаты или номер снимка",
            reply_markup=back_keyboard()
        )
    
    @dp.message(SearchStates.waiting_for_village)
    async def process_search(message: types.Message, state: FSMContext):
        query = message.text.strip()
        if not query:
            return
        
        await state.clear()
        user_id = message.from_user.id
        db.set_last_query(user_id, query)
        
        logger.info(f"🔍 ПОИСК: пользователь {user_id} ищет '{query}'")
        
        # ========== 1. ПРОБУЕМ РАСПАРСИТЬ КАК КООРДИНАТЫ ==========
        lat, lon = parse_coordinates(query)
        if lat and lon:
            logger.info(f"  📍 Определены координаты: {lat}, {lon}")
            
            from services.kml_catalog import KMLCatalog
            kml_catalog = KMLCatalog()
            
            if kml_catalog.is_empty():
                await message.answer(
                    f"📍 <b>Координаты получены:</b>\n\n"
                    f"Широта: {lat:.5f}\nДолгота: {lon:.5f}\n\n"
                    f"❌ Каталог KML пуст. Сначала обработайте KML файл через настройки.",
                    parse_mode="HTML",
                    reply_markup=search_result_keyboard(query)
                )
                return
            
            # Ищем снимки, в которые попадает точка
            photos_covering_point = find_photos_by_coordinates(lat, lon, kml_catalog, village_db, db)
            
            if photos_covering_point:
                db.set_last_photos(user_id, photos_covering_point)
                db.set_last_villages(user_id, f"поиск по координатам ({lat:.5f}, {lon:.5f})")
                
                result_text = (
                    f"📍 <b>Поиск по координатам:</b>\n\n"
                    f"Широта: {lat:.5f}\nДолгота: {lon:.5f}\n\n"
                    f"✅ <b>Найдено снимков, покрывающих эту точку: {len(photos_covering_point)}</b>\n\n"
                    f"📸 <b>Снимки:</b>\n" + "\n".join([f"• {p}" for p in photos_covering_point])
                )
                
                await message.answer(
                    result_text,
                    parse_mode="HTML",
                    reply_markup=photos_keyboard(photos_covering_point)
                )
                return
            else:
                # Если нет снимков, показываем ближайшую деревню
                nearest_village = None
                min_distance = float('inf')
                
                for v in village_db.villages:
                    if v.get('lat') and v.get('lon'):
                        try:
                            v_lat = float(v['lat'])
                            v_lon = float(v['lon'])
                            # Евклидово расстояние (приблизительное)
                            distance = math.sqrt((v_lat - lat) ** 2 + (v_lon - lon) ** 2)
                            if distance < min_distance:
                                min_distance = distance
                                nearest_village = v
                        except:
                            continue
                
                if nearest_village:
                    # Переводим расстояние в километры (1 градус ≈ 111 км)
                    distance_km = min_distance * 111
                    
                    await message.answer(
                        f"📍 <b>Координаты получены:</b>\n\n"
                        f"Широта: {lat:.5f}\nДолгота: {lon:.5f}\n\n"
                        f"❌ <b>Снимки, покрывающие эту точку, не найдены</b>\n\n"
                        f"🔍 <b>Ближайшая деревня:</b>\n"
                        f"• <b>{nearest_village['name']}</b> ({nearest_village.get('type', 'деревня')})\n"
                        f"  🏠 Район: {nearest_village.get('district', 'не указан')}\n"
                        f"  📏 Расстояние: ~{distance_km:.1f} км\n\n"
                        f"💡 Попробуйте поискать снимки по названию этой деревни",
                        parse_mode="HTML",
                        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                            [InlineKeyboardButton(text="🔍 ПОИСК ПО ДЕРЕВНЕ", callback_data=f"search_village_{nearest_village['name']}")],
                            [InlineKeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ", callback_data="back_to_main")]
                        ])
                    )
                else:
                    await message.answer(
                        f"📍 <b>Координаты получены:</b>\n\n"
                        f"Широта: {lat:.5f}\nДолгота: {lon:.5f}\n\n"
                        f"❌ <b>Снимки, покрывающие эту точку, не найдены</b>\n"
                        f"❌ Ближайшая деревня не найдена в каталоге",
                        parse_mode="HTML",
                        reply_markup=search_result_keyboard(query)
                    )
                return
        
        # ========== 2. ПРОБУЕМ РАСПАРСИТЬ КАК НОМЕР СНИМКА ==========
        def parse_photo_number(text: str) -> Optional[str]:
            text = text.strip().upper()
            full_match = re.match(r'([NS]\d+[EW]\d+)-(\d+)-(\d+)', text)
            if full_match:
                return f"{full_match.group(1)}-{full_match.group(2)}-{full_match.group(3)}"
            short_match = re.match(r'(\d+)-(\d+)$', text)
            if short_match:
                return text
            return None
        
        photo_num = parse_photo_number(query)
        if photo_num:
            logger.info(f"  🖼️ Определен номер снимка: {photo_num}")
            results = db.afs_catalog.search_by_frame_name(photo_num)
            if results:
                photos = [r['frame'] for r in results]
                db.set_last_photos(user_id, photos)
                db.set_last_villages(user_id, f"поиск по номеру {photo_num}")
                
                await message.answer(
                    f"✅ <b>Найдено по номеру снимка '{photo_num}':</b>\n\n"
                    f"📸 <b>Снимки ({len(photos)} шт.):</b>\n" + "\n".join([f"• {p}" for p in photos]),
                    parse_mode="HTML",
                    reply_markup=photos_keyboard(photos)
                )
                return
            else:
                await message.answer(
                    f"❌ <b>Снимок '{photo_num}' не найден в каталоге</b>\n\n"
                    f"Проверьте правильность номера или создайте каталог через обработку KML",
                    parse_mode="HTML",
                    reply_markup=search_result_keyboard(query)
                )
                return
        
        # ========== 3. ПОИСК ПО НАЗВАНИЮ ДЕРЕВНИ ==========
        results = db.search_by_village(query)
        
        if results:
            photos = []
            for r in results:
                photos.extend(r['photos'])
            photos = list(dict.fromkeys(photos))
            
            villages = []
            for r in results:
                villages.extend(r['villages'])
            villages = sorted(list(set(villages)))
            villages_text = ', '.join(villages[:15])
            if len(villages) > 15:
                villages_text += f" и ещё {len(villages)-15}"
            
            db.set_last_photos(user_id, photos)
            db.set_last_villages(user_id, villages_text)
            
            result_text = f"✅ <b>Найдено по запросу '{query}':</b>\n\n"
            result_text += f"📍 <b>Населенные пункты:</b> {villages_text}"
            result_text += f"\n\n📸 <b>Снимки ({len(photos)} шт.):</b>\n" + "\n".join([f"• {p}" for p in photos])
            
            await message.answer(
                result_text,
                parse_mode="HTML",
                reply_markup=photos_keyboard(photos)
            )
        else:
            logger.info(f"❌ Результаты для '{query}' не найдены")
            
            await message.answer(
                f"❌ <b>Ничего не найдено для '{query}'</b>\n\n"
                f"Попробуйте:\n"
                f"• Ввести полное название деревни\n"
                f"• Ввести координаты в формате: 56.2345 34.1234\n"
                f"• Ввести координаты в формате: N 56° 19.938', E 034° 20.525'\n"
                f"• Ввести номер снимка: N56E34-266-016 или 266-016\n"
                f"• Посмотреть список всех деревень в меню",
                parse_mode="HTML",
                reply_markup=search_result_keyboard(query)
            )