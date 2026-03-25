import asyncio
import re
from aiogram import types, F
from aiogram.fsm.context import FSMContext

from states.states import SearchStates
from keyboards.inline import photos_keyboard, search_result_keyboard, back_keyboard
from config import logger


def parse_coordinates(text: str):
    """Парсит координаты из текста"""
    # Формат: 56.2345 34.1234
    decimal_match = re.search(r'(-?\d+\.\d+)[,\s]+(-?\d+\.\d+)', text)
    if decimal_match:
        try:
            lat = float(decimal_match.group(1))
            lon = float(decimal_match.group(2))
            return lat, lon
        except:
            pass
    
    # Формат: 56°13'41″ N 34°08'10″ E
    dms_pattern = r'(\d+)°(\d+)′([\d.]+)″\s*([NSEW]?)\s+(\d+)°(\d+)′([\d.]+)″\s*([NSEW]?)'
    match = re.search(dms_pattern, text, re.IGNORECASE)
    if match:
        try:
            lat_deg, lat_min, lat_sec = int(match.group(1)), int(match.group(2)), float(match.group(3))
            lat_dir = match.group(4).upper() if match.group(4) else ''
            lon_deg, lon_min, lon_sec = int(match.group(5)), int(match.group(6)), float(match.group(7))
            lon_dir = match.group(8).upper() if match.group(8) else ''
            
            lat = lat_deg + lat_min/60 + lat_sec/3600
            lon = lon_deg + lon_min/60 + lon_sec/3600
            
            if lat_dir == 'S':
                lat = -lat
            if lon_dir == 'W':
                lon = -lon
            
            return lat, lon
        except:
            pass
    
    return None, None


def parse_photo_number(text: str) -> str:
    """Парсит номер снимка из текста"""
    # Формат: N56E34-266-016 или 266-016
    text = text.strip().upper()
    
    # Полный формат N56E34-266-016
    full_match = re.match(r'([NS]\d+[EW]\d+)-(\d+)-(\d+)', text)
    if full_match:
        return f"{full_match.group(1)}-{full_match.group(2)}-{full_match.group(3)}"
    
    # Сокращенный формат 266-016
    short_match = re.match(r'(\d+)-(\d+)$', text)
    if short_match:
        # Нужно найти квадрат по координатам? Пока возвращаем как есть
        return text
    
    return None


def register_search_handlers(dp, db, village_db):
    
    @dp.message(F.text == "🔍 ПОИСК")
    async def menu_search(message: types.Message, state: FSMContext):
        await message.answer(
            "🔍 <b>Режим поиска аэрофотоснимков</b>\n\n"
            "Введите название деревни, координаты или номер снимка:\n\n"
            "📌 <b>Примеры:</b>\n"
            "• <b>По названию деревни:</b> Горбово, Полунино\n"
            "• <b>По координатам:</b> 56.2345 34.1234 или 56°13'41″ N 34°08'10″ E\n"
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
        
        # Пробуем распарсить как координаты
        lat, lon = parse_coordinates(query)
        if lat and lon:
            logger.info(f"  📍 Определены координаты: {lat}, {lon}")
            # Поиск деревни по координатам
            nearest_village = None
            min_distance = float('inf')
            
            for v in village_db.villages:
                if v.get('lat') and v.get('lon'):
                    try:
                        v_lat = float(v['lat'])
                        v_lon = float(v['lon'])
                        # Евклидово расстояние (приблизительно)
                        distance = ((v_lat - lat) ** 2 + (v_lon - lon) ** 2) ** 0.5
                        if distance < min_distance:
                            min_distance = distance
                            nearest_village = v
                    except:
                        continue
            
            if nearest_village and min_distance < 0.1:  # ~10 км
                await message.answer(
                    f"📍 <b>Найдена деревня по координатам:</b>\n\n"
                    f"• <b>{nearest_village['name']}</b> ({nearest_village.get('type', 'деревня')})\n"
                    f"  🏠 Район: {nearest_village.get('district', 'не указан')}\n\n"
                    f"🔍 Ищу снимки для этой деревни...",
                    parse_mode="HTML"
                )
                query = nearest_village['name']
            else:
                await message.answer(
                    f"📍 <b>Координаты получены</b>\n\n"
                    f"Широта: {lat}\nДолгота: {lon}\n\n"
                    f"❌ Ближайшая деревня не найдена в каталоге",
                    parse_mode="HTML",
                    reply_markup=search_result_keyboard(query)
                )
                return
        
        # Пробуем распарсить как номер снимка
        photo_num = parse_photo_number(query)
        if photo_num:
            logger.info(f"  🖼️ Определен номер снимка: {photo_num}")
            # Ищем снимок в каталоге АФС
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
        
        # Поиск по названию деревни
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
                f"❌ <b>Ничего не найдено para '{query}'</b>\n\n"
                f"Попробуйте:\n"
                f"• Ввести полное название деревни\n"
                f"• Ввести координаты в формате: 56.2345 34.1234\n"
                f"• Ввести номер снимка: N56E34-266-016 или 266-016\n"
                f"• Посмотреть список всех деревень в меню",
                parse_mode="HTML",
                reply_markup=search_result_keyboard(query)
            )