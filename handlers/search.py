import asyncio
from aiogram import types, F
from aiogram.fsm.context import FSMContext

from states.states import SearchStates
from keyboards.inline import photos_keyboard, search_result_keyboard, back_keyboard
from config import logger


def register_search_handlers(dp, db, village_db):
    
    @dp.message(F.text == "🔍 ПОИСК СНИМКОВ")
    async def menu_search(message: types.Message, state: FSMContext):
        await message.answer(
            "🔍 <b>Поиск аэрофотоснимков</b>\n\n"
            "Введите название деревни:\n\n"
            "📌 <b>Примеры:</b>\n"
            "• Горбово\n"
            "• Полунино\n"
            "• Есинка\n\n"
            "💡 <i>Можно вводить как полное название, так и его часть</i>",
            parse_mode="HTML"
        )
        await state.set_state(SearchStates.waiting_for_village)
    
    @dp.message(F.text == "📋 ВСЕ ДЕРЕВНИ")
    async def menu_villages(message: types.Message):
        villages = village_db.villages
        
        if not villages:
            await message.answer(
                "📭 Список деревень пуст.\n\n"
                "Добавьте населенные пункты через:\n"
                "⚙️ НАСТРОЙКИ → 🏘️ НАСЕЛЕННЫЕ ПУНКТЫ",
                reply_markup=back_keyboard()
            )
            return
        
        villages_sorted = sorted(villages, key=lambda x: x['name'])
        chunks = [villages_sorted[i:i+20] for i in range(0, len(villages_sorted), 20)]
        
        for i, chunk in enumerate(chunks):
            text = f"📋 <b>Населенные пункты ({len(villages_sorted)} шт.)</b>\n\n" if i == 0 else ""
            for v in chunk:
                coords = f"📍 {v['lat']}, {v['lon']}" if v.get('lat') and v.get('lon') else "📍 координаты не указаны"
                text += f"• <b>{v['name']}</b> ({v.get('type', 'деревня')})\n"
                text += f"  {coords}\n"
                text += f"  🏠 Район: {v.get('district', 'не указан')}\n\n"
            
            await message.answer(text, parse_mode="HTML")
        
        await message.answer(
            "💡 Чтобы найти снимки, нажмите 🔍 ПОИСК СНИМКОВ и введите название деревни",
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
        
        logger.info(f"🔍 ПОИСК: '{query}'")
        
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
            
            result_text = (
                f"✅ <b>Найдено по запросу '{query}':</b>\n\n"
                f"📍 <b>Населенные пункты:</b> {villages_text}\n\n"
                f"📸 <b>Снимки ({len(photos)} шт.):</b>\n"
            )
            
            await message.answer(
                result_text,
                parse_mode="HTML",
                reply_markup=photos_keyboard(photos)
            )
        else:
            await message.answer(
                f"❌ <b>Ничего не найдено для '{query}'</b>\n\n"
                f"Попробуйте:\n"
                f"• Ввести полное название деревни\n"
                f"• Посмотреть список всех деревень в меню\n\n"
                f"💡 <i>Если деревня есть в списке, но снимки не найдены — возможно, каталог АФС пуст. Создайте его через KML обработку.</i>",
                parse_mode="HTML",
                reply_markup=search_result_keyboard(query)
            )