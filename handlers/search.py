# handlers/search.py
import asyncio
from aiogram import types, F
from aiogram.fsm.context import FSMContext

from states.states import SearchStates
from keyboards.inline import photos_keyboard, search_result_keyboard, back_keyboard
from utils.helpers import safe_delete_message
from config import logger


def register_search_handlers(dp, db, village_db):
    
    @dp.message(F.text == "🔍 ПОИСК")
    async def menu_search(message: types.Message, state: FSMContext):
        await message.answer(
            "🔍 <b>Режим поиска аэрофотоснимков</b>\n\n"
            "Введите название деревни, и я найду все связанные с ней снимки.\n\n"
            "📝 <b>Примеры:</b> Горбово, Полунино, Дураково\n\n"
            "💡 <i>Можно вводить как полное название, так и его часть</i>",
            parse_mode="HTML"
        )
        await state.set_state(SearchStates.waiting_for_village)
    
    @dp.message(F.text == "📋 СПИСОК ДЕРЕВЕНЬ")
    async def menu_villages(message: types.Message):
        villages = db.get_all_villages_list()
        if not villages:
            await message.answer("📭 Список деревень пуст. Добавьте населенные пункты через ⚙️ НАСТРОЙКА → ЗАГРУЗКА НП")
            return
        
        chunks = [villages[i:i+25] for i in range(0, len(villages), 25)]
        for i, chunk in enumerate(chunks):
            text = f"📋 <b>Все деревни ({len(villages)} шт.):</b>\n\n" if i == 0 else ""
            text += "\n".join([f"• {v}" for v in chunk])
            await message.answer(text, parse_mode="HTML")
        
        await message.answer(
            "💡 Чтобы найти снимки, нажмите 🔍 ПОИСК и введите название деревни",
            reply_markup=back_keyboard()
        )
    
    @dp.message(SearchStates.waiting_for_village)
    async def process_search(message: types.Message, state: FSMContext):
        query = message.text
        if not query:
            return
        
        await state.clear()
        user_id = message.from_user.id
        db.set_last_query(user_id, query)
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
            
            await message.answer(
                f"✅ <b>Найдено по запросу '{query}':</b>\n\n"
                f"📍 <b>Населенные пункты:</b> {villages_text}\n\n"
                f"📸 <b>Снимки ({len(photos)} шт.):</b>\n" + "\n".join([f"• {p}" for p in photos]),
                parse_mode="HTML",
                reply_markup=photos_keyboard(photos)
            )
        else:
            await message.answer(
                f"❌ <b>Ничего не найдено для '{query}'</b>\n\n"
                f"Попробуйте:\n"
                f"• Ввести полное название деревни\n"
                f"• Проверить правильность написания\n"
                f"• Посмотреть список всех деревень в меню",
                parse_mode="HTML",
                reply_markup=search_result_keyboard(query)
            )