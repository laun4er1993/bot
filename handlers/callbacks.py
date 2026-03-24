# handlers/callbacks.py
import os
import time
from aiogram import types, F
from aiogram.fsm.context import FSMContext
from aiogram.types import BufferedInputFile

from keyboards.inline import (
    get_np_settings_keyboard, get_delete_district_keyboard,
    get_confirm_delete_district_keyboard, get_confirm_clear_all_keyboard,
    get_district_keyboard, get_more_districts_keyboard,
    locus_instruction_keyboard, locus_download_keyboard,
    back_to_locus_keyboard, back_keyboard, photos_keyboard,
    photo_details_keyboard, get_catalog_settings_keyboard,
    get_afs_catalog_keyboard
)
from utils.helpers import safe_edit_text, safe_answer_callback, safe_delete_message
from config import logger
from handlers.start import cmd_start


def register_callbacks(dp, village_db, db):
    
    @dp.callback_query(lambda c: c.data == "catalog_settings")
    async def catalog_settings_handler(callback: types.CallbackQuery):
        """Меню настроек каталога АФС"""
        await safe_edit_text(
            callback.message,
            "⚙️ <b>Настройки каталога АФС</b>\n\n"
            "Управление каталогом аэрофотоснимков (АФС):\n"
            "• Просмотр и статистика\n"
            "• Сравнение с результатами KML\n"
            "• Загрузка общего каталога\n"
            "• Экспорт в TXT",
            parse_mode="HTML",
            reply_markup=get_catalog_settings_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "show_more_districts")
    async def show_more_districts(callback: types.CallbackQuery):
        await safe_edit_text(
            callback.message,
            "🌐 <b>Выберите район для загрузки</b>\n\n"
            f"Всего доступно районов: {len(AVAILABLE_DISTRICTS)}\n"
            f"Выберите из списка ниже:",
            parse_mode="HTML",
            reply_markup=get_more_districts_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "back_to_districts")
    async def back_to_districts(callback: types.CallbackQuery):
        await safe_edit_text(
            callback.message,
            "🌐 <b>Выберите район для загрузки</b>\n\n"
            "Выберите район из списка ниже:",
            parse_mode="HTML",
            reply_markup=get_district_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "locus_instruction")
    async def locus_instruction(callback: types.CallbackQuery):
        await safe_edit_text(
            callback.message,
            "📖 <b>Инструкция по работе с Locus Maps</b>\n\n"
            "1️⃣ Скачайте приложение Locus Maps из магазина приложений\n"
            "2️⃣ Скачайте карту Ржевского района по ссылке ниже\n"
            "3️⃣ Скачайте MBTILES файл нужного снимка\n"
            "4️⃣ Откройте MBTILES файл в приложении Locus Maps\n"
            "5️⃣ Снимок отобразится на карте как дополнительный слой\n\n"
            "📥 <b>Полезные ссылки:</b>",
            reply_markup=locus_instruction_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "locus_download")
    async def locus_download(callback: types.CallbackQuery):
        await safe_edit_text(
            callback.message,
            "📥 <b>Скачать Locus Maps</b>\n\n"
            "Нажмите кнопку для скачивания приложения:",
            reply_markup=locus_download_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "back_to_locus")
    async def back_to_locus(callback: types.CallbackQuery):
        await safe_edit_text(
            callback.message,
            "🗺️ <b>Locus Maps</b>\n\nВыберите действие:",
            reply_markup=back_to_locus_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "back_to_main")
    async def back_to_main(callback: types.CallbackQuery, state: FSMContext):
        await state.clear()
        await safe_delete_message(callback.message)
        await cmd_start(callback.message)
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data.startswith("photo_"))
    async def process_photo(callback: types.CallbackQuery):
        photo = callback.data.replace("photo_", "")
        details = db.get_photo_details(photo)
        
        await safe_edit_text(
            callback.message,
            details or f"📸 <b>Снимок {photo}</b>\n\n❌ Информация отсутствует",
            parse_mode="HTML",
            reply_markup=photo_details_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "back_to_photos")
    async def back_to_photos(callback: types.CallbackQuery):
        user_id = callback.from_user.id
        photos = db.get_last_photos(user_id)
        villages = db.get_last_villages(user_id)
        query = db.get_last_query(user_id)
        
        if photos:
            await safe_edit_text(
                callback.message,
                f"✅ <b>Найдено по запросу '{query}':</b>\n\n"
                f"📍 <b>Деревни:</b> {villages}\n\n"
                f"📸 <b>Снимки ({len(photos)} шт.):</b>\n" + "\n".join([f"• {p}" for p in photos]),
                parse_mode="HTML",
                reply_markup=photos_keyboard(photos)
            )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "try_again")
    async def try_again(callback: types.CallbackQuery, state: FSMContext):
        await safe_delete_message(callback.message)
        await callback.message.answer("🔍 Введите название деревни:")
        await state.set_state(SearchStates.waiting_for_village)
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "show_villages")
    async def show_villages(callback: types.CallbackQuery):
        await safe_delete_message(callback.message)
        
        villages = db.get_all_villages_list()
        if not villages:
            await callback.message.answer("📭 Список деревень пуст. Добавьте населенные пункты через ⚙️ НАСТРОЙКА → ЗАГРУЗКА НП")
            await safe_answer_callback(callback)
            return
        
        chunks = [villages[i:i+25] for i in range(0, len(villages), 25)]
        for i, chunk in enumerate(chunks):
            text = f"📋 <b>Все деревни ({len(villages)} шт.):</b>\n\n" if i == 0 else ""
            text += "\n".join([f"• {v}" for v in chunk])
            await callback.message.answer(text, parse_mode="HTML")
        
        await callback.message.answer("💡 Нажмите 🔍 ПОИСК и введите название деревни", reply_markup=back_keyboard())
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "process_kml_again")
    async def process_kml_again(callback: types.CallbackQuery, state: FSMContext):
        await safe_delete_message(callback.message)
        await callback.message.answer(
            "📤 <b>Загрузите KML файл</b>\n\n"
            "Отправьте мне KML файл с каталогом снимков.\n"
            "После загрузки я найду населенные пункты в каждом кадре и создам подробный отчет с полными описаниями.",
            parse_mode="HTML"
        )
        await state.set_state(SearchStates.waiting_for_kml)
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "no_op")
    async def no_op(callback: types.CallbackQuery):
        await safe_answer_callback(callback)