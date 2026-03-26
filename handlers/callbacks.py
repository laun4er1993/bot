import os
import time
from aiogram import types, F
from aiogram.fsm.context import FSMContext
from aiogram.types import BufferedInputFile, InlineKeyboardMarkup, InlineKeyboardButton

from keyboards.inline import (
    get_np_settings_keyboard,
    get_delete_district_keyboard,
    get_confirm_delete_district_keyboard,
    get_confirm_clear_all_keyboard,
    get_district_keyboard,
    locus_instruction_keyboard,
    locus_download_keyboard,
    back_to_locus_keyboard,
    back_keyboard,
    photos_keyboard,
    get_catalog_settings_keyboard,
    get_afs_catalog_keyboard,
    map_download_keyboard,
    get_kml_management_keyboard,
    get_settings_main_keyboard,
    get_all_districts_keyboard
)
from utils.helpers import safe_edit_text, safe_answer_callback, safe_delete_message
from config import logger
from handlers.start import cmd_start
from states.states import SearchStates


def register_callbacks(dp, village_db, db):
    
    @dp.callback_query(lambda c: c.data == "catalog_settings")
    async def catalog_settings_handler(callback: types.CallbackQuery):
        await safe_edit_text(
            callback.message,
            "📸 <b>Управление каталогом АФС</b>\n\n"
            "Каталог аэрофотоснимков (АФС) содержит:\n"
            "• Номера снимков\n"
            "• Описания кадров\n"
            "• Связи с населенными пунктами\n\n"
            "Выберите действие:",
            parse_mode="HTML",
            reply_markup=get_catalog_settings_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "show_more_districts")
    async def show_more_districts(callback: types.CallbackQuery):
        from api_sources.config import AVAILABLE_DISTRICTS
        await safe_edit_text(
            callback.message,
            "🌐 <b>Выберите район для загрузки</b>\n\n"
            f"Всего доступно районов: {len(AVAILABLE_DISTRICTS)}\n"
            f"Выберите из списка ниже:",
            parse_mode="HTML",
            reply_markup=get_all_districts_keyboard()
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
    
    @dp.callback_query(lambda c: c.data == "map_rzhev")
    async def map_rzhev_handler(callback: types.CallbackQuery):
        from config import MAP_RZHEV_URL
        await safe_edit_text(
            callback.message,
            "🗺️ <b>Карта Ржевского района для Locus Maps</b>\n\n"
            f"Ссылка: <a href='{MAP_RZHEV_URL}'>Скачать карту</a>\n\n"
            "Нажмите кнопку для скачивания:",
            parse_mode="HTML",
            reply_markup=map_download_keyboard(MAP_RZHEV_URL)
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "back_to_locus")
    async def back_to_locus(callback: types.CallbackQuery):
        await safe_edit_text(
            callback.message,
            "🗺️ <b>Locus Maps</b>\n\n"
            "Выберите действие:",
            parse_mode="HTML",
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
        
        villages = db.afs_catalog.get_villages_for_frame(photo)
        
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔙 НАЗАД К СПИСКУ", callback_data="back_to_photos")],
            [InlineKeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ", callback_data="back_to_main")]
        ])
        
        if len(villages) > 5:
            keyboard.inline_keyboard.insert(
                0,
                [InlineKeyboardButton(text="📋 ПОКАЗАТЬ ВСЕ НП", callback_data=f"show_all_villages_{photo}")]
            )
        
        await safe_edit_text(
            callback.message,
            details or f"📸 <b>Снимок {photo}</b>\n\n❌ Информация отсутствует",
            parse_mode="HTML",
            reply_markup=keyboard
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data.startswith("show_all_villages_"))
    async def show_all_villages_handler(callback: types.CallbackQuery):
        """Показывает все населенные пункты для снимка с полным описанием"""
        photo_num = callback.data.replace("show_all_villages_", "")
        
        details = db.get_photo_details_with_full_villages(photo_num)
        
        if not details:
            await callback.answer("Нет данных о населенных пунктах")
            return
        
        await callback.message.answer(
            details,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 НАЗАД К СНИМКУ", callback_data=f"photo_{photo_num}")],
                [InlineKeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ", callback_data="back_to_main")]
            ])
        )
        await callback.answer()
    
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
                f"📍 <b>Населенные пункты:</b> {villages}\n\n"
                f"📸 <b>Снимки ({len(photos)} шт.):</b>\n" + "\n".join([f"• {p}" for p in photos]),
                parse_mode="HTML",
                reply_markup=photos_keyboard(photos)
            )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "try_again")
    async def try_again(callback: types.CallbackQuery, state: FSMContext):
        await safe_delete_message(callback.message)
        await callback.message.answer(
            "🔍 <b>Поиск аэрофотоснимков</b>\n\n"
            "Введите название деревни:",
            parse_mode="HTML"
        )
        await state.set_state(SearchStates.waiting_for_village)
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "show_villages")
    async def show_villages(callback: types.CallbackQuery):
        """Показывает полную информацию о всех населенных пунктах"""
        await safe_delete_message(callback.message)
        
        villages = village_db.villages
        if not villages:
            await callback.message.answer(
                "📭 Список деревень пуст.\n\n"
                "Добавьте населенные пункты через:\n"
                "⚙️ НАСТРОЙКИ → 🏘️ НАСЕЛЕННЫЕ ПУНКТЫ",
                reply_markup=back_keyboard()
            )
            await safe_answer_callback(callback)
            return
        
        villages_sorted = sorted(villages, key=lambda x: x['name'])
        chunks = [villages_sorted[i:i+15] for i in range(0, len(villages_sorted), 15)]
        
        for i, chunk in enumerate(chunks):
            text = f"📋 <b>Населенные пункты ({len(villages_sorted)} шт.)</b>\n\n" if i == 0 else ""
            
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
            
            await callback.message.answer(text, parse_mode="HTML")
        
        await callback.message.answer(
            "💡 Нажмите 🔍 ПОИСК и введите название деревни, координаты или номер снимка",
            reply_markup=back_keyboard()
        )
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
    
    @dp.callback_query(lambda c: c.data == "back_to_settings_main")
    async def back_to_settings_main(callback: types.CallbackQuery):
        await safe_edit_text(
            callback.message,
            "⚙️ <b>Центр управления ботом</b>\n\n"
            "Выберите категорию для настройки:",
            parse_mode="HTML",
            reply_markup=get_settings_main_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "kml_management_menu")
    async def kml_management_menu(callback: types.CallbackQuery):
        from services.kml_catalog import KMLCatalog
        kml_catalog = KMLCatalog()
        stats = kml_catalog.get_statistics()
        
        text = (
            f"🗺️ <b>Управление KML файлами</b>\n\n"
            f"📊 <b>Статистика:</b>\n"
            f"• Всего файлов: {stats['total']}\n"
            f"• С описаниями: {stats['with_description']}\n"
            f"• Файлов на диске: {stats['with_file']}\n\n"
        )
        
        if stats['recent_items']:
            text += f"📌 <b>Последние добавленные:</b>\n"
            for item in stats['recent_items'][:3]:
                text += f"• {item['frame']}\n"
            text += "\n"
        
        await safe_edit_text(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=get_kml_management_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "np_settings_menu")
    async def np_settings_menu(callback: types.CallbackQuery):
        stats = village_db.get_stats()
        districts = village_db.get_districts()
        
        text = (
            f"🏘️ <b>Управление населенными пунктами</b>\n\n"
            f"📊 <b>Статистика:</b>\n"
            f"• Всего: {stats['total']}\n"
            f"• С координатами: {stats['with_coords']}\n"
            f"• Без координат: {stats['total'] - stats['with_coords']}\n"
        )
        
        if districts:
            text += f"\n📍 <b>Районы ({len(districts)}):</b>\n"
            for d in districts[:5]:
                text += f"• {d}: {len(village_db.get_villages_by_district(d))} НП\n"
            if len(districts) > 5:
                text += f"• ... и ещё {len(districts)-5}\n"
        
        await safe_edit_text(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=get_np_settings_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "catalog_settings_menu")
    async def catalog_settings_menu(callback: types.CallbackQuery):
        stats = db.afs_catalog.get_statistics()
        
        text = (
            f"📸 <b>Каталог АФС</b>\n\n"
            f"📊 <b>Статистика:</b>\n"
            f"• Всего снимков: {stats['total']}\n"
            f"• С описаниями: {stats['with_description']}\n"
            f"• Снимков с НП: {stats['with_villages']}\n"
            f"• Связей НП-снимки: {stats['total_village_links']}\n\n"
        )
        
        if stats['total'] == 0:
            text += "ℹ️ <b>Как создать каталог:</b>\n"
            text += "1. Загрузите населенные пункты\n"
            text += "2. Обработайте KML файл\n"
            text += "3. Нажмите 'Создать каталог АФС'\n"
        
        await safe_edit_text(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=get_catalog_settings_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "afs_stats")
    async def afs_stats_handler(callback: types.CallbackQuery):
        stats = db.afs_catalog.get_statistics()
        
        text = (
            f"📊 <b>Статистика каталога АФС</b>\n\n"
            f"• Всего снимков: {stats['total']}\n"
            f"• С описаниями: {stats['with_description']}\n"
            f"• Без описаний: {stats['without_description']}\n"
            f"• Снимков с НП: {stats['with_villages']}\n"
            f"• Снимков без НП: {stats['without_villages']}\n"
            f"• Всего связей: {stats['total_village_links']}\n"
            f"• Средняя длина описания: {stats['avg_description_length']} символов\n"
        )
        
        await safe_edit_text(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=get_catalog_settings_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "show_afs_catalog")
    async def show_afs_catalog(callback: types.CallbackQuery):
        if db.afs_catalog.is_empty():
            await callback.message.answer(
                "📭 Каталог АФС пуст.\n\n"
                "Создайте каталог через обработку KML.",
                reply_markup=back_keyboard()
            )
            await callback.answer()
            return
        
        text, total_pages, current = db.afs_catalog.get_catalog_text(
            with_descriptions=False, page=1, per_page=50
        )
        
        await safe_edit_text(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=get_afs_catalog_keyboard(
                has_catalog=True, page=current, total_pages=total_pages
            )
        )
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data.startswith("afs_page_"))
    async def afs_page_handler(callback: types.CallbackQuery):
        page = int(callback.data.replace("afs_page_", ""))
        text, total_pages, current = db.afs_catalog.get_catalog_text(
            with_descriptions=False, page=page, per_page=50
        )
        
        await safe_edit_text(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=get_afs_catalog_keyboard(
                has_catalog=True, page=current, total_pages=total_pages
            )
        )
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "download_afs_catalog")
    async def download_afs_catalog(callback: types.CallbackQuery):
        if db.afs_catalog.is_empty():
            await callback.message.answer("❌ Каталог АФС пуст")
            await callback.answer()
            return
        
        try:
            filepath = db.afs_catalog.export_to_txt()
            if filepath and os.path.exists(filepath):
                with open(filepath, 'rb') as f:
                    await callback.message.answer_document(
                        BufferedInputFile(f.read(), filename=os.path.basename(filepath)),
                        caption=f"📁 Каталог АФС: {len(db.afs_catalog.catalog)} снимков",
                        parse_mode="HTML"
                    )
                os.unlink(filepath)
        except Exception as e:
            await callback.message.answer(f"❌ Ошибка: {e}")
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "clear_afs_catalog")
    async def clear_afs_catalog(callback: types.CallbackQuery):
        removed = db.afs_catalog.clear()
        await safe_edit_text(
            callback.message,
            f"🗑️ <b>Каталог АФС очищен</b>\n\nУдалено: {removed} снимков",
            parse_mode="HTML",
            reply_markup=get_catalog_settings_keyboard()
        )
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "village_stats")
    async def village_stats(callback: types.CallbackQuery):
        stats = village_db.get_stats()
        districts = village_db.get_districts()
        
        text = (
            f"📊 <b>Статистика населенных пунктов</b>\n\n"
            f"• Всего: {stats['total']}\n"
            f"• С координатами: {stats['with_coords']}\n"
            f"• Без координат: {stats['total'] - stats['with_coords']}\n"
        )
        
        if districts:
            text += f"\n📍 <b>Районы:</b>\n"
            for d in districts[:10]:
                text += f"• {d}: {len(village_db.get_villages_by_district(d))} НП\n"
            if len(districts) > 10:
                text += f"• ... и ещё {len(districts)-10}\n"
        
        await safe_edit_text(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=get_np_settings_keyboard()
        )
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "download_villages_txt")
    async def download_villages_txt(callback: types.CallbackQuery):
        if not village_db.villages:
            await callback.message.answer("❌ Каталог населенных пунктов пуст")
            await callback.answer()
            return
        
        try:
            filepath = village_db.export_to_txt()
            if filepath and os.path.exists(filepath):
                with open(filepath, 'rb') as f:
                    await callback.message.answer_document(
                        BufferedInputFile(f.read(), filename=os.path.basename(filepath)),
                        caption=f"📁 Каталог НП: {village_db.stats['total']} записей",
                        parse_mode="HTML"
                    )
                os.unlink(filepath)
        except Exception as e:
            await callback.message.answer(f"❌ Ошибка: {e}")
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "delete_district_start")
    async def delete_district_start(callback: types.CallbackQuery):
        districts = village_db.get_districts()
        await safe_edit_text(
            callback.message,
            "🗑️ <b>Удаление района</b>\n\nВыберите район для удаления:",
            parse_mode="HTML",
            reply_markup=get_delete_district_keyboard(districts)
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data.startswith("delete_district_confirm_"))
    async def delete_district_confirm(callback: types.CallbackQuery):
        district = callback.data.replace("delete_district_confirm_", "")
        count = len(village_db.get_villages_by_district(district))
        await safe_edit_text(
            callback.message,
            f"🗑️ <b>Удаление района {district}</b>\n\n⚠️ Будет удалено {count} населенных пунктов.\n\nВы уверены?",
            parse_mode="HTML",
            reply_markup=get_confirm_delete_district_keyboard(district, count)
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data.startswith("confirm_delete_district_"))
    async def delete_district_execute(callback: types.CallbackQuery):
        district = callback.data.replace("confirm_delete_district_", "")
        removed, with_coords = village_db.remove_district(district)
        await safe_edit_text(
            callback.message,
            f"✅ <b>Район {district} удален</b>\n\nУдалено: {removed} НП (из них с координатами: {with_coords})",
            parse_mode="HTML",
            reply_markup=get_np_settings_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "clear_all_catalog")
    async def clear_all_catalog_confirm(callback: types.CallbackQuery):
        total = village_db.stats['total']
        if total == 0:
            await safe_edit_text(callback.message, "📭 Каталог уже пуст", reply_markup=get_np_settings_keyboard())
            await safe_answer_callback(callback)
            return
        await safe_edit_text(
            callback.message,
            f"⚠️ <b>ОЧИСТКА ВСЕГО КАТАЛОГА</b>\n\nУдалить все {total} населенных пунктов?\n\nЭто действие необратимо!",
            parse_mode="HTML",
            reply_markup=get_confirm_clear_all_keyboard(total)
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "confirm_clear_all")
    async def clear_all_catalog_execute(callback: types.CallbackQuery):
        removed = village_db.clear_all()
        await safe_edit_text(
            callback.message,
            f"✅ <b>Каталог населенных пунктов очищен</b>\n\nУдалено: {removed} записей",
            parse_mode="HTML",
            reply_markup=get_np_settings_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "download_from_web_start")
    async def download_from_web_start(callback: types.CallbackQuery, state: FSMContext):
        await safe_edit_text(
            callback.message,
            "🌐 <b>Загрузка из интернета</b>\n\nВыберите район для загрузки населенных пунктов:",
            parse_mode="HTML",
            reply_markup=get_district_keyboard()
        )
        await state.set_state(SearchStates.waiting_for_district_select)
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "add_village_manual")
    async def add_village_manual_start(callback: types.CallbackQuery, state: FSMContext):
        await safe_edit_text(
            callback.message,
            "✏️ <b>Добавление населенного пункта вручную</b>\n\n"
            "Введите данные в формате:\n"
            "<code>название,тип,широта,долгота,район</code>\n\n"
            "📌 <b>Пример:</b>\n"
            "<code>Горбово,деревня,56.2345,34.1234,Ржевский</code>\n\n"
            "Типы: деревня, село, посёлок, хутор, станция, урочище",
            parse_mode="HTML"
        )
        await state.set_state(SearchStates.waiting_for_add_village)
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "load_catalog_txt")
    async def load_catalog_txt_start(callback: types.CallbackQuery, state: FSMContext):
        await safe_edit_text(
            callback.message,
            "📂 <b>Загрузка каталога населенных пунктов</b>\n\n"
            "Отправьте TXT файл в формате:\n"
            "<code>Название Тип Широта Долгота Район</code>\n\n"
            "📌 <b>Пример строки:</b>\n"
            "<code>Горбово деревня 56.2345 34.1234 Ржевский</code>\n\n"
            "Если координаты неизвестны, укажите '-'",
            parse_mode="HTML"
        )
        await state.set_state(SearchStates.waiting_for_txt_upload)
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "check_bot_status")
    async def check_bot_status(callback: types.CallbackQuery):
        from services.kml_catalog import KMLCatalog
        np_stats = village_db.get_stats()
        afs_stats = db.afs_catalog.get_statistics()
        kml_catalog = KMLCatalog()
        kml_stats = kml_catalog.get_statistics()
        yandex_status = "✅ Доступен" if db.get_yandex_disk_status() else "❌ Недоступен"
        
        text = (
            f"🔧 <b>Статус бота</b>\n\n"
            f"📊 <b>Данные:</b>\n"
            f"{'✅' if np_stats['total'] > 0 else '❌'} Населенные пункты: {np_stats['total']}\n"
            f"{'✅' if afs_stats['total'] > 0 else '❌'} Каталог АФС: {afs_stats['total']} снимков\n"
            f"{'✅' if kml_stats['total'] > 0 else '❌'} Каталог KML: {kml_stats['total']} файлов\n"
            f"{'✅' if len(db.photo_files) > 0 else '❌'} Файлы на Яндекс.Диске: {len(db.photo_files)}\n\n"
            f"☁️ <b>Яндекс.Диск:</b> {yandex_status}\n\n"
            f"🤖 <b>Бот:</b> ✅ Активен"
        )
        
        await safe_edit_text(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=get_kml_management_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "enable_bot")
    async def enable_bot(callback: types.CallbackQuery):
        await safe_edit_text(
            callback.message,
            "🤖 <b>Управление ботом</b>\n\n"
            "Бот работает в штатном режиме.\n\n"
            "Для изменения статуса используйте команду /start",
            parse_mode="HTML",
            reply_markup=get_settings_main_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data.startswith("search_village_"))
    async def search_village_handler(callback: types.CallbackQuery, state: FSMContext):
        """Обработчик поиска по деревне из результата поиска по координатам"""
        village_name = callback.data.replace("search_village_", "")
        
        await safe_edit_text(
            callback.message,
            f"🔍 <b>Поиск снимков для деревни:</b> {village_name}\n\n"
            f"⏳ Подождите, идет поиск...",
            parse_mode="HTML"
        )
        
        results = db.search_by_village(village_name)
        
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
            
            db.set_last_photos(callback.from_user.id, photos)
            db.set_last_villages(callback.from_user.id, villages_text)
            db.set_last_query(callback.from_user.id, village_name)
            
            result_text = f"✅ <b>Найдено по запросу '{village_name}':</b>\n\n"
            result_text += f"📍 <b>Населенные пункты:</b> {villages_text}"
            result_text += f"\n\n📸 <b>Снимки ({len(photos)} шт.):</b>\n" + "\n".join([f"• {p}" for p in photos])
            
            await safe_edit_text(
                callback.message,
                result_text,
                parse_mode="HTML",
                reply_markup=photos_keyboard(photos)
            )
        else:
            await safe_edit_text(
                callback.message,
                f"❌ <b>Не найдено снимков для деревни '{village_name}'</b>\n\n"
                f"Возможно, каталог АФС пуст. Создайте его через обработку KML.",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
        
        await safe_answer_callback(callback)