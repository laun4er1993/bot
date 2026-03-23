# handlers/callbacks.py
import os
import time
from aiogram import types, F
from aiogram.fsm.context import FSMContext
from aiogram.types import FSInputFile

from keyboards.inline import (
    get_settings_keyboard, get_delete_district_keyboard,
    get_confirm_delete_district_keyboard, get_confirm_clear_all_keyboard,
    get_district_keyboard, get_more_districts_keyboard,
    locus_instruction_keyboard, locus_download_keyboard,
    back_to_locus_keyboard, stats_back_keyboard, back_keyboard,
    photos_keyboard, photo_details_keyboard
)
from utils.helpers import safe_edit_text, safe_answer_callback, safe_delete_message
from config import logger, TEMP_DIR


def register_callbacks(dp, village_db, db):
    
    @dp.callback_query(lambda c: c.data == "back_to_settings")
    async def back_to_settings(callback: types.CallbackQuery):
        stats = village_db.get_stats()
        text = (
            f"⚙️ <b>Настройки базы населенных пунктов</b>\n\n"
            f"📊 <b>Статистика:</b>\n"
            f"• Всего записей: {stats['total']}\n"
            f"• С координатами: {stats['with_coords']}\n"
        )
        if stats['last_update']:
            text += f"• Обновлено: {stats['last_update']}\n"
        
        districts = village_db.get_districts()
        if districts:
            text += f"\n📍 <b>Районы в каталоге:</b>\n"
            for d in districts:
                count = len(village_db.get_villages_by_district(d))
                text += f"• {d} район: {count} НП\n"
        
        await safe_edit_text(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=get_settings_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "village_stats")
    async def show_stats(callback: types.CallbackQuery):
        stats = village_db.get_stats()
        text = (
            f"📊 <b>Статистика каталога населенных пунктов</b>\n\n"
            f"• Всего записей: {stats['total']}\n"
            f"• С координатами: {stats['with_coords']}\n"
            f"• Без координат: {stats['total'] - stats['with_coords']}\n"
        )
        if stats['last_update']:
            text += f"• Обновлено: {stats['last_update']}\n"
        if stats['source_file']:
            text += f"• Источник: {stats['source_file']}\n\n"
        
        districts = village_db.get_districts()
        if districts:
            text += f"📍 <b>Районы в каталоге:</b>\n"
            for d in districts:
                count = len(village_db.get_villages_by_district(d))
                with_coords = sum(1 for v in village_db.get_villages_by_district(d) if v.get('lat') and v.get('lon') and v['lat'].strip() and v['lon'].strip())
                text += f"• {d} район: {count} НП (из них с координатами: {with_coords})\n"
        
        if village_db.villages:
            text += f"\n📝 <b>Примеры записей (первые 10):</b>\n"
            for v in village_db.villages[:10]:
                coords = f"({v['lat']}, {v['lon']})" if v['lat'] and v['lon'] else "(без координат)"
                text += f"• {v['name']} ({v['type']}) - {v['district']} район {coords}\n"
        
        await safe_edit_text(
            callback.message,
            text,
            reply_markup=stats_back_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "download_villages_txt")
    async def download_villages_txt(callback: types.CallbackQuery):
        if not village_db.villages:
            await callback.message.answer("❌ Каталог пуст. Сначала добавьте данные.")
            await safe_answer_callback(callback)
            return
        
        try:
            filepath = village_db.export_to_txt()
            
            await callback.message.answer_document(
                FSInputFile(filepath, filename=os.path.basename(filepath)),
                caption=f"📁 <b>Каталог населенных пунктов</b>\nВсего: {village_db.stats['total']} записей\nС координатами: {village_db.stats['with_coords']}",
                parse_mode="HTML"
            )
            os.unlink(filepath)
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            await callback.message.answer("❌ Ошибка при создании файла.")
        
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "delete_district_start")
    async def delete_district_start(callback: types.CallbackQuery):
        districts = village_db.get_districts()
        await safe_edit_text(
            callback.message,
            "🗑️ <b>Удаление района</b>\n\n"
            "Выберите район для удаления:",
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
            f"🗑️ <b>Удаление района {district}</b>\n\n"
            f"⚠️ <b>ВНИМАНИЕ!</b> Это действие удалит все населенные пункты района {district} из каталога.\n\n"
            f"Вы уверены?",
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
            f"✅ <b>Район {district} удален!</b>\n\n"
            f"📊 <b>Результат:</b>\n"
            f"• Удалено записей: {removed}\n"
            f"• Из них с координатами: {with_coords}\n\n"
            f"Текущее состояние каталога:\n"
            f"• Всего записей: {village_db.stats['total']}\n"
            f"• С координатами: {village_db.stats['with_coords']}",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "clear_all_catalog")
    async def clear_all_catalog_confirm(callback: types.CallbackQuery):
        total = village_db.stats['total']
        if total == 0:
            await safe_edit_text(
                callback.message,
                "📭 Каталог уже пуст.",
                reply_markup=back_keyboard()
            )
            await safe_answer_callback(callback)
            return
        
        await safe_edit_text(
            callback.message,
            f"⚠️ <b>ОЧИСТКА ВСЕГО КАТАЛОГА</b>\n\n"
            f"В каталоге находится {total} населенных пунктов.\n\n"
            f"<b>Это действие НЕОБРАТИМО!</b>\n\n"
            f"Вы уверены, что хотите удалить все данные?",
            parse_mode="HTML",
            reply_markup=get_confirm_clear_all_keyboard(total)
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "confirm_clear_all")
    async def clear_all_catalog_execute(callback: types.CallbackQuery):
        removed = village_db.clear_all()
        
        await safe_edit_text(
            callback.message,
            f"✅ <b>Каталог полностью очищен!</b>\n\n"
            f"📊 <b>Результат:</b>\n"
            f"• Удалено записей: {removed}\n\n"
            f"Теперь каталог пуст. Вы можете добавить новые НП через настройки.",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data.startswith("merge_"))
    async def process_merge(callback: types.CallbackQuery, state: FSMContext):
        action, district = callback.data.replace("merge_", "").split("_", 1)
        data = await state.get_data()
        temp_txt = data.get('temp_txt')
        villages = data.get('villages', [])
        
        if not temp_txt or not os.path.exists(temp_txt):
            await safe_edit_text(
                callback.message,
                "❌ Временный файл не найден. Попробуйте загрузить данные заново.",
                reply_markup=back_keyboard()
            )
            await safe_answer_callback(callback)
            return
        
        if action == "download":
            await callback.message.answer_document(
                FSInputFile(temp_txt, filename=os.path.basename(temp_txt)),
                caption=f"📁 Данные для {district} района"
            )
            await safe_answer_callback(callback)
            return
        
        elif action == "append":
            try:
                stats = village_db.add_villages_batch(villages)
                
                os.unlink(temp_txt)
                
                await state.clear()
                
                await safe_edit_text(
                    callback.message,
                    f"✅ <b>Каталог дополнен данными {district} района!</b>\n\n"
                    f"📊 <b>Результат:</b>\n"
                    f"• Добавлено новых записей: {stats['added']}\n"
                    f"• Пропущено дубликатов: {stats['duplicates']}\n"
                    f"• Ошибок: {stats['errors']}\n\n"
                    f"📊 <b>Текущее состояние каталога:</b>\n"
                    f"• Всего записей: {village_db.stats['total']}\n"
                    f"• С координатами: {village_db.stats['with_coords']}",
                    parse_mode="HTML",
                    reply_markup=back_keyboard()
                )
            except Exception as e:
                logger.error(f"Ошибка: {e}")
                await safe_edit_text(
                    callback.message,
                    f"❌ Ошибка при дополнении каталога:\n{str(e)}",
                    reply_markup=back_keyboard()
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
        from handlers.start import cmd_start
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
            await callback.message.answer("📭 Список деревень пуст. Добавьте населенные пункты через ⚙️ НАСТРОЙКИ")
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
            "После загрузки я найду населенные пункты в каждом кадре и создам подробный отчет.",
            parse_mode="HTML"
        )
        await state.set_state(SearchStates.waiting_for_kml)
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "no_op")
    async def no_op(callback: types.CallbackQuery):
        await safe_answer_callback(callback)