# handlers/kml.py
import os
import time
import tempfile
from aiogram import types, F
from aiogram.fsm.context import FSMContext
from aiogram.types import BufferedInputFile

from states.states import SearchStates
from keyboards.inline import (
    process_kml_again_keyboard, back_keyboard, get_kml_result_keyboard,
    get_afs_catalog_keyboard, get_afs_compare_keyboard, get_afs_catalog_load_keyboard,
    get_afs_settings_keyboard
)
from utils.helpers import safe_delete_message, safe_edit_text, safe_answer_callback
from config import logger, KML_MARGIN_M, KML_USE_INTERSECTS
from services.afs_catalog import AFSCatalog

# Глобальная переменная для хранения последних результатов KML и данных для сравнения
last_kml_results = None
last_kml_compare_data = None
afs_catalog = AFSCatalog()
current_afs_page = 1


def register_kml_handlers(dp, kml_processor, village_db):
    global last_kml_results, last_kml_compare_data, current_afs_page
    
    @dp.message(F.text == "🔄 ОБРАБОТАТЬ KML")
    async def menu_process_kml(message: types.Message, state: FSMContext):
        if not village_db.villages:
            await message.answer(
                "❌ <b>Невозможно обработать KML файл</b>\n\n"
                "Каталог населенных пунктов пуст.\n\n"
                "Пожалуйста, сначала загрузите населенные пункты:\n"
                "• через ⚙️ ЗАГРУЗКА НП → 📤 Загрузить каталог (TXT)\n"
                "• или через ⚙️ ЗАГРУЗКА НП → 🌐 Загрузить из интернета",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return
        
        method_text = "пересечение (точки на границе)" if KML_USE_INTERSECTS else "строгое вхождение"
        await message.answer(
            f"📤 <b>Загрузите KML файл</b>\n\n"
            f"Отправьте мне KML файл с каталогом снимков.\n"
            f"После загрузки я:\n"
            f"• Найду населенные пункты в каждом кадре\n"
            f"• Добавлю полные описания снимков из базы данных\n"
            f"• Создам подробный TXT отчет со статистикой\n\n"
            f"📌 <b>Параметры обработки:</b>\n"
            f"• Буфер: {KML_MARGIN_M} м\n"
            f"• Метод проверки: {method_text}\n\n"
            f"<i>Файл должен содержать Placemark с названиями Frame-XXX</i>",
            parse_mode="HTML"
        )
        await state.set_state(SearchStates.waiting_for_kml)
    
    @dp.message(SearchStates.waiting_for_kml, F.document)
    async def process_kml_upload(message: types.Message, state: FSMContext):
        global last_kml_results
        
        if not message.document.file_name.endswith('.kml'):
            await message.answer("❌ Неверный формат. Отправьте файл с расширением .kml")
            await state.clear()
            return
        
        await message.answer("⏳ Обработка файла... Это может занять несколько секунд.")
        
        try:
            file_info = await message.bot.get_file(message.document.file_id)
            with tempfile.NamedTemporaryFile(delete=False, suffix='.kml') as tmp:
                await message.bot.download_file(file_info.file_path, tmp)
                tmp_path = tmp.name
                original_filename = message.document.file_name
            
            data = kml_processor.process_kml_file(tmp_path)
            os.unlink(tmp_path)
            
            stats = data['stats']
            
            if stats['total_frames'] > 0:
                last_kml_results = data
                report_path = kml_processor.generate_report(data, original_filename)
                
                result_text = (
                    f"✅ <b>Обработка KML завершена!</b>\n\n"
                    f"📊 <b>Статистика:</b>\n"
                    f"• Всего снимков: {stats['total_frames']}\n"
                    f"• Снимков с населенными пунктами: {stats['frames_with_np']}\n"
                    f"• Снимков без НП: {stats['frames_without_np']}\n"
                    f"• Всего связей (НП в кадрах): {stats['total_relations']}\n"
                )
                
                if stats['frames_with_np'] > 0:
                    result_text += f"• Среднее НП на кадр: {stats['avg_np_per_frame']}\n\n"
                    
                    seen_frames = set()
                    unique_top_frames = []
                    for frame in data['top_frames'][:4]:
                        if frame['frame'] not in seen_frames:
                            seen_frames.add(frame['frame'])
                            unique_top_frames.append(frame)
                    
                    if unique_top_frames:
                        result_text += f"🏆 <b>Топ-{len(unique_top_frames)} снимков по количеству НП:</b>\n"
                        for frame in unique_top_frames:
                            result_text += f"• {frame['frame']}: {frame['count']} НП\n"
                        result_text += "\n"
                    
                    if data['district_stats']:
                        result_text += f"📍 <b>Статистика по районам:</b>\n"
                        total = stats['total_relations']
                        for district in data['district_stats'][:5]:
                            percent = (district['count'] / total * 100) if total > 0 else 0
                            result_text += f"• {district['district']} район: {district['count']} НП ({percent:.1f}%)\n"
                
                if stats.get('errors', 0) > 0:
                    result_text += f"\n⚠️ <b>Ошибок при обработке:</b> {stats['errors']}\n"
                
                await message.answer(result_text, parse_mode="HTML")
                
                try:
                    if os.path.exists(report_path):
                        with open(report_path, 'rb') as f:
                            await message.answer_document(
                                BufferedInputFile(f.read(), filename=os.path.basename(report_path)),
                                caption="📄 <b>Детальный отчет по обработке KML</b>\n\nФайл содержит:\n• Общую статистику\n• Полные описания снимков\n• Список НП по каждому снимку\n• Статистику по районам\n• Параметры обработки",
                                parse_mode="HTML"
                            )
                        os.unlink(report_path)
                    else:
                        await message.answer("❌ Не удалось создать отчет.")
                except Exception as e:
                    logger.error(f"Ошибка отправки отчета: {e}")
                    await message.answer("❌ Не удалось отправить отчет.")
                
                await message.answer(
                    "📁 <b>Действия с каталогом АФС</b>\n\n"
                    "Выберите действие:",
                    parse_mode="HTML",
                    reply_markup=get_kml_result_keyboard()
                )
            else:
                await message.answer(
                    "❌ В KML файле не найдено снимков с названиями Frame-XXX",
                    reply_markup=back_keyboard()
                )
            
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            await message.answer(f"❌ Ошибка при обработке KML:\n{str(e)}")
        
        await state.clear()
    
    @dp.message(SearchStates.waiting_for_kml)
    async def process_kml_invalid(message: types.Message, state: FSMContext):
        await message.answer("❌ Отправьте KML файл (с расширением .kml)")
        await state.clear()
    
    @dp.callback_query(lambda c: c.data == "create_afs_catalog")
    async def create_afs_catalog_handler(callback: types.CallbackQuery):
        global last_kml_results
        
        if not last_kml_results or not last_kml_results.get('results'):
            await callback.message.answer(
                "❌ Нет данных для создания каталога АФС.\n"
                "Сначала обработайте KML файл.",
                reply_markup=back_keyboard()
            )
            await callback.answer()
            return
        
        results = last_kml_results['results']
        stats = afs_catalog.create_from_kml_results(results)
        
        await callback.message.answer(
            f"✅ <b>Каталог АФС создан!</b>\n\n"
            f"📊 <b>Статистика:</b>\n"
            f"• Добавлено новых снимков: {stats['added']}\n"
            f"• Пропущено дубликатов: {stats['duplicates']}\n"
            f"• Всего снимков в каталоге: {stats['total']}\n\n"
            f"Теперь вы можете просмотреть или скачать каталог через ⚙️ НАСТРОЙКА КАТАЛОГА.",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "append_afs_catalog")
    async def append_afs_catalog_handler(callback: types.CallbackQuery):
        global last_kml_results
        
        if not last_kml_results or not last_kml_results.get('results'):
            await callback.message.answer(
                "❌ Нет данных для дополнения каталога АФС.\n"
                "Сначала обработайте KML файл.",
                reply_markup=back_keyboard()
            )
            await callback.answer()
            return
        
        results = last_kml_results['results']
        stats = afs_catalog.add_from_kml_results(results)
        
        await callback.message.answer(
            f"✅ <b>Каталог АФС дополнен!</b>\n\n"
            f"📊 <b>Статистика:</b>\n"
            f"• Добавлено новых снимков: {stats['added']}\n"
            f"• Обновлено описаний: {stats['updated']}\n"
            f"• Пропущено дубликатов: {stats['duplicates']}\n"
            f"• Всего снимков в каталоге: {stats['total']}\n\n"
            f"Теперь вы можете просмотреть или скачать каталог через ⚙️ НАСТРОЙКА КАТАЛОГА.",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "replace_afs_catalog")
    async def replace_afs_catalog_handler(callback: types.CallbackQuery):
        global last_kml_results
        
        if not last_kml_results or not last_kml_results.get('results'):
            await callback.message.answer(
                "❌ Нет данных для замены каталога АФС.\n"
                "Сначала обработайте KML файл.",
                reply_markup=back_keyboard()
            )
            await callback.answer()
            return
        
        results = last_kml_results['results']
        stats = afs_catalog.replace_with_kml_results(results)
        
        await callback.message.answer(
            f"✅ <b>Каталог АФС заменен!</b>\n\n"
            f"📊 <b>Статистика:</b>\n"
            f"• Добавлено снимков: {stats['added']}\n"
            f"• Удалено старых: {stats['removed']}\n"
            f"• Всего снимков в каталоге: {stats['total']}\n\n"
            f"Теперь вы можете просмотреть или скачать каталог через ⚙️ НАСТРОЙКА КАТАЛОГА.",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "process_kml_again")
    async def process_kml_again(callback: types.CallbackQuery, state: FSMContext):
        await safe_delete_message(callback.message)
        
        if not village_db.villages:
            await callback.message.answer(
                "❌ <b>Невозможно обработать KML файл</b>\n\n"
                "Каталог населенных пунктов пуст.\n\n"
                "Пожалуйста, сначала загрузите населенные пункты:\n"
                "• через ⚙️ ЗАГРУЗКА НП → 📤 Загрузить каталог (TXT)\n"
                "• или через ⚙️ ЗАГРУЗКА НП → 🌐 Загрузить из интернета",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            await callback.answer()
            return
        
        method_text = "пересечение (точки на границе)" if KML_USE_INTERSECTS else "строгое вхождение"
        await callback.message.answer(
            f"📤 <b>Загрузите KML файл</b>\n\n"
            f"Отправьте мне KML файл с каталогом снимков.\n"
            f"После загрузки я:\n"
            f"• Найду населенные пункты в каждом кадре\n"
            f"• Добавлю полные описания снимков из базы данных\n"
            f"• Создам подробный TXT отчет со статистикой\n\n"
            f"📌 <b>Параметры обработки:</b>\n"
            f"• Буфер: {KML_MARGIN_M} м\n"
            f"• Метод проверки: {method_text}\n\n"
            f"<i>Файл должен содержать Placemark с названиями Frame-XXX</i>",
            parse_mode="HTML"
        )
        await state.set_state(SearchStates.waiting_for_kml)
        await callback.answer()