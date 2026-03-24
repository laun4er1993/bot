# handlers/kml.py
import os
import tempfile
from aiogram import types, F
from aiogram.fsm.context import FSMContext
from aiogram.types import FSInputFile

from states.states import SearchStates
from keyboards.inline import (
    process_kml_again_keyboard, back_keyboard, get_kml_result_keyboard,
    get_afs_catalog_keyboard
)
from utils.helpers import safe_delete_message
from config import logger, KML_MARGIN_M, KML_USE_INTERSECTS
from services.afs_catalog import AFSCatalog

# Глобальная переменная для хранения последних результатов KML
last_kml_results = None
afs_catalog = AFSCatalog()


def register_kml_handlers(dp, kml_processor):
    global last_kml_results
    
    @dp.message(F.text == "🔄 ОБРАБОТАТЬ KML")
    async def menu_process_kml(message: types.Message, state: FSMContext):
        # Проверяем, есть ли населенные пункты в каталоге
        if not message.bot_data.get('village_db') or not message.bot_data['village_db'].villages:
            await message.answer(
                "❌ <b>Невозможно обработать KML файл</b>\n\n"
                "Каталог населенных пунктов пуст.\n\n"
                "Пожалуйста, сначала загрузите населенные пункты:\n"
                "• через ⚙️ НАСТРОЙКИ → 📤 Загрузить каталог (TXT)\n"
                "• или через ⚙️ НАСТРОЙКИ → 🌐 Загрузить из интернета",
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
                # Сохраняем результаты для дальнейшего использования
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
                    
                    if data['top_frames']:
                        result_text += f"🏆 <b>Топ-3 снимка по количеству НП:</b>\n"
                        for frame in data['top_frames'][:3]:
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
                
                await message.answer_document(
                    FSInputFile(report_path, filename=os.path.basename(report_path)),
                    caption="📄 <b>Детальный отчет по обработке KML</b>\n\nФайл содержит:\n• Общую статистику\n• Полные описания снимков\n• Список НП по каждому снимку\n• Статистику по районам\n• Параметры обработки",
                    parse_mode="HTML"
                )
                
                os.unlink(report_path)
                
                # Показываем кнопки для работы с каталогом АФС
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
        
        # Создаем каталог из результатов
        stats = afs_catalog.create_from_kml_results(results)
        
        await callback.message.answer(
            f"✅ <b>Каталог АФС создан!</b>\n\n"
            f"📊 <b>Статистика:</b>\n"
            f"• Добавлено новых снимков: {stats['added']}\n"
            f"• Пропущено дубликатов: {stats['duplicates']}\n"
            f"• Всего снимков в каталоге: {stats['total']}\n\n"
            f"Теперь вы можете просмотреть или скачать каталог.",
            parse_mode="HTML",
            reply_markup=get_afs_catalog_keyboard(has_catalog=not afs_catalog.is_empty())
        )
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "show_afs_catalog")
    async def show_afs_catalog_handler(callback: types.CallbackQuery):
        catalog_text = afs_catalog.get_catalog_text(with_descriptions=False)
        
        if len(catalog_text) > 4000:
            # Если текст слишком длинный, отправляем частями
            chunks = [catalog_text[i:i+4000] for i in range(0, len(catalog_text), 4000)]
            for i, chunk in enumerate(chunks):
                if i == 0:
                    await callback.message.answer(chunk, parse_mode="HTML")
                else:
                    await callback.message.answer(chunk, parse_mode="HTML")
        else:
            await callback.message.answer(catalog_text, parse_mode="HTML")
        
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "download_afs_catalog")
    async def download_afs_catalog_handler(callback: types.CallbackQuery):
        if afs_catalog.is_empty():
            await callback.message.answer("❌ Каталог АФС пуст. Сначала создайте каталог.")
            await callback.answer()
            return
        
        try:
            filepath = afs_catalog.export_to_txt()
            
            await callback.message.answer_document(
                FSInputFile(filepath, filename=os.path.basename(filepath)),
                caption=f"📁 <b>Каталог АФС</b>\nВсего: {len(afs_catalog.catalog)} снимков",
                parse_mode="HTML"
            )
            os.unlink(filepath)
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            await callback.message.answer("❌ Ошибка при создании файла.")
        
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "clear_afs_catalog")
    async def clear_afs_catalog_handler(callback: types.CallbackQuery):
        removed = afs_catalog.clear()
        
        await callback.message.answer(
            f"🗑️ <b>Каталог АФС очищен!</b>\n\n"
            f"Удалено снимков: {removed}",
            parse_mode="HTML",
            reply_markup=get_afs_catalog_keyboard(has_catalog=False)
        )
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "process_kml_again")
    async def process_kml_again(callback: types.CallbackQuery, state: FSMContext):
        await safe_delete_message(callback.message)
        
        # Проверяем, есть ли населенные пункты в каталоге
        if not callback.bot_data.get('village_db') or not callback.bot_data['village_db'].villages:
            await callback.message.answer(
                "❌ <b>Невозможно обработать KML файл</b>\n\n"
                "Каталог населенных пунктов пуст.\n\n"
                "Пожалуйста, сначала загрузите населенные пункты:\n"
                "• через ⚙️ НАСТРОЙКИ → 📤 Загрузить каталог (TXT)\n"
                "• или через ⚙️ НАСТРОЙКИ → 🌐 Загрузить из интернета",
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