# handlers/kml.py
import os
import tempfile
from aiogram import types, F
from aiogram.fsm.context import FSMContext
from aiogram.types import FSInputFile

from states.states import SearchStates
from keyboards.inline import process_kml_again_keyboard, back_keyboard
from utils.helpers import safe_delete_message
from config import logger, KML_MARGIN_M, KML_USE_INTERSECTS


def register_kml_handlers(dp, kml_processor):
    
    @dp.message(F.text == "🔄 ОБРАБОТАТЬ KML")
    async def menu_process_kml(message: types.Message, state: FSMContext):
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
                            if frame.get('description'):
                                desc_preview = frame['description'][:100] if frame['description'] else ""
                                result_text += f"  <i>{desc_preview}...</i>\n"
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
                
                await message.answer(
                    "🔄 Хотите обработать другой KML файл? Нажмите кнопку ниже:",
                    reply_markup=process_kml_again_keyboard()
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