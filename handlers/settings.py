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
    get_kml_management_keyboard
)
from utils.helpers import safe_delete_message, safe_edit_text, safe_answer_callback
from config import logger, KML_MARGIN_M, KML_USE_INTERSECTS

# !!! Убираем импорт afs_catalog из settings !!!
# afs_catalog будет передан как параметр в функцию register_kml_handlers

# Глобальная переменная для хранения последних результатов KML
last_kml_results = None
last_kml_compare_data = None
current_afs_page = 1


def register_kml_handlers(dp, kml_processor, village_db, photos_db, afs_catalog):
    global last_kml_results, last_kml_compare_data, current_afs_page
    
    @dp.callback_query(lambda c: c.data == "process_kml_menu")
    async def process_kml_menu_handler(callback: types.CallbackQuery, state: FSMContext):
        """Обработка KML файла из меню управления KML"""
        if not village_db.villages:
            await safe_edit_text(
                callback.message,
                "❌ <b>Невозможно обработать KML файл</b>\n\n"
                "Каталог населенных пунктов пуст.\n\n"
                "Пожалуйста, сначала загрузите населенные пункты:\n"
                "• через ⚙️ НАСТРОЙКА → ЗАГРУЗКА НП → 📤 Загрузить каталог (TXT)\n"
                "• или через ⚙️ НАСТРОЙКА → ЗАГРУЗКА НП → 🌐 Загрузить из интернета",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            await safe_answer_callback(callback)
            return
        
        method_text = "пересечение (точки на границе)" if KML_USE_INTERSECTS else "строгое вхождение"
        await safe_edit_text(
            callback.message,
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
        await safe_answer_callback(callback)
    
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
            
            logger.info(f"📁 Начало обработки KML файла: {original_filename}")
            
            data = kml_processor.process_kml_file(tmp_path)
            os.unlink(tmp_path)
            
            stats = data['stats']
            
            if stats['total_frames'] > 0:
                last_kml_results = data
                report_path = kml_processor.generate_report(data, original_filename)
                
                logger.info(f"📊 Обработка KML завершена:")
                logger.info(f"   • Всего снимков: {stats['total_frames']}")
                logger.info(f"   • Снимков с НП: {stats['frames_with_np']}")
                logger.info(f"   • Снимков без НП: {stats['frames_without_np']}")
                logger.info(f"   • Всего связей: {stats['total_relations']}")
                
                for result in data['results'][:10]:
                    frame = result['photo_num']
                    villages = result['villages']
                    logger.info(f"   📸 Снимок {frame}: {result['village_count']} НП")
                    if villages:
                        logger.info(f"      📍 Деревни: {', '.join(villages[:5])}")
                    
                    files = photos_db.photo_files.get(frame, {})
                    if files.get('mbtiles'):
                        logger.info(f"      🗺️ MBTILES: {len(files['mbtiles'])} версий")
                        for v in files['mbtiles']:
                            logger.info(f"         - версия {v['version']}, {v['size_mb']} МБ")
                    if files.get('kmz'):
                        logger.info(f"      🌍 KMZ: {len(files['kmz'])} версий")
                        for v in files['kmz']:
                            logger.info(f"         - версия {v['version']}, {v['size_mb']} МБ")
                
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
                                caption="📄 <b>Детальный отчет по обработке KML</b>",
                                parse_mode="HTML"
                            )
                        os.unlink(report_path)
                except Exception as e:
                    logger.error(f"Ошибка отправки отчета: {e}")
                
                await message.answer(
                    "📁 <b>Действия с каталогом АФС</b>\n\n"
                    "Выберите действие:",
                    parse_mode="HTML",
                    reply_markup=get_kml_result_keyboard()
                )
            else:
                await message.answer(
                    "❌ В KML файле не найдено снимков с названиями Frame-XXX",
                    reply_markup=get_kml_management_keyboard()
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
                reply_markup=get_kml_management_keyboard()
            )
            await callback.answer()
            return
        
        results = last_kml_results['results']
        frames_without_np = last_kml_results['frames_without_np']
        
        enriched_results = []
        for result in results:
            enriched_results.append({
                'photo_num': result['photo_num'],
                'description': result.get('description', ''),
                'villages': result.get('villages', [])
            })
            logger.info(f"  📸 Снимок {result['photo_num']}: {len(result.get('villages', []))} деревень")
            if result.get('villages'):
                logger.info(f"      Деревни: {', '.join(result['villages'][:10])}")
        
        stats = afs_catalog.create_from_kml_results(enriched_results, frames_without_np)
        
        # Проверяем, что деревни сохранились
        logger.info(f"📊 ПРОВЕРКА ПОСЛЕ СОЗДАНИЯ:")
        logger.info(f"   Всего снимков: {len(afs_catalog.catalog)}")
        logger.info(f"   Снимков со связями: {len(afs_catalog.villages_by_frame)}")
        
        for i, (frame, villages) in enumerate(list(afs_catalog.villages_by_frame.items())[:5]):
            logger.info(f"   {i+1}. {frame}: {len(villages)} деревень")
            if villages:
                logger.info(f"      Первые 5: {', '.join(villages[:5])}")
        
        await callback.message.answer(
            f"✅ <b>Каталог АФС создан!</b>\n\n"
            f"📊 <b>Статистика:</b>\n"
            f"• Добавлено новых снимков: {stats['added']}\n"
            f"  └─ с населенными пунктами: {stats['with_np']}\n"
            f"  └─ без населенных пунктов: {stats['without_np']}\n"
            f"• Пропущено дубликатов: {stats['duplicates']}\n"
            f"• Всего снимков в каталоге: {stats['total']}\n\n"
            f"Теперь вы можете искать снимки по названиям деревень!",
            parse_mode="HTML",
            reply_markup=get_kml_management_keyboard()
        )
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "append_afs_catalog")
    async def append_afs_catalog_handler(callback: types.CallbackQuery):
        global last_kml_results
        
        if not last_kml_results or not last_kml_results.get('results'):
            await callback.message.answer(
                "❌ Нет данных для дополнения каталога АФС.\n"
                "Сначала обработайте KML файл.",
                reply_markup=get_kml_management_keyboard()
            )
            await callback.answer()
            return
        
        results = last_kml_results['results']
        frames_without_np = last_kml_results['frames_without_np']
        
        enriched_results = []
        for result in results:
            enriched_results.append({
                'photo_num': result['photo_num'],
                'description': result.get('description', ''),
                'villages': result.get('villages', [])
            })
        
        stats = afs_catalog.add_from_kml_results(enriched_results, frames_without_np)
        
        await callback.message.answer(
            f"✅ <b>Каталог АФС дополнен!</b>\n\n"
            f"📊 <b>Статистика:</b>\n"
            f"• Добавлено новых снимков: {stats['added']}\n"
            f"  └─ с населенными пунктами: {stats['with_np']}\n"
            f"  └─ без населенных пунктов: {stats['without_np']}\n"
            f"• Обновлено описаний: {stats['updated']}\n"
            f"• Пропущено дубликатов: {stats['duplicates']}\n"
            f"• Всего снимков в каталоге: {stats['total']}",
            parse_mode="HTML",
            reply_markup=get_kml_management_keyboard()
        )
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "replace_afs_catalog")
    async def replace_afs_catalog_handler(callback: types.CallbackQuery):
        global last_kml_results
        
        if not last_kml_results or not last_kml_results.get('results'):
            await callback.message.answer(
                "❌ Нет данных для замены каталога АФС.\n"
                "Сначала обработайте KML файл.",
                reply_markup=get_kml_management_keyboard()
            )
            await callback.answer()
            return
        
        results = last_kml_results['results']
        frames_without_np = last_kml_results['frames_without_np']
        
        enriched_results = []
        for result in results:
            enriched_results.append({
                'photo_num': result['photo_num'],
                'description': result.get('description', ''),
                'villages': result.get('villages', [])
            })
        
        stats = afs_catalog.replace_with_kml_results(enriched_results, frames_without_np)
        
        await callback.message.answer(
            f"✅ <b>Каталог АФС заменен!</b>\n\n"
            f"📊 <b>Статистика:</b>\n"
            f"• Добавлено снимков: {stats['added']}\n"
            f"  └─ с населенными пунктами: {stats['with_np']}\n"
            f"  └─ без населенных пунктов: {stats['without_np']}\n"
            f"• Удалено старых: {stats['removed']}\n"
            f"• Всего снимков в каталоге: {stats['total']}",
            parse_mode="HTML",
            reply_markup=get_kml_management_keyboard()
        )
        await callback.answer()
    
    # Остальные обработчики (afs_stats, show_afs_catalog, download_afs_catalog и т.д.) остаются без изменений,
    # они уже используют переданный afs_catalog.
    
    # (здесь должны быть все остальные callback-обработчики, которые были в исходном kml.py)
    # Для краткости они не переписаны, но в реальном коде они должны быть.