import os
import time
import tempfile
from aiogram import types, F
from aiogram.fsm.context import FSMContext
from aiogram.types import BufferedInputFile, InlineKeyboardMarkup, InlineKeyboardButton

from states.states import SearchStates
from keyboards.inline import (
    process_kml_again_keyboard, back_keyboard, get_kml_result_keyboard,
    get_afs_catalog_keyboard, get_afs_compare_keyboard, get_afs_catalog_load_keyboard,
    get_kml_management_keyboard
)
from utils.helpers import safe_delete_message, safe_edit_text, safe_answer_callback
from config import logger, KML_MARGIN_M, KML_USE_INTERSECTS

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
        
        # Обновляем ссылки для всех снимков в каталоге
        await callback.message.answer("⏳ Поиск файлов на Яндекс.Диске... Это может занять несколько секунд.")
        
        refresh_stats = photos_db.refresh_all_photo_links()
        
        await callback.message.answer(
            f"✅ <b>Каталог АФС создан!</b>\n\n"
            f"📊 <b>Статистика каталога:</b>\n"
            f"• Добавлено новых снимков: {stats['added']}\n"
            f"  └─ с населенными пунктами: {stats['with_np']}\n"
            f"  └─ без населенных пунктов: {stats['without_np']}\n"
            f"• Пропущено дубликатов: {stats['duplicates']}\n"
            f"• Всего снимков в каталоге: {stats['total']}\n\n"
            f"📥 <b>Поиск файлов на Яндекс.Диске:</b>\n"
            f"• Найдено ссылок: {refresh_stats['found']}\n"
            f"• Не найдено: {refresh_stats['not_found']}\n"
            f"• Обработано: {refresh_stats['total']}\n\n"
            f"Теперь вы можете искать снимки по названиям деревень и скачивать файлы!",
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
        
        # Обновляем ссылки для всех снимков
        await callback.message.answer("⏳ Поиск файлов на Яндекс.Диске...")
        refresh_stats = photos_db.refresh_all_photo_links()
        
        await callback.message.answer(
            f"✅ <b>Каталог АФС дополнен!</b>\n\n"
            f"📊 <b>Статистика:</b>\n"
            f"• Добавлено новых снимков: {stats['added']}\n"
            f"  └─ с населенными пунктами: {stats['with_np']}\n"
            f"  └─ без населенных пунктов: {stats['without_np']}\n"
            f"• Обновлено описаний: {stats['updated']}\n"
            f"• Пропущено дубликатов: {stats['duplicates']}\n"
            f"• Всего снимков в каталоге: {stats['total']}\n\n"
            f"📥 <b>Поиск файлов:</b> найдено {refresh_stats['found']} из {refresh_stats['total']}",
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
        
        # Обновляем ссылки для всех снимков
        await callback.message.answer("⏳ Поиск файлов на Яндекс.Диске...")
        refresh_stats = photos_db.refresh_all_photo_links()
        
        await callback.message.answer(
            f"✅ <b>Каталог АФС заменен!</b>\n\n"
            f"📊 <b>Статистика:</b>\n"
            f"• Добавлено снимков: {stats['added']}\n"
            f"  └─ с населенными пунктами: {stats['with_np']}\n"
            f"  └─ без населенных пунктов: {stats['without_np']}\n"
            f"• Удалено старых: {stats['removed']}\n"
            f"• Всего снимков в каталоге: {stats['total']}\n\n"
            f"📥 <b>Поиск файлов:</b> найдено {refresh_stats['found']} из {refresh_stats['total']}",
            parse_mode="HTML",
            reply_markup=get_kml_management_keyboard()
        )
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "afs_stats")
    async def afs_stats_handler(callback: types.CallbackQuery):
        stats = afs_catalog.get_statistics()
        
        text = (
            f"📊 <b>Статистика каталога АФС</b>\n\n"
            f"• Всего снимков: {stats['total']}\n"
            f"• С описаниями: {stats['with_description']}\n"
            f"• Без описаний: {stats['without_description']}\n"
            f"• Снимков с населенными пунктами: {stats['with_villages']}\n"
            f"• Снимков без населенных пунктов: {stats['without_villages']}\n"
            f"• Всего связей (НП в кадрах): {stats['total_village_links']}\n"
            f"• Средняя длина описания: {stats['avg_description_length']} символов\n\n"
        )
        
        if not afs_catalog.is_empty():
            text += f"📌 <b>Последние 5 снимков:</b>\n"
            for item in afs_catalog.catalog[-5:]:
                frame = item['frame']
                villages = afs_catalog.get_villages_for_frame(frame)
                text += f"• {frame}"
                if villages:
                    text += f" ({len(villages)} НП)"
                text += "\n"
        
        await safe_edit_text(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=get_afs_catalog_keyboard(has_catalog=not afs_catalog.is_empty())
        )
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "show_afs_catalog")
    async def show_afs_catalog_handler(callback: types.CallbackQuery):
        global current_afs_page
        
        if afs_catalog.is_empty():
            await callback.message.answer(
                "📭 Каталог АФС пуст.\n\n"
                "Чтобы создать каталог:\n"
                "1. Обработайте KML файл\n"
                "2. Нажмите 'Создать каталог АФС'",
                reply_markup=get_kml_management_keyboard()
            )
            await callback.answer()
            return
        
        current_afs_page = 1
        text, total_pages, current = afs_catalog.get_catalog_text(
            with_descriptions=False,
            page=1,
            per_page=50
        )
        
        await safe_edit_text(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=get_afs_catalog_keyboard(
                has_catalog=True,
                page=current,
                total_pages=total_pages
            )
        )
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data.startswith("afs_page_"))
    async def afs_page_handler(callback: types.CallbackQuery):
        global current_afs_page
        
        page = int(callback.data.replace("afs_page_", ""))
        current_afs_page = page
        
        text, total_pages, current = afs_catalog.get_catalog_text(
            with_descriptions=False,
            page=page,
            per_page=50
        )
        
        await safe_edit_text(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=get_afs_catalog_keyboard(
                has_catalog=not afs_catalog.is_empty(),
                page=current,
                total_pages=total_pages
            )
        )
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "download_afs_catalog")
    async def download_afs_catalog_handler(callback: types.CallbackQuery):
        if afs_catalog.is_empty():
            await callback.message.answer("❌ Каталог АФС пуст. Сначала создайте каталог.")
            await callback.answer()
            return
        
        try:
            filepath = afs_catalog.export_to_txt()
            
            if filepath and os.path.exists(filepath):
                with open(filepath, 'rb') as f:
                    await callback.message.answer_document(
                        BufferedInputFile(f.read(), filename=os.path.basename(filepath)),
                        caption=f"📁 <b>Каталог АФС</b>\nВсего: {len(afs_catalog.catalog)} снимков",
                        parse_mode="HTML"
                    )
                os.unlink(filepath)
            else:
                await callback.message.answer("❌ Ошибка при создании файла.")
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            await callback.message.answer(f"❌ Ошибка при создании файла: {e}")
        
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "clear_afs_catalog")
    async def clear_afs_catalog_handler(callback: types.CallbackQuery):
        removed = afs_catalog.clear()
        
        await safe_edit_text(
            callback.message,
            f"🗑️ <b>Каталог АФС очищен!</b>\n\n"
            f"Удалено снимков: {removed}",
            parse_mode="HTML",
            reply_markup=get_afs_catalog_keyboard(has_catalog=False)
        )
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "compare_afs_with_kml")
    async def compare_afs_with_kml_handler(callback: types.CallbackQuery):
        global last_kml_results, last_kml_compare_data
        
        if not last_kml_results or not last_kml_results.get('results'):
            await callback.message.answer(
                "❌ Нет данных для сравнения.\n"
                "Сначала обработайте KML файл.",
                reply_markup=get_kml_management_keyboard()
            )
            await callback.answer()
            return
        
        kml_catalog = []
        for result in last_kml_results['results']:
            kml_catalog.append({
                'frame': result.get('photo_num', ''),
                'description': result.get('description', '') or '',
                'villages': result.get('villages', [])
            })
        for frame_data in last_kml_results['frames_without_np']:
            kml_catalog.append({
                'frame': frame_data.get('frame', ''),
                'description': frame_data.get('description', '') or '',
                'villages': []
            })
        
        diff = afs_catalog.compare_with_catalog(kml_catalog)
        last_kml_compare_data = kml_catalog
        
        text = (
            f"🔄 <b>Сравнение каталогов</b>\n\n"
            f"📊 <b>Результат сравнения:</b>\n"
            f"• Новые снимки в KML: {len(diff['new'])}\n"
            f"• Отсутствуют в KML: {len(diff['missing'])}\n"
            f"• Различаются описания: {len(diff['different'])}\n\n"
        )
        
        if diff['new']:
            text += f"🆕 <b>Новые снимки в KML (можно добавить):</b>\n"
            for frame in diff['new'][:10]:
                text += f"• {frame}\n"
            if len(diff['new']) > 10:
                text += f"... и ещё {len(diff['new']) - 10}\n"
            text += "\n"
        
        if diff['different']:
            text += f"📝 <b>Снимки с разными описаниями:</b>\n"
            for item in diff['different'][:5]:
                text += f"• {item['frame']}\n"
            if len(diff['different']) > 5:
                text += f"... и ещё {len(diff['different']) - 5}\n"
            text += "\n"
        
        await safe_edit_text(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=get_afs_compare_keyboard()
        )
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "afs_add_new")
    async def afs_add_new_handler(callback: types.CallbackQuery):
        global last_kml_compare_data
        
        if not last_kml_compare_data:
            await callback.message.answer(
                "❌ Нет данных для добавления.",
                reply_markup=get_kml_management_keyboard()
            )
            await callback.answer()
            return
        
        diff = afs_catalog.compare_with_catalog(last_kml_compare_data)
        new_items = [item for item in last_kml_compare_data if item['frame'] in diff['new']]
        
        stats = afs_catalog.merge_with_catalog(new_items)
        
        await safe_edit_text(
            callback.message,
            f"✅ <b>Новые снимки добавлены!</b>\n\n"
            f"📊 <b>Результат:</b>\n"
            f"• Добавлено: {len(new_items)}\n"
            f"• Всего снимков в каталоге: {stats['total']}",
            parse_mode="HTML",
            reply_markup=get_afs_catalog_keyboard(has_catalog=True)
        )
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "afs_update_descriptions")
    async def afs_update_descriptions_handler(callback: types.CallbackQuery):
        global last_kml_compare_data
        
        if not last_kml_compare_data:
            await callback.message.answer(
                "❌ Нет данных для обновления.",
                reply_markup=get_kml_management_keyboard()
            )
            await callback.answer()
            return
        
        stats = afs_catalog.merge_with_catalog(last_kml_compare_data)
        
        await safe_edit_text(
            callback.message,
            f"✅ <b>Описания обновлены!</b>\n\n"
            f"📊 <b>Результат:</b>\n"
            f"• Обновлено описаний: {stats['updated']}\n"
            f"• Добавлено новых: {stats['added']}\n"
            f"• Всего снимков в каталоге: {stats['total']}",
            parse_mode="HTML",
            reply_markup=get_afs_catalog_keyboard(has_catalog=True)
        )
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "afs_download_merged")
    async def afs_download_merged_handler(callback: types.CallbackQuery):
        if afs_catalog.is_empty():
            await callback.message.answer("❌ Каталог АФС пуст.")
            await callback.answer()
            return
        
        try:
            filepath = afs_catalog.export_to_txt(f"afs_merged_{time.strftime('%Y%m%d_%H%M%S')}.txt")
            
            if filepath and os.path.exists(filepath):
                with open(filepath, 'rb') as f:
                    await callback.message.answer_document(
                        BufferedInputFile(f.read(), filename=os.path.basename(filepath)),
                        caption=f"📁 <b>Объединенный каталог АФС</b>\nВсего: {len(afs_catalog.catalog)} снимков",
                        parse_mode="HTML"
                    )
                os.unlink(filepath)
            else:
                await callback.message.answer("❌ Ошибка при создании файла.")
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            await callback.message.answer(f"❌ Ошибка при создании файла: {e}")
        
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "load_common_afs_catalog")
    async def load_common_afs_catalog_handler(callback: types.CallbackQuery, state: FSMContext):
        await safe_edit_text(
            callback.message,
            "📤 <b>Загрузка общего каталога АФС</b>\n\n"
            "Отправьте TXT файл с каталогом АФС.\n\n"
            "📌 <b>Формат файла:</b>\n"
            "Каждая строка должна содержать номер снимка, описание и деревни через символ |\n\n"
            "<code>N56E34-266-016|Описание снимка|Горбово,Полунино</code>\n\n"
            "Выберите действие:",
            parse_mode="HTML",
            reply_markup=get_afs_catalog_load_keyboard()
        )
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "afs_merge_common")
    async def afs_merge_common_handler(callback: types.CallbackQuery, state: FSMContext):
        await state.update_data(afs_action="merge")
        await safe_edit_text(
            callback.message,
            "📤 <b>Дополнение каталога АФС</b>\n\n"
            "Отправьте TXT файл с каталогом АФС в формате:\n"
            "<code>номер_снимка|описание|деревни</code>\n\n"
            "Файл будет добавлен к существующему каталогу.\n\n"
            "Отправьте TXT файл:",
            parse_mode="HTML"
        )
        await state.set_state(SearchStates.waiting_for_afs_upload)
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "afs_replace_common")
    async def afs_replace_common_handler(callback: types.CallbackQuery, state: FSMContext):
        await state.update_data(afs_action="replace")
        await safe_edit_text(
            callback.message,
            "📤 <b>Замена каталога АФС</b>\n\n"
            "Отправьте TXT файл с каталогом АФС в формате:\n"
            "<code>номер_снимка|описание|деревни</code>\n\n"
            "Текущий каталог будет заменен новым.\n\n"
            "Отправьте TXT файл:",
            parse_mode="HTML"
        )
        await state.set_state(SearchStates.waiting_for_afs_upload)
        await callback.answer()
    
    @dp.message(SearchStates.waiting_for_afs_upload, F.document)
    async def process_afs_txt_upload(message: types.Message, state: FSMContext):
        if not message.document.file_name.endswith('.txt'):
            await message.answer("❌ Отправьте TXT файл (с расширением .txt)")
            await state.clear()
            return
        
        await message.answer("⏳ Загрузка и обработка файла...")
        
        try:
            file_info = await message.bot.get_file(message.document.file_id)
            with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as tmp:
                await message.bot.download_file(file_info.file_path, tmp)
                tmp_path = tmp.name
            
            with open(tmp_path, 'r', encoding='utf-8') as f:
                content = f.read()
            os.unlink(tmp_path)
            
            lines = content.strip().split('\n')
            new_catalog = []
            
            for line in lines:
                line = line.strip()
                if not line or line.startswith('=') or line.startswith('Номер'):
                    continue
                
                parts = line.split('|')
                if len(parts) >= 2:
                    frame = parts[0].strip()
                    description = parts[1].strip() if len(parts) > 1 else ''
                    villages_str = parts[2].strip() if len(parts) > 2 else ''
                    villages = [v.strip() for v in villages_str.split(',') if v.strip()] if villages_str else []
                    
                    if description == 'None':
                        description = ''
                    
                    new_catalog.append({
                        'frame': frame,
                        'description': description,
                        'villages': villages
                    })
            
            if not new_catalog:
                await message.answer("❌ В файле не найдено корректных записей")
                await state.clear()
                return
            
            data = await state.get_data()
            action = data.get('afs_action', 'merge')
            
            if action == 'merge':
                stats = afs_catalog.merge_with_catalog(new_catalog)
                await message.answer(
                    f"✅ <b>Каталог АФС дополнен!</b>\n\n"
                    f"📊 <b>Результат:</b>\n"
                    f"• Добавлено новых: {stats['added']}\n"
                    f"• Обновлено описаний: {stats['updated']}\n"
                    f"• Пропущено дубликатов: {stats['duplicates']}\n"
                    f"• Всего снимков в каталоге: {stats['total']}",
                    parse_mode="HTML",
                    reply_markup=get_afs_catalog_keyboard(has_catalog=True)
                )
            else:
                old_count = len(afs_catalog.catalog)
                afs_catalog.catalog = []
                afs_catalog.villages_by_frame = {}
                
                for item in new_catalog:
                    frame = item['frame']
                    description = item['description']
                    villages = item['villages']
                    
                    afs_catalog.catalog.append({'frame': frame, 'description': description})
                    if villages:
                        afs_catalog.villages_by_frame[frame] = villages
                
                afs_catalog._save()
                
                await message.answer(
                    f"✅ <b>Каталог АФС заменен!</b>\n\n"
                    f"📊 <b>Результат:</b>\n"
                    f"• Удалено старых: {old_count}\n"
                    f"• Добавлено новых: {len(new_catalog)}\n"
                    f"• Всего снимков в каталоге: {len(new_catalog)}",
                    parse_mode="HTML",
                    reply_markup=get_afs_catalog_keyboard(has_catalog=True)
                )
            
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            await message.answer(f"❌ Ошибка при загрузке файла:\n{str(e)}")
        
        await state.clear()
    
    @dp.message(SearchStates.waiting_for_afs_upload)
    async def process_afs_txt_invalid(message: types.Message, state: FSMContext):
        await message.answer("❌ Отправьте TXT файл (с расширением .txt)")
        await state.clear()