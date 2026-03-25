# handlers/settings.py
import os
import time
import tempfile
import asyncio
import shutil
from aiogram import types, F
from aiogram.fsm.context import FSMContext
from aiogram.types import FSInputFile, BufferedInputFile

from states.states import SearchStates
from keyboards.inline import (
    get_settings_main_keyboard, get_np_settings_keyboard, get_catalog_settings_keyboard,
    get_kml_management_keyboard, get_kml_catalog_keyboard,
    get_district_keyboard, get_more_districts_keyboard, get_delete_district_keyboard,
    get_confirm_delete_district_keyboard, get_confirm_clear_all_keyboard,
    get_merge_keyboard, back_keyboard, loading_in_progress_keyboard,
    stats_back_keyboard, get_status_keyboard
)
from utils.helpers import safe_edit_text, safe_answer_callback
from config import logger, TEMP_DIR, DATA_DIR, KML_DIR
from api_sources import APISourceManager, AVAILABLE_DISTRICTS
from services.afs_catalog import AFSCatalog
from services.kml_catalog import KMLCatalog

# Глобальные переменные
active_download = False
active_download_user_id = None
bot_enabled = True
afs_catalog = AFSCatalog()
kml_catalog = KMLCatalog()
current_kml_page = 1


def register_settings_handlers(dp, village_db, photos_db):
    global active_download, active_download_user_id, bot_enabled, current_kml_page
    
    @dp.message(F.text == "⚙️ НАСТРОЙКА")
    async def menu_settings_main(message: types.Message):
        await message.answer(
            "⚙️ <b>Центр управления ботом</b>\n\n"
            "Выберите действие:",
            parse_mode="HTML",
            reply_markup=get_settings_main_keyboard()
        )
    
    @dp.callback_query(lambda c: c.data == "back_to_settings_main")
    async def back_to_settings_main(callback: types.CallbackQuery):
        await safe_edit_text(
            callback.message,
            "⚙️ <b>Центр управления ботом</b>\n\n"
            "Выберите действие:",
            parse_mode="HTML",
            reply_markup=get_settings_main_keyboard()
        )
        await safe_answer_callback(callback)
    
    # ========== УПРАВЛЕНИЕ KML ==========
    
    @dp.callback_query(lambda c: c.data == "kml_management_menu")
    async def kml_management_menu(callback: types.CallbackQuery):
        """Меню управления KML"""
        stats = kml_catalog.get_statistics()
        text = (
            f"🔄 <b>Управление каталогом KML</b>\n\n"
            f"📊 <b>Статистика каталога:</b>\n"
            f"• Всего файлов: {stats['total']}\n"
            f"• С описаниями: {stats['with_description']}\n"
            f"• Без описаний: {stats['without_description']}\n"
            f"• Файлов на диске: {stats['with_file']}\n\n"
        )
        
        if stats['recent_items']:
            text += f"📌 <b>Последние добавленные файлы:</b>\n"
            for item in stats['recent_items']:
                text += f"• {item['frame']}\n"
                if item.get('file_name'):
                    text += f"  📄 {item['file_name']}\n"
            text += "\n"
        
        await safe_edit_text(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=get_kml_management_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "refresh_kml_catalog")
    async def refresh_kml_catalog_handler(callback: types.CallbackQuery):
        """Обновление каталога KML"""
        await safe_edit_text(
            callback.message,
            "🔄 <b>Обновление каталога KML</b>\n\n"
            "Выполняется проверка и обновление описаний из базы данных...\n"
            "Это может занять несколько секунд.",
            parse_mode="HTML"
        )
        await safe_answer_callback(callback)
        
        try:
            stats = kml_catalog.refresh_catalog(photos_db)
            
            text = (
                f"✅ <b>Обновление каталога KML завершено!</b>\n\n"
                f"📊 <b>Результат:</b>\n"
                f"• Всего файлов: {stats['total']}\n"
                f"• Обновлено описаний: {stats['updated']}\n"
                f"• Не найдено описаний: {stats['failed']}\n\n"
            )
            
            if stats['details']:
                text += f"📝 <b>Обновленные записи:</b>\n"
                for detail in stats['details'][:5]:
                    text += f"• {detail['frame']}\n"
                if len(stats['details']) > 5:
                    text += f"... и ещё {len(stats['details']) - 5}\n"
            
            await safe_edit_text(
                callback.message,
                text,
                parse_mode="HTML",
                reply_markup=get_kml_management_keyboard()
            )
            
        except Exception as e:
            logger.error(f"Ошибка обновления каталога KML: {e}")
            await safe_edit_text(
                callback.message,
                f"❌ <b>Ошибка при обновлении каталога KML</b>\n\n{str(e)}",
                parse_mode="HTML",
                reply_markup=get_kml_management_keyboard()
            )
    
    @dp.callback_query(lambda c: c.data == "add_kml_manual")
    async def add_kml_manual_start(callback: types.CallbackQuery, state: FSMContext):
        await safe_edit_text(
            callback.message,
            "📝 <b>Добавление KML файла вручную</b>\n\n"
            "Введите данные в формате:\n"
            "<code>номер_снимка|описание</code>\n\n"
            "📌 <b>Пример:</b>\n"
            "<code>N56E34-266-016|Снимок Бахмутово, дата 28.10.1942</code>\n\n"
            "💡 <b>Примечания:</b>\n"
            "• Описание может быть пустым\n"
            "• Если файл уже существует, он будет пропущен\n\n"
            "Введите данные одной строкой:",
            parse_mode="HTML"
        )
        await state.set_state(SearchStates.waiting_for_add_kml)
        await safe_answer_callback(callback)
    
    @dp.message(SearchStates.waiting_for_add_kml)
    async def add_kml_parse(message: types.Message, state: FSMContext):
        data = message.text.strip()
        
        if '|' not in data:
            await message.answer(
                "❌ Неверный формат. Ожидается:\n"
                "<code>номер_снимка|описание</code>\n\n"
                "Пример: <code>N56E34-266-016|Описание снимка</code>",
                parse_mode="HTML"
            )
            return
        
        parts = data.split('|', 1)
        frame = parts[0].strip()
        description = parts[1].strip() if len(parts) > 1 else ''
        
        if not frame:
            await message.answer("❌ Номер снимка не может быть пустым")
            return
        
        stats = kml_catalog.add_kml(frame, description)
        
        if stats['added'] > 0:
            await message.answer(
                f"✅ <b>KML файл добавлен!</b>\n\n"
                f"📊 <b>Данные:</b>\n"
                f"• Номер снимка: {frame}\n"
                f"• Описание: {description if description else 'не указано'}\n\n"
                f"Всего файлов в каталоге: {stats['total']}",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
        else:
            await message.answer(
                f"❌ <b>KML файл не добавлен!</b>\n\n"
                f"Файл с номером {frame} уже существует в каталоге.",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
        
        await state.clear()
    
    @dp.callback_query(lambda c: c.data == "load_kml_catalog")
    async def load_kml_catalog_start(callback: types.CallbackQuery, state: FSMContext):
        await safe_edit_text(
            callback.message,
            "📤 <b>Загрузка KML файлов в каталог</b>\n\n"
            "Отправьте KML файл (или несколько файлов одним сообщением).\n\n"
            "📌 <b>Требования к KML файлу:</b>\n"
            "• Файл должен содержать Placemark с названием Frame-XXX\n"
            "• Бот автоматически извлечет номер снимка и описание\n\n"
            "⚠️ <b>Важно:</b>\n"
            "• Если файл уже существует в каталоге, он будет пропущен\n"
            "• Добавятся только новые файлы\n\n"
            "Отправьте KML файл:",
            parse_mode="HTML"
        )
        await state.set_state(SearchStates.waiting_for_kml_upload)
        await safe_answer_callback(callback)
    
    @dp.message(SearchStates.waiting_for_kml_upload, F.document)
    async def process_kml_file_upload(message: types.Message, state: FSMContext):
        if not message.document.file_name.endswith('.kml'):
            await message.answer("❌ Отправьте KML файл (с расширением .kml)")
            return
        
        await message.answer("⏳ Загрузка и обработка KML файла...")
        
        try:
            file_info = await message.bot.get_file(message.document.file_id)
            with tempfile.NamedTemporaryFile(delete=False, suffix='.kml') as tmp:
                await message.bot.download_file(file_info.file_path, tmp)
                tmp_path = tmp.name
                original_filename = message.document.file_name
            
            stats = kml_catalog.add_kml_from_file(tmp_path, original_filename)
            
            os.unlink(tmp_path)
            
            if stats['error']:
                await message.answer(
                    f"❌ <b>Ошибка обработки KML файла</b>\n\n"
                    f"{stats.get('error_msg', 'Неизвестная ошибка')}",
                    parse_mode="HTML",
                    reply_markup=get_kml_management_keyboard()
                )
            elif stats['duplicate']:
                await message.answer(
                    f"⚠️ <b>KML файл не добавлен</b>\n\n"
                    f"Файл с номером снимка {stats['frame']} уже существует в каталоге.",
                    parse_mode="HTML",
                    reply_markup=get_kml_management_keyboard()
                )
            else:
                await message.answer(
                    f"✅ <b>KML файл добавлен!</b>\n\n"
                    f"📊 <b>Данные:</b>\n"
                    f"• Номер снимка: {stats['frame']}\n"
                    f"• Имя файла: {original_filename}\n"
                    f"• Всего файлов в каталоге: {stats['total']}",
                    parse_mode="HTML",
                    reply_markup=get_kml_management_keyboard()
                )
            
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            await message.answer(f"❌ Ошибка при загрузке KML файла:\n{str(e)}")
        
        await state.clear()
    
    @dp.message(SearchStates.waiting_for_kml_upload)
    async def process_kml_file_invalid(message: types.Message, state: FSMContext):
        await message.answer("❌ Отправьте KML файл (с расширением .kml)")
        await state.clear()
    
    @dp.callback_query(lambda c: c.data == "kml_stats")
    async def kml_stats_handler(callback: types.CallbackQuery):
        stats = kml_catalog.get_statistics()
        
        text = (
            f"📊 <b>Статистика каталога KML</b>\n\n"
            f"• Всего файлов: {stats['total']}\n"
            f"• С описаниями: {stats['with_description']}\n"
            f"• Без описаний: {stats['without_description']}\n"
            f"• Файлов на диске: {stats['with_file']}\n\n"
        )
        
        if not kml_catalog.is_empty():
            text += f"📌 <b>Последние 5 файлов:</b>\n"
            for item in kml_catalog.catalog[-5:]:
                text += f"• {item['frame']}"
                if item.get('file_name'):
                    text += f" ({item['file_name']})"
                text += "\n"
        
        await safe_edit_text(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=get_kml_management_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data.startswith("kml_page_"))
    async def kml_page_handler(callback: types.CallbackQuery):
        global current_kml_page
        
        page = int(callback.data.replace("kml_page_", ""))
        current_kml_page = page
        
        text, total_pages, current = kml_catalog.get_catalog_text(
            with_descriptions=False,
            page=page,
            per_page=50
        )
        
        await safe_edit_text(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=get_kml_catalog_keyboard(
                has_catalog=not kml_catalog.is_empty(),
                page=current,
                total_pages=total_pages
            )
        )
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "show_kml_catalog")
    async def show_kml_catalog_handler(callback: types.CallbackQuery):
        global current_kml_page
        
        if kml_catalog.is_empty():
            await callback.message.answer(
                "📭 Каталог KML пуст.\n\n"
                "Чтобы добавить файлы:\n"
                "• Добавьте вручную\n"
                "• Отправьте KML файл",
                reply_markup=back_keyboard()
            )
            await callback.answer()
            return
        
        current_kml_page = 1
        text, total_pages, current = kml_catalog.get_catalog_text(
            with_descriptions=False,
            page=1,
            per_page=50
        )
        
        await safe_edit_text(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=get_kml_catalog_keyboard(
                has_catalog=True,
                page=current,
                total_pages=total_pages
            )
        )
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "download_kml_catalog")
    async def download_kml_catalog_handler(callback: types.CallbackQuery):
        if kml_catalog.is_empty():
            await callback.message.answer("❌ Каталог KML пуст. Сначала добавьте файлы.")
            await callback.answer()
            return
        
        try:
            filepath = kml_catalog.export_to_txt()
            
            if filepath and os.path.exists(filepath):
                with open(filepath, 'rb') as f:
                    await callback.message.answer_document(
                        BufferedInputFile(f.read(), filename=os.path.basename(filepath)),
                        caption=f"📁 <b>Каталог KML</b>\nВсего: {len(kml_catalog.catalog)} файлов",
                        parse_mode="HTML"
                    )
                os.unlink(filepath)
            else:
                await callback.message.answer("❌ Ошибка при создании файла.")
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            await callback.message.answer(f"❌ Ошибка при создании файла: {e}")
        
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "clear_kml_catalog")
    async def clear_kml_catalog_handler(callback: types.CallbackQuery):
        removed = kml_catalog.clear()
        
        await safe_edit_text(
            callback.message,
            f"🗑️ <b>Каталог KML очищен!</b>\n\n"
            f"Удалено файлов: {removed}",
            parse_mode="HTML",
            reply_markup=get_kml_management_keyboard()
        )
        await callback.answer()
    
    # ========== УПРАВЛЕНИЕ НАСЕЛЕННЫМИ ПУНКТАМИ ==========
    
    @dp.callback_query(lambda c: c.data == "np_settings_menu")
    async def np_settings_menu(callback: types.CallbackQuery):
        stats = village_db.get_stats()
        text = (
            f"📥 <b>Управление каталогом населенных пунктов</b>\n\n"
            f"📊 <b>Статистика каталога:</b>\n"
            f"• Всего записей: {stats['total']}\n"
            f"• С координатами: {stats['with_coords']}\n"
            f"• Без координат: {stats['total'] - stats['with_coords']}\n"
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
            reply_markup=get_np_settings_keyboard()
        )
        await safe_answer_callback(callback)
    
    # ========== УПРАВЛЕНИЕ КАТАЛОГОМ АФС ==========
    
    @dp.callback_query(lambda c: c.data == "catalog_settings_menu")
    async def catalog_settings_menu(callback: types.CallbackQuery):
        """Меню настроек каталога АФС"""
        stats = afs_catalog.get_statistics()
        
        text = (
            f"📁 <b>Управление каталогом АФС</b>\n\n"
            f"📊 <b>Статистика каталога:</b>\n"
            f"• Всего снимков: {stats['total']}\n"
            f"• С описаниями: {stats['with_description']}\n"
            f"• Без описаний: {stats['without_description']}\n"
            f"• Средняя длина описания: {stats['avg_description_length']} символов\n\n"
        )
        
        if stats['recent_items']:
            text += f"📌 <b>Последние добавленные снимки:</b>\n"
            for item in stats['recent_items']:
                text += f"• {item['frame']}\n"
            text += "\n"
        
        await safe_edit_text(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=get_catalog_settings_keyboard()
        )
        await safe_answer_callback(callback)
    
    # ========== ПРОВЕРКА РАБОТОСПОСОБНОСТИ ==========
    
    @dp.callback_query(lambda c: c.data == "check_bot_status")
    async def check_bot_status(callback: types.CallbackQuery):
        np_stats = village_db.get_stats()
        np_status = "✅" if np_stats['total'] > 0 else "❌"
        np_text = f"{np_status} Населенные пункты: {np_stats['total']} записей"
        
        afs_stats = afs_catalog.get_statistics()
        afs_status = "✅" if afs_stats['total'] > 0 else "❌"
        afs_text = f"{afs_status} Каталог АФС: {afs_stats['total']} снимков"
        
        kml_stats = kml_catalog.get_statistics()
        kml_status = "✅" if kml_stats['total'] > 0 else "❌"
        kml_text = f"{kml_status} Каталог KML: {kml_stats['total']} файлов"
        
        bot_status = "✅ Включен" if bot_enabled else "❌ Выключен"
        
        files_status = []
        data_dir = "data"
        if os.path.exists(data_dir):
            files = os.listdir(data_dir)
            files_status.append(f"📁 data/ — {len(files)} файлов")
        
        text = (
            f"🔧 <b>ПРОВЕРКА РАБОТОСПОСОБНОСТИ БОТА</b>\n\n"
            f"📊 <b>Статус данных:</b>\n"
            f"{np_text}\n"
            f"{afs_text}\n"
            f"{kml_text}\n\n"
            f"🤖 <b>Статус бота:</b>\n"
            f"{bot_status}\n\n"
            f"📂 <b>Файловая система:</b>\n"
            + "\n".join(files_status) + "\n\n"
            f"🔄 Для обновления статуса нажмите кнопку ниже."
        )
        
        await safe_edit_text(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=get_status_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "enable_bot")
    async def enable_bot(callback: types.CallbackQuery):
        global bot_enabled
        bot_enabled = not bot_enabled
        
        status_text = "ВКЛЮЧЕН" if bot_enabled else "ВЫКЛЮЧЕН"
        await safe_edit_text(
            callback.message,
            f"🤖 <b>Статус бота изменен</b>\n\n"
            f"Бот теперь {status_text}.\n\n"
            f"{'✅ Все функции доступны.' if bot_enabled else '⚠️ Бот не будет отвечать на запросы.'}",
            parse_mode="HTML",
            reply_markup=get_settings_main_keyboard()
        )
        await safe_answer_callback(callback)
    
    # ========== ДОБАВЛЕНИЕ НП ВРУЧНУЮ ==========
    
    @dp.callback_query(lambda c: c.data == "add_village_manual")
    async def add_village_manual_start(callback: types.CallbackQuery, state: FSMContext):
        await safe_edit_text(
            callback.message,
            "📝 <b>Добавление населенного пункта вручную</b>\n\n"
            "Введите данные в формате:\n"
            "<code>название,тип,широта,долгота,район</code>\n\n"
            "📌 <b>Пример:</b>\n"
            "<code>Горбово,деревня,56.2345,34.1234,Ржевский</code>\n\n"
            "💡 <b>Примечания:</b>\n"
            "• Тип может быть: деревня, село, посёлок, хутор, станция, урочище\n"
            "• Координаты могут быть пустыми: <code>Горбово,деревня,,,Ржевский</code>\n"
            "• Если НП уже существует, он будет пропущен\n\n"
            "Введите данные одной строкой:",
            parse_mode="HTML"
        )
        await state.set_state(SearchStates.waiting_for_add_village)
        await safe_answer_callback(callback)
    
    @dp.message(SearchStates.waiting_for_add_village)
    async def add_village_parse(message: types.Message, state: FSMContext):
        data = message.text.strip()
        parts = data.split(',')
        
        if len(parts) < 5:
            await message.answer(
                "❌ Неверный формат. Ожидается:\n"
                "<code>название,тип,широта,долгота,район</code>\n\n"
                "Пример: <code>Горбово,деревня,56.2345,34.1234,Ржевский</code>",
                parse_mode="HTML"
            )
            return
        
        name = parts[0].strip()
        village_type = parts[1].strip() if parts[1].strip() else "деревня"
        lat = parts[2].strip() if len(parts) > 2 else ""
        lon = parts[3].strip() if len(parts) > 3 else ""
        district = parts[4].strip() if len(parts) > 4 else ""
        
        if not name:
            await message.answer("❌ Название не может быть пустым")
            return
        
        valid_types = ['деревня', 'село', 'посёлок', 'хутор', 'станция', 'урочище']
        if village_type not in valid_types:
            await message.answer(f"❌ Неверный тип. Доступные: {', '.join(valid_types)}")
            return
        
        district_normalized = district.replace(" район", "").strip()
        if district_normalized not in AVAILABLE_DISTRICTS:
            await message.answer(
                f"❌ Район '{district}' не найден.\n\n"
                f"Доступные районы: {', '.join(AVAILABLE_DISTRICTS[:10])}...\n"
                f"Введите полное название района (например: Ржевский)"
            )
            return
        
        if lat and lon:
            try:
                float(lat)
                float(lon)
            except ValueError:
                await message.answer("❌ Неверный формат координат. Используйте числа с точкой")
                return
        
        village = {
            "name": name,
            "type": village_type,
            "lat": lat,
            "lon": lon,
            "district": district_normalized
        }
        
        success, msg = village_db.add_village(village)
        
        if success:
            await message.answer(
                f"✅ {msg}\n\n"
                f"📊 <b>Данные добавленного НП:</b>\n"
                f"• Название: {village['name']}\n"
                f"• Тип: {village['type']}\n"
                f"• Координаты: {village['lat'] if village['lat'] else 'не указаны'} {village['lon'] if village['lon'] else ''}\n"
                f"• Район: {village['district']}\n\n"
                f"Теперь этот НП доступен для поиска снимков!",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
        else:
            await message.answer(f"❌ {msg}", reply_markup=back_keyboard())
        
        await state.clear()
    
    # ========== ЗАГРУЗКА КАТАЛОГА НП ИЗ TXT ==========
    
    @dp.callback_query(lambda c: c.data == "load_catalog_txt")
    async def load_catalog_txt_start(callback: types.CallbackQuery, state: FSMContext):
        await safe_edit_text(
            callback.message,
            "📤 <b>Загрузка каталога населенных пунктов</b>\n\n"
            "Отправьте TXT файл в формате:\n"
            "<code>Название Тип Широта Долгота Район</code>\n\n"
            "📌 <b>Пример строки:</b>\n"
            "<code>Горбово деревня 56.2345 34.1234 Ржевский</code>\n"
            "<code>Полунино деревня - - Ржевский</code>\n\n"
            "⚠️ <b>Важно:</b>\n"
            "• Если НП уже существует в каталоге, он будет пропущен\n"
            "• Поля lat, lon могут быть пустыми (используйте -)\n"
            "• Добавятся только новые НП\n\n"
            "Отправьте TXT файл:",
            parse_mode="HTML"
        )
        await state.set_state(SearchStates.waiting_for_txt_upload)
        await safe_answer_callback(callback)
    
    @dp.message(SearchStates.waiting_for_txt_upload, F.document)
    async def process_txt_upload(message: types.Message, state: FSMContext):
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
            if not lines:
                await message.answer("❌ Файл пуст")
                await state.clear()
                return
            
            if not lines[0].startswith('Название'):
                await message.answer(
                    "❌ Неверный формат файла.\n\n"
                    "Ожидается заголовок: <code>Название Тип Широта Долгота Район</code>\n\n"
                    "Первая строка должна начинаться с 'Название'",
                    parse_mode="HTML"
                )
                await state.clear()
                return
            
            villages = []
            for line in lines[1:]:
                line = line.strip()
                if not line:
                    continue
                
                parts = line.split()
                if len(parts) >= 5:
                    name = parts[0]
                    village_type = parts[1]
                    lat = parts[2] if parts[2] != '-' else ''
                    lon = parts[3] if parts[3] != '-' else ''
                    district = parts[4]
                    
                    if len(parts) > 5:
                        name = ' '.join(parts[:-4])
                        village_type = parts[-4]
                        lat = parts[-3] if parts[-3] != '-' else ''
                        lon = parts[-2] if parts[-2] != '-' else ''
                        district = parts[-1]
                    
                    district_normalized = district.replace(" район", "").strip()
                    if district_normalized in AVAILABLE_DISTRICTS:
                        villages.append({
                            'name': name,
                            'type': village_type,
                            'lat': lat,
                            'lon': lon,
                            'district': district_normalized
                        })
            
            if not villages:
                await message.answer("❌ В файле не найдено корректных записей для добавления")
                await state.clear()
                return
            
            stats = village_db.add_villages_batch(villages)
            
            await message.answer(
                f"✅ <b>Обработка TXT завершена!</b>\n\n"
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
            await message.answer(f"❌ Ошибка при загрузке TXT:\n{str(e)}")
        
        await state.clear()
    
    @dp.message(SearchStates.waiting_for_txt_upload)
    async def process_txt_invalid(message: types.Message, state: FSMContext):
        await message.answer("❌ Отправьте TXT файл с расширением .txt")
        await state.clear()
    
    # ========== ЗАГРУЗКА ИЗ ИНТЕРНЕТА ==========
    
    @dp.callback_query(lambda c: c.data == "download_from_web_start")
    async def download_from_web_start(callback: types.CallbackQuery, state: FSMContext):
        global active_download, active_download_user_id
        
        if active_download:
            await safe_edit_text(
                callback.message,
                f"⚠️ <b>Загрузка уже выполняется!</b>\n\n"
                f"Пользователь {active_download_user_id} уже загружает данные.\n"
                f"Пожалуйста, подождите окончания загрузки.",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            await safe_answer_callback(callback, "Загрузка уже выполняется", show_alert=True)
            return
        
        await safe_edit_text(
            callback.message,
            "🌐 <b>Загрузка данных из интернета</b>\n\n"
            "Бот выполнит поиск на dic.academic.ru и Wikipedia.\n"
            "Это может занять 10-15 минут.\n\n"
            "<b>Выберите район:</b>",
            parse_mode="HTML",
            reply_markup=get_district_keyboard()
        )
        await state.set_state(SearchStates.waiting_for_district_select)
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data.startswith("select_district_"))
    async def process_district_select(callback: types.CallbackQuery, state: FSMContext):
        global active_download, active_download_user_id
        
        district = callback.data.replace("select_district_", "")
        
        active_download = True
        active_download_user_id = callback.from_user.id
        
        await safe_edit_text(
            callback.message,
            f"⏳ <b>Загрузка данных для {district} района...</b>\n\n"
            f"🔍 Выполняется поиск на dic.academic.ru и Wikipedia.\n"
            f"⏱️ Это может занять 10-15 минут.\n"
            f"<i>Нажмите кнопку ниже для остановки загрузки...</i>",
            parse_mode="HTML",
            reply_markup=loading_in_progress_keyboard()
        )
        await safe_answer_callback(callback, f"⏳ Начинаю загрузку для {district} района...")
        
        try:
            api_manager = APISourceManager()
            download_task = asyncio.create_task(api_manager.fetch_district_data(district))
            
            try:
                villages = await asyncio.wait_for(download_task, timeout=1500.0)
            except asyncio.CancelledError:
                logger.info(f"Загрузка для района {district} отменена пользователем")
                await safe_edit_text(
                    callback.message,
                    f"⏹️ <b>Загрузка данных для {district} района отменена</b>\n\n"
                    f"Вы можете попробовать позже или выбрать другой район.",
                    parse_mode="HTML",
                    reply_markup=back_keyboard()
                )
                await safe_answer_callback(callback, "Загрузка отменена")
                return
            
            await api_manager.close_session()
            
            if not villages:
                await safe_edit_text(
                    callback.message,
                    f"❌ <b>Не удалось загрузить данные для {district} района</b>\n\n"
                    f"Возможные причины:\n"
                    f"• Нет данных в источниках\n"
                    f"• Проблемы с подключением\n"
                    f"• Превышено время ожидания\n\n"
                    f"Попробуйте другой район или загрузите TXT вручную.",
                    parse_mode="HTML",
                    reply_markup=back_keyboard()
                )
                await safe_answer_callback(callback)
                return
            
            timestamp = time.strftime('%Y%m%d_%H%M%S')
            os.makedirs(TEMP_DIR, exist_ok=True)
            temp_txt = os.path.join(TEMP_DIR, f"{district}_{timestamp}.txt")
            
            with open(temp_txt, 'w', encoding='utf-8') as f:
                f.write("Название Тип Широта Долгота Район\n")
                for v in villages:
                    lat = v.get('lat', '') if v.get('lat') else '-'
                    lon = v.get('lon', '') if v.get('lon') else '-'
                    f.write(f"{v['name']} {v['type']} {lat} {lon} {v['district']}\n")
            
            await state.update_data(temp_txt=temp_txt, villages=villages)
            
            with_coords = sum(1 for v in villages if v.get('lat') and v.get('lon'))
            
            await safe_edit_text(
                callback.message,
                f"✅ <b>Данные для {district} района загружены!</b>\n\n"
                f"📊 <b>Статистика:</b>\n"
                f"• Всего населенных пунктов: {len(villages)}\n"
                f"• С координатами: {with_coords}\n"
                f"• Без координат: {len(villages) - with_coords}\n\n"
                f"<b>Что сделать с этими данными?</b>",
                parse_mode="HTML",
                reply_markup=get_merge_keyboard(district)
            )
            
        except asyncio.TimeoutError:
            await safe_edit_text(
                callback.message,
                "❌ <b>Превышено время ожидания</b>\n\n"
                "Загрузка данных заняла слишком много времени.\n"
                "Попробуйте позже или выберите другой район.",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            await safe_edit_text(
                callback.message,
                f"❌ <b>Ошибка при загрузке данных</b>\n\n"
                f"{str(e)}",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
        finally:
            active_download = False
            active_download_user_id = None
            if 'api_manager' in locals():
                await api_manager.close_session()
    
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
    
    # ========== УДАЛЕНИЕ РАЙОНА ==========
    
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
    
    # ========== ОЧИСТКА КАТАЛОГА НП ==========
    
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
            f"⚠️ <b>ОЧИСТКА ВСЕГО КАТАЛОГА НП</b>\n\n"
            f"В каталоге находится {total} населенных пунктов.\n\n"
            f"<b>Это действие НЕОБРАТИМО!</b>\n\n"
            f"Вы уверены?",
            parse_mode="HTML",
            reply_markup=get_confirm_clear_all_keyboard(total)
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "confirm_clear_all")
    async def clear_all_catalog_execute(callback: types.CallbackQuery):
        removed = village_db.clear_all()
        
        await safe_edit_text(
            callback.message,
            f"✅ <b>Каталог НП полностью очищен!</b>\n\n"
            f"📊 <b>Результат:</b>\n"
            f"• Удалено записей: {removed}\n\n"
            f"Теперь каталог пуст. Вы можете добавить новые НП.",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        await safe_answer_callback(callback)
    
    # ========== СТАТИСТИКА КАТАЛОГА НП ==========
    
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
    
    # ========== СКАЧИВАНИЕ КАТАЛОГА НП ==========
    
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
    
    # ========== НАВИГАЦИЯ ==========
    
    @dp.callback_query(lambda c: c.data == "show_more_districts")
    async def show_more_districts(callback: types.CallbackQuery):
        await safe_edit_text(
            callback.message,
            "🌐 <b>Выберите район для загрузки</b>\n\n"
            f"Всего доступно районов: {len(AVAILABLE_DISTRICTS)}\n"
            f"Выберите из списка ниже:",
            parse_mode="HTML",
            reply_markup=get_district_keyboard()
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
    
    @dp.callback_query(lambda c: c.data == "np_settings")
    async def back_to_np_settings(callback: types.CallbackQuery):
        stats = village_db.get_stats()
        text = (
            f"📥 <b>Управление каталогом населенных пунктов</b>\n\n"
            f"📊 <b>Статистика каталога:</b>\n"
            f"• Всего записей: {stats['total']}\n"
            f"• С координатами: {stats['with_coords']}\n"
            f"• Без координат: {stats['total'] - stats['with_coords']}\n"
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
            reply_markup=get_np_settings_keyboard()
        )
        await safe_answer_callback(callback)