import os
import time
import tempfile
import asyncio
from aiogram import types, F
from aiogram.fsm.context import FSMContext
from aiogram.types import FSInputFile, BufferedInputFile, InlineKeyboardMarkup, InlineKeyboardButton

from states.states import SearchStates
from keyboards.inline import (
    get_settings_main_keyboard,
    get_np_settings_keyboard,
    get_catalog_settings_keyboard,
    get_kml_management_keyboard,
    get_kml_catalog_keyboard,
    get_district_keyboard,
    get_all_districts_keyboard,
    get_delete_district_keyboard,
    get_confirm_delete_district_keyboard,
    get_confirm_clear_all_keyboard,
    get_merge_keyboard,
    back_keyboard,
    loading_in_progress_keyboard,
    stats_back_keyboard,
    get_status_keyboard,
    get_afs_catalog_keyboard
)
from utils.helpers import safe_edit_text, safe_answer_callback, safe_delete_message
from config import logger, TEMP_DIR, ADMIN_PASSWORD
from api_sources import APISourceManager, AVAILABLE_DISTRICTS
from services.kml_catalog import KMLCatalog

# Глобальные переменные
active_download = False
active_download_user_id = None
bot_enabled = True
kml_catalog = KMLCatalog()
current_kml_page = 1


def register_settings_handlers(dp, village_db, photos_db, afs_catalog):
    """Регистрирует обработчики настроек"""
    global active_download, active_download_user_id, bot_enabled, current_kml_page
    
    @dp.message(F.text == "⚙️ НАСТРОЙКИ")
    async def menu_settings_with_password(message: types.Message, state: FSMContext):
        """Запрос пароля перед входом в настройки"""
        await message.answer(
            "🔐 <b>ДОСТУП К НАСТРОЙКАМ ЗАЩИЩЕН</b>\n\n"
            "┌─────────────────────────────────┐\n"
            "│ 🔒 Введите пароль для входа    │\n"
            "│                                 │\n"
            "│ 💡 <i>Пароль могут получить     │\n"
            "│    только администраторы</i>    │\n"
            "└─────────────────────────────────┘\n\n"
            "📝 <b>Введите пароль:</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="cancel_password")]
            ])
        )
        await state.set_state(SearchStates.waiting_for_admin_password)
    
    @dp.message(SearchStates.waiting_for_admin_password)
    async def check_password(message: types.Message, state: FSMContext):
        """Проверка введенного пароля"""
        user_password = message.text.strip()
        
        if user_password == ADMIN_PASSWORD:
            logger.info(f"✅ Пользователь {message.from_user.id} успешно вошел в настройки")
            
            # Удаляем сообщение с паролем
            await safe_delete_message(message)
            
            await message.answer(
                "✅ <b>ДОСТУП РАЗРЕШЕН</b>\n\n"
                "┌─────────────────────────────────┐\n"
                "│ 🔓 Добро пожаловать в центр     │\n"
                "│    управления ботом              │\n"
                "└─────────────────────────────────┘\n\n"
                "⚙️ <b>Выберите категорию для настройки:</b>",
                parse_mode="HTML",
                reply_markup=get_settings_main_keyboard()
            )
            await state.clear()
        else:
            logger.warning(f"❌ Неудачная попытка входа от {message.from_user.id}")
            await message.answer(
                "❌ <b>НЕВЕРНЫЙ ПАРОЛЬ</b>\n\n"
                "┌─────────────────────────────────┐\n"
                "│ ⚠️ Доступ запрещен              │\n"
                "│                                 │\n"
                "│ 🔄 Нажмите ⚙️ НАСТРОЙКИ         │\n"
                "│    чтобы попробовать снова      │\n"
                "└─────────────────────────────────┘",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            await state.clear()
    
    @dp.callback_query(lambda c: c.data == "cancel_password")
    async def cancel_password(callback: types.CallbackQuery, state: FSMContext):
        """Отмена ввода пароля"""
        await state.clear()
        await safe_delete_message(callback.message)
        await callback.message.answer(
            "🔐 <b>Вход в настройки отменен</b>\n\n"
            "┌─────────────────────────────────┐\n"
            "│ 🔒 Доступ к настройкам закрыт   │\n"
            "└─────────────────────────────────┘",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "change_admin_password")
    async def change_password_start(callback: types.CallbackQuery, state: FSMContext):
        """Начало смены пароля администратора"""
        await safe_edit_text(
            callback.message,
            "🔐 <b>СМЕНА ПАРОЛЯ АДМИНИСТРАТОРА</b>\n\n"
            "┌─────────────────────────────────┐\n"
            "│ 📝 Введите новый пароль         │\n"
            "│                                 │\n"
            "│ 💡 <i>Пароль должен содержать    │\n"
            "│    не менее 4 символов</i>       │\n"
            "└─────────────────────────────────┘\n\n"
            "🔑 <b>Введите новый пароль:</b>",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="back_to_settings_main")]
            ])
        )
        await state.set_state(SearchStates.waiting_for_new_password)
        await safe_answer_callback(callback)
    
    @dp.message(SearchStates.waiting_for_new_password)
    async def change_password_save(message: types.Message, state: FSMContext):
        """Сохранение нового пароля"""
        new_password = message.text.strip()
        
        if len(new_password) < 4:
            await message.answer(
                "❌ <b>ОШИБКА</b>\n\n"
                "┌─────────────────────────────────┐\n"
                "│ ⚠️ Пароль должен содержать      │\n"
                "│    не менее 4 символов          │\n"
                "└─────────────────────────────────┘\n\n"
                "📝 Попробуйте снова:",
                parse_mode="HTML"
            )
            return
        
        # Обновляем пароль в глобальной переменной
        global ADMIN_PASSWORD
        ADMIN_PASSWORD = new_password
        
        # Сохраняем в файл для постоянного хранения
        try:
            with open("data/admin_password.txt", "w", encoding='utf-8') as f:
                f.write(new_password)
            logger.info("✅ Пароль сохранен в файл")
        except Exception as e:
            logger.error(f"Ошибка сохранения пароля: {e}")
        
        await message.answer(
            "✅ <b>ПАРОЛЬ УСПЕШНО ИЗМЕНЕН</b>\n\n"
            "┌─────────────────────────────────┐\n"
            f"│ 🔑 Новый пароль:               │\n"
            f"│ <code>{new_password}</code>      │\n"
            "│                                 │\n"
            "│ 💡 <i>Сохраните пароль в        │\n"
            "│    надежном месте</i>            │\n"
            "└─────────────────────────────────┘",
            parse_mode="HTML",
            reply_markup=get_settings_main_keyboard()
        )
        await state.clear()
    
    @dp.callback_query(lambda c: c.data == "back_to_settings_main")
    async def back_to_settings_main(callback: types.CallbackQuery):
        await safe_edit_text(
            callback.message,
            "⚙️ <b>Центр управления ботом</b>\n\n"
            "┌─────────────────────────────────┐\n"
            "│ 🔧 Выберите категорию           │\n"
            "│    для настройки                │\n"
            "└─────────────────────────────────┘",
            parse_mode="HTML",
            reply_markup=get_settings_main_keyboard()
        )
        await safe_answer_callback(callback)
    
    # ========== УПРАВЛЕНИЕ KML ==========
    
    @dp.callback_query(lambda c: c.data == "kml_management_menu")
    async def kml_management_menu(callback: types.CallbackQuery):
        stats = kml_catalog.get_statistics()
        
        text = (
            f"🗺️ <b>УПРАВЛЕНИЕ KML ФАЙЛАМИ</b>\n\n"
            f"┌─────────────────────────────────┐\n"
            f"│ 📊 <b>Статистика:</b>            │\n"
            f"│                                 │\n"
            f"│ • Всего файлов: {stats['total']}                │\n"
            f"│ • С описаниями: {stats['with_description']}       │\n"
            f"│ • Файлов на диске: {stats['with_file']}          │\n"
            f"└─────────────────────────────────┘\n\n"
        )
        
        if stats['recent_items']:
            text += f"📌 <b>Последние добавленные:</b>\n"
            for item in stats['recent_items'][:3]:
                text += f"• <code>{item['frame']}</code>\n"
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
            logger.error(f"Ошибка: {e}")
            await safe_edit_text(
                callback.message,
                f"❌ <b>Ошибка при обновлении</b>\n\n{str(e)}",
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
            "<code>N56E34-266-016|Снимок Бахмутово</code>\n\n"
            "Введите данные:",
            parse_mode="HTML"
        )
        await state.set_state(SearchStates.waiting_for_add_kml)
        await safe_answer_callback(callback)
    
    @dp.message(SearchStates.waiting_for_add_kml)
    async def add_kml_parse(message: types.Message, state: FSMContext):
        data = message.text.strip()
        
        if '|' not in data:
            await message.answer("❌ Неверный формат. Ожидается: номер_снимка|описание")
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
                f"• Номер снимка: {frame}\n"
                f"• Описание: {description if description else 'не указано'}\n"
                f"• Всего файлов: {stats['total']}",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
        else:
            await message.answer(
                f"❌ <b>Файл не добавлен!</b>\n\nФайл с номером {frame} уже существует.",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
        
        await state.clear()
    
    @dp.callback_query(lambda c: c.data == "load_kml_catalog")
    async def load_kml_catalog_start(callback: types.CallbackQuery, state: FSMContext):
        await safe_edit_text(
            callback.message,
            "📤 <b>Загрузка KML файлов</b>\n\n"
            "Отправьте KML файл с Placemark Frame-XXX.\n\n"
            "Отправьте файл:",
            parse_mode="HTML"
        )
        await state.set_state(SearchStates.waiting_for_kml_upload)
        await safe_answer_callback(callback)
    
    @dp.message(SearchStates.waiting_for_kml_upload, F.document)
    async def process_kml_file_upload(message: types.Message, state: FSMContext):
        if not message.document.file_name.endswith('.kml'):
            await message.answer("❌ Отправьте KML файл")
            return
        
        await message.answer("⏳ Загрузка...")
        
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
                    f"❌ <b>Ошибка</b>\n\n{stats.get('error_msg', 'Неизвестная ошибка')}",
                    parse_mode="HTML",
                    reply_markup=get_kml_management_keyboard()
                )
            elif stats['duplicate']:
                await message.answer(
                    f"⚠️ <b>Файл не добавлен</b>\n\nФайл {stats['frame']} уже существует.",
                    parse_mode="HTML",
                    reply_markup=get_kml_management_keyboard()
                )
            else:
                await message.answer(
                    f"✅ <b>KML файл добавлен!</b>\n\n"
                    f"• Номер снимка: {stats['frame']}\n"
                    f"• Имя файла: {original_filename}\n"
                    f"• Всего файлов: {stats['total']}",
                    parse_mode="HTML",
                    reply_markup=get_kml_management_keyboard()
                )
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            await message.answer(f"❌ Ошибка: {str(e)}")
        
        await state.clear()
    
    @dp.message(SearchStates.waiting_for_kml_upload)
    async def process_kml_file_invalid(message: types.Message, state: FSMContext):
        await message.answer("❌ Отправьте KML файл")
        await state.clear()
    
    @dp.callback_query(lambda c: c.data == "kml_stats")
    async def kml_stats_handler(callback: types.CallbackQuery):
        stats = kml_catalog.get_statistics()
        text = (
            f"📊 <b>Статистика KML</b>\n\n"
            f"• Всего файлов: {stats['total']}\n"
            f"• С описаниями: {stats['with_description']}\n"
            f"• Без описаний: {stats['without_description']}\n"
            f"• Файлов на диске: {stats['with_file']}\n"
        )
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
            with_descriptions=False, page=page, per_page=50
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
            await callback.message.answer("📭 Каталог KML пуст", reply_markup=back_keyboard())
            await callback.answer()
            return
        current_kml_page = 1
        text, total_pages, current = kml_catalog.get_catalog_text(
            with_descriptions=False, page=1, per_page=50
        )
        await safe_edit_text(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=get_kml_catalog_keyboard(
                has_catalog=True, page=current, total_pages=total_pages
            )
        )
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "download_kml_catalog")
    async def download_kml_catalog_handler(callback: types.CallbackQuery):
        if kml_catalog.is_empty():
            await callback.message.answer("❌ Каталог пуст")
            await callback.answer()
            return
        try:
            filepath = kml_catalog.export_to_txt()
            if filepath and os.path.exists(filepath):
                with open(filepath, 'rb') as f:
                    await callback.message.answer_document(
                        BufferedInputFile(f.read(), filename=os.path.basename(filepath)),
                        caption=f"📁 Каталог KML: {len(kml_catalog.catalog)} файлов",
                        parse_mode="HTML"
                    )
                os.unlink(filepath)
        except Exception as e:
            await callback.message.answer(f"❌ Ошибка: {e}")
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "clear_kml_catalog")
    async def clear_kml_catalog_handler(callback: types.CallbackQuery):
        removed = kml_catalog.clear()
        await safe_edit_text(
            callback.message,
            f"🗑️ <b>Каталог KML очищен</b>\n\nУдалено: {removed}",
            parse_mode="HTML",
            reply_markup=get_kml_management_keyboard()
        )
        await callback.answer()
    
    # ========== УПРАВЛЕНИЕ НАСЕЛЕННЫМИ ПУНКТАМИ ==========
    
    @dp.callback_query(lambda c: c.data == "np_settings_menu")
    async def np_settings_menu(callback: types.CallbackQuery):
        stats = village_db.get_stats()
        text = (
            f"🏘️ <b>Управление населенными пунктами</b>\n\n"
            f"📊 <b>Статистика:</b>\n"
            f"• Всего: {stats['total']}\n"
            f"• С координатами: {stats['with_coords']}\n"
            f"• Без координат: {stats['total'] - stats['with_coords']}\n"
        )
        if stats['last_update']:
            text += f"• Обновлено: {stats['last_update']}\n"
        
        districts = village_db.get_districts()
        if districts:
            text += f"\n📍 <b>Районы:</b>\n"
            for d in districts:
                text += f"• {d}: {len(village_db.get_villages_by_district(d))} НП\n"
        
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
        stats = afs_catalog.get_statistics()
        
        text = (
            f"📸 <b>Каталог АФС</b>\n\n"
            f"📊 <b>Статистика:</b>\n"
            f"• Всего снимков: {stats['total']}\n"
            f"• С описаниями: {stats['with_description']}\n"
            f"• Без описаний: {stats['without_description']}\n"
            f"• Снимков с НП: {stats['with_villages']}\n"
            f"• Снимков без НП: {stats['without_villages']}\n"
            f"• Всего связей (НП в кадрах): {stats['total_village_links']}\n"
            f"• Средняя длина описания: {stats['avg_description_length']} символов\n\n"
        )
        
        if stats['recent_items']:
            text += f"📌 <b>Последние 5 снимков:</b>\n"
            for item in stats['recent_items']:
                frame = item['frame']
                villages = afs_catalog.get_villages_for_frame(frame)
                text += f"• {frame}"
                if villages:
                    text += f" ({len(villages)} НП)"
                text += "\n"
            text += "\n"
        
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
        stats = afs_catalog.get_statistics()
        text = (
            f"📊 <b>Статистика АФС</b>\n\n"
            f"• Всего снимков: {stats['total']}\n"
            f"• С описаниями: {stats['with_description']}\n"
            f"• Без описаний: {stats['without_description']}\n"
            f"• Снимков с населенными пунктами: {stats['with_villages']}\n"
            f"• Снимков без населенных пунктов: {stats['without_villages']}\n"
            f"• Всего связей (НП в кадрах): {stats['total_village_links']}\n"
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
    async def show_afs_catalog_handler(callback: types.CallbackQuery):
        if afs_catalog.is_empty():
            await callback.message.answer("📭 Каталог АФС пуст", reply_markup=back_keyboard())
            await callback.answer()
            return
        
        text, total_pages, current = afs_catalog.get_catalog_text(
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
    
    @dp.callback_query(lambda c: c.data == "download_afs_catalog")
    async def download_afs_catalog_handler(callback: types.CallbackQuery):
        if afs_catalog.is_empty():
            await callback.message.answer("❌ Каталог пуст")
            await callback.answer()
            return
        try:
            filepath = afs_catalog.export_to_txt()
            if filepath and os.path.exists(filepath):
                with open(filepath, 'rb') as f:
                    await callback.message.answer_document(
                        BufferedInputFile(f.read(), filename=os.path.basename(filepath)),
                        caption=f"📁 Каталог АФС: {len(afs_catalog.catalog)} снимков",
                        parse_mode="HTML"
                    )
                os.unlink(filepath)
        except Exception as e:
            await callback.message.answer(f"❌ Ошибка: {e}")
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "clear_afs_catalog")
    async def clear_afs_catalog_handler(callback: types.CallbackQuery):
        removed = afs_catalog.clear()
        await safe_edit_text(
            callback.message,
            f"🗑️ <b>Каталог АФС очищен</b>\n\nУдалено: {removed}",
            parse_mode="HTML",
            reply_markup=get_catalog_settings_keyboard()
        )
        await callback.answer()
    
    # ========== ЗАГРУЗКА ИЗ ИНТЕРНЕТА ==========
    
    @dp.callback_query(lambda c: c.data == "download_from_web_start")
    async def download_from_web_start(callback: types.CallbackQuery, state: FSMContext):
        global active_download, active_download_user_id
        
        if active_download:
            await safe_edit_text(
                callback.message,
                f"⚠️ <b>Загрузка уже выполняется</b>\n\nПользователь {active_download_user_id} уже загружает данные.",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            await safe_answer_callback(callback, "Загрузка уже выполняется", show_alert=True)
            return
        
        await safe_edit_text(
            callback.message,
            "🌐 <b>Загрузка из интернета</b>\n\nВыберите район:",
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
            f"⏳ <b>Загрузка {district} района...</b>\n\nЭто может занять 10-15 минут.",
            parse_mode="HTML",
            reply_markup=loading_in_progress_keyboard()
        )
        await safe_answer_callback(callback, f"⏳ Загрузка {district} района...")
        
        try:
            api_manager = APISourceManager()
            download_task = asyncio.create_task(api_manager.fetch_district_data(district))
            
            try:
                villages = await asyncio.wait_for(download_task, timeout=1500.0)
            except asyncio.CancelledError:
                await safe_edit_text(
                    callback.message,
                    f"⏹️ <b>Загрузка отменена</b>",
                    parse_mode="HTML",
                    reply_markup=back_keyboard()
                )
                await safe_answer_callback(callback, "Загрузка отменена")
                return
            
            await api_manager.close_session()
            
            if not villages:
                await safe_edit_text(
                    callback.message,
                    f"❌ <b>Не удалось загрузить данные</b>",
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
                f"✅ <b>Данные загружены!</b>\n\n"
                f"• Всего: {len(villages)}\n"
                f"• С координатами: {with_coords}\n"
                f"• Без координат: {len(villages) - with_coords}\n\n"
                f"<b>Что сделать?</b>",
                parse_mode="HTML",
                reply_markup=get_merge_keyboard(district)
            )
        except asyncio.TimeoutError:
            await safe_edit_text(
                callback.message,
                "❌ <b>Превышено время ожидания</b>",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            await safe_edit_text(
                callback.message,
                f"❌ <b>Ошибка</b>\n\n{str(e)}",
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
                "❌ Временный файл не найден",
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
                    f"✅ <b>Каталог дополнен!</b>\n\n"
                    f"• Добавлено: {stats['added']}\n"
                    f"• Пропущено дубликатов: {stats['duplicates']}\n"
                    f"• Всего записей: {village_db.stats['total']}",
                    parse_mode="HTML",
                    reply_markup=back_keyboard()
                )
            except Exception as e:
                await safe_edit_text(
                    callback.message,
                    f"❌ Ошибка: {str(e)}",
                    reply_markup=back_keyboard()
                )
        
        await safe_answer_callback(callback)
    
    # ========== ДОБАВЛЕНИЕ НП ВРУЧНУЮ ==========
    
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
    
    @dp.message(SearchStates.waiting_for_add_village)
    async def add_village_parse(message: types.Message, state: FSMContext):
        data = message.text.strip()
        parts = data.split(',')
        
        if len(parts) < 5:
            await message.answer("❌ Неверный формат. Ожидается: название,тип,широта,долгота,район")
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
                f"❌ Район '{district}' не найден.\n"
                f"Доступные: {', '.join(AVAILABLE_DISTRICTS[:10])}..."
            )
            return
        
        if lat and lon:
            try:
                float(lat)
                float(lon)
            except ValueError:
                await message.answer("❌ Неверный формат координат")
                return
        
        village = {"name": name, "type": village_type, "lat": lat, "lon": lon, "district": district_normalized}
        success, msg = village_db.add_village(village)
        
        if success:
            await message.answer(
                f"✅ {msg}\n\n"
                f"• Название: {name}\n"
                f"• Тип: {village_type}\n"
                f"• Район: {district_normalized}",
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
    
    @dp.message(SearchStates.waiting_for_txt_upload, F.document)
    async def process_txt_upload(message: types.Message, state: FSMContext):
        if not message.document.file_name.endswith('.txt'):
            await message.answer("❌ Отправьте TXT файл")
            await state.clear()
            return
        
        await message.answer("⏳ Загрузка...")
        
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
                await message.answer("❌ Неверный формат. Ожидается заголовок: Название Тип Широта Долгота Район")
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
                        villages.append({'name': name, 'type': village_type, 'lat': lat, 'lon': lon, 'district': district_normalized})
            
            if not villages:
                await message.answer("❌ Нет корректных записей")
                await state.clear()
                return
            
            stats = village_db.add_villages_batch(villages)
            await message.answer(
                f"✅ <b>Загрузка завершена!</b>\n\n"
                f"• Добавлено: {stats['added']}\n"
                f"• Пропущено дубликатов: {stats['duplicates']}\n"
                f"• Всего записей: {village_db.stats['total']}",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
        except Exception as e:
            await message.answer(f"❌ Ошибка: {str(e)}")
        
        await state.clear()
    
    @dp.message(SearchStates.waiting_for_txt_upload)
    async def process_txt_invalid(message: types.Message, state: FSMContext):
        await message.answer("❌ Отправьте TXT файл")
        await state.clear()
    
    # ========== УДАЛЕНИЕ РАЙОНА ==========
    
    @dp.callback_query(lambda c: c.data == "delete_district_start")
    async def delete_district_start(callback: types.CallbackQuery):
        districts = village_db.get_districts()
        await safe_edit_text(
            callback.message,
            "🗑️ <b>Удаление района</b>\n\nВыберите район:",
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
            f"🗑️ <b>Удаление {district}</b>\n\n⚠️ Удалить {count} НП?",
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
            reply_markup=back_keyboard()
        )
        await safe_answer_callback(callback)
    
    # ========== ОЧИСТКА КАТАЛОГА НП ==========
    
    @dp.callback_query(lambda c: c.data == "clear_all_catalog")
    async def clear_all_catalog_confirm(callback: types.CallbackQuery):
        total = village_db.stats['total']
        if total == 0:
            await safe_edit_text(callback.message, "📭 Каталог уже пуст", reply_markup=back_keyboard())
            await safe_answer_callback(callback)
            return
        await safe_edit_text(
            callback.message,
            f"⚠️ <b>ОЧИСТКА КАТАЛОГА</b>\n\nУдалить все {total} НП?",
            parse_mode="HTML",
            reply_markup=get_confirm_clear_all_keyboard(total)
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "confirm_clear_all")
    async def clear_all_catalog_execute(callback: types.CallbackQuery):
        removed = village_db.clear_all()
        await safe_edit_text(
            callback.message,
            f"✅ <b>Каталог очищен</b>\n\nУдалено: {removed} НП",
            parse_mode="HTML",
            reply_markup=back_keyboard()
        )
        await safe_answer_callback(callback)
    
    # ========== СТАТИСТИКА КАТАЛОГА НП ==========
    
    @dp.callback_query(lambda c: c.data == "village_stats")
    async def show_stats(callback: types.CallbackQuery):
        stats = village_db.get_stats()
        text = (
            f"📊 <b>Статистика НП</b>\n\n"
            f"• Всего: {stats['total']}\n"
            f"• С координатами: {stats['with_coords']}\n"
            f"• Без координат: {stats['total'] - stats['with_coords']}\n"
        )
        if stats['last_update']:
            text += f"• Обновлено: {stats['last_update']}\n"
        
        districts = village_db.get_districts()
        if districts:
            text += f"\n📍 <b>Районы:</b>\n"
            for d in districts:
                text += f"• {d}: {len(village_db.get_villages_by_district(d))} НП\n"
        
        if village_db.villages:
            text += f"\n📝 <b>Примеры (первые 5):</b>\n"
            for v in village_db.villages[:5]:
                coords = f"({v['lat']}, {v['lon']})" if v['lat'] and v['lon'] else "(без координат)"
                text += f"• {v['name']} ({v['type']}) - {v['district']} {coords}\n"
        
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
            await callback.message.answer("❌ Каталог пуст")
            await safe_answer_callback(callback)
            return
        try:
            filepath = village_db.export_to_txt()
            await callback.message.answer_document(
                FSInputFile(filepath, filename=os.path.basename(filepath)),
                caption=f"📁 Каталог НП: {village_db.stats['total']} записей",
                parse_mode="HTML"
            )
            os.unlink(filepath)
        except Exception as e:
            await callback.message.answer(f"❌ Ошибка: {e}")
        await safe_answer_callback(callback)
    
    # ========== НАВИГАЦИЯ ==========
    
    @dp.callback_query(lambda c: c.data == "show_more_districts")
    async def show_more_districts(callback: types.CallbackQuery):
        await safe_edit_text(
            callback.message,
            "🌐 <b>Выберите район</b>",
            parse_mode="HTML",
            reply_markup=get_all_districts_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "back_to_districts")
    async def back_to_districts(callback: types.CallbackQuery):
        await safe_edit_text(
            callback.message,
            "🌐 <b>Выберите район</b>",
            parse_mode="HTML",
            reply_markup=get_district_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "enable_bot")
    async def enable_bot(callback: types.CallbackQuery):
        global bot_enabled
        bot_enabled = not bot_enabled
        status_text = "ВКЛЮЧЕН" if bot_enabled else "ВЫКЛЮЧЕН"
        await safe_edit_text(
            callback.message,
            f"🤖 <b>Статус бота</b>\n\nБот теперь {status_text}.",
            parse_mode="HTML",
            reply_markup=get_settings_main_keyboard()
        )
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "check_bot_status")
    async def check_bot_status(callback: types.CallbackQuery):
        np_stats = village_db.get_stats()
        afs_stats = afs_catalog.get_statistics()
        kml_stats = kml_catalog.get_statistics()
        bot_status = "✅ Включен" if bot_enabled else "❌ Выключен"
        
        text = (
            f"🔧 <b>Статус бота</b>\n\n"
            f"📊 <b>Данные:</b>\n"
            f"{'✅' if np_stats['total'] > 0 else '❌'} Населенные пункты: {np_stats['total']}\n"
            f"{'✅' if afs_stats['total'] > 0 else '❌'} Каталог АФС: {afs_stats['total']}\n"
            f"{'✅' if kml_stats['total'] > 0 else '❌'} Каталог KML: {kml_stats['total']}\n\n"
            f"🤖 <b>Бот:</b> {bot_status}"
        )
        
        await safe_edit_text(
            callback.message,
            text,
            parse_mode="HTML",
            reply_markup=get_status_keyboard()
        )
        await safe_answer_callback(callback)