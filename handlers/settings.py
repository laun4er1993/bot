import os
import time
import tempfile
import asyncio
from aiogram import types, F
from aiogram.fsm.context import FSMContext
from aiogram.types import FSInputFile, BufferedInputFile

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
        
        # Также обновляем в config (для сохранения между перезапусками)
        import config
        config.ADMIN_PASSWORD = new_password
        
        # Пытаемся сохранить в переменную окружения (если возможно)
        os.environ['ADMIN_PASSWORD'] = new_password
        
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
    
    # ========== ОСТАЛЬНЫЕ ОБРАБОТЧИКИ НАСТРОЕК (БЕЗ ИЗМЕНЕНИЙ) ==========
    
    @dp.callback_query(lambda c: c.data == "np_settings_menu")
    async def np_settings_menu(callback: types.CallbackQuery):
        stats = village_db.get_stats()
        districts = village_db.get_districts()
        
        text = (
            f"🏘️ <b>УПРАВЛЕНИЕ НАСЕЛЕННЫМИ ПУНКТАМИ</b>\n\n"
            f"┌─────────────────────────────────┐\n"
            f"│ 📊 <b>Статистика:</b>            │\n"
            f"│                                 │\n"
            f"│ • Всего: {stats['total']}                      │\n"
            f"│ • С координатами: {stats['with_coords']}               │\n"
            f"│ • Без координат: {stats['total'] - stats['with_coords']}           │\n"
            f"└─────────────────────────────────┘\n"
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
        stats = afs_catalog.get_statistics()
        
        text = (
            f"📸 <b>КАТАЛОГ АФС</b>\n\n"
            f"┌─────────────────────────────────┐\n"
            f"│ 📊 <b>Статистика:</b>            │\n"
            f"│                                 │\n"
            f"│ • Всего снимков: {stats['total']}               │\n"
            f"│ • С описаниями: {stats['with_description']}         │\n"
            f"│ • Снимков с НП: {stats['with_villages']}            │\n"
            f"│ • Связей НП-снимки: {stats['total_village_links']}        │\n"
            f"└─────────────────────────────────┘\n\n"
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
    
    # ... остальные обработчики настроек остаются без изменений ...
    # (afs_stats, show_afs_catalog, download_afs_catalog, clear_afs_catalog,
    #  village_stats, download_villages_txt, delete_district_start,
    #  delete_district_confirm, delete_district_execute, clear_all_catalog_confirm,
    #  clear_all_catalog_execute, download_from_web_start, add_village_manual_start,
    #  load_catalog_txt_start, check_bot_status, enable_bot, show_more_districts,
    #  back_to_districts и т.д.)