from typing import List
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from api_sources.config import AVAILABLE_DISTRICTS


def get_settings_main_keyboard() -> InlineKeyboardMarkup:
    """Главное меню настроек - улучшенная структура"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗺️ УПРАВЛЕНИЕ KML", callback_data="kml_management_menu")],
        [InlineKeyboardButton(text="🏘️ НАСЕЛЕННЫЕ ПУНКТЫ", callback_data="np_settings_menu")],
        [InlineKeyboardButton(text="📸 КАТАЛОГ АФС", callback_data="catalog_settings_menu")],
        [InlineKeyboardButton(text="🔧 ПРОВЕРКА БОТА", callback_data="check_bot_status")],
        [InlineKeyboardButton(text="🔛 ВКЛ/ВЫКЛ БОТА", callback_data="enable_bot")],
        [InlineKeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ", callback_data="back_to_main")]
    ])


def get_kml_management_keyboard() -> InlineKeyboardMarkup:
    """Меню управления KML"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 ОБРАБОТАТЬ KML", callback_data="process_kml_menu")],
        [InlineKeyboardButton(text="📤 ЗАГРУЗИТЬ KML", callback_data="load_kml_catalog")],
        [InlineKeyboardButton(text="📊 СТАТИСТИКА KML", callback_data="kml_stats")],
        [InlineKeyboardButton(text="📋 КАТАЛОГ KML", callback_data="show_kml_catalog")],
        [InlineKeyboardButton(text="📥 СКАЧАТЬ KML TXT", callback_data="download_kml_catalog")],
        [InlineKeyboardButton(text="🗑️ ОЧИСТИТЬ KML", callback_data="clear_kml_catalog")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_settings_main")]
    ])


def get_kml_catalog_keyboard(has_catalog: bool = False, page: int = 1, total_pages: int = 1) -> InlineKeyboardMarkup:
    """Клавиатура для просмотра каталога KML"""
    keyboard = []
    
    if total_pages > 1:
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="◀️ НАЗАД", callback_data=f"kml_page_{page-1}"))
        if page < total_pages:
            nav_buttons.append(InlineKeyboardButton(text="ВПЕРЕД ▶️", callback_data=f"kml_page_{page+1}"))
        if nav_buttons:
            keyboard.append(nav_buttons)
    
    if has_catalog:
        keyboard.append([InlineKeyboardButton(text="⚙️ УПРАВЛЕНИЕ KML", callback_data="kml_management_menu")])
    
    keyboard.append([InlineKeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ", callback_data="back_to_main")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_np_settings_keyboard() -> InlineKeyboardMarkup:
    """Меню населенных пунктов"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ ДОБАВИТЬ ВРУЧНУЮ", callback_data="add_village_manual")],
        [InlineKeyboardButton(text="📂 ЗАГРУЗИТЬ TXT", callback_data="load_catalog_txt")],
        [InlineKeyboardButton(text="🌐 ЗАГРУЗИТЬ ИЗ ИНТЕРНЕТА", callback_data="download_from_web_start")],
        [InlineKeyboardButton(text="🗑️ УДАЛИТЬ РАЙОН", callback_data="delete_district_start")],
        [InlineKeyboardButton(text="🗑️ ОЧИСТИТЬ ВСЕ", callback_data="clear_all_catalog")],
        [InlineKeyboardButton(text="📊 СТАТИСТИКА", callback_data="village_stats")],
        [InlineKeyboardButton(text="📥 СКАЧАТЬ TXT", callback_data="download_villages_txt")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_settings_main")]
    ])


def get_catalog_settings_keyboard() -> InlineKeyboardMarkup:
    """Меню каталога АФС"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 СТАТИСТИКА", callback_data="afs_stats")],
        [InlineKeyboardButton(text="📋 ПОКАЗАТЬ КАТАЛОГ", callback_data="show_afs_catalog")],
        [InlineKeyboardButton(text="📥 СКАЧАТЬ TXT", callback_data="download_afs_catalog")],
        [InlineKeyboardButton(text="🔄 СРАВНИТЬ С KML", callback_data="compare_afs_with_kml")],
        [InlineKeyboardButton(text="🗑️ ОЧИСТИТЬ", callback_data="clear_afs_catalog")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_settings_main")]
    ])


def get_kml_result_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура после обработки KML"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📁 СОЗДАТЬ КАТАЛОГ АФС", callback_data="create_afs_catalog")],
        [InlineKeyboardButton(text="➕ ДОПОЛНИТЬ КАТАЛОГ", callback_data="append_afs_catalog")],
        [InlineKeyboardButton(text="🔄 ЗАМЕНИТЬ КАТАЛОГ", callback_data="replace_afs_catalog")],
        [InlineKeyboardButton(text="📋 ПОКАЗАТЬ КАТАЛОГ", callback_data="show_afs_catalog")],
        [InlineKeyboardButton(text="🔄 ДРУГОЙ KML", callback_data="process_kml_menu")],
        [InlineKeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ", callback_data="back_to_main")]
    ])


def get_afs_catalog_keyboard(has_catalog: bool = False, page: int = 1, total_pages: int = 1) -> InlineKeyboardMarkup:
    """Клавиатура просмотра каталога АФС"""
    keyboard = []
    
    if total_pages > 1:
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="◀️ НАЗАД", callback_data=f"afs_page_{page-1}"))
        if page < total_pages:
            nav_buttons.append(InlineKeyboardButton(text="ВПЕРЕД ▶️", callback_data=f"afs_page_{page+1}"))
        if nav_buttons:
            keyboard.append(nav_buttons)
    
    keyboard.append([InlineKeyboardButton(text="⚙️ НАСТРОЙКИ", callback_data="catalog_settings_menu")])
    keyboard.append([InlineKeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ", callback_data="back_to_main")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_afs_compare_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для сравнения каталогов АФС"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ ДОБАВИТЬ НОВЫЕ", callback_data="afs_add_new")],
        [InlineKeyboardButton(text="🔄 ОБНОВИТЬ ОПИСАНИЯ", callback_data="afs_update_descriptions")],
        [InlineKeyboardButton(text="📥 СКАЧАТЬ РЕЗУЛЬТАТ", callback_data="afs_download_merged")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="catalog_settings_menu")]
    ])


def get_afs_catalog_load_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для загрузки общего каталога АФС"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ ДОПОЛНИТЬ", callback_data="afs_merge_common")],
        [InlineKeyboardButton(text="🔄 ЗАМЕНИТЬ", callback_data="afs_replace_common")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="catalog_settings_menu")]
    ])


def get_district_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора района (с пагинацией)"""
    keyboard = []
    # Показываем первые 18 районов, остальные через "Еще"
    for district in AVAILABLE_DISTRICTS[:18]:
        keyboard.append([InlineKeyboardButton(text=f"📍 {district}", callback_data=f"select_district_{district}")])
    
    if len(AVAILABLE_DISTRICTS) > 18:
        keyboard.append([InlineKeyboardButton(text="📋 ПОКАЗАТЬ ВСЕ", callback_data="show_more_districts")])
    
    keyboard.append([InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_settings_main")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_all_districts_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура со ВСЕМИ районами (для кнопки 'Показать все')"""
    keyboard = []
    for district in AVAILABLE_DISTRICTS:
        keyboard.append([InlineKeyboardButton(text=f"📍 {district}", callback_data=f"select_district_{district}")])
    
    keyboard.append([InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_districts")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_delete_district_keyboard(districts) -> InlineKeyboardMarkup:
    """Клавиатура выбора района для удаления"""
    if not districts:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📭 НЕТ РАЙОНОВ", callback_data="no_op")],
            [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_settings_main")]
        ])
    
    keyboard = []
    for district in districts[:20]:
        keyboard.append([InlineKeyboardButton(text=f"🗑️ {district}", callback_data=f"delete_district_confirm_{district}")])
    
    if len(districts) > 20:
        keyboard.append([InlineKeyboardButton(text=f"... и ещё {len(districts)-20}", callback_data="no_op")])
    
    keyboard.append([InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_settings_main")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_confirm_delete_district_keyboard(district: str, count: int) -> InlineKeyboardMarkup:
    """Клавиатура подтверждения удаления района"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"✅ ДА, УДАЛИТЬ ({count} НП)", callback_data=f"confirm_delete_district_{district}")],
        [InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="delete_district_start")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_settings_main")]
    ])


def get_confirm_clear_all_keyboard(total: int) -> InlineKeyboardMarkup:
    """Клавиатура подтверждения очистки всего каталога НП"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"⚠️ ДА, УДАЛИТЬ ВСЕ {total} ЗАПИСЕЙ", callback_data="confirm_clear_all")],
        [InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="np_settings_menu")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_settings_main")]
    ])


def get_merge_keyboard(district: str) -> InlineKeyboardMarkup:
    """Клавиатура для выбора действия с загруженными данными из интернета"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ ДОПОЛНИТЬ КАТАЛОГ", callback_data=f"merge_append_{district}")],
        [InlineKeyboardButton(text="📥 СКАЧАТЬ TXT", callback_data=f"merge_download_{district}")],
        [InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="back_to_main")]
    ])


def back_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура возврата в главное меню"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ", callback_data="back_to_main")]
    ])


def photos_keyboard(photos: List[str]) -> InlineKeyboardMarkup:
    """Клавиатура со списком снимков (по 3 в ряд)"""
    keyboard = []
    row = []
    for p in photos:
        # Сокращаем длинные имена для кнопок
        display_name = p if len(p) <= 18 else p[:15] + "..."
        row.append(InlineKeyboardButton(text=display_name, callback_data=f"photo_{p}"))
        if len(row) == 3:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    
    keyboard.append([InlineKeyboardButton(text="📋 ВСЕ ДЕРЕВНИ", callback_data="show_villages")])
    keyboard.append([InlineKeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ", callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def locus_menu_keyboard() -> InlineKeyboardMarkup:
    """Меню Locus Maps и карт"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📖 ИНСТРУКЦИЯ", callback_data="locus_instruction")],
        [InlineKeyboardButton(text="📥 СКАЧАТЬ LOCUS MAPS", callback_data="locus_download")],
        [InlineKeyboardButton(text="🗺️ КАРТА РЖЕВА", callback_data="map_rzhev")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_main")]
    ])


def locus_instruction_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура инструкции Locus Maps"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📖 ПОЛНАЯ ИНСТРУКЦИЯ (PDF)", url="https://disk.yandex.ru/i/sE2Jy99in7MCxw")],
        [InlineKeyboardButton(text="📥 СКАЧАТЬ LOCUS MAPS", callback_data="locus_download")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_locus")],
        [InlineKeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ", callback_data="back_to_main")]
    ])


def locus_download_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура скачивания Locus Maps"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 LOCUS MAPS (ANDROID)", url="https://disk.yandex.ru/d/uUgVGkMoq3WITw")],
        [InlineKeyboardButton(text="📖 ИНСТРУКЦИЯ", callback_data="locus_instruction")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_locus")],
        [InlineKeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ", callback_data="back_to_main")]
    ])


def back_to_locus_keyboard() -> InlineKeyboardMarkup:
    """Возврат в меню Locus Maps"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📖 ИНСТРУКЦИЯ", callback_data="locus_instruction")],
        [InlineKeyboardButton(text="📥 СКАЧАТЬ LOCUS MAPS", callback_data="locus_download")],
        [InlineKeyboardButton(text="🗺️ КАРТА РЖЕВА", callback_data="map_rzhev")],
        [InlineKeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ", callback_data="back_to_main")]
    ])


def map_download_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура скачивания карты"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 СКАЧАТЬ КАРТУ", url="https://disk.yandex.ru/d/mrxZWJqLuAtnNA")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_locus")],
        [InlineKeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ", callback_data="back_to_main")]
    ])


def search_result_keyboard(query: str) -> InlineKeyboardMarkup:
    """Клавиатура при отсутствии результатов"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 ПОПРОБОВАТЬ СНОВА", callback_data="try_again")],
        [InlineKeyboardButton(text="📋 ВСЕ ДЕРЕВНИ", callback_data="show_villages")],
        [InlineKeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ", callback_data="back_to_main")]
    ])


def photo_details_keyboard(photo_num: str = None) -> InlineKeyboardMarkup:
    """Клавиатура для деталей снимка"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 НАЗАД К СПИСКУ", callback_data="back_to_photos")],
        [InlineKeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ", callback_data="back_to_main")]
    ])


def process_kml_again_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для повторной обработки KML"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 ОБРАБОТАТЬ ДРУГОЙ KML", callback_data="process_kml_menu")],
        [InlineKeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ", callback_data="back_to_main")]
    ])


def stats_back_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура возврата из статистики"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="np_settings_menu")],
        [InlineKeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ", callback_data="back_to_main")]
    ])


def loading_in_progress_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для отмены загрузки"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏹️ ОСТАНОВИТЬ", callback_data="cancel_download")]
    ])


def get_status_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для статуса бота"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 ОБНОВИТЬ", callback_data="check_bot_status")],
        [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="back_to_settings_main")]
    ])