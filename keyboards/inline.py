# keyboards/inline.py
from typing import List
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from api_sources import AVAILABLE_DISTRICTS


def get_settings_main_keyboard() -> InlineKeyboardMarkup:
    """Главное меню настроек"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 УПРАВЛЕНИЕ KML", callback_data="kml_management_menu")],
        [InlineKeyboardButton(text="📥 ЗАГРУЗКА НП", callback_data="np_settings_menu")],
        [InlineKeyboardButton(text="📁 УПРАВЛЕНИЕ АФС", callback_data="catalog_settings_menu")],
        [InlineKeyboardButton(text="🔧 ПРОВЕРКА РАБОТОСПОСОБНОСТИ", callback_data="check_bot_status")],
        [InlineKeyboardButton(text="🔛 ВКЛЮЧИТЬ БОТА", callback_data="enable_bot")],
        [InlineKeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ", callback_data="back_to_main")]
    ])


def get_kml_management_keyboard() -> InlineKeyboardMarkup:
    """Меню управления KML"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Добавить KML вручную", callback_data="add_kml_manual")],
        [InlineKeyboardButton(text="📤 Загрузить каталог KML", callback_data="load_kml_catalog")],
        [InlineKeyboardButton(text="📊 Статистика KML", callback_data="kml_stats")],
        [InlineKeyboardButton(text="📋 Показать каталог KML", callback_data="show_kml_catalog")],
        [InlineKeyboardButton(text="📥 Скачать каталог KML (TXT)", callback_data="download_kml_catalog")],
        [InlineKeyboardButton(text="🗑️ Очистить каталог KML", callback_data="clear_kml_catalog")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_settings_main")]
    ])


def get_np_settings_keyboard() -> InlineKeyboardMarkup:
    """Меню настроек населенных пунктов"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Добавить НП вручную", callback_data="add_village_manual")],
        [InlineKeyboardButton(text="📤 Загрузить каталог НП (TXT)", callback_data="load_catalog_txt")],
        [InlineKeyboardButton(text="🌐 Загрузить из интернета", callback_data="download_from_web_start")],
        [InlineKeyboardButton(text="🗑️ Удалить район", callback_data="delete_district_start")],
        [InlineKeyboardButton(text="🗑️ Очистить весь каталог НП", callback_data="clear_all_catalog")],
        [InlineKeyboardButton(text="📊 Статистика каталога НП", callback_data="village_stats")],
        [InlineKeyboardButton(text="📤 Скачать каталог НП (TXT)", callback_data="download_villages_txt")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_settings_main")]
    ])


def get_catalog_settings_keyboard() -> InlineKeyboardMarkup:
    """Меню настроек каталога АФС"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика АФС", callback_data="afs_stats")],
        [InlineKeyboardButton(text="📋 Показать каталог АФС", callback_data="show_afs_catalog")],
        [InlineKeyboardButton(text="📥 Скачать каталог АФС (TXT)", callback_data="download_afs_catalog")],
        [InlineKeyboardButton(text="🔄 Сравнить с KML", callback_data="compare_afs_with_kml")],
        [InlineKeyboardButton(text="🗑️ Очистить каталог АФС", callback_data="clear_afs_catalog")],
        [InlineKeyboardButton(text="📤 Загрузить общий каталог АФС", callback_data="load_common_afs_catalog")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_settings_main")]
    ])


def get_kml_result_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для результатов обработки KML"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📁 Создать каталог АФС", callback_data="create_afs_catalog")],
        [InlineKeyboardButton(text="➕ Дополнить каталог АФС", callback_data="append_afs_catalog")],
        [InlineKeyboardButton(text="🔄 Заменить каталог АФС", callback_data="replace_afs_catalog")],
        [InlineKeyboardButton(text="📋 Показать каталог АФС", callback_data="show_afs_catalog")],
        [InlineKeyboardButton(text="⚙️ Настройки АФС", callback_data="catalog_settings_menu")],
        [InlineKeyboardButton(text="🔄 Обработать другой KML", callback_data="kml_management_menu")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")]
    ])


def get_kml_catalog_keyboard(has_catalog: bool = False, page: int = 1, total_pages: int = 1) -> InlineKeyboardMarkup:
    """Клавиатура для просмотра каталога KML"""
    keyboard = []
    
    if total_pages > 1:
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"kml_page_{page-1}"))
        if page < total_pages:
            nav_buttons.append(InlineKeyboardButton(text="Вперед ▶️", callback_data=f"kml_page_{page+1}"))
        if nav_buttons:
            keyboard.append(nav_buttons)
    
    if has_catalog:
        keyboard.append([InlineKeyboardButton(text="⚙️ Управление KML", callback_data="kml_management_menu")])
    
    keyboard.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_afs_catalog_keyboard(has_catalog: bool = False, page: int = 1, total_pages: int = 1) -> InlineKeyboardMarkup:
    """Клавиатура для просмотра каталога АФС"""
    keyboard = []
    
    if total_pages > 1:
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"afs_page_{page-1}"))
        if page < total_pages:
            nav_buttons.append(InlineKeyboardButton(text="Вперед ▶️", callback_data=f"afs_page_{page+1}"))
        if nav_buttons:
            keyboard.append(nav_buttons)
    
    if has_catalog:
        keyboard.append([InlineKeyboardButton(text="⚙️ Настройки АФС", callback_data="catalog_settings_menu")])
    
    keyboard.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_afs_compare_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для сравнения каталогов АФС"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить новые", callback_data="afs_add_new")],
        [InlineKeyboardButton(text="🔄 Обновить описания", callback_data="afs_update_descriptions")],
        [InlineKeyboardButton(text="📥 Скачать результат", callback_data="afs_download_merged")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="catalog_settings_menu")]
    ])


def get_afs_catalog_load_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для загрузки общего каталога АФС"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Дополнить существующий", callback_data="afs_merge_common")],
        [InlineKeyboardButton(text="🔄 Заменить существующий", callback_data="afs_replace_common")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="catalog_settings_menu")]
    ])


def get_district_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора района для загрузки из интернета"""
    keyboard = []
    for district in AVAILABLE_DISTRICTS[:5]:
        keyboard.append([InlineKeyboardButton(text=f"📍 {district} район", callback_data=f"select_district_{district}")])
    
    remaining_districts = AVAILABLE_DISTRICTS[5:]
    if remaining_districts:
        keyboard.append([InlineKeyboardButton(text="📋 Ещё районы", callback_data="show_more_districts")])
    
    keyboard.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_settings_main")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_more_districts_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура со всеми районами"""
    keyboard = []
    for district in AVAILABLE_DISTRICTS[5:]:
        keyboard.append([InlineKeyboardButton(text=f"📍 {district} район", callback_data=f"select_district_{district}")])
    keyboard.append([InlineKeyboardButton(text="🔙 Назад к районам", callback_data="back_to_districts")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_delete_district_keyboard(districts) -> InlineKeyboardMarkup:
    """Клавиатура выбора района для удаления"""
    if not districts:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📭 Нет районов для удаления", callback_data="no_op")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_settings_main")]
        ])
    
    keyboard = []
    for district in districts:
        keyboard.append([InlineKeyboardButton(text=f"🗑️ {district} район", callback_data=f"delete_district_confirm_{district}")])
    keyboard.append([InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_settings_main")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def get_confirm_delete_district_keyboard(district: str, count: int) -> InlineKeyboardMarkup:
    """Клавиатура подтверждения удаления района"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"✅ Да, удалить {district} район ({count} НП)", callback_data=f"confirm_delete_district_{district}")],
        [InlineKeyboardButton(text="❌ Нет, отмена", callback_data="delete_district_start")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_settings_main")]
    ])


def get_confirm_clear_all_keyboard(total: int) -> InlineKeyboardMarkup:
    """Клавиатура подтверждения очистки всего каталога НП"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"⚠️ ДА, УДАЛИТЬ ВСЕ {total} ЗАПИСЕЙ", callback_data="confirm_clear_all")],
        [InlineKeyboardButton(text="❌ Нет, отмена", callback_data="np_settings_menu")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_settings_main")]
    ])


def get_merge_keyboard(district: str) -> InlineKeyboardMarkup:
    """Клавиатура для выбора действия с загруженными данными из интернета"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Дополнить каталог НП", callback_data=f"merge_append_{district}")],
        [InlineKeyboardButton(text="📥 Скачать результат (TXT)", callback_data=f"merge_download_{district}")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="back_to_main")]
    ])


def back_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура возврата в главное меню"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])


def photos_keyboard(photos: List[str]) -> InlineKeyboardMarkup:
    """Клавиатура со списком снимков"""
    keyboard = []
    row = []
    for p in photos:
        row.append(InlineKeyboardButton(text=p, callback_data=f"photo_{p}"))
        if len(row) == 3:
            keyboard.append(row)
            row = []
    if row:
        keyboard.append(row)
    keyboard.append([InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=keyboard)


def locus_menu_keyboard() -> InlineKeyboardMarkup:
    """Меню Locus Maps"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📖 Инструкция", callback_data="locus_instruction")],
        [InlineKeyboardButton(text="📥 Скачать Locus Maps", callback_data="locus_download")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")]
    ])


def locus_instruction_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура инструкции Locus Maps"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📖 Полная инструкция (PDF)", url="https://disk.yandex.ru/i/sE2Jy99in7MCxw")],
        [InlineKeyboardButton(text="📥 Скачать Locus Maps", callback_data="locus_download")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_locus")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")]
    ])


def locus_download_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура скачивания Locus Maps"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Locus Maps (Android)", url="https://disk.yandex.ru/d/uUgVGkMoq3WITw")],
        [InlineKeyboardButton(text="📖 Инструкция", callback_data="locus_instruction")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_locus")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")]
    ])


def back_to_locus_keyboard() -> InlineKeyboardMarkup:
    """Возврат в меню Locus Maps"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📖 Инструкция", callback_data="locus_instruction")],
        [InlineKeyboardButton(text="📥 Скачать Locus Maps", callback_data="locus_download")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")]
    ])


def map_download_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура скачивания карты"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Скачать карту", url="https://disk.yandex.ru/d/mrxZWJqLuAtnNA")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")]
    ])


def search_result_keyboard(query: str) -> InlineKeyboardMarkup:
    """Клавиатура при отсутствии результатов поиска"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Попробовать снова", callback_data="try_again")],
        [InlineKeyboardButton(text="📋 Список деревень", callback_data="show_villages")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")]
    ])


def photo_details_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для деталей снимка"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад к списку", callback_data="back_to_photos")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")]
    ])


def process_kml_again_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для повторной обработки KML"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обработать другой KML", callback_data="kml_management_menu")]
    ])


def stats_back_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура возврата из статистики НП"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад", callback_data="np_settings_menu")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="back_to_main")]
    ])


def loading_in_progress_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для отмены загрузки"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏹️ Остановить загрузку", callback_data="cancel_download")]
    ])


def get_status_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для статуса бота"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить статус", callback_data="check_bot_status")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_settings_main")]
    ])