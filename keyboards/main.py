from aiogram.types import ReplyKeyboardMarkup, KeyboardButton


def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Главное меню - улучшенная навигация"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔍 ПОИСК СНИМКОВ"), KeyboardButton(text="📋 ВСЕ ДЕРЕВНИ")],
            [KeyboardButton(text="📖 ПОМОЩЬ"), KeyboardButton(text="🗺️ КАРТЫ")],
            [KeyboardButton(text="🔄 KML ОБРАБОТКА"), KeyboardButton(text="⚙️ НАСТРОЙКИ")],
            [KeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ")]
        ],
        resize_keyboard=True
    )


def back_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура возврата в главное меню"""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ")]],
        resize_keyboard=True
    )