from aiogram.types import ReplyKeyboardMarkup, KeyboardButton


def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Главное меню"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔍 ПОИСК"), KeyboardButton(text="📋 СПИСОК ДЕРЕВЕНЬ")],
            [KeyboardButton(text="📖 ИНСТРУКЦИЯ"), KeyboardButton(text="🗺️ LOCUS MAPS")],
            [KeyboardButton(text="📐 КАЛЬКУЛЯТОР КООРДИНАТ"), KeyboardButton(text="⚙️ НАСТРОЙКИ")],
            [KeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ")]
        ],
        resize_keyboard=True
    )


def back_keyboard():
    """Клавиатура возврата в главное меню"""
    from keyboards.inline import back_keyboard
    return back_keyboard()