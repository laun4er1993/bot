# keyboards/main.py
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton


def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Главное меню"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔍 ПОИСК"), KeyboardButton(text="📋 СПИСОК ДЕРЕВЕНЬ")],
            [KeyboardButton(text="📖 ИНСТРУКЦИЯ"), KeyboardButton(text="🗺️ КАРТА РЖЕВ")],
            [KeyboardButton(text="🗺️ LOCUS MAPS"), KeyboardButton(text="🔄 ОБРАБОТАТЬ KML")],
            [KeyboardButton(text="⚙️ НАСТРОЙКИ")]
        ],
        resize_keyboard=True
    )


def back_keyboard():
    """Клавиатура возврата в главное меню (импортируется из inline)"""
    from keyboards.inline import back_keyboard
    return back_keyboard()