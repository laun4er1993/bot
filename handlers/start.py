from aiogram import types, F
from aiogram.filters import Command

from keyboards.main import get_main_keyboard
from keyboards.inline import back_keyboard, locus_menu_keyboard, map_download_keyboard


async def cmd_start(message: types.Message):
    welcome_text = (
        f"✈️ <b>Добро пожаловать, {message.from_user.full_name}!</b>\n\n"
        f"🛩️ <b>Бот для поиска аэрофотоснимков Тверской области</b>\n\n"
        f"📌 <b>Основные возможности:</b>\n"
        f"• 🔍 <b>ПОИСК</b> — найдите снимки по названию деревни, координатам или номеру снимка\n"
        f"• 📋 <b>СПИСОК ДЕРЕВЕНЬ</b> — все доступные населенные пункты\n"
        f"• 📖 <b>ИНСТРУКЦИЯ</b> — подробная помощь по боту\n"
        f"• 🗺️ <b>КАРТА РЖЕВ</b> — скачать карту для Locus Maps\n"
        f"• 🗺️ <b>LOCUS MAPS</b> — инструкция и скачивание приложения\n"
        f"• ⚙️ <b>НАСТРОЙКИ</b> — управление данными, обработка KML, загрузка НП\n\n"
        f"👇 <b>Выберите действие:</b>"
    )
    await message.answer(welcome_text, parse_mode="HTML", reply_markup=get_main_keyboard())


def register_start_handlers(dp):
    
    @dp.message(Command("start"))
    async def start_handler(message: types.Message):
        await cmd_start(message)
    
    @dp.message(F.text == "🏠 ГЛАВНОЕ МЕНЮ")
    async def main_menu_button(message: types.Message):
        await cmd_start(message)
    
    @dp.message(F.text == "📖 ИНСТРУКЦИЯ")
    async def menu_instruction(message: types.Message):
        instruction_text = (
            "📖 <b>ИНСТРУКЦИЯ ПО ИСПОЛЬЗОВАНИЮ БОТА</b>\n\n"
            "🔍 <b>ПОИСК СНИМКОВ</b>\n"
            "• Нажмите «🔍 ПОИСК»\n"
            "• Введите название деревни (можно часть)\n"
            "• Или введите координаты: 56.2345 34.1234\n"
            "• Или введите номер снимка: N56E34-266-016 или 266-016\n"
            "• Нажмите на номер снимка для просмотра описания и скачивания\n\n"
            "🗺️ <b>LOCUS MAPS</b>\n"
            "• Скачайте приложение из меню «🗺️ LOCUS MAPS»\n"
            "• Загрузите карту Ржевского района\n"
            "• Скачайте MBTILES файл снимка\n"
            "• Откройте MBTILES файл в приложении для просмотра\n\n"
            "🔄 <b>KML ОБРАБОТКА (в меню НАСТРОЙКИ)</b>\n"
            "• Загрузите KML файл с каталогом снимков\n"
            "• Бот найдет населенные пункты в каждом кадре\n"
            "• Создаст подробный TXT отчет со статистикой\n"
            "• Позволит создать каталог АФС для поиска\n\n"
            "⚙️ <b>НАСТРОЙКИ</b>\n"
            "• Управление KML файлами\n"
            "• Управление населенными пунктами\n"
            "• Управление каталогом АФС\n\n"
            "🛩️ <b>ПРИЯТНОГО ИСПОЛЬЗОВАНИЯ!</b>"
        )
        await message.answer(instruction_text, parse_mode="HTML", reply_markup=back_keyboard())
    
    @dp.message(F.text == "🗺️ КАРТА РЖЕВ")
    async def menu_map(message: types.Message):
        await message.answer(
            "🗺️ <b>Карта Ржевского района для Locus Maps</b>\n\n"
            "Нажмите кнопку для скачивания:",
            parse_mode="HTML",
            reply_markup=map_download_keyboard()
        )
    
    @dp.message(F.text == "🗺️ LOCUS MAPS")
    async def menu_locus(message: types.Message):
        await message.answer(
            "🗺️ <b>Locus Maps</b>\n\n"
            "Выберите действие:",
            reply_markup=locus_menu_keyboard()
        )