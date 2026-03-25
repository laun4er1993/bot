from aiogram import types, F
from aiogram.filters import Command

from keyboards.main import get_main_keyboard, back_keyboard
from keyboards.inline import locus_menu_keyboard, map_download_keyboard, back_to_locus_keyboard


async def cmd_start(message: types.Message):
    welcome_text = (
        f"✈️ <b>Добро пожаловать, {message.from_user.full_name}!</b>\n\n"
        f"🛩️ <b>Бот для поиска аэрофотоснимков Тверской области</b>\n\n"
        f"📌 <b>Основные возможности:</b>\n"
        f"• 🔍 <b>ПОИСК СНИМКОВ</b> — найдите снимки по названию деревни\n"
        f"• 📋 <b>ВСЕ ДЕРЕВНИ</b> — полный список населенных пунктов\n"
        f"• 📖 <b>ПОМОЩЬ</b> — подробная инструкция\n"
        f"• 🗺️ <b>КАРТЫ</b> — карты и приложения\n"
        f"• 🔄 <b>KML ОБРАБОТКА</b> — анализ каталогов снимков\n"
        f"• ⚙️ <b>НАСТРОЙКИ</b> — управление данными\n\n"
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
    
    @dp.message(F.text == "📖 ПОМОЩЬ")
    async def menu_instruction(message: types.Message):
        instruction_text = (
            "📖 <b>ИНСТРУКЦИЯ ПО ИСПОЛЬЗОВАНИЮ БОТА</b>\n\n"
            "🔍 <b>ПОИСК СНИМКОВ</b>\n"
            "• Нажмите «🔍 ПОИСК СНИМКОВ»\n"
            "• Введите название деревни (можно часть)\n"
            "• Нажмите на номер снимка для просмотра и скачивания\n\n"
            "🗺️ <b>КАРТЫ И ПРИЛОЖЕНИЯ</b>\n"
            "• Нажмите «🗺️ КАРТЫ»\n"
            "• Скачайте приложение Locus Maps\n"
            "• Загрузите карту Ржевского района\n"
            "• Скачайте MBTILES файл снимка и откройте в приложении\n\n"
            "🔄 <b>KML ОБРАБОТКА</b>\n"
            "• Загрузите KML файл с каталогом снимков\n"
            "• Бот найдет населенные пункты в каждом кадре\n"
            "• Создаст подробный TXT отчет со статистикой\n"
            "• Позволит создать каталог АФС для поиска\n\n"
            "⚙️ <b>НАСТРОЙКИ</b>\n"
            "• <b>Управление KML</b> — загрузка и каталогизация KML файлов\n"
            "• <b>Населенные пункты</b> — добавление, удаление, загрузка из интернета\n"
            "• <b>Каталог АФС</b> — просмотр и управление каталогом снимков\n\n"
            "🛩️ <b>ПРИЯТНОГО ИСПОЛЬЗОВАНИЯ!</b>"
        )
        await message.answer(instruction_text, parse_mode="HTML", reply_markup=back_keyboard())
    
    @dp.message(F.text == "🗺️ КАРТЫ")
    async def menu_maps(message: types.Message):
        text = (
            "🗺️ <b>КАРТЫ И ПРИЛОЖЕНИЯ</b>\n\n"
            "Выберите необходимое действие:"
        )
        await message.answer(text, parse_mode="HTML", reply_markup=locus_menu_keyboard())
    
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