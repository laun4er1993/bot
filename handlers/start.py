# handlers/start.py
from aiogram import types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

from keyboards.main import get_main_keyboard
from keyboards.inline import back_keyboard


def register_start_handlers(dp):
    
    @dp.message(Command("start"))
    async def cmd_start(message: types.Message):
        welcome_text = (
            f"✈️ <b>Добро пожаловать, {message.from_user.full_name}!</b>\n\n"
            f"<b>🛩️ Бот для поиска аэрофотоснимков</b>\n\n"
            f"📌 <b>Основные возможности:</b>\n"
            f"• 🔍 <b>ПОИСК</b> — найдите снимки по названию деревни\n"
            f"• 📋 <b>СПИСОК ДЕРЕВЕНЬ</b> — все доступные населенные пункты\n"
            f"• 📖 <b>ИНСТРУКЦИЯ</b> — подробная помощь по боту\n"
            f"• 🗺️ <b>КАРТА РЖЕВ</b> — скачать карту для Locus Maps\n"
            f"• 🗺️ <b>LOCUS MAPS</b> — инструкция и скачивание приложения\n"
            f"• 🔄 <b>ОБРАБОТАТЬ KML</b> — загрузить каталог снимков, найти НП и создать отчет\n"
            f"• ⚙️ <b>ЗАГРУЗКА НП</b> — управление каталогом населенных пунктов\n"
            f"• ⚙️ <b>НАСТРОЙКА КАТАЛОГА</b> — управление каталогом аэрофотоснимков (АФС)\n\n"
            f"👇 <b>Выберите действие:</b>"
        )
        await message.answer(welcome_text, parse_mode="HTML", reply_markup=get_main_keyboard())
    
    @dp.message(F.text == "/start")
    async def cmd_start_button(message: types.Message):
        await cmd_start(message)
    
    @dp.message(F.text == "📖 ИНСТРУКЦИЯ")
    async def menu_instruction(message: types.Message):
        instruction_text = (
            "📖 <b>ИНСТРУКЦИЯ ПО ИСПОЛЬЗОВАНИЮ БОТА</b>\n\n"
            "🔍 <b>ПОИСК СНИМКОВ</b>\n"
            "• Нажмите «🔍 ПОИСК»\n"
            "• Введите название деревни (можно часть названия)\n"
            "• Нажмите на номер снимка для просмотра описания и скачивания\n\n"
            "🗺️ <b>LOCUS MAPS</b>\n"
            "• Скачайте приложение из меню «🗺️ LOCUS MAPS»\n"
            "• Загрузите карту Ржевского района\n"
            "• Скачайте MBTILES файл снимка\n"
            "• Откройте MBTILES файл в приложении для просмотра\n\n"
            "🔄 <b>ОБРАБОТКА KML</b>\n"
            "• Загрузите KML файл с каталогом снимков\n"
            "• Бот найдет населенные пункты в каждом кадре\n"
            "• Создаст подробный TXT отчет со статистикой\n"
            "• В отчете будут полные описания снимков и список НП по каждому кадру\n\n"
            "⚙️ <b>ЗАГРУЗКА НП</b>\n"
            "• Добавление НП вручную\n"
            "• Загрузка каталога населенных пунктов из TXT файла\n"
            "• Автоматическая загрузка из интернета (dic.academic.ru + Wikipedia)\n"
            "• Удаление районов или очистка всего каталога\n"
            "• Просмотр статистики каталога НП\n"
            "• Экспорт каталога НП в TXT\n\n"
            "⚙️ <b>НАСТРОЙКА КАТАЛОГА</b>\n"
            "• Просмотр и управление каталогом аэрофотоснимков (АФС)\n"
            "• Создание каталога из обработанных KML файлов\n"
            "• Сравнение каталогов\n"
            "• Загрузка общего каталога АФС\n"
            "• Экспорт каталога АФС в TXT\n\n"
            "🛩️ <b>ПРИЯТНОГО ИСПОЛЬЗОВАНИЯ!</b>"
        )
        await message.answer(instruction_text, parse_mode="HTML", reply_markup=back_keyboard())
    
    @dp.message(F.text == "🗺️ КАРТА РЖЕВ")
    async def menu_map(message: types.Message):
        from keyboards.inline import map_download_keyboard
        await message.answer(
            "🗺️ <b>Карта Ржевского района для Locus Maps</b>\n\n"
            "Нажмите кнопку для скачивания карты:",
            parse_mode="HTML",
            reply_markup=map_download_keyboard()
        )
    
    @dp.message(F.text == "🗺️ LOCUS MAPS")
    async def menu_locus(message: types.Message):
        from keyboards.inline import locus_menu_keyboard
        await message.answer(
            "🗺️ <b>Locus Maps</b>\n\n"
            "Выберите действие:",
            reply_markup=locus_menu_keyboard()
        )