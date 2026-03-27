import os
import re
import math
from utils.helpers import safe_edit_text
from aiogram import types, F
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from states.states import SearchStates
from keyboards.inline import back_keyboard
from config import logger
from services.coord_calculator import CoordCalculator


def register_coord_calculator_handlers(dp):
    
    @dp.message(F.text == "📐 КАЛЬКУЛЯТОР КООРДИНАТ")
    async def menu_coord_calculator(message: types.Message, state: FSMContext):
        await message.answer(
            "📐 <b>Калькулятор координат военных захоронений</b>\n\n"
            "Этот инструмент позволяет пересчитать координаты захоронений\n"
            "из системы координат топографических карт (километровая сетка)\n"
            "в географические координаты (широта/долгота) и СК-42.\n\n"
            "📌 <b>Доступные форматы ввода:</b>\n"
            "• <b>Полный формат:</b> O36|O36-141|39800|85500\n"
            "• <b>С указанием зоны:</b> O36|39800|85500\n"
            "• <b>С указанием листа:</b> O36-141|39800|85500\n"
            "• <b>Только координаты:</b> 39800 85500 (будет использована зона O36 по умолчанию)\n\n"
            "📌 <b>Примеры:</b>\n"
            "• O36|O36-141|39800|85500\n"
            "• O36|39800|85500\n"
            "• 39800 85500\n\n"
            "Введите данные:",
            parse_mode="HTML"
        )
        await state.set_state(SearchStates.waiting_for_coord_calc)
    
    @dp.message(SearchStates.waiting_for_coord_calc)
    async def process_coord_calc(message: types.Message, state: FSMContext):
        query = message.text.strip()
        if not query:
            return
        
        await state.clear()
        
        logger.info(f"📐 КАЛЬКУЛЯТОР: пользователь {message.from_user.id} ввел '{query}'")
        
        # Парсим ввод
        parts = query.replace('|', ' ').split()
        
        zone = None
        sheet = None
        x_doc = None
        y_doc = None
        
        # Определяем формат ввода
        if len(parts) == 4:
            # Формат: O36 O36-141 39800 85500
            zone = parts[0].upper()
            sheet = parts[1].upper()
            try:
                x_doc = float(parts[2])
                y_doc = float(parts[3])
            except:
                pass
        elif len(parts) == 3:
            # Формат: O36 39800 85500
            zone = parts[0].upper()
            try:
                x_doc = float(parts[1])
                y_doc = float(parts[2])
            except:
                pass
        elif len(parts) == 2:
            # Формат: 39800 85500
            try:
                x_doc = float(parts[0])
                y_doc = float(parts[1])
                # Используем зону по умолчанию O36
                zone = 'O36'
            except:
                pass
        
        # Если не удалось распарсить координаты
        if x_doc is None or y_doc is None:
            await message.answer(
                "❌ <b>Не удалось распознать ввод</b>\n\n"
                "Пожалуйста, используйте один из форматов:\n"
                "• O36|O36-141|39800|85500\n"
                "• O36|39800|85500\n"
                "• 39800 85500\n\n"
                "Координаты должны быть в метрах (целые числа)",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return
        
        # Получаем параметры листа, если указан
        offset_x = None
        offset_y = None
        if sheet:
            sheet_params = CoordCalculator.get_sheet_params(sheet)
            if sheet_params:
                zone = sheet_params.get('zone', zone)
                offset_x = sheet_params.get('offset_x')
                offset_y = sheet_params.get('offset_y')
                logger.info(f"  Найдены параметры для листа {sheet}: зона={zone}")
        
        # Выполняем расчет
        result = CoordCalculator.process_burial_coords(
            zone=zone,
            sheet=sheet or '',
            x_doc=x_doc,
            y_doc=y_doc,
            offset_x=offset_x,
            offset_y=offset_y
        )
        
        if not result['success']:
            await message.answer(
                f"❌ <b>Ошибка расчета</b>\n\n"
                f"{chr(10).join(result['errors'])}\n\n"
                f"Доступные зоны: {', '.join(CoordCalculator.get_supported_zones())}",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return
        
        # Форматируем результат
        response_text = (
            f"📐 <b>Результат пересчета координат</b>\n\n"
            f"📌 <b>Исходные данные:</b>\n"
            f"• Номенклатурная зона: <code>{result['zone']}</code>\n"
            f"• Номенклатурный лист: <code>{result['sheet'] or 'не указан'}</code>\n"
            f"• X как в документе: <code>{result['x_doc']:.0f}</code> м\n"
            f"• Y как в документе: <code>{result['y_doc']:.0f}</code> м\n\n"
            f"📌 <b>Этап 1: Полные координаты</b>\n"
            f"• X полный: <code>{result['x_full']:.0f}</code> м\n"
            f"• Y полный: <code>{result['y_full']:.0f}</code> м\n\n"
            f"📌 <b>Этап 2: Координаты СК-42 (Пулково-42)</b>\n"
            f"• X СК-42: <code>{result['x_sk42']:.0f}</code> м\n"
            f"• Y СК-42: <code>{result['y_sk42']:.0f}</code> м\n\n"
            f"📌 <b>Этап 3: Географические координаты (широта/долгота)</b>\n"
            f"• Десятичные градусы: <code>{CoordCalculator.format_coordinates(result['latitude'], result['longitude'], 'dd')}</code>\n"
            f"• Градусы/минуты/секунды: <code>{CoordCalculator.format_coordinates(result['latitude'], result['longitude'], 'dms')}</code>\n\n"
            f"💡 <b>Примечание:</b> Координаты можно использовать для поиска на картах Google Maps, Яндекс.Карты или в GPS-навигаторах."
        )
        
        await message.answer(
            response_text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔄 НОВЫЙ РАСЧЕТ", callback_data="coord_calc_new")],
                [InlineKeyboardButton(text="📋 ИНСТРУКЦИЯ", callback_data="coord_calc_help")],
                [InlineKeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ", callback_data="back_to_main")]
            ])
        )
    
    @dp.callback_query(lambda c: c.data == "coord_calc_new")
    async def coord_calc_new(callback: types.CallbackQuery, state: FSMContext):
        await menu_coord_calculator(callback.message, state)
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "coord_calc_help")
    async def coord_calc_help(callback: types.CallbackQuery):
        help_text = (
            "📖 <b>Инструкция по работе с калькулятором координат</b>\n\n"
            "🔢 <b>Что делает этот инструмент?</b>\n"
            "Пересчитывает координаты военных захоронений из системы\n"
            "километровой сетки топографических карт в географические\n"
            "координаты (широта/долгота) для использования в GPS-навигаторах.\n\n"
            "📌 <b>Как пользоваться?</b>\n"
            "1. Возьмите данные с топографической карты\n"
            "2. Определите номенклатурную зону (O35, O36, N36 и т.д.)\n"
            "3. Определите координаты в метрах от рамки карты\n"
            "4. Введите данные в одном из форматов:\n\n"
            "• <b>Полный формат:</b>\n"
            "  <code>O36|O36-141|39800|85500</code>\n\n"
            "• <b>С указанием зоны:</b>\n"
            "  <code>O36|39800|85500</code>\n\n"
            "• <b>Только координаты:</b>\n"
            "  <code>39800 85500</code> (будет использована зона O36)\n\n"
            "📍 <b>Доступные номенклатурные зоны:</b>\n"
            f"{', '.join(CoordCalculator.get_supported_zones())}\n\n"
            "🛩️ <b>После расчета вы получите:</b>\n"
            "• Полные координаты (X полный, Y полный)\n"
            "• Координаты в системе СК-42 (Пулково-42)\n"
            "• Географические координаты (широта/долгота)\n\n"
            "💡 <b>Совет:</b> Полученные географические координаты можно\n"
            "использовать для поиска на Google Maps или Яндекс.Картах."
        )
        await safe_edit_text(
            callback.message,
            help_text,
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔙 НАЗАД", callback_data="coord_calc_back")],
                [InlineKeyboardButton(text="🏠 ГЛАВНОЕ МЕНЮ", callback_data="back_to_main")]
            ])
        )
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "coord_calc_back")
    async def coord_calc_back(callback: types.CallbackQuery, state: FSMContext):
        await menu_coord_calculator(callback.message, state)
        await callback.answer()