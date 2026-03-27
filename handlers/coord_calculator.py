import os
import re
import math
import logging
from typing import Optional, Tuple, Dict, List
from aiogram import types, F
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from states.states import SearchStates
from keyboards.inline import back_keyboard
from config import logger
from services.zone_params import (
    ZoneParamsCollector,
    get_sheet_params,
    detect_zone_by_coords,
    calculate_coordinates,
    get_all_zones,
    get_all_sheets
)
from utils.helpers import safe_edit_text, safe_answer_callback


async def get_zone_collector() -> ZoneParamsCollector:
    """Возвращает глобальный экземпляр ZoneParamsCollector"""
    global zone_collector
    if zone_collector is None:
        zone_collector = ZoneParamsCollector()
    return zone_collector


def parse_coord_input(text: str) -> Tuple[Optional[float], Optional[float], Optional[str], Optional[str]]:
    """
    Парсит ввод для калькулятора координат
    Возвращает: (x, y, zone_number, sheet_name)
    """
    text = text.strip()
    original = text
    
    x = None
    y = None
    zone_number = None
    sheet_name = None
    
    # Ищем номенклатурный лист
    sheet_pattern = r'([A-Z]-\d{2}-\d{3})'
    sheet_match = re.search(sheet_pattern, text, re.IGNORECASE)
    if sheet_match:
        sheet_name = sheet_match.group(1).upper()
        text = re.sub(sheet_pattern, '', text, flags=re.IGNORECASE)
    
    # Ищем зону
    zone_pattern = r'^(\d)\s+'
    zone_match = re.match(zone_pattern, text)
    if zone_match:
        zone_number = zone_match.group(1)
        text = re.sub(zone_pattern, '', text)
    
    # Ищем числа
    numbers = re.findall(r'\d+\.?\d*', text)
    
    if len(numbers) >= 2:
        try:
            x = float(numbers[0])
            y = float(numbers[1])
        except:
            pass
    
    if x is None or y is None:
        x_match = re.search(r'x[=:]\s*(\d+\.?\d*)', original, re.IGNORECASE)
        y_match = re.search(r'y[=:]\s*(\d+\.?\d*)', original, re.IGNORECASE)
        if x_match and y_match:
            x = float(x_match.group(1))
            y = float(y_match.group(1))
    
    return x, y, zone_number, sheet_name


def format_result(result: Dict) -> str:
    """Форматирует результат для вывода пользователю"""
    if not result.get('success', False):
        errors = result.get('errors', ['Неизвестная ошибка'])
        return f"❌ <b>Ошибка расчета</b>\n\n{chr(10).join(errors)}"
    
    region_names = {
        'tver': 'Тверская область',
        'smolensk': 'Смоленская область',
        'moscow': 'Московская область',
        'unknown': 'За пределами известных регионов'
    }
    region_name = region_names.get(result.get('region', 'unknown'), 'Неизвестный регион')
    
    text = (
        f"📐 <b>Результат пересчета координат</b>\n\n"
        f"📌 <b>Исходные данные:</b>\n"
        f"• X: <code>{result['x_input']:.0f}</code> м\n"
        f"• Y: <code>{result['y_input']:.0f}</code> м\n"
    )
    
    if result.get('sheet'):
        text += f"• Номенклатурный лист: <code>{result['sheet']}</code>\n"
    
    if result.get('zone_number'):
        text += f"• Зона: <code>{result['zone_number']}</code>\n"
    
    text += f"• Тип ввода: <code>{result.get('input_type', 'неизвестно')}</code>\n\n"
    
    if result.get('x_full') and result.get('y_full'):
        text += (
            f"📌 <b>Этап 1: Полные координаты</b>\n"
            f"• X полный: <code>{result['x_full']:.0f}</code> м\n"
            f"• Y полный: <code>{result['y_full']:.0f}</code> м\n\n"
        )
    
    text += (
        f"📌 <b>Этап 2: Координаты СК-42 (Пулково-42)</b>\n"
        f"• X СК-42: <code>{result['x_sk42']:.0f}</code> м\n"
        f"• Y СК-42: <code>{result['y_sk42']:.0f}</code> м\n"
        f"• Зона: <code>{result.get('zone', 'Неизвестно')}</code>\n"
        f"• EPSG: <code>{result.get('epsg', 'Неизвестно')}</code>\n\n"
    )
    
    text += (
        f"📌 <b>Этап 3: Географические координаты (WGS-84)</b>\n"
        f"• Десятичные градусы: <code>{result['latitude']:.6f}° N, {result['longitude']:.6f}° E</code>\n"
        f"• Градусы/минуты/секунды: <code>{result['latitude_dms']}, {result['longitude_dms']}</code>\n\n"
        f"📍 <b>Местоположение:</b> {region_name}"
    )
    
    return text


async def menu_coord_calculator(message: types.Message, state: FSMContext):
    """Меню калькулятора координат"""
    sheets = get_all_sheets()
    sheets_preview = ', '.join(sheets[:5])
    
    await message.answer(
        "📐 <b>Калькулятор координат Гаусса-Крюгера</b>\n\n"
        "Введите координаты в одном из форматов:\n\n"
        "📌 <b>Примеры:</b>\n"
        "• <code>20530 90630</code> (локальные координаты, зона 6 по умолчанию)\n"
        "• <code>6 20530 90630</code> (локальные координаты с указанием зоны)\n"
        "• <code>O-36-141 48626 51905</code> (короткие координаты с листом)\n"
        "• <code>6148626 6451905</code> (зональные координаты СК-42)\n\n"
        "📌 <b>Доступные листы карты:</b>\n"
        f"<code>{sheets_preview}...</code>\n\n"
        "💡 <i>Для Смоленской области используйте листы: N-35-9, O-35-21 и др.</i>",
        parse_mode="HTML"
    )
    await state.set_state(SearchStates.waiting_for_coord_calc)


async def coord_calc_help(callback: types.CallbackQuery):
    """Помощь по калькулятору"""
    help_text = (
        "📖 <b>Помощь по калькулятору координат</b>\n\n"
        "🔢 <b>Что делает калькулятор?</b>\n"
        "Пересчитывает координаты из системы километровой сетки\n"
        "топографических карт в географические координаты\n"
        "(широта/долгота) для использования в GPS-навигаторах.\n\n"
        "📌 <b>Форматы ввода:</b>\n"
        "• <code>20530 90630</code> — локальные координаты (зона 6 по умолчанию)\n"
        "• <code>6 20530 90630</code> — локальные координаты с зоной\n"
        "• <code>O-36-141 48626 51905</code> — короткие координаты с листом\n"
        "• <code>6148626 6451905</code> — зональные координаты СК-42\n\n"
        "🗺️ <b>Поддерживаемые зоны:</b> 4, 5, 6, 7, 8, 9\n"
        "🗺️ <b>Поддерживаемые регионы:</b> Тверская, Смоленская, Московская области\n\n"
        "💡 <b>Совет:</b> Для коротких координат (последние 5 цифр) требуется указать лист карты."
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
    await safe_answer_callback(callback)


zone_collector = None


def register_coord_calculator_handlers(dp):
    
    @dp.message(F.text == "📐 КАЛЬКУЛЯТОР КООРДИНАТ")
    async def coord_calc_start(message: types.Message, state: FSMContext):
        await menu_coord_calculator(message, state)
    
    @dp.message(SearchStates.waiting_for_coord_calc)
    async def process_coord_calc(message: types.Message, state: FSMContext):
        query = message.text.strip()
        if not query:
            return
        
        await state.clear()
        
        logger.info(f"КАЛЬКУЛЯТОР: пользователь {message.from_user.id} ввел '{query}'")
        
        x, y, zone_number, sheet_name = parse_coord_input(query)
        
        if x is None or y is None:
            await message.answer(
                "❌ <b>Не удалось распознать координаты</b>\n\n"
                "Пожалуйста, используйте один из форматов:\n"
                "• <code>20530 90630</code>\n"
                "• <code>6 20530 90630</code>\n"
                "• <code>O-36-141 48626 51905</code>\n"
                "• <code>6148626 6451905</code>",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return
        
        await message.answer("⏳ Выполняется расчет...")
        
        result = calculate_coordinates(x, y, zone_number, sheet_name)
        
        result_text = format_result(result)
        
        await message.answer(
            result_text,
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
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data == "coord_calc_help")
    async def coord_calc_help_handler(callback: types.CallbackQuery):
        await coord_calc_help(callback)
    
    @dp.callback_query(lambda c: c.data == "coord_calc_back")
    async def coord_calc_back(callback: types.CallbackQuery, state: FSMContext):
        await menu_coord_calculator(callback.message, state)
        await safe_answer_callback(callback)