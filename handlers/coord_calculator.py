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
from services.zone_params import ZoneParamsCollector, get_sheet_params, detect_zone_by_coords

# Глобальный экземпляр коллектора
zone_collector = None


async def get_zone_collector() -> ZoneParamsCollector:
    """Возвращает глобальный экземпляр ZoneParamsCollector"""
    global zone_collector
    if zone_collector is None:
        zone_collector = ZoneParamsCollector()
    return zone_collector


def parse_coord_input(text: str) -> Tuple[Optional[float], Optional[float], Optional[str]]:
    """
    Парсит ввод для калькулятора координат
    Возвращает: (x, y, sheet_name)
    Поддерживаемые форматы:
    - "x=20530 y=90630"
    - "20530 90630"
    - "Н.П-1 x=20530 y=90630"
    - "O36-141 20530 90630"
    - "20530 90630 O36-141"
    """
    text = text.strip()
    original = text
    
    x = None
    y = None
    sheet_name = None
    
    # Ищем номенклатурный лист (формат: O36-141, N36-017, P37-122 и т.д.)
    sheet_pattern = r'([A-Z]\d{2}-\d{3})'
    sheet_match = re.search(sheet_pattern, text, re.IGNORECASE)
    if sheet_match:
        sheet_name = sheet_match.group(1).upper()
        # Удаляем номенклатуру из текста для поиска координат
        text = re.sub(sheet_pattern, '', text, flags=re.IGNORECASE)
    
    # Ищем числа (координаты)
    numbers = re.findall(r'\d+\.?\d*', text)
    
    if len(numbers) >= 2:
        try:
            x = float(numbers[0])
            y = float(numbers[1])
        except:
            pass
    
    # Если нашли только одно число, ищем второе в формате x=... y=...
    if x is None or y is None:
        x_match = re.search(r'x[=:]\s*(\d+\.?\d*)', original, re.IGNORECASE)
        y_match = re.search(r'y[=:]\s*(\d+\.?\d*)', original, re.IGNORECASE)
        if x_match and y_match:
            x = float(x_match.group(1))
            y = float(y_match.group(1))
    
    return x, y, sheet_name


async def calculate_coordinates(x: float, y: float, sheet_name: str = None) -> Dict:
    """
    Вычисляет географические координаты по x и y
    Автоматически определяет зону и параметры
    """
    collector = await get_zone_collector()
    
    result = {
        'x': x,
        'y': y,
        'sheet': sheet_name,
        'zone': None,
        'zone_number': None,
        'epsg': None,
        'central_meridian': None,
        'x_full': None,
        'y_full': None,
        'x_sk42': None,
        'y_sk42': None,
        'latitude': None,
        'longitude': None,
        'latitude_dms': None,
        'longitude_dms': None,
        'success': False,
        'errors': []
    }
    
    # 1. Определяем зону по координатам (первая цифра y)
    y_str = str(int(y))
    if len(y_str) >= 7:
        zone_digit = y_str[0]
        if zone_digit in ['5', '6', '7']:
            result['zone_number'] = zone_digit
            logger.info(f"  Определена зона по y: {zone_digit}")
    
    # 2. Если указан номенклатурный лист, используем его параметры
    sheet_params = None
    if sheet_name:
        sheet_params = get_sheet_params(sheet_name)
        if sheet_params:
            result['zone_number'] = sheet_params['zone']
            logger.info(f"  Используем параметры листа {sheet_name}: зона {sheet_params['zone']}")
    
    # 3. Получаем параметры зоны
    zones = collector.ZONES_BASE
    zone_number = result['zone_number']
    
    if zone_number and zone_number in zones:
        zone = zones[zone_number]
        result['zone'] = zone['name']
        result['epsg'] = zone['epsg']
        result['central_meridian'] = zone['central_meridian']
        
        # Параметры для пересчета (из файла)
        zone_params = {
            '5': {'offset_x': 6200000, 'offset_y': 5500000, 'dx': 807, 'dy': 83},
            '6': {'offset_x': 6200000, 'offset_y': 6500000, 'dx': 802, 'dy': 34},
            '7': {'offset_x': 6210000, 'offset_y': 6650000, 'dx': 801, 'dy': -13},
        }
        
        params = zone_params.get(zone_number, zone_params['6'])
        
        # 4. Вычисляем полные координаты
        x_full = params['offset_x'] + x
        y_full = params['offset_y'] + y
        result['x_full'] = x_full
        result['y_full'] = y_full
        
        # 5. Вычисляем координаты СК-42
        x_sk42 = x_full + params['dx']
        y_sk42 = y_full + params['dy']
        result['x_sk42'] = x_sk42
        result['y_sk42'] = y_sk42
        
        # 6. Пересчет в географические координаты
        # 1° широты = 111,000 м
        # 1° долготы на широте 56° = 85,000 м
        lat = x_sk42 / 111000
        lon = zone['central_meridian'] + (y_sk42 - (int(zone_number) * 1000000)) / 85000
        
        result['latitude'] = lat
        result['longitude'] = lon
        
        # Форматируем в градусы/минуты/секунды
        result['latitude_dms'] = decimal_to_dms(lat, 'N')
        result['longitude_dms'] = decimal_to_dms(lon, 'E')
        
        result['success'] = True
        
        # Проверяем, попадает ли точка в Тверскую область
        if 55.5 <= lat <= 58.5 and 30 <= lon <= 38:
            logger.info(f"  ✅ Точка в Тверской области: {lat:.4f}, {lon:.4f}")
        else:
            logger.info(f"  ⚠️ Точка вне Тверской области: {lat:.4f}, {lon:.4f}")
        
    else:
        result['errors'].append(f"Не удалось определить зону для y={y}")
    
    return result


def decimal_to_dms(deg: float, direction: str) -> str:
    """Преобразует десятичные градусы в формат градусы/минуты/секунды"""
    if deg < 0:
        deg = -deg
        direction = 'S' if direction == 'N' else 'W'
    
    degrees = int(deg)
    minutes_float = (deg - degrees) * 60
    minutes = int(minutes_float)
    seconds = (minutes_float - minutes) * 60
    
    return f"{degrees}°{minutes:02d}'{seconds:.1f}\" {direction}"


def format_result(result: Dict) -> str:
    """Форматирует результат для вывода пользователю"""
    if not result['success']:
        return f"❌ <b>Ошибка расчета</b>\n\n{chr(10).join(result['errors'])}"
    
    text = (
        f"📐 <b>Результат пересчета координат</b>\n\n"
        f"📌 <b>Исходные данные:</b>\n"
        f"• X: <code>{result['x']:.0f}</code> м\n"
        f"• Y: <code>{result['y']:.0f}</code> м\n"
    )
    
    if result['sheet']:
        text += f"• Номенклатурный лист: <code>{result['sheet']}</code>\n"
    
    text += (
        f"• Зона: <code>{result['zone']}</code>\n"
        f"• EPSG: <code>{result['epsg']}</code>\n\n"
        f"📌 <b>Этап 1: Полные координаты</b>\n"
        f"• X полный: <code>{result['x_full']:.0f}</code> м\n"
        f"• Y полный: <code>{result['y_full']:.0f}</code> м\n\n"
        f"📌 <b>Этап 2: Координаты СК-42 (Пулково-42)</b>\n"
        f"• X СК-42: <code>{result['x_sk42']:.0f}</code> м\n"
        f"• Y СК-42: <code>{result['y_sk42']:.0f}</code> м\n\n"
        f"📌 <b>Этап 3: Географические координаты (WGS-84)</b>\n"
        f"• Десятичные градусы: <code>{result['latitude']:.6f}° N, {result['longitude']:.6f}° E</code>\n"
        f"• Градусы/минуты/секунды: <code>{result['latitude_dms']}, {result['longitude_dms']}</code>\n\n"
    )
    
    # Проверка на Тверскую область
    if 55.5 <= result['latitude'] <= 58.5 and 30 <= result['longitude'] <= 38:
        text += "📍 <b>Точка находится в Тверской области</b>"
    else:
        text += "⚠️ <b>Точка находится за пределами Тверской области</b>"
    
    return text


async def menu_coord_calculator(message: types.Message, state: FSMContext):
    """Меню калькулятора координат"""
    await message.answer(
        "📐 <b>Калькулятор координат Гаусса-Крюгера</b>\n\n"
        "Введите координаты в одном из форматов:\n\n"
        "📌 <b>Примеры:</b>\n"
        "• <code>20530 90630</code>\n"
        "• <code>x=20530 y=90630</code>\n"
        "• <code>Н.П-1 x=20530 y=90630</code>\n"
        "• <code>O36-141 20530 90630</code>\n\n"
        "💡 <i>Если известен номенклатурный лист, укажите его для более точного расчета</i>",
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
        "📌 <b>Как вводить координаты?</b>\n"
        "• <code>20530 90630</code> — два числа через пробел\n"
        "• <code>x=20530 y=90630</code> — с указанием осей\n"
        "• <code>O36-141 20530 90630</code> — с номенклатурным листом\n\n"
        "🗺️ <b>Поддерживаемые зоны:</b>\n"
        "• Зона 5 (EPSG:28405) — западная часть области\n"
        "• Зона 6 (EPSG:28406) — центральная часть\n"
        "• Зона 7 (EPSG:28407) — восточная часть\n\n"
        "💡 <b>Совет:</b> Если точка находится в Тверской области,\n"
        "калькулятор автоматически определит правильную зону."
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
        
        logger.info(f"📐 КАЛЬКУЛЯТОР: пользователь {message.from_user.id} ввел '{query}'")
        
        # Парсим ввод
        x, y, sheet_name = parse_coord_input(query)
        
        if x is None or y is None:
            await message.answer(
                "❌ <b>Не удалось распознать координаты</b>\n\n"
                "Пожалуйста, введите данные в одном из форматов:\n"
                "• <code>20530 90630</code>\n"
                "• <code>x=20530 y=90630</code>\n"
                "• <code>O36-141 20530 90630</code>",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return
        
        # Выполняем расчет
        await message.answer("⏳ Выполняется расчет...")
        
        result = await calculate_coordinates(x, y, sheet_name)
        
        # Форматируем и отправляем результат
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
        await callback.answer()
    
    @dp.callback_query(lambda c: c.data == "coord_calc_help")
    async def coord_calc_help_handler(callback: types.CallbackQuery):
        await coord_calc_help(callback)
    
    @dp.callback_query(lambda c: c.data == "coord_calc_back")
    async def coord_calc_back(callback: types.CallbackQuery, state: FSMContext):
        await menu_coord_calculator(callback.message, state)
        await callback.answer()