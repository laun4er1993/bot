import re
from aiogram import types, F
from aiogram.fsm.context import FSMContext
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from states.states import SearchStates
from keyboards.inline import back_keyboard
from config import logger
from services.zone_params import auto_detect_zone, get_zone_params, calculate_geographic, get_all_zones
from utils.helpers import safe_edit_text


def format_coords(lat: float, lon: float) -> str:
    """Форматирует координаты в десятичные градусы и DMS"""
    lat_deg = int(lat)
    lat_min = int((lat - lat_deg) * 60)
    lat_sec = ((lat - lat_deg) * 60 - lat_min) * 60
    lon_deg = int(lon)
    lon_min = int((lon - lon_deg) * 60)
    lon_sec = ((lon - lon_deg) * 60 - lon_min) * 60
    dms = f"{lat_deg}°{lat_min:02d}'{lat_sec:.1f}\" N, {lon_deg}°{lon_min:02d}'{lon_sec:.1f}\" E"
    return f"{lat:.6f}° N, {lon:.6f}° E\n{dms}"


async def menu_coord_calculator(message: types.Message, state: FSMContext):
    await message.answer(
        "📐 <b>Калькулятор координат военных захоронений</b>\n\n"
        "Введите локальные координаты в метрах (x и y) через пробел или в формате:\n"
        "• <code>x=20530 y=90630</code>\n"
        "• <code>20530 90630</code>\n"
        "• <code>Н.П-1 x=20530 y=90630</code>\n\n"
        "Также можно указать зону в начале: <code>O36 20530 90630</code>\n\n"
        "Если зона не указана, бот попытается определить её автоматически.\n\n"
        "Введите данные:",
        parse_mode="HTML"
    )
    await state.set_state(SearchStates.waiting_for_coord_calc)


async def coord_calc_help(callback: types.CallbackQuery):
    help_text = (
        "📖 <b>Инструкция по работе с калькулятором координат</b>\n\n"
        "🔢 <b>Что делает?</b>\n"
        "Пересчитывает локальные координаты (x,y) с топографических карт\n"
        "в географические координаты (широта/долгота) с учётом зоны проекции.\n\n"
        "📌 <b>Как пользоваться?</b>\n"
        "1. Введите локальные координаты в метрах (x и y).\n"
        "2. Если известна зона (например, O36), укажите её перед координатами.\n"
        "3. Если зона не указана, бот автоматически определит её.\n\n"
        "📌 <b>Примеры ввода:</b>\n"
        "• <code>20530 90630</code>\n"
        "• <code>x=20530 y=90630</code>\n"
        "• <code>O36 20530 90630</code>\n"
        "• <code>Н.П-1 x=20530 y=90630</code>\n\n"
        "📍 <b>Доступные зоны:</b>\n"
        f"{', '.join(get_all_zones())}\n\n"
        "🛩️ <b>После расчёта вы получите:</b>\n"
        "• Полные координаты\n"
        "• Координаты СК-42\n"
        "• Географические координаты (десятичные и DMS)\n"
        "• Ссылку на карту"
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


def parse_input(text: str):
    """Парсит ввод, возвращает (zone, x, y)"""
    text = text.strip()
    # Убираем возможные "Н.П-1" и т.п.
    # Ищем числа
    numbers = re.findall(r'[-+]?\d+', text)
    if len(numbers) < 2:
        return None, None, None
    # Первые два числа могут быть x и y
    x = float(numbers[0])
    y = float(numbers[1])
    # Проверяем, есть ли в тексте обозначение зоны (например, O36)
    zone_match = re.search(r'\b([A-Z]\d{2})\b', text, re.IGNORECASE)
    zone = zone_match.group(1).upper() if zone_match else None
    return zone, x, y


def register_coord_calculator_handlers(dp):
    
    @dp.message(SearchStates.waiting_for_coord_calc)
    async def process_coord_calc(message: types.Message, state: FSMContext):
        query = message.text.strip()
        if not query:
            return

        await state.clear()
        user_id = message.from_user.id
        logger.info(f"📐 КАЛЬКУЛЯТОР: пользователь {user_id} ввел '{query}'")

        zone, x, y = parse_input(query)
        if x is None or y is None:
            await message.answer(
                "❌ <b>Не удалось распознать координаты</b>\n\n"
                "Пожалуйста, введите два числа через пробел (x и y).\n"
                "Пример: <code>20530 90630</code>",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            return

        # Если зона не указана, пытаемся определить автоматически
        if not zone:
            detection = auto_detect_zone(x, y)
            if detection:
                zone, zone_params, (lat, lon) = detection
                logger.info(f"Автоопределение зоны: {zone}, координаты: {lat:.5f}, {lon:.5f}")
            else:
                await message.answer(
                    "❌ <b>Не удалось определить зону автоматически</b>\n\n"
                    "Пожалуйста, укажите зону вручную, например: <code>O36 20530 90630</code>",
                    parse_mode="HTML",
                    reply_markup=back_keyboard()
                )
                return
        else:
            zone_params = get_zone_params(zone)
            if not zone_params:
                await message.answer(
                    f"❌ <b>Зона '{zone}' не найдена в базе данных</b>\n\n"
                    f"Доступные зоны: {', '.join(get_all_zones())}",
                    parse_mode="HTML",
                    reply_markup=back_keyboard()
                )
                return
            lat, lon = calculate_geographic(x, y, zone_params)

        # Вычисляем промежуточные значения для вывода
        offset_x = zone_params['offset_x']
        offset_y = zone_params['offset_y']
        dx = zone_params['dx']
        dy = zone_params['dy']
        x_full = offset_x + x
        y_full = offset_y + y
        x_sk = x_full + dx
        y_sk = y_full + dy
        epsg = zone_params['epsg']
        cm = zone_params['central_meridian']

        response_text = (
            f"📐 <b>Результат пересчета координат</b>\n\n"
            f"📌 <b>Исходные данные:</b>\n"
            f"• Зона: <code>{zone}</code> (EPSG:{epsg}, центр. меридиан {cm}°)\n"
            f"• x = <code>{x:.0f}</code> м\n"
            f"• y = <code>{y:.0f}</code> м\n\n"
            f"📌 <b>Полные координаты:</b>\n"
            f"• X полный = <code>{x_full:.0f}</code> м\n"
            f"• Y полный = <code>{y_full:.0f}</code> м\n\n"
            f"📌 <b>Координаты СК-42 (Пулково-42):</b>\n"
            f"• X СК-42 = <code>{x_sk:.0f}</code> м\n"
            f"• Y СК-42 = <code>{y_sk:.0f}</code> м\n\n"
            f"📌 <b>Географические координаты (WGS-84):</b>\n"
            f"<code>{format_coords(lat, lon)}</code>\n\n"
            f"🗺️ <a href='https://maps.google.com/maps?q={lat},{lon}'>Открыть на Google Maps</a>\n"
            f"🗺️ <a href='https://yandex.ru/maps/?pt={lon},{lat}&z=12'>Открыть на Яндекс.Картах</a>"
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
    async def coord_calc_new_handler(callback: types.CallbackQuery, state: FSMContext):
        await menu_coord_calculator(callback.message, state)
        await callback.answer()

    @dp.callback_query(lambda c: c.data == "coord_calc_help")
    async def coord_calc_help_handler(callback: types.CallbackQuery):
        await coord_calc_help(callback)

    @dp.callback_query(lambda c: c.data == "coord_calc_back")
    async def coord_calc_back_handler(callback: types.CallbackQuery, state: FSMContext):
        await menu_coord_calculator(callback.message, state)
        await callback.answer()