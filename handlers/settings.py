# handlers/settings.py
import os
import tempfile
import asyncio
from aiogram import types, F
from aiogram.fsm.context import FSMContext
from aiogram.types import FSInputFile

from states.states import SearchStates
from keyboards.inline import (
    get_settings_keyboard, get_district_keyboard, get_more_districts_keyboard,
    get_delete_district_keyboard, get_confirm_delete_district_keyboard,
    get_confirm_clear_all_keyboard, get_merge_keyboard, back_keyboard
)
from utils.helpers import safe_edit_text, safe_answer_callback
from config import logger, TEMP_DIR
from api_sources import APISourceManager, AVAILABLE_DISTRICTS


def register_settings_handlers(dp, village_db):
    
    @dp.message(F.text == "⚙️ НАСТРОЙКИ")
    async def menu_settings(message: types.Message):
        stats = village_db.get_stats()
        text = (
            f"⚙️ <b>Настройки базы населенных пунктов</b>\n\n"
            f"📊 <b>Статистика каталога:</b>\n"
            f"• Всего записей: {stats['total']}\n"
            f"• С координатами: {stats['with_coords']}\n"
            f"• Без координат: {stats['total'] - stats['with_coords']}\n"
        )
        if stats['last_update']:
            text += f"• Обновлено: {stats['last_update']}\n"
        if stats['source_file']:
            text += f"• Источник: {stats['source_file']}\n"
        
        districts = village_db.get_districts()
        if districts:
            text += f"\n📍 <b>Районы в каталоге:</b>\n"
            for d in districts:
                count = len(village_db.get_villages_by_district(d))
                text += f"• {d} район: {count} НП\n"
        
        await message.answer(text, parse_mode="HTML", reply_markup=get_settings_keyboard())
    
    @dp.callback_query(lambda c: c.data == "add_village_manual")
    async def add_village_manual_start(callback: types.CallbackQuery, state: FSMContext):
        await safe_edit_text(
            callback.message,
            "📝 <b>Добавление населенного пункта вручную</b>\n\n"
            "Введите данные в формате:\n"
            "<code>название,тип,широта,долгота,район</code>\n\n"
            "📌 <b>Пример:</b>\n"
            "<code>Горбово,деревня,56.2345,34.1234,Ржевский</code>\n\n"
            "💡 <b>Примечания:</b>\n"
            "• Тип может быть: деревня, село, посёлок, хутор, станция, урочище\n"
            "• Координаты могут быть пустыми: <code>Горбово,деревня,,,Ржевский</code>\n"
            "• Если НП уже существует, он будет пропущен\n\n"
            "Введите данные одной строкой:",
            parse_mode="HTML"
        )
        await state.set_state(SearchStates.waiting_for_add_village)
        await safe_answer_callback(callback)
    
    @dp.message(SearchStates.waiting_for_add_village)
    async def add_village_parse(message: types.Message, state: FSMContext):
        data = message.text.strip()
        parts = data.split(',')
        
        if len(parts) < 5:
            await message.answer(
                "❌ Неверный формат. Ожидается:\n"
                "<code>название,тип,широта,долгота,район</code>\n\n"
                "Пример: <code>Горбово,деревня,56.2345,34.1234,Ржевский</code>",
                parse_mode="HTML"
            )
            return
        
        name = parts[0].strip()
        village_type = parts[1].strip() if parts[1].strip() else "деревня"
        lat = parts[2].strip() if len(parts) > 2 else ""
        lon = parts[3].strip() if len(parts) > 3 else ""
        district = parts[4].strip() if len(parts) > 4 else ""
        
        if not name:
            await message.answer("❌ Название не может быть пустым")
            return
        
        valid_types = ['деревня', 'село', 'посёлок', 'хутор', 'станция', 'урочище']
        if village_type not in valid_types:
            await message.answer(f"❌ Неверный тип. Доступные: {', '.join(valid_types)}")
            return
        
        district_normalized = district.replace(" район", "").strip()
        if district_normalized not in AVAILABLE_DISTRICTS:
            await message.answer(
                f"❌ Район '{district}' не найден.\n\n"
                f"Доступные районы: {', '.join(AVAILABLE_DISTRICTS[:10])}...\n"
                f"Введите полное название района (например: Ржевский)"
            )
            return
        
        if lat and lon:
            try:
                float(lat)
                float(lon)
            except ValueError:
                await message.answer("❌ Неверный формат координат. Используйте числа с точкой")
                return
        
        village = {
            "name": name,
            "type": village_type,
            "lat": lat,
            "lon": lon,
            "district": district_normalized
        }
        
        success, msg = village_db.add_village(village)
        
        if success:
            await message.answer(
                f"✅ {msg}\n\n"
                f"📊 <b>Данные добавленного НП:</b>\n"
                f"• Название: {village['name']}\n"
                f"• Тип: {village['type']}\n"
                f"• Координаты: {village['lat'] if village['lat'] else 'не указаны'} {village['lon'] if village['lon'] else ''}\n"
                f"• Район: {village['district']}\n\n"
                f"Теперь этот НП доступен для поиска снимков!",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
        else:
            await message.answer(f"❌ {msg}", reply_markup=back_keyboard())
        
        await state.clear()
    
    @dp.callback_query(lambda c: c.data == "load_catalog_txt")
    async def load_catalog_txt_start(callback: types.CallbackQuery, state: FSMContext):
        await safe_edit_text(
            callback.message,
            "📤 <b>Загрузка каталога населенных пунктов</b>\n\n"
            "Отправьте TXT файл в формате:\n"
            "<code>Название Тип Широта Долгота Район</code>\n\n"
            "📌 <b>Пример строки:</b>\n"
            "<code>Горбово деревня 56.2345 34.1234 Ржевский</code>\n"
            "<code>Полунино деревня - - Ржевский</code>\n\n"
            "⚠️ <b>Важно:</b>\n"
            "• Если НП уже существует в каталоге, он будет пропущен\n"
            "• Поля lat, lon могут быть пустыми (используйте -)\n"
            "• Добавятся только новые НП\n\n"
            "Отправьте TXT файл:",
            parse_mode="HTML"
        )
        await state.set_state(SearchStates.waiting_for_txt_upload)
        await safe_answer_callback(callback)
    
    @dp.message(SearchStates.waiting_for_txt_upload, F.document)
    async def process_txt_upload(message: types.Message, state: FSMContext):
        if not message.document.file_name.endswith('.txt'):
            await message.answer("❌ Отправьте TXT файл (с расширением .txt)")
            await state.clear()
            return
        
        await message.answer("⏳ Загрузка и обработка файла...")
        
        try:
            file_info = await message.bot.get_file(message.document.file_id)
            with tempfile.NamedTemporaryFile(delete=False, suffix='.txt') as tmp:
                await message.bot.download_file(file_info.file_path, tmp)
                tmp_path = tmp.name
            
            with open(tmp_path, 'r', encoding='utf-8') as f:
                content = f.read()
            os.unlink(tmp_path)
            
            lines = content.strip().split('\n')
            if not lines:
                await message.answer("❌ Файл пуст")
                await state.clear()
                return
            
            if not lines[0].startswith('Название'):
                await message.answer(
                    "❌ Неверный формат файла.\n\n"
                    "Ожидается заголовок: <code>Название Тип Широта Долгота Район</code>\n\n"
                    "Первая строка должна начинаться с 'Название'",
                    parse_mode="HTML"
                )
                await state.clear()
                return
            
            villages = []
            for line in lines[1:]:
                line = line.strip()
                if not line:
                    continue
                
                parts = line.split()
                if len(parts) >= 5:
                    name = parts[0]
                    village_type = parts[1]
                    lat = parts[2] if parts[2] != '-' else ''
                    lon = parts[3] if parts[3] != '-' else ''
                    district = parts[4]
                    
                    if len(parts) > 5:
                        name = ' '.join(parts[:-4])
                        village_type = parts[-4]
                        lat = parts[-3] if parts[-3] != '-' else ''
                        lon = parts[-2] if parts[-2] != '-' else ''
                        district = parts[-1]
                    
                    district_normalized = district.replace(" район", "").strip()
                    if district_normalized in AVAILABLE_DISTRICTS:
                        villages.append({
                            'name': name,
                            'type': village_type,
                            'lat': lat,
                            'lon': lon,
                            'district': district_normalized
                        })
            
            if not villages:
                await message.answer("❌ В файле не найдено корректных записей для добавления")
                await state.clear()
                return
            
            stats = village_db.add_villages_batch(villages)
            
            await message.answer(
                f"✅ <b>Обработка TXT завершена!</b>\n\n"
                f"📊 <b>Результат:</b>\n"
                f"• Добавлено новых записей: {stats['added']}\n"
                f"• Пропущено дубликатов: {stats['duplicates']}\n"
                f"• Ошибок: {stats['errors']}\n\n"
                f"📊 <b>Текущее состояние каталога:</b>\n"
                f"• Всего записей: {village_db.stats['total']}\n"
                f"• С координатами: {village_db.stats['with_coords']}",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
            
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            await message.answer(f"❌ Ошибка при загрузке TXT:\n{str(e)}")
        
        await state.clear()
    
    @dp.message(SearchStates.waiting_for_txt_upload)
    async def process_txt_invalid(message: types.Message, state: FSMContext):
        await message.answer("❌ Отправьте TXT файл с расширением .txt")
        await state.clear()
    
    @dp.callback_query(lambda c: c.data == "download_from_web_start")
    async def download_from_web_start(callback: types.CallbackQuery, state: FSMContext):
        await safe_edit_text(
            callback.message,
            "🌐 <b>Загрузка данных из интернета</b>\n\n"
            "Бот выполнит поиск на dic.academic.ru и Wikipedia.\n"
            "Это может занять 10-15 минут.\n\n"
            "<b>Выберите район:</b>",
            parse_mode="HTML",
            reply_markup=get_district_keyboard()
        )
        await state.set_state(SearchStates.waiting_for_district_select)
        await safe_answer_callback(callback)
    
    @dp.callback_query(lambda c: c.data.startswith("select_district_"))
    async def process_district_select(callback: types.CallbackQuery, state: FSMContext):
        district = callback.data.replace("select_district_", "")
        
        await safe_edit_text(
            callback.message,
            f"⏳ <b>Загрузка данных для {district} района...</b>\n\n"
            f"🔍 Выполняется поиск на dic.academic.ru и Wikipedia.\n"
            f"⏱️ Это может занять 10-15 минут.\n"
            f"<i>Пожалуйста, подождите...</i>",
            parse_mode="HTML"
        )
        await safe_answer_callback(callback, f"⏳ Начинаю загрузку для {district} района...")
        
        try:
            api_manager = APISourceManager()
            villages = await asyncio.wait_for(
                api_manager.fetch_district_data(district),
                timeout=1500.0
            )
            await api_manager.close_session()
            
            if not villages:
                await safe_edit_text(
                    callback.message,
                    f"❌ <b>Не удалось загрузить данные для {district} района</b>\n\n"
                    f"Возможные причины:\n"
                    f"• Нет данных в источниках\n"
                    f"• Проблемы с подключением\n"
                    f"• Превышено время ожидания\n\n"
                    f"Попробуйте другой район или загрузите TXT вручную.",
                    parse_mode="HTML",
                    reply_markup=back_keyboard()
                )
                await safe_answer_callback(callback)
                return
            
            timestamp = time.strftime('%Y%m%d_%H%M%S')
            os.makedirs(TEMP_DIR, exist_ok=True)
            temp_txt = os.path.join(TEMP_DIR, f"{district}_{timestamp}.txt")
            
            with open(temp_txt, 'w', encoding='utf-8') as f:
                f.write("Название Тип Широта Долгота Район\n")
                for v in villages:
                    lat = v.get('lat', '') if v.get('lat') else '-'
                    lon = v.get('lon', '') if v.get('lon') else '-'
                    f.write(f"{v['name']} {v['type']} {lat} {lon} {v['district']}\n")
            
            await state.update_data(temp_txt=temp_txt, villages=villages)
            
            with_coords = sum(1 for v in villages if v.get('lat') and v.get('lon'))
            
            await safe_edit_text(
                callback.message,
                f"✅ <b>Данные для {district} района загружены!</b>\n\n"
                f"📊 <b>Статистика:</b>\n"
                f"• Всего населенных пунктов: {len(villages)}\n"
                f"• С координатами: {with_coords}\n"
                f"• Без координат: {len(villages) - with_coords}\n\n"
                f"<b>Что сделать с этими данными?</b>",
                parse_mode="HTML",
                reply_markup=get_merge_keyboard(district)
            )
            
        except asyncio.TimeoutError:
            await safe_edit_text(
                callback.message,
                "❌ <b>Превышено время ожидания</b>\n\n"
                "Загрузка данных заняла слишком много времени.\n"
                "Попробуйте позже или выберите другой район.",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
        except Exception as e:
            logger.error(f"Ошибка: {e}")
            await safe_edit_text(
                callback.message,
                f"❌ <b>Ошибка при загрузке данных</b>\n\n"
                f"{str(e)}",
                parse_mode="HTML",
                reply_markup=back_keyboard()
            )
        finally:
            if 'api_manager' in locals():
                await api_manager.close_session()