import asyncio
import logging
import os
import sys
from typing import Optional, Dict, List, Tuple

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import (
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery
)

# Токен из переменных окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    logging.critical("❌ ОШИБКА: BOT_TOKEN не найден!")
    sys.exit(1)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ========== КЛАСС ДЛЯ РАБОТЫ С ДАННЫМИ ==========

class PhotosDatabase:
    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.multi_keys_file = os.path.join(data_dir, "multi_keys.txt")
        self.details_file = os.path.join(data_dir, "details.txt")
        
        # Данные из multi_keys.txt
        self.locations: List[Dict] = []  # список всех записей
        self.village_to_locations: Dict[str, List[int]] = {}  # деревня -> индексы записей
        self.photo_to_locations: Dict[str, List[int]] = {}  # снимок -> индексы записей
        
        # Данные из details.txt
        self.photo_details: Dict[str, str] = {}  # снимок -> детальное описание
        
        # История пользователей
        self.user_last_results: Dict[int, List[Dict]] = {}  # последние результаты поиска
        self.user_last_query: Dict[int, str] = {}
        
        self.load_all_data()
    
    def load_all_data(self) -> None:
        os.makedirs(self.data_dir, exist_ok=True)
        self.load_multi_keys()
        self.load_details()
        self.log_stats()
    
    def load_multi_keys(self) -> None:
        """Загружает данные из multi_keys.txt"""
        try:
            if os.path.exists(self.multi_keys_file):
                with open(self.multi_keys_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                    logger.info(f"📄 Читаем файл {self.multi_keys_file}, всего строк: {len(lines)}")
                    
                    for idx, line in enumerate(lines):
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue
                        
                        # Формат: категория|список_деревень|снимок1|снимок2|...
                        parts = line.split('|')
                        
                        if len(parts) >= 3:
                            category = parts[0].strip()           # Ржев
                            villages_str = parts[1].strip()       # список деревень через запятую
                            photos = [p.strip() for p in parts[2:] if p.strip()]  # номера снимков
                            
                            # Разбиваем деревни
                            villages = [v.strip() for v in villages_str.split(',') if v.strip()]
                            
                            # Сохраняем запись
                            record = {
                                'id': idx,
                                'category': category,
                                'villages_str': villages_str,
                                'villages': villages,
                                'photos': photos
                            }
                            self.locations.append(record)
                            
                            # Индексируем по деревням
                            for village in villages:
                                village_lower = village.lower()
                                if village_lower not in self.village_to_locations:
                                    self.village_to_locations[village_lower] = []
                                self.village_to_locations[village_lower].append(idx)
                            
                            # Индексируем по снимкам
                            for photo in photos:
                                photo_lower = photo.lower()
                                if photo_lower not in self.photo_to_locations:
                                    self.photo_to_locations[photo_lower] = []
                                self.photo_to_locations[photo_lower].append(idx)
                            
                            logger.info(f"  Строка {idx}: {len(villages)} деревень, {len(photos)} снимков")
                
                logger.info(f"✅ Загружено {len(self.locations)} записей из multi_keys.txt")
            else:
                logger.warning(f"⚠️ Файл {self.multi_keys_file} не найден")
                self._create_example_multi_keys()
                
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки multi_keys: {e}")
    
    def _create_example_multi_keys(self) -> None:
        """Создает пример файла multi_keys.txt"""
        example = '''# Формат: КАТЕГОРИЯ|СПИСОК_ДЕРЕВЕНЬ|СНИМОК1|СНИМОК2|...
Ржев|Горбово,Нов.Ивановское,Ковынево,Скворцово,Дураково,Добрая,Мурылево,Ханино,Горы Казеки|N56E34-237-044|N56E34-237-045
Ржев|Старшевицы,Бельково,Харино,Дешевка|N56E34-237-053
Ржев|Полунино,Галахово,Тимофеево,Ердихино,Федорково|N56E34-237-048
'''
        with open(self.multi_keys_file, 'w', encoding='utf-8') as f:
            f.write(example)
    
    def load_details(self) -> None:
        """Загружает детальную информацию о снимках из details.txt"""
        try:
            if os.path.exists(self.details_file):
                with open(self.details_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith('#'):
                            continue
                        
                        # Формат: НОМЕР_СНИМКА===ПОДРОБНОЕ_ОПИСАНИЕ
                        if '===' in line:
                            photo_num, details = line.split('===', 1)
                            photo_num = photo_num.strip()
                            details = details.strip()
                            
                            if photo_num and details:
                                self.photo_details[photo_num] = details
                
                logger.info(f"✅ Загружено {len(self.photo_details)} описаний снимков из details.txt")
            else:
                logger.warning(f"⚠️ Файл {self.details_file} не найден")
                self._create_example_details()
                
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки details: {e}")
    
    def _create_example_details(self) -> None:
        """Создает пример файла с деталями снимков"""
        example = '''# Формат: НОМЕР_СНИМКА===ПОДРОБНОЕ_ОПИСАНИЕ

N56E34-237-044===📸 **Снимок N56E34-237-044**
📅 Дата съемки: 1943-07-15
🛩️ Высота: 4500 м
🎞️ Масштаб: 1:15000
📋 Описание: Район деревень Горбово, Нов.Ивановское, Ковынево.
'''
        with open(self.details_file, 'w', encoding='utf-8') as f:
            f.write(example)
    
    def search_by_village(self, query: str) -> List[Dict]:
        """
        Ищет записи по названию деревни
        Возвращает список записей, где встречается искомое слово
        """
        if not query:
            return []
        
        query_lower = query.lower().strip()
        found_indices = set()
        
        logger.info(f"🔍 Поиск деревни: '{query}'")
        
        # Точное совпадение с названием деревни
        if query_lower in self.village_to_locations:
            for idx in self.village_to_locations[query_lower]:
                found_indices.add(idx)
                logger.info(f"  ✓ Точное совпадение: '{query}' в записи {idx}")
        
        # Частичное совпадение (если слово содержится в названии деревни)
        for village, indices in self.village_to_locations.items():
            if query_lower in village and len(query_lower) > 2:
                for idx in indices:
                    if idx not in found_indices:
                        found_indices.add(idx)
                        logger.info(f"  ✓ Частичное совпадение: '{query}' в '{village}' (запись {idx})")
        
        # Возвращаем записи в порядке возрастания индекса
        results = [self.locations[idx] for idx in sorted(found_indices)]
        logger.info(f"✅ Найдено записей: {len(results)}")
        
        return results
    
    def search_by_photo(self, query: str) -> List[Dict]:
        """
        Ищет записи по номеру снимка
        """
        if not query:
            return []
        
        query_lower = query.lower().strip()
        
        if query_lower in self.photo_to_locations:
            indices = self.photo_to_locations[query_lower]
            return [self.locations[idx] for idx in indices]
        
        return []
    
    def get_photo_details(self, photo_num: str) -> Optional[str]:
        """Возвращает детальное описание снимка"""
        return self.photo_details.get(photo_num)
    
    def set_last_results(self, user_id: int, results: List[Dict]):
        self.user_last_results[user_id] = results
    
    def get_last_results(self, user_id: int) -> Optional[List[Dict]]:
        return self.user_last_results.get(user_id)
    
    def set_last_query(self, user_id: int, query: str):
        self.user_last_query[user_id] = query
    
    def get_last_query(self, user_id: int) -> Optional[str]:
        return self.user_last_query.get(user_id)
    
    def log_stats(self):
        logger.info(f"📊 Статистика:")
        logger.info(f"   • Записей в multi_keys.txt: {len(self.locations)}")
        logger.info(f"   • Уникальных деревень: {len(self.village_to_locations)}")
        logger.info(f"   • Уникальных снимков: {len(self.photo_to_locations)}")
        logger.info(f"   • Описаний в details.txt: {len(self.photo_details)}")

db = PhotosDatabase()

# ========== КЛАВИАТУРЫ ==========

def back_to_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 В начало", callback_data="back_to_main")]
    ])

def back_to_results_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад к результатам", callback_data="back_to_results")]
    ])

def results_keyboard(results: List[Dict]) -> InlineKeyboardMarkup:
    """Клавиатура для выбора результата поиска"""
    keyboard = []
    
    for i, record in enumerate(results):
        # Показываем первые несколько деревень
        villages = record['villages'][:3]
        short_desc = ', '.join(villages)
        if len(record['villages']) > 3:
            short_desc += f" и ещё {len(record['villages'])-3}"
        
        button_text = f"📌 Вариант {i+1}: {short_desc} ({len(record['photos'])} снимков)"
        keyboard.append([InlineKeyboardButton(
            text=button_text,
            callback_data=f"record_{record['id']}"
        )])
    
    keyboard.append([InlineKeyboardButton(text="🔙 Новый поиск", callback_data="back_to_search")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

def photos_keyboard(photos: List[str], record_id: int) -> InlineKeyboardMarkup:
    """Клавиатура со списком снимков"""
    keyboard = []
    row = []
    
    for i, photo in enumerate(photos):
        row.append(InlineKeyboardButton(text=photo, callback_data=f"photo_{photo}"))
        if len(row) == 3 or i == len(photos) - 1:
            keyboard.append(row)
            row = []
    
    keyboard.append([InlineKeyboardButton(text="🔙 Назад к результатам", callback_data="back_to_results")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard)

# ========== ОБРАБОТЧИКИ КОМАНД ==========

@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    await message.answer(
        f"👋 Привет, {message.from_user.full_name}!\n\n"
        f"🛩️ **Поиск аэрофотоснимков Ржевского района**\n\n"
        f"🔍 Введите название деревни:\n\n"
        f"📋 **Примеры:**\n"
        f"• Горбово\n"
        f"• Полунино\n"
        f"• Дураково\n\n"
        f"💡 Если деревня встречается в нескольких списках, "
        f"бот покажет все варианты!",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardRemove()
    )

@dp.message(Command("help"))
async def cmd_help(message: types.Message) -> None:
    await message.answer(
        "🛩️ **Помощь по поиску снимков:**\n\n"
        "1️⃣ Введите название деревни\n"
        "2️⃣ Бот покажет все списки, где встречается эта деревня\n"
        "3️⃣ Выберите нужный вариант\n"
        "4️⃣ Нажмите на номер снимка для просмотра детальной информации\n\n"
        "🔙 Кнопки «Назад» возвращают к предыдущему шагу",
        parse_mode="Markdown"
    )

# ========== ОБРАБОТЧИК ТЕКСТА ==========

@dp.message()
async def handle_message(message: types.Message) -> None:
    text = message.text
    user_id = message.from_user.id
    
    if not text:
        return
    
    db.set_last_query(user_id, text)
    
    # Сначала ищем по деревне
    results = db.search_by_village(text)
    
    # Если ничего не нашли, пробуем искать по снимку
    if not results:
        results = db.search_by_photo(text)
        search_type = "снимку"
    else:
        search_type = "деревне"
    
    if results:
        db.set_last_results(user_id, results)
        
        if len(results) == 1:
            # Один результат - показываем сразу снимки
            record = results[0]
            
            # Показываем список деревень
            villages_text = ', '.join(record['villages'])
            
            await message.answer(
                f"✅ **Найден по {search_type}:**\n{villages_text}\n\n"
                f"📸 **Снимки ({len(record['photos'])} шт.):**",
                parse_mode="Markdown",
                reply_markup=photos_keyboard(record['photos'], record['id'])
            )
        else:
            # Несколько результатов - показываем выбор
            await message.answer(
                f"🔍 **Найдено {len(results)} вариантов по запросу '{text}':**\n\n"
                f"Выберите нужный:",
                parse_mode="Markdown",
                reply_markup=results_keyboard(results)
            )
    else:
        await message.answer(
            f"❌ Ничего не найдено для '{text}'\n\n"
            f"Попробуйте другое название деревни",
            reply_markup=back_to_main_keyboard()
        )

# ========== ОБРАБОТЧИКИ КНОПОК ==========

@dp.callback_query(lambda c: c.data.startswith('record_'))
async def process_record_select(callback: CallbackQuery):
    record_id = int(callback.data.replace('record_', ''))
    
    # Находим запись по id
    record = None
    for r in db.locations:
        if r['id'] == record_id:
            record = r
            break
    
    if record:
        villages_text = ', '.join(record['villages'])
        
        await callback.message.edit_text(
            f"✅ **{villages_text}**\n\n"
            f"📸 **Снимки ({len(record['photos'])} шт.):**",
            parse_mode="Markdown",
            reply_markup=photos_keyboard(record['photos'], record_id)
        )
    
    await callback.answer()

@dp.callback_query(lambda c: c.data.startswith('photo_'))
async def process_photo_select(callback: CallbackQuery):
    photo = callback.data.replace('photo_', '')
    
    # Получаем детальное описание снимка
    details = db.get_photo_details(photo)
    
    if details:
        text = details
    else:
        text = f"📸 **Снимок {photo}**\n\n*Нет подробного описания*"
    
    await callback.message.edit_text(
        text,
        parse_mode="Markdown",
        reply_markup=back_to_results_keyboard()
    )
    
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_results")
async def process_back_to_results(callback: CallbackQuery):
    user_id = callback.from_user.id
    last_results = db.get_last_results(user_id)
    last_query = db.get_last_query(user_id)
    
    if last_results and len(last_results) > 1:
        await callback.message.edit_text(
            f"🔍 **Найдено {len(last_results)} вариантов по запросу '{last_query}':**\n\n"
            f"Выберите нужный:",
            parse_mode="Markdown",
            reply_markup=results_keyboard(last_results)
        )
    elif last_results and len(last_results) == 1:
        record = last_results[0]
        villages_text = ', '.join(record['villages'])
        
        await callback.message.edit_text(
            f"✅ **{villages_text}**\n\n"
            f"📸 **Снимки ({len(record['photos'])} шт.):**",
            parse_mode="Markdown",
            reply_markup=photos_keyboard(record['photos'], record['id'])
        )
    else:
        await callback.message.edit_text(
            "🔍 Введите название деревни",
            reply_markup=back_to_main_keyboard()
        )
    
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_search")
async def process_back_to_search(callback: CallbackQuery):
    await callback.message.edit_text(
        "🔍 Введите название деревни",
        reply_markup=back_to_main_keyboard()
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_main")
async def process_back_to_main(callback: CallbackQuery):
    await callback.message.delete()
    await cmd_start(callback.message)
    await callback.answer()

# ========== ЗАПУСК ==========

async def delete_webhook() -> None:
    try:
        info = await bot.get_webhook_info()
        if info.url:
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("✅ Webhook удален")
    except Exception as e:
        logger.error(f"Ошибка удаления webhook: {e}")

async def main() -> None:
    logger.info("🛩️ Бот для поиска аэрофотоснимков запускается...")
    await delete_webhook()
    logger.info("🔄 Polling...")
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")