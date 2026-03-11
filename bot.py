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
        
        # История пользователей
        self.user_last_photos: Dict[int, List[str]] = {}  # последние найденные снимки
        self.user_last_villages: Dict[int, str] = {}      # последние найденные деревни
        self.user_last_query: Dict[int, str] = {}         # последний запрос
        
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
        found_records = []
        seen_ids = set()
        
        logger.info(f"🔍 Поиск деревни: '{query}'")
        
        # Перебираем все записи
        for record in self.locations:
            # Проверяем каждую деревню в записи
            for village in record['villages']:
                village_lower = village.lower()
                
                # Точное совпадение
                if village_lower == query_lower:
                    if record['id'] not in seen_ids:
                        found_records.append(record)
                        seen_ids.add(record['id'])
                        logger.info(f"  ✓ Точное совпадение: '{query}' в записи {record['id']} (деревня {village})")
                    break  # Нашли совпадение в этой записи, переходим к следующей
                
                # Частичное совпадение для длинных слов
                elif len(query_lower) > 2 and query_lower in village_lower:
                    if record['id'] not in seen_ids:
                        found_records.append(record)
                        seen_ids.add(record['id'])
                        logger.info(f"  ✓ Частичное совпадение: '{query}' в '{village}' (запись {record['id']})")
                    break  # Нашли совпадение в этой записи, переходим к следующей
        
        logger.info(f"✅ Найдено записей: {len(found_records)}")
        return found_records
    
    def get_all_photos_from_records(self, records: List[Dict]) -> List[str]:
        """Собирает все уникальные снимки из списка записей"""
        all_photos = []
        for record in records:
            all_photos.extend(record['photos'])
        # Убираем дубликаты, сохраняя порядок
        unique_photos = []
        for photo in all_photos:
            if photo not in unique_photos:
                unique_photos.append(photo)
        return unique_photos
    
    def get_all_villages_from_records(self, records: List[Dict]) -> List[str]:
        """Собирает все уникальные деревни из списка записей"""
        all_villages = []
        for record in records:
            all_villages.extend(record['villages'])
        # Убираем дубликаты, сохраняя порядок
        unique_villages = []
        for village in all_villages:
            if village not in unique_villages:
                unique_villages.append(village)
        return unique_villages
    
    def get_photo_details(self, photo_num: str) -> Optional[str]:
        """Возвращает детальное описание снимка"""
        return self.photo_details.get(photo_num)
    
    def set_last_photos(self, user_id: int, photos: List[str]):
        self.user_last_photos[user_id] = photos
    
    def get_last_photos(self, user_id: int) -> Optional[List[str]]:
        return self.user_last_photos.get(user_id)
    
    def set_last_villages(self, user_id: int, villages_text: str):
        self.user_last_villages[user_id] = villages_text
    
    def get_last_villages(self, user_id: int) -> Optional[str]:
        return self.user_last_villages.get(user_id)
    
    def set_last_query(self, user_id: int, query: str):
        self.user_last_query[user_id] = query
    
    def get_last_query(self, user_id: int) -> Optional[str]:
        return self.user_last_query.get(user_id)
    
    def log_stats(self):
        logger.info(f"📊 Статистика:")
        logger.info(f"   • Записей в multi_keys.txt: {len(self.locations)}")

# Создаем экземпляр базы данных
db = PhotosDatabase()

# ========== КЛАВИАТУРЫ ==========

def back_to_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 В начало", callback_data="back_to_main")]
    ])

def back_to_photos_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔙 Назад к списку снимков", callback_data="back_to_photos")]
    ])

def photos_keyboard(photos: List[str]) -> InlineKeyboardMarkup:
    """Клавиатура со списком всех найденных снимков"""
    keyboard = []
    row = []
    
    for i, photo in enumerate(photos):
        row.append(InlineKeyboardButton(text=photo, callback_data=f"photo_{photo}"))
        if len(row) == 3 or i == len(photos) - 1:
            keyboard.append(row)
            row = []
    
    keyboard.append([InlineKeyboardButton(text="🔙 Новый поиск", callback_data="back_to_search")])
    
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
        f"💡 Бот покажет **все снимки** из всех списков, "
        f"где встречается эта деревня!",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardRemove()
    )

@dp.message(Command("help"))
async def cmd_help(message: types.Message) -> None:
    await message.answer(
        "🛩️ **Помощь по поиску снимков:**\n\n"
        "1️⃣ Введите название деревни\n"
        "2️⃣ Бот найдет все списки с этой деревней\n"
        "3️⃣ Покажет **все снимки** из всех найденных списков\n"
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
    
    # Ищем записи по деревне
    results = db.search_by_village(text)
    
    if results:
        # Собираем все уникальные снимки из всех найденных записей
        all_photos = db.get_all_photos_from_records(results)
        
        # Собираем все уникальные деревни для информации
        all_villages = db.get_all_villages_from_records(results)
        
        # Формируем текст с деревнями (показываем первые 10)
        if len(all_villages) > 10:
            villages_text = ', '.join(all_villages[:10]) + f" и ещё {len(all_villages)-10}"
        else:
            villages_text = ', '.join(all_villages)
        
        # Сохраняем для кнопки "Назад"
        db.set_last_photos(user_id, all_photos)
        db.set_last_villages(user_id, villages_text)
        
        await message.answer(
            f"✅ **Найдено по запросу '{text}':**\n"
            f"📍 Деревни: {villages_text}\n\n"
            f"📸 **Все снимки ({len(all_photos)} шт.):**",
            parse_mode="Markdown",
            reply_markup=photos_keyboard(all_photos)
        )
        
        logger.info(f"Пользователь {user_id} искал '{text}', найдено {len(all_photos)} снимков")
    else:
        await message.answer(
            f"❌ Ничего не найдено для '{text}'\n\n"
            f"Попробуйте другое название деревни",
            reply_markup=back_to_main_keyboard()
        )

# ========== ОБРАБОТЧИКИ КНОПОК ==========

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
        reply_markup=back_to_photos_keyboard()
    )
    
    await callback.answer()

@dp.callback_query(lambda c: c.data == "back_to_photos")
async def process_back_to_photos(callback: CallbackQuery):
    user_id = callback.from_user.id
    last_photos = db.get_last_photos(user_id)
    last_villages = db.get_last_villages(user_id)
    last_query = db.get_last_query(user_id)
    
    if last_photos:
        await callback.message.edit_text(
            f"✅ **Найдено по запросу '{last_query}':**\n"
            f"📍 Деревни: {last_villages}\n\n"
            f"📸 **Все снимки ({len(last_photos)} шт.):**",
            parse_mode="Markdown",
            reply_markup=photos_keyboard(last_photos)
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