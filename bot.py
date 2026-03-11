import asyncio
import logging
import os
import sys
from typing import Optional, Dict, List

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
        
        # Данные из details.txt
        self.photo_details: Dict[str, str] = {}  # снимок -> детальное описание
        
        # История пользователей
        self.user_last_photos: Dict[int, List[str]] = {}  # последние найденные снимки
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
                                'villages': villages,
                                'villages_str': villages_str,
                                'photos': photos
                            }
                            self.locations.append(record)
                            
                            logger.info(f"  Строка {idx}: {len(villages)} деревень, {len(photos)} снимков")
                
                logger.info(f"✅ Загружено {len(self.locations)} записей из multi_keys.txt")
            else:
                logger.warning(f"⚠️ Файл {self.multi_keys_file} не найден")
                
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки multi_keys: {e}")
    
def load_details(self) -> None:
    """Загружает детальную информацию о снимках из details.txt (многострочный формат)"""
    try:
        if os.path.exists(self.details_file):
            with open(self.details_file, 'r', encoding='utf-8') as f:
                content = f.read()
                logger.info(f"📄 Читаем файл {self.details_file}, размер: {len(content)} байт")
                
                # Разделяем по маркеру ===, но сохраняем структуру
                entries = content.split('===')
                
                for i in range(len(entries) - 1):
                    # Получаем номер снимка (последняя строка перед ===)
                    lines = entries[i].strip().split('\n')
                    photo_num = lines[-1].strip() if lines else ""
                    
                    # Получаем описание (всё что после === до следующего === или конца файла)
                    description = entries[i + 1].strip()
                    
                    if photo_num and description and not photo_num.startswith('#'):
                        self.photo_details[photo_num] = description
                        logger.info(f"  ✅ Загружен снимок: {photo_num} (описание: {len(description)} символов)")
                
                logger.info(f"✅ Загружено {len(self.photo_details)} описаний снимков из details.txt")
        else:
            logger.warning(f"⚠️ Файл {self.details_file} не найден")
            
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки details: {e}", exc_info=True)
        
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
                        logger.info(f"  ✓ Точное совпадение: '{query}' в записи {record['id']}")
                    break
                
                # Частичное совпадение для длинных слов
                elif len(query_lower) > 2 and query_lower in village_lower:
                    if record['id'] not in seen_ids:
                        found_records.append(record)
                        seen_ids.add(record['id'])
                        logger.info(f"  ✓ Частичное совпадение: '{query}' в записи {record['id']}")
                    break
        
        logger.info(f"✅ Найдено записей: {len(found_records)}")
        return found_records
    
    def get_all_photos_from_records(self, records: List[Dict]) -> List[str]:
        """Собирает все уникальные снимки из списка записей"""
        all_photos = []
        for record in records:
            all_photos.extend(record['photos'])
        # Убираем дубликаты
        unique_photos = []
        for photo in all_photos:
            if photo not in unique_photos:
                unique_photos.append(photo)
        return unique_photos
    
    def get_photo_details(self, photo_num: str) -> Optional[str]:
        """Возвращает детальное описание снимка"""
        details = self.photo_details.get(photo_num)
        if details:
            logger.info(f"✅ Найдены детали для снимка {photo_num}")
        else:
            logger.warning(f"❌ Детали не найдены для снимка {photo_num}")
        return details
    
    def set_last_photos(self, user_id: int, photos: List[str]):
        self.user_last_photos[user_id] = photos
    
    def get_last_photos(self, user_id: int) -> Optional[List[str]]:
        return self.user_last_photos.get(user_id)
    
    def set_last_query(self, user_id: int, query: str):
        self.user_last_query[user_id] = query
    
    def get_last_query(self, user_id: int) -> Optional[str]:
        return self.user_last_query.get(user_id)
    
    def log_stats(self):
        logger.info(f"📊 Статистика:")
        logger.info(f"   • Записей в multi_keys.txt: {len(self.locations)}")
        logger.info(f"   • Описаний в details.txt: {len(self.photo_details)}")

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
        f"🛩️ **Поиск информации об аэрофотоснимках Ржевского района**\n\n"
        f"🔍 Введите название деревни:\n\n"
        f"📋 **Примеры:**\n"
        f"• Горбово\n"
        f"• Полунино\n"
        f"• Дураково\n\n"
        f"📸 Бот покажет **все снимки**, где встречается эта деревня, "
        f"и вы сможете посмотреть детальную информацию о каждом снимке.",
        parse_mode="Markdown",
        reply_markup=ReplyKeyboardRemove()
    )

@dp.message(Command("help"))
async def cmd_help(message: types.Message) -> None:
    await message.answer(
        "🛩️ **Помощь по поиску снимков:**\n\n"
        "1️⃣ Введите название деревни\n"
        "2️⃣ Бот покажет список всех снимков, где есть эта деревня\n"
        "3️⃣ Нажмите на номер снимка\n"
        "4️⃣ Получите детальную информацию:\n"
        "   • Дата съемки\n"
        "   • Масштаб\n"
        "   • Номер вылета\n"
        "   • Качество\n"
        "   • Квадрат, наложение, кадр\n"
        "   • Владелец\n\n"
        "🔙 Кнопка «Назад» возвращает к списку снимков",
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
        
        # Сохраняем для кнопки "Назад"
        db.set_last_photos(user_id, all_photos)
        
        # Показываем список снимков
        photos_list = "\n".join([f"• {photo}" for photo in all_photos])
        
        await message.answer(
            f"✅ **Найдено по запросу '{text}':**\n\n"
            f"📸 **Снимки ({len(all_photos)} шт.):**\n\n"
            f"{photos_list}\n\n"
            f"👇 **Нажмите на номер снимка для просмотра информации:**",
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
    user_id = callback.from_user.id
    
    logger.info(f"Пользователь {user_id} выбрал снимок {photo}")
    
    # Получаем детальное описание снимка из details.txt
    details = db.get_photo_details(photo)
    
    if details:
        text = details
        logger.info(f"✅ Детали найдены для {photo}")
    else:
        # Если нет описания, показываем базовую информацию
        text = (
            f"📸 **Снимок {photo}**\n\n"
            f"❌ Подробная информация отсутствует в базе данных\n\n"
            f"📍 Номер снимка: {photo}"
        )
        logger.warning(f"❌ Детали не найдены для {photo}")
    
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
    last_query = db.get_last_query(user_id)
    
    if last_photos:
        photos_list = "\n".join([f"• {photo}" for photo in last_photos])
        
        await callback.message.edit_text(
            f"✅ **Найдено по запросу '{last_query}':**\n\n"
            f"📸 **Снимки ({len(last_photos)} шт.):**\n\n"
            f"{photos_list}\n\n"
            f"👇 **Нажмите на номер снимка для просмотра информации:**",
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
    logger.info("🛩️ Бот для поиска информации об аэрофотоснимках запускается...")
    
    # Проверяем загрузку данных
    logger.info(f"📊 Загружено локаций: {len(db.locations)}")
    logger.info(f"📊 Загружено описаний снимков: {len(db.photo_details)}")
    
    if len(db.photo_details) == 0:
        logger.warning("⚠️ ВНИМАНИЕ: Не загружено ни одного описания снимков!")
        logger.warning("⚠️ Проверьте файл data/details.txt")
    
    await delete_webhook()
    logger.info("🔄 Polling...")
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")