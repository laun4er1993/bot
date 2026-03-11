import asyncio
import logging
import os
import sys
from typing import Dict, Optional

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

# Токен берется из переменных окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")

if not BOT_TOKEN:
    logging.critical("❌ ОШИБКА: BOT_TOKEN не найден в переменных окружения!")
    sys.exit(1)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Инициализация бота
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ========== РАБОТА С ФАЙЛОМ ==========

class TextDatabase:
    """Класс для работы с текстовым файлом базы данных"""
    
    def __init__(self, file_path: str = "data/database.txt"):
        self.file_path = file_path
        self.data: Dict[str, str] = {}
        self.load_data()
    
    def load_data(self) -> None:
        """Загружает данные из текстового файла"""
        try:
            # Проверяем существование папки data
            os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
            
            # Пытаемся прочитать файл
            if os.path.exists(self.file_path):
                with open(self.file_path, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()
                        if not line or line.startswith('#'):  # Пропускаем пустые строки и комментарии
                            continue
                        
                        # Разделяем по запятой (максимум 2 части)
                        parts = line.split(',', 1)
                        if len(parts) == 2:
                            key = parts[0].strip().lower()
                            value = parts[1].strip()
                            self.data[key] = value
                        else:
                            logger.warning(f"Строка {line_num} имеет неверный формат: {line}")
                
                logger.info(f"✅ Загружено {len(self.data)} записей из {self.file_path}")
            else:
                # Создаем пример файла, если его нет
                self._create_example_file()
                logger.info(f"📝 Создан пример файла {self.file_path}")
                
        except Exception as e:
            logger.error(f"❌ Ошибка при загрузке данных: {e}", exc_info=True)
            self.data = {}
    
    def _create_example_file(self) -> None:
        """Создает пример файла с данными"""
        example_data = [
            "# Формат: ключ,значение",
            "привет,здравствуйте!",
            "как дела,хорошо! а у тебя?",
            "пока,до свидания!",
            "кто ты,я бот помощник",
            "",
            "# Добавляйте свои записи ниже"
        ]
        
        with open(self.file_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(example_data))
    
    def find_answer(self, text: str) -> Optional[str]:
        """Ищет ответ на текст пользователя"""
        if not text or not self.data:
            return None
        
        text_lower = text.lower().strip()
        
        # Прямое совпадение
        if text_lower in self.data:
            return self.data[text_lower]
        
        # Поиск по вхождению ключа в текст
        for key, value in self.data.items():
            if key in text_lower:
                return value
        
        return None
    
    def add_record(self, key: str, value: str) -> bool:
        """Добавляет новую запись в файл"""
        try:
            key = key.strip().lower()
            value = value.strip()
            
            if not key or not value:
                return False
            
            # Добавляем в файл
            with open(self.file_path, 'a', encoding='utf-8') as f:
                f.write(f"\n{key},{value}")
            
            # Обновляем данные в памяти
            self.data[key] = value
            logger.info(f"✅ Добавлена запись: {key} -> {value}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка при добавлении записи: {e}")
            return False
    
    def get_stats(self) -> Dict:
        """Возвращает статистику базы данных"""
        return {
            "total_records": len(self.data),
            "file_path": self.file_path
        }

# Создаем экземпляр базы данных
db = TextDatabase()

# ========== КЛАВИАТУРЫ ==========

def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Создает главную клавиатуру"""
    keyboard = [
        [KeyboardButton(text="🔍 Поиск"), KeyboardButton(text="📊 Статистика")],
        [KeyboardButton(text="➕ Добавить запись"), KeyboardButton(text="❓ Помощь")]
    ]
    return ReplyKeyboardMarkup(
        keyboard=keyboard,
        resize_keyboard=True,
        input_field_placeholder="Выберите действие или напишите текст..."
    )

# ========== ОБРАБОТЧИКИ КОМАНД ==========

@dp.message(Command("start"))
async def cmd_start(message: types.Message) -> None:
    """Обработчик команды /start"""
    await message.answer(
        f"👋 Привет, {message.from_user.full_name}!\n"
        f"Я бот с базой знаний из текстового файла!\n\n"
        f"📝 Просто напиши мне что-нибудь, и я поищу ответ в файле.",
        reply_markup=get_main_keyboard()
    )
    logger.info(f"User {message.from_user.id} started the bot")


@dp.message(Command("help"))
@dp.message(lambda msg: msg.text == "❓ Помощь")
async def cmd_help(message: types.Message) -> None:
    """Обработчик команды /help"""
    await message.answer(
        "🤖 **Как пользоваться ботом:**\n\n"
        "• **Просто напиши текст** - я поищу ответ в базе\n"
        "• **➕ Добавить запись** - новая пара вопрос-ответ\n"
        "• **📊 Статистика** - информация о базе данных\n"
        "• **🔍 Поиск** - режим поиска (можно просто писать)\n\n"
        "**Формат базы данных:**\n"
        "Файл `data/database.txt` с записями: `ключ,значение`",
        parse_mode="Markdown",
        reply_markup=get_main_keyboard()
    )


@dp.message(Command("stats"))
@dp.message(lambda msg: msg.text == "📊 Статистика")
async def cmd_stats(message: types.Message) -> None:
    """Показывает статистику базы данных"""
    stats = db.get_stats()
    await message.answer(
        f"📊 **Статистика базы данных:**\n\n"
        f"• Всего записей: **{stats['total_records']}**\n"
        f"• Файл: `{stats['file_path']}`\n\n"
        f"💡 Отправь мне любое сообщение для поиска ответа!",
        parse_mode="Markdown"
    )


# Состояние для добавления записи
user_states = {}

@dp.message(lambda msg: msg.text == "➕ Добавить запись")
async def cmd_add_start(message: types.Message) -> None:
    """Начинает процесс добавления записи"""
    user_states[message.from_user.id] = {"state": "waiting_for_key"}
    await message.answer(
        "📝 **Добавление новой записи**\n\n"
        "Введите **ключ** (слово, на которое будет реагировать бот):",
        parse_mode="Markdown"
    )


@dp.message()
async def handle_message(message: types.Message) -> None:
    """Основной обработчик сообщений"""
    user_id = message.from_user.id
    text = message.text
    
    if not text:
        return
    
    # Проверяем состояние пользователя (добавление записи)
    if user_id in user_states:
        state = user_states[user_id]["state"]
        
        if state == "waiting_for_key":
            # Сохраняем ключ и ждем значение
            user_states[user_id] = {
                "state": "waiting_for_value",
                "key": text
            }
            await message.answer(
                f"✅ Ключ **'{text}'** сохранен.\n\n"
                f"Теперь введите **значение** (ответ бота):",
                parse_mode="Markdown"
            )
            
        elif state == "waiting_for_value":
            # Получаем ключ и значение, сохраняем
            key = user_states[user_id]["key"]
            value = text
            
            # Добавляем в базу
            if db.add_record(key, value):
                await message.answer(
                    f"✅ **Запись успешно добавлена!**\n\n"
                    f"• Ключ: **{key}**\n"
                    f"• Значение: **{value}**\n\n"
                    f"Теперь попробуйте написать '{key}'",
                    parse_mode="Markdown",
                    reply_markup=get_main_keyboard()
                )
            else:
                await message.answer(
                    "❌ Ошибка при добавлении записи. Попробуйте снова.",
                    reply_markup=get_main_keyboard()
                )
            
            # Очищаем состояние
            del user_states[user_id]
        
        return
    
    # Обработка кнопки "Поиск"
    if text == "🔍 Поиск":
        await message.answer(
            "🔍 Режим поиска активен!\n"
            "Напишите текст, и я найду ответ в базе данных."
        )
        return
    
    # ПОИСК В БАЗЕ ДАННЫХ - основная логика
    answer = db.find_answer(text)
    
    if answer:
        await message.answer(
            f"✅ **Найдено в базе:**\n\n{answer}",
            parse_mode="Markdown"
        )
        logger.info(f"Найден ответ для '{text}' -> '{answer}'")
    else:
        await message.answer(
            f"❌ **Не найдено**\n\n"
            f"Запрос: '{text}'\n\n"
            f"Этого слова нет в базе данных.\n"
            f"Можете добавить его через кнопку **➕ Добавить запись**",
            parse_mode="Markdown"
        )


async def delete_webhook_and_start() -> None:
    """Удаляет вебхук и запускает polling"""
    logger.info("🔄 Проверяем наличие активного webhook...")
    
    try:
        webhook_info = await bot.get_webhook_info()
        
        if webhook_info.url:
            logger.warning(f"⚠️ Найден активный webhook: {webhook_info.url}")
            await bot.delete_webhook(drop_pending_updates=True)
            logger.info("✅ Webhook успешно удален")
        else:
            logger.info("✅ Активных webhook не найдено")
            
    except Exception as e:
        logger.error(f"❌ Ошибка при удалении webhook: {e}")


async def main() -> None:
    """Главная функция запуска бота"""
    logger.info("🚀 Бот запускается...")
    
    try:
        bot_info = await bot.get_me()
        logger.info(f"✅ Бот @{bot_info.username} авторизован")
        
        # Показываем статистику базы данных при запуске
        stats = db.get_stats()
        logger.info(f"📊 База данных: {stats['total_records']} записей в {stats['file_path']}")
        
        await delete_webhook_and_start()
        
        logger.info("🔄 Начинаем polling...")
        await dp.start_polling(
            bot,
            allowed_updates=["message"],
            skip_updates=True
        )
        
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}", exc_info=True)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("👋 Бот остановлен")