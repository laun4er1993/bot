import os
import logging

# ========== КОНФИГУРАЦИЯ ==========

BOT_TOKEN = os.getenv("BOT_TOKEN")
YANDEX_DISK_TOKEN = os.getenv("YANDEX_DISK_TOKEN")

# Пароль для доступа к настройкам
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")

# Пути к файлам
DATA_DIR = "data"
VILLAGES_FILE = os.path.join(DATA_DIR, "villages.txt")
MULTI_KEYS_FILE = os.path.join(DATA_DIR, "multi_keys.txt")
DETAILS_FILE = os.path.join(DATA_DIR, "details.txt")
EXPORT_DIR = os.path.join(DATA_DIR, "export")
TEMP_DIR = os.path.join(DATA_DIR, "temp")
AFS_CATALOG_FILE = os.path.join(DATA_DIR, "afs_catalog.txt")
KML_CATALOG_FILE = os.path.join(DATA_DIR, "kml_catalog.txt")
KML_DIR = os.path.join(DATA_DIR, "kml")
LINKS_CONFIG_FILE = os.path.join(DATA_DIR, "links_config.txt")
LOG_FILE = os.path.join(DATA_DIR, "bot.log")
PASSWORD_FILE = os.path.join(DATA_DIR, "admin_password.txt")

# Параметры
MAX_RETRIES = 7
MIN_REQUEST_INTERVAL = 3.0
MAX_CONCURRENT_DIC = 2
KML_MARGIN_M = 100.0
KML_USE_INTERSECTS = True
KML_CACHE_POLYGONS = True

# ========== СОЗДАНИЕ ДИРЕКТОРИЙ ==========

os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(EXPORT_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)
os.makedirs(KML_DIR, exist_ok=True)

# ========== ЗАГРУЗКА ПАРОЛЯ ИЗ ФАЙЛА ==========

if os.path.exists(PASSWORD_FILE):
    try:
        with open(PASSWORD_FILE, 'r', encoding='utf-8') as f:
            saved_password = f.read().strip()
            if saved_password:
                ADMIN_PASSWORD = saved_password
    except Exception as e:
        logging.error(f"Ошибка загрузки пароля: {e}")

# ========== НАСТРОЙКИ ЛОГИРОВАНИЯ ==========

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

try:
    file_handler = logging.FileHandler(LOG_FILE, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.info(f"✅ Логирование в файл: {LOG_FILE}")
except Exception as e:
    logger.warning(f"⚠️ Не удалось создать файл логов: {e}")

logger.info("🚀 Конфигурация бота загружена")

# ========== ССЫЛКИ ==========

LOCUS_DOWNLOAD_URL_DEFAULT = "https://disk.yandex.ru/d/uUgVGkMoq3WITw"
MAP_RZHEV_URL_DEFAULT = "https://disk.yandex.ru/d/mrxZWJqLuAtnNA"
LOCUS_INSTRUCTION_URL_DEFAULT = "https://disk.yandex.ru/i/sE2Jy99in7MCxw"

LOCUS_DOWNLOAD_URL = LOCUS_DOWNLOAD_URL_DEFAULT
MAP_RZHEV_URL = MAP_RZHEV_URL_DEFAULT
LOCUS_INSTRUCTION_URL = LOCUS_INSTRUCTION_URL_DEFAULT


def load_links_config():
    global LOCUS_DOWNLOAD_URL, MAP_RZHEV_URL, LOCUS_INSTRUCTION_URL
    if os.path.exists(LINKS_CONFIG_FILE):
        try:
            with open(LINKS_CONFIG_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('#'):
                        continue
                    parts = line.split('=', 1)
                    if len(parts) == 2:
                        key, value = parts[0].strip(), parts[1].strip()
                        if key == 'LOCUS_DOWNLOAD_URL':
                            LOCUS_DOWNLOAD_URL = value
                        elif key == 'MAP_RZHEV_URL':
                            MAP_RZHEV_URL = value
                        elif key == 'LOCUS_INSTRUCTION_URL':
                            LOCUS_INSTRUCTION_URL = value
            logger.info(f"✅ Загружены ссылки из {LINKS_CONFIG_FILE}")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки ссылок: {e}")
            save_links_config()
    else:
        save_links_config()


def save_links_config():
    try:
        with open(LINKS_CONFIG_FILE, 'w', encoding='utf-8') as f:
            f.write("# Файл конфигурации ссылок бота\n")
            f.write("# Изменяйте значения вручную или через меню бота\n\n")
            f.write(f"LOCUS_DOWNLOAD_URL={LOCUS_DOWNLOAD_URL}\n")
            f.write(f"MAP_RZHEV_URL={MAP_RZHEV_URL}\n")
            f.write(f"LOCUS_INSTRUCTION_URL={LOCUS_INSTRUCTION_URL}\n")
        logger.info(f"✅ Сохранены ссылки в {LINKS_CONFIG_FILE}")
    except Exception as e:
        logger.error(f"❌ Ошибка сохранения ссылок: {e}")


def update_link(link_type: str, new_url: str):
    global LOCUS_DOWNLOAD_URL, MAP_RZHEV_URL, LOCUS_INSTRUCTION_URL
    if link_type == "locus_download":
        LOCUS_DOWNLOAD_URL = new_url
    elif link_type == "map_rzhev":
        MAP_RZHEV_URL = new_url
    elif link_type == "locus_instruction":
        LOCUS_INSTRUCTION_URL = new_url
    logger.info(f"✅ Обновлена ссылка {link_type}: {new_url}")


load_links_config()
logger.info("✅ Конфигурация полностью загружена")