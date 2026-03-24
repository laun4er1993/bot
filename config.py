# config.py
import os
import logging

# ========== КОНФИГУРАЦИЯ ==========

BOT_TOKEN = os.getenv("BOT_TOKEN")
YANDEX_DISK_TOKEN = os.getenv("YANDEX_DISK_TOKEN")

# Пути к файлам
DATA_DIR = "data"
VILLAGES_FILE = os.path.join(DATA_DIR, "villages.txt")
MULTI_KEYS_FILE = os.path.join(DATA_DIR, "multi_keys.txt")
DETAILS_FILE = os.path.join(DATA_DIR, "details.txt")
EXPORT_DIR = os.path.join(DATA_DIR, "export")
TEMP_DIR = os.path.join(DATA_DIR, "temp")
AFS_CATALOG_FILE = os.path.join(DATA_DIR, "afs_catalog.txt")  # Каталог АФС

# Параметры
MAX_RETRIES = 7
MIN_REQUEST_INTERVAL = 3.0
MAX_CONCURRENT_DIC = 2
KML_MARGIN_M = 100.0
KML_USE_INTERSECTS = True  # Использовать intersects вместо contains
KML_CACHE_POLYGONS = True  # Кэшировать полигоны

# Настройки логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
)
logger = logging.getLogger(__name__)