from handlers.start import register_start_handlers
from handlers.search import register_search_handlers
from handlers.kml import register_kml_handlers
from handlers.settings import register_settings_handlers
from handlers.callbacks import register_callbacks
from handlers.coord_calculator import register_coord_calculator_handlers

__all__ = [
    'register_start_handlers',
    'register_search_handlers',
    'register_kml_handlers',
    'register_settings_handlers',
    'register_callbacks',
    'register_coord_calculator_handlers'
]