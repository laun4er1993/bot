# keyboards/__init__.py
from keyboards.main import get_main_keyboard, back_keyboard
from keyboards.inline import (
    get_settings_keyboard, get_district_keyboard, get_more_districts_keyboard,
    get_delete_district_keyboard, get_confirm_delete_district_keyboard,
    get_confirm_clear_all_keyboard, get_merge_keyboard, photos_keyboard,
    locus_instruction_keyboard, locus_download_keyboard, back_to_locus_keyboard,
    map_download_keyboard, search_result_keyboard, photo_details_keyboard,
    process_kml_again_keyboard, stats_back_keyboard
)

__all__ = [
    'get_main_keyboard',
    'back_keyboard',
    'get_settings_keyboard',
    'get_district_keyboard',
    'get_more_districts_keyboard',
    'get_delete_district_keyboard',
    'get_confirm_delete_district_keyboard',
    'get_confirm_clear_all_keyboard',
    'get_merge_keyboard',
    'photos_keyboard',
    'locus_instruction_keyboard',
    'locus_download_keyboard',
    'back_to_locus_keyboard',
    'map_download_keyboard',
    'search_result_keyboard',
    'photo_details_keyboard',
    'process_kml_again_keyboard',
    'stats_back_keyboard'
]