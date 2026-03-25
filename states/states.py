# states/states.py
from aiogram.fsm.state import State, StatesGroup

class SearchStates(StatesGroup):
    waiting_for_village = State()
    waiting_for_kml = State()
    waiting_for_txt_upload = State()
    waiting_for_district_select = State()
    waiting_for_add_village = State()
    waiting_for_afs_upload = State()  # Состояние для загрузки каталога АФС
    waiting_for_add_kml = State()     # Состояние для добавления KML вручную
    waiting_for_kml_upload = State()  # Состояние для загрузки каталога KML
    downloading_in_progress = State()