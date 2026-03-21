# utils.py
# Вспомогательные функции

import re
import logging
from typing import List, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

from .config import (
    SERVICE_VILLAGE_WORDS, SERVICE_SETTLEMENT_WORDS,
    MIN_NAME_LENGTH, MAX_NAME_LENGTH, TYPE_MAPPING
)


def is_valid_name(name: str) -> bool:
    """Проверяет, является ли текст валидным названием населенного пункта"""
    if not name or len(name) < MIN_NAME_LENGTH or len(name) > MAX_NAME_LENGTH:
        return False
    
    name_lower = name.lower()
    for word in SERVICE_VILLAGE_WORDS:
        if word in name_lower:
            return False
    
    if not re.search(r'[а-яА-ЯёЁ]', name):
        return False
    
    if name.isdigit():
        return False
    
    return True


def is_valid_settlement_name(name: str) -> bool:
    """Проверяет, является ли текст валидным названием сельского поселения"""
    if not name or len(name) < 3 or len(name) > 50:
        return False
    
    name_lower = name.lower()
    
    if re.match(r'^\d+\s+(мая|января|февраля|марта|апреля|июня|июля|августа|сентября|октября|ноября|декабря)', name_lower):
        return False
    
    if name.isdigit():
        return False
    
    for word in SERVICE_SETTLEMENT_WORDS:
        if word in name_lower:
            return False
    
    if not re.search(r'[а-яА-ЯёЁ]', name):
        return False
    
    return True


def expand_type(short_type: str) -> str:
    """Преобразует сокращение типа в полное название"""
    if not short_type:
        return 'деревня'
    clean_type = short_type.rstrip('.').lower().strip()
    return TYPE_MAPPING.get(clean_type, clean_type if clean_type in TYPE_MAPPING.values() else 'деревня')


def find_column_index(headers: List[str], possible_names: List[str]) -> Optional[int]:
    """Находит индекс колонки по возможным названиям"""
    for i, header in enumerate(headers):
        for name in possible_names:
            if name in header:
                return i
    return None


def clean_village_name(name: str) -> str:
    """Очищает название НП от цифр и лишних пробелов"""
    name = re.sub(r'^\d+\s*', '', name)
    name = re.sub(r'\[\d+\]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def extract_settlement_from_text(text: str) -> Optional[str]:
    """Извлекает название сельского поселения из текста"""
    match = re.search(r'«([^»]+)»', text)
    if match:
        return match.group(1).strip()
    
    settlement = re.sub(r'^сельское\s+поселение\s*', '', text, flags=re.IGNORECASE)
    settlement = re.sub(r'\s+\(.*?\)', '', settlement).strip()
    
    if settlement and len(settlement) > 2:
        return settlement
    
    return None


def validate_coordinates(lat: float, lon: float) -> bool:
    """Проверяет, что координаты в пределах Тверской области"""
    return 55.0 <= lat <= 58.0 and 30.0 <= lon <= 38.0