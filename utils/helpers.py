# utils/helpers.py
import asyncio
import logging
from typing import Optional, Dict, List
from aiogram import types
from aiogram.types import CallbackQuery
from aiogram.exceptions import TelegramBadRequest

logger = logging.getLogger(__name__)


async def safe_edit_text(message, text: str, parse_mode: str = "HTML", reply_markup=None):
    """Безопасное редактирование сообщения с обработкой ошибок"""
    try:
        await message.edit_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            pass
        elif "message can't be edited" in str(e):
            await message.answer(text, parse_mode=parse_mode, reply_markup=reply_markup)
        else:
            raise


async def safe_answer_callback(callback: CallbackQuery, text: str = None, show_alert: bool = False):
    """Безопасный ответ на callback с обработкой устаревших запросов"""
    try:
        if text:
            await callback.answer(text, show_alert=show_alert)
        else:
            await callback.answer()
    except TelegramBadRequest as e:
        if "query is too old" in str(e) or "timeout expired" in str(e):
            logger.debug(f"Callback expired: {e}")
        else:
            logger.warning(f"Callback answer error: {e}")


async def safe_delete_message(message):
    """Безопасное удаление сообщения"""
    try:
        await message.delete()
    except Exception:
        pass