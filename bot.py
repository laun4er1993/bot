# ========== ОБРАБОТЧИКИ ИНЛАЙН-КНОПОК LOCUS MAPS ==========

@dp.callback_query(lambda c: c.data == "locus_instruction")
async def process_locus_instruction(callback: CallbackQuery):
    """Инструкция по Locus Maps от ПО Сокол"""
    instruction_text = (
        "📖 **Инструкция по Locus Maps**\n\n"
        "Ссылка на инструкцию от ПО Сокол:\n"
        "🔗 https://posokol.net/manuals/locus-maps-guide\n\n"
        "📌 Инструкция содержит:\n"
        "• Установку приложения\n"
        "• Настройку карт\n"
        "• Загрузку слоев\n"
        "• Работу с координатами\n\n"
        "Нажмите на ссылку выше, чтобы открыть инструкцию."
    )
    
    # Создаем клавиатуру с ссылкой и навигацией
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📖 Открыть инструкцию", url="https://posokol.net/manuals/locus-maps-guide")],
        [InlineKeyboardButton(text="📥 Скачать карты", callback_data="locus_download")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_locus")],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])
    
    await callback.message.edit_text(
        instruction_text,
        parse_mode="Markdown",
        reply_markup=keyboard
    )
    await callback.answer()

@dp.callback_query(lambda c: c.data == "locus_download")
async def process_locus_download(callback: CallbackQuery):
    """Скачивание карт для Locus Maps от ПО Сокол"""
    download_text = (
        "📥 **Скачать карты для Locus Maps**\n\n"
        "Ссылки на карты от ПО Сокол:\n\n"
        "1️⃣ **Топографическая карта Ржевского района**\n"
        "   🔗 https://posokol.net/maps/rzhev-topo.sqlitedb\n\n"
        "2️⃣ **Карта высот (рельеф)**\n"
        "   🔗 https://posokol.net/maps/rzhev-elevation.sqlitedb\n\n"
        "3️⃣ **Спутниковые снимки 1942-1943**\n"
        "   🔗 https://posokol.net/maps/rzhev-1942.mbtiles\n\n"
        "4️⃣ **Гибридная карта (снимки + топо)**\n"
        "   🔗 https://posokol.net/maps/rzhev-hybrid.sqlitedb\n\n"
        "📌 **Инструкция по установке:**\n"
        "1. Скачайте нужный файл\n"
        "2. Поместите в папку Locus/maps/ на вашем устройстве\n"
        "3. Откройте Locus Maps и выберите карту в разделе 'Загруженные'"
    )
    
    # Создаем клавиатуру со ссылками на скачивание
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗺️ Топографическая карта", url="https://posokol.net/maps/rzhev-topo.sqlitedb")],
        [InlineKeyboardButton(text="⛰️ Карта высот", url="https://posokol.net/maps/rzhev-elevation.sqlitedb")],
        [InlineKeyboardButton(text="🛩️ Снимки 1942-43", url="https://posokol.net/maps/rzhev-1942.mbtiles")],
        [InlineKeyboardButton(text="🗺️ Гибридная карта", url="https://posokol.net/maps/rzhev-hybrid.sqlitedb")],
        [InlineKeyboardButton(text="📖 Инструкция", callback_data="locus_instruction")],
        [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_locus")],
        [InlineKeyboardButton(text="🏠 В главное меню", callback_data="back_to_main")]
    ])
    
    await callback.message.edit_text(
        download_text,
        parse_mode="Markdown",
        reply_markup=keyboard
    )
    await callback.answer()