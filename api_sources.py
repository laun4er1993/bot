# В обработчике выбора района увеличиваем таймаут

@dp.callback_query(lambda c: c.data.startswith("select_district_"))
async def process_district_select(callback: CallbackQuery, state: FSMContext):
    """Обрабатывает выбор района с увеличенным таймаутом"""
    district = callback.data.replace("select_district_", "")
    
    await state.update_data(selected_district=district)
    
    await callback.message.edit_text(
        f"⏳ <b>Загрузка данных для {district} района...</b>\n\n"
        f"Это может занять до 5-7 минут. Я сообщу, когда данные будут готовы.\n"
        f"Бот ищет максимальное количество информации: страницы района, сельских поселений, списки населенных пунктов и координаты.",
        parse_mode="HTML"
    )
    
    await callback.answer("⏳ Начинаю загрузку...")
    
    try:
        api_manager = APISourceManager()
        
        # Увеличиваем общий таймаут до 420 секунд (7 минут)
        villages = await asyncio.wait_for(
            api_manager.fetch_district_data(district),
            timeout=420.0
        )
        
        await api_manager.close_session()
        
        await state.update_data(
            downloaded_data=villages,
            total_count=len(villages),
            with_coords=sum(1 for v in villages if v.get('lat'))
        )
        
        with_coords = sum(1 for v in villages if v.get('lat'))
        
        stats_text = f"📊 <b>Результаты для {district} района:</b>\n\n"
        stats_text += f"• Всего уникальных: {len(villages)} записей\n"
        stats_text += f"• С координатами: {with_coords}\n"
        stats_text += f"• Без координат: {len(villages) - with_coords}\n"
        
        # Создаем CSV файл
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        temp_dir = "data/temp"
        os.makedirs(temp_dir, exist_ok=True)
        temp_csv = os.path.join(temp_dir, f"{district}_{timestamp}.csv")
        
        with open(temp_csv, 'w', encoding='utf-8', newline='') as f:
            fieldnames = ['name', 'type', 'lat', 'lon', 'district']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(villages)
        
        # Создаем TXT файл для скачивания
        txt_filename = f"населенные_пункты_{district}_{timestamp}.txt"
        txt_path = generate_simple_txt_from_data(villages, txt_filename)
        
        await state.update_data(
            temp_csv=temp_csv,
            temp_txt=txt_path,
            txt_filename=txt_filename
        )
        
        # Показываем меню выбора действия
        await callback.message.edit_text(
            f"✅ <b>Данные для {district} района загружены!</b>\n\n"
            f"{stats_text}\n\n"
            f"<b>Что сделать с этими данными?</b>",
            parse_mode="HTML",
            reply_markup=get_merge_action_keyboard(district)
        )
        
    except asyncio.TimeoutError:
        logger.error("Таймаут при загрузке данных")
        await callback.message.edit_text(
            "❌ <b>Ошибка загрузки</b>\n\n"
            "Превышено время ожидания ответа от серверов.\n"
            "Попробуйте позже или выберите другой район.",
            parse_mode="HTML",
            reply_markup=back_to_settings_keyboard()
        )
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных: {e}")
        await callback.message.edit_text(
            f"❌ <b>Ошибка загрузки</b>\n\n{str(e)}",
            parse_mode="HTML",
            reply_markup=back_to_settings_keyboard()
        )
    finally:
        if 'api_manager' in locals():
            await api_manager.close_session()