# services/kml_catalog.py
import os
import time
import logging
from typing import List, Dict, Optional, Tuple
from bs4 import BeautifulSoup

from config import KML_CATALOG_FILE, DATA_DIR

logger = logging.getLogger(__name__)


class KMLCatalog:
    """Класс для работы с каталогом KML файлов"""
    
    def __init__(self):
        self.catalog: List[Dict] = []
        self._load()
    
    def _load(self):
        """Загружает каталог из файла"""
        if not os.path.exists(KML_CATALOG_FILE):
            self._create_empty()
            return
        
        try:
            with open(KML_CATALOG_FILE, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                if not lines:
                    self._create_empty()
                    return
                
                for line in lines[1:]:  # Пропускаем заголовок
                    line = line.strip()
                    if not line:
                        continue
                    
                    parts = line.split('|')
                    if len(parts) >= 2:
                        self.catalog.append({
                            'frame': parts[0],
                            'description': parts[1] if len(parts) > 1 and parts[1] != 'None' else '',
                            'file_name': parts[2] if len(parts) > 2 else ''
                        })
            
            logger.info(f"✅ Загружено {len(self.catalog)} KML файлов в каталог")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки каталога KML: {e}")
            self._create_empty()
    
    def _create_empty(self):
        """Создает пустой каталог"""
        os.makedirs(os.path.dirname(KML_CATALOG_FILE), exist_ok=True)
        with open(KML_CATALOG_FILE, 'w', encoding='utf-8') as f:
            f.write("Номер_снимка|Описание|Имя_файла\n")
        self.catalog = []
    
    def _save(self):
        """Сохраняет каталог в файл"""
        try:
            with open(KML_CATALOG_FILE, 'w', encoding='utf-8') as f:
                f.write("Номер_снимка|Описание|Имя_файла\n")
                for item in self.catalog:
                    description = item.get('description', '')
                    if description is None:
                        description = ''
                    description = str(description).replace('|', '\\|').replace('\n', ' ')
                    file_name = item.get('file_name', '')
                    file_name = str(file_name).replace('|', '\\|')
                    f.write(f"{item['frame']}|{description}|{file_name}\n")
            logger.debug(f"✅ Каталог KML сохранен, записей: {len(self.catalog)}")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения каталога KML: {e}")
    
    def add_kml(self, frame: str, description: str = '', file_name: str = '') -> Dict:
        """Добавляет KML файл в каталог"""
        stats = {'added': 0, 'duplicate': 0, 'total': 0}
        
        existing_frames = {item['frame'] for item in self.catalog}
        
        if frame in existing_frames:
            stats['duplicate'] += 1
            return stats
        
        self.catalog.append({
            'frame': frame,
            'description': description,
            'file_name': file_name
        })
        stats['added'] += 1
        stats['total'] = len(self.catalog)
        self._save()
        
        return stats
    
    def add_kml_from_file(self, kml_path: str, original_filename: str) -> Dict:
        """Добавляет KML файл в каталог из загруженного файла"""
        stats = {'added': 0, 'duplicate': 0, 'error': 0, 'frame': ''}
        
        try:
            # Парсим KML файл для извлечения информации
            with open(kml_path, 'r', encoding='utf-8') as f:
                soup = BeautifulSoup(f.read(), 'xml')
            
            # Ищем Placemark с именем Frame-XXX
            frame = None
            description = ''
            
            for placemark in soup.find_all('Placemark'):
                name_elem = placemark.find('name')
                if name_elem and name_elem.text.startswith('Frame-'):
                    frame = name_elem.text.replace('Frame-', '')
                    
                    # Ищем описание
                    desc_elem = placemark.find('description')
                    if desc_elem:
                        description = desc_elem.text.strip()
                    break
            
            if not frame:
                stats['error'] = 1
                stats['error_msg'] = "Не найден Placemark с названием Frame-XXX"
                return stats
            
            # Проверяем на дубликат
            existing_frames = {item['frame'] for item in self.catalog}
            if frame in existing_frames:
                stats['duplicate'] = 1
                return stats
            
            # Сохраняем файл в папку data/kml/
            kml_dir = os.path.join(DATA_DIR, "kml")
            os.makedirs(kml_dir, exist_ok=True)
            
            # Генерируем уникальное имя файла
            timestamp = time.strftime('%Y%m%d_%H%M%S')
            saved_filename = f"{frame}_{timestamp}.kml"
            saved_path = os.path.join(kml_dir, saved_filename)
            
            # Копируем файл
            import shutil
            shutil.copy2(kml_path, saved_path)
            
            # Добавляем в каталог
            self.catalog.append({
                'frame': frame,
                'description': description,
                'file_name': saved_filename
            })
            stats['added'] = 1
            stats['frame'] = frame
            stats['total'] = len(self.catalog)
            self._save()
            
        except Exception as e:
            logger.error(f"Ошибка обработки KML файла: {e}")
            stats['error'] = 1
            stats['error_msg'] = str(e)
        
        return stats
    
    def add_kml_batch(self, kml_files: List[Dict]) -> Dict:
        """Добавляет список KML файлов с проверкой на дубликаты"""
        stats = {'added': 0, 'duplicates': 0, 'errors': 0, 'total': 0, 'files': []}
        
        existing_frames = {item['frame'] for item in self.catalog}
        
        for item in kml_files:
            frame = item.get('frame', '')
            description = item.get('description', '')
            file_name = item.get('file_name', '')
            
            if description is None:
                description = ''
            if not frame:
                stats['errors'] += 1
                continue
            
            if frame in existing_frames:
                stats['duplicates'] += 1
                continue
            
            self.catalog.append({
                'frame': frame,
                'description': description,
                'file_name': file_name
            })
            stats['added'] += 1
            stats['files'].append({'frame': frame, 'file_name': file_name})
            existing_frames.add(frame)
        
        stats['total'] = len(self.catalog)
        self._save()
        
        return stats
    
    def clear(self) -> int:
        """Очищает каталог"""
        removed = len(self.catalog)
        self.catalog = []
        self._save()
        return removed
    
    def get_catalog(self) -> List[Dict]:
        """Возвращает копию каталога"""
        return self.catalog.copy()
    
    def get_catalog_text(self, with_descriptions: bool = False, page: int = 1, per_page: int = 50) -> Tuple[str, int, int]:
        """Возвращает текстовое представление каталога с пагинацией"""
        if not self.catalog:
            return "📭 Каталог KML пуст", 0, 1
        
        total_pages = (len(self.catalog) + per_page - 1) // per_page
        page = max(1, min(page, total_pages))
        
        start = (page - 1) * per_page
        end = start + per_page
        items = self.catalog[start:end]
        
        text = f"📁 <b>Каталог KML</b> (всего: {len(self.catalog)} файлов, страница {page}/{total_pages})\n\n"
        
        for i, item in enumerate(items, start + 1):
            text += f"{i}. {item['frame']}"
            if with_descriptions and item.get('description'):
                desc_preview = str(item['description'])[:100].replace('\n', ' ')
                if len(str(item['description'])) > 100:
                    desc_preview += "..."
                text += f"\n   📝 {desc_preview}"
            if item.get('file_name'):
                text += f"\n   📄 {item['file_name']}"
            text += "\n"
        
        return text, total_pages, page
    
    def export_to_txt(self, filename: str = None) -> str:
        """Экспортирует каталог в TXT файл"""
        from config import EXPORT_DIR
        
        if not filename:
            filename = f"kml_catalog_{time.strftime('%Y%m%d_%H%M%S')}.txt"
        
        os.makedirs(EXPORT_DIR, exist_ok=True)
        file_path = os.path.join(EXPORT_DIR, filename)
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write("КАТАЛОГ KML ФАЙЛОВ\n")
                f.write("=" * 80 + "\n")
                f.write(f"Дата создания: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Всего файлов: {len(self.catalog)}\n")
                f.write("=" * 80 + "\n\n")
                
                for i, item in enumerate(self.catalog, 1):
                    f.write(f"{i}. {item['frame']}\n")
                    if item.get('description'):
                        f.write(f"\n   Описание:\n")
                        f.write(f"   {'-' * 76}\n")
                        desc_lines = str(item['description']).split('\n')
                        for line in desc_lines:
                            f.write(f"   {line}\n")
                        f.write(f"   {'-' * 76}\n")
                    if item.get('file_name'):
                        f.write(f"\n   Файл: {item['file_name']}\n")
                    f.write("\n")
        except Exception as e:
            logger.error(f"Ошибка экспорта каталога KML: {e}")
            return None
        
        return file_path
    
    def is_empty(self) -> bool:
        """Проверяет, пуст ли каталог"""
        return len(self.catalog) == 0
    
    def get_statistics(self) -> Dict:
        """Возвращает статистику каталога"""
        total = len(self.catalog)
        with_description = sum(1 for item in self.catalog if item.get('description'))
        
        return {
            'total': total,
            'with_description': with_description,
            'without_description': total - with_description
        }