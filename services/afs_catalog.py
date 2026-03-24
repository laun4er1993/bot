# services/afs_catalog.py
import os
import time
import logging
from typing import List, Dict, Optional

from config import AFS_CATALOG_FILE

logger = logging.getLogger(__name__)


class AFSCatalog:
    """Класс для работы с каталогом аэрофотоснимков (АФС)"""
    
    def __init__(self):
        self.catalog: List[Dict] = []
        self._load()
    
    def _load(self):
        """Загружает каталог из файла"""
        if not os.path.exists(AFS_CATALOG_FILE):
            self._create_empty()
            return
        
        try:
            with open(AFS_CATALOG_FILE, 'r', encoding='utf-8') as f:
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
                            'description': parts[1] if len(parts) > 1 else ''
                        })
            
            logger.info(f"✅ Загружено {len(self.catalog)} снимков в каталог АФС")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки каталога АФС: {e}")
            self._create_empty()
    
    def _create_empty(self):
        """Создает пустой каталог"""
        os.makedirs(os.path.dirname(AFS_CATALOG_FILE), exist_ok=True)
        with open(AFS_CATALOG_FILE, 'w', encoding='utf-8') as f:
            f.write("Номер_снимка|Описание\n")
        self.catalog = []
    
    def _save(self):
        """Сохраняет каталог в файл"""
        with open(AFS_CATALOG_FILE, 'w', encoding='utf-8') as f:
            f.write("Номер_снимка|Описание\n")
            for item in self.catalog:
                f.write(f"{item['frame']}|{item['description']}\n")
    
    def create_from_kml_results(self, results: List[Dict]) -> Dict:
        """
        Создает каталог АФС из результатов обработки KML
        
        Args:
            results: список результатов из KMLProcessor
        
        Returns:
            словарь со статистикой
        """
        stats = {'added': 0, 'duplicates': 0, 'total': 0}
        
        # Собираем существующие номера для проверки дубликатов
        existing_frames = {item['frame'] for item in self.catalog}
        
        for result in results:
            frame = result['photo_num']
            description = result.get('description', '')
            
            if frame in existing_frames:
                stats['duplicates'] += 1
                continue
            
            self.catalog.append({
                'frame': frame,
                'description': description
            })
            stats['added'] += 1
            existing_frames.add(frame)
        
        stats['total'] = len(self.catalog)
        self._save()
        
        return stats
    
    def get_catalog(self) -> List[Dict]:
        """Возвращает копию каталога"""
        return self.catalog.copy()
    
    def get_catalog_text(self, with_descriptions: bool = False) -> str:
        """
        Возвращает текстовое представление каталога
        
        Args:
            with_descriptions: включать ли описания в текст
        """
        if not self.catalog:
            return "📭 Каталог АФС пуст"
        
        text = f"📁 <b>Каталог АФС ({len(self.catalog)} снимков)</b>\n\n"
        
        for i, item in enumerate(self.catalog, 1):
            text += f"{i}. {item['frame']}"
            if with_descriptions and item['description']:
                # Показываем только первые 100 символов описания
                desc_preview = item['description'][:100]
                if len(item['description']) > 100:
                    desc_preview += "..."
                text += f"\n   📝 {desc_preview}"
            text += "\n"
        
        return text
    
    def export_to_txt(self, filename: str = None) -> str:
        """Экспортирует каталог в TXT файл"""
        from config import EXPORT_DIR
        
        if not filename:
            filename = f"afs_catalog_{time.strftime('%Y%m%d_%H%M%S')}.txt"
        
        os.makedirs(EXPORT_DIR, exist_ok=True)
        file_path = os.path.join(EXPORT_DIR, filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("КАТАЛОГ АЭРОФОТОСНИМКОВ (АФС)\n")
            f.write("=" * 80 + "\n")
            f.write(f"Дата создания: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Всего снимков: {len(self.catalog)}\n")
            f.write("=" * 80 + "\n\n")
            
            for i, item in enumerate(self.catalog, 1):
                f.write(f"{i}. {item['frame']}\n")
                if item['description']:
                    f.write(f"\n   Описание:\n")
                    f.write(f"   {'-' * 76}\n")
                    f.write(f"   {item['description']}\n")
                    f.write(f"   {'-' * 76}\n")
                f.write("\n")
        
        return file_path
    
    def clear(self) -> int:
        """Очищает каталог"""
        removed = len(self.catalog)
        self.catalog = []
        self._save()
        return removed
    
    def is_empty(self) -> bool:
        """Проверяет, пуст ли каталог"""
        return len(self.catalog) == 0