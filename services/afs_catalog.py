# services/afs_catalog.py
import os
import time
import logging
from typing import List, Dict, Optional, Tuple

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
                            'description': parts[1] if len(parts) > 1 and parts[1] != 'None' else ''
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
        try:
            with open(AFS_CATALOG_FILE, 'w', encoding='utf-8') as f:
                f.write("Номер_снимка|Описание\n")
                for item in self.catalog:
                    description = item.get('description', '')
                    if description is None:
                        description = ''
                    description = str(description).replace('|', '\\|').replace('\n', ' ')
                    f.write(f"{item['frame']}|{description}\n")
            logger.debug(f"✅ Каталог АФС сохранен, записей: {len(self.catalog)}")
        except Exception as e:
            logger.error(f"❌ Ошибка сохранения каталога АФС: {e}")
    
    def create_from_kml_results(self, results: List[Dict], frames_without_np: List[Dict]) -> Dict:
        """Создает каталог АФС из ВСЕХ результатов обработки KML"""
        stats = {'added': 0, 'duplicates': 0, 'total': 0, 'with_np': 0, 'without_np': 0}
        
        existing_frames = {item['frame'] for item in self.catalog}
        
        all_frames = []
        for result in results:
            all_frames.append({
                'frame': result.get('photo_num', ''),
                'description': result.get('description', ''),
                'has_np': True
            })
        
        for frame_data in frames_without_np:
            all_frames.append({
                'frame': frame_data.get('frame', ''),
                'description': frame_data.get('description', ''),
                'has_np': False
            })
        
        logger.info(f"📁 СОЗДАНИЕ КАТАЛОГА АФС из {len(all_frames)} снимков (с НП: {len(results)}, без НП: {len(frames_without_np)})")
        
        for frame_data in all_frames:
            frame = frame_data['frame']
            description = frame_data['description']
            has_np = frame_data['has_np']
            
            if not frame:
                continue
            
            if frame in existing_frames:
                stats['duplicates'] += 1
                continue
            
            self.catalog.append({
                'frame': frame,
                'description': description
            })
            stats['added'] += 1
            existing_frames.add(frame)
            
            if has_np:
                stats['with_np'] += 1
            else:
                stats['without_np'] += 1
        
        stats['total'] = len(self.catalog)
        self._save()
        
        logger.info(f"✅ Создание каталога АФС завершено: добавлено {stats['added']}, из них с НП: {stats['with_np']}, без НП: {stats['without_np']}")
        
        return stats
    
    def add_from_kml_results(self, results: List[Dict], frames_without_np: List[Dict]) -> Dict:
        """Дополняет существующий каталог из ВСЕХ результатов обработки KML"""
        stats = {'added': 0, 'updated': 0, 'duplicates': 0, 'total': 0, 'with_np': 0, 'without_np': 0}
        
        existing = {item['frame']: item for item in self.catalog}
        
        all_frames = []
        for result in results:
            all_frames.append({
                'frame': result.get('photo_num', ''),
                'description': result.get('description', ''),
                'has_np': True
            })
        
        for frame_data in frames_without_np:
            all_frames.append({
                'frame': frame_data.get('frame', ''),
                'description': frame_data.get('description', ''),
                'has_np': False
            })
        
        for frame_data in all_frames:
            frame = frame_data['frame']
            description = frame_data['description']
            has_np = frame_data['has_np']
            
            if not frame:
                continue
            
            if frame in existing:
                if existing[frame].get('description', '') != description and description:
                    existing[frame]['description'] = description
                    stats['updated'] += 1
                else:
                    stats['duplicates'] += 1
            else:
                self.catalog.append({
                    'frame': frame,
                    'description': description
                })
                stats['added'] += 1
                existing[frame] = self.catalog[-1]
                if has_np:
                    stats['with_np'] += 1
                else:
                    stats['without_np'] += 1
        
        stats['total'] = len(self.catalog)
        self._save()
        
        return stats
    
    def get_catalog(self) -> List[Dict]:
        """Возвращает копию каталога"""
        return self.catalog.copy()
    
    def get_statistics(self) -> Dict:
        """Возвращает расширенную статистику каталога"""
        total = len(self.catalog)
        with_description = sum(1 for item in self.catalog if item.get('description') and item['description'].strip())
        
        recent_items = []
        if self.catalog:
            recent_items = self.catalog[-5:]
        
        desc_lengths = [len(str(item.get('description', ''))) for item in self.catalog if item.get('description')]
        avg_desc_length = sum(desc_lengths) / len(desc_lengths) if desc_lengths else 0
        
        return {
            'total': total,
            'with_description': with_description,
            'without_description': total - with_description,
            'recent_items': recent_items,
            'avg_description_length': round(avg_desc_length, 0)
        }
    
    def get_photo_details(self, photo_num: str) -> Optional[str]:
        """Возвращает описание снимка из каталога АФС"""
        for item in self.catalog:
            if item['frame'] == photo_num:
                return item.get('description', '')
        return None
    
    def clear(self) -> int:
        """Очищает каталог"""
        removed = len(self.catalog)
        self.catalog = []
        self._save()
        logger.info(f"🗑️ Каталог АФС очищен, удалено {removed} снимков")
        return removed
    
    def is_empty(self) -> bool:
        return len(self.catalog) == 0