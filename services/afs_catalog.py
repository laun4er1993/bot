import os
import time
import logging
import re
from typing import List, Dict, Optional, Tuple

from config import AFS_CATALOG_FILE

logger = logging.getLogger(__name__)


class AFSCatalog:
    """Класс для работы с каталогом аэрофотоснимков (АФС)"""
    
    def __init__(self):
        self.catalog: List[Dict] = []
        self.villages_by_frame: Dict[str, List[str]] = {}
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
                
                for line in lines[1:]:
                    line = line.strip()
                    if not line:
                        continue
                    
                    parts = line.split('|')
                    if len(parts) >= 2:
                        frame = parts[0]
                        description = parts[1] if len(parts) > 1 and parts[1] != 'None' else ''
                        villages_str = parts[2] if len(parts) > 2 else ''
                        
                        self.catalog.append({
                            'frame': frame,
                            'description': description
                        })
                        
                        if villages_str:
                            villages = [v.strip() for v in villages_str.split(',') if v.strip()]
                            self.villages_by_frame[frame] = villages
            
            logger.info(f"✅ Загружено {len(self.catalog)} снимков в каталог АФС")
            logger.info(f"✅ Загружено {sum(len(v) for v in self.villages_by_frame.values())} связей деревень со снимками")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки каталога АФС: {e}")
            self._create_empty()
    
    def _create_empty(self):
        os.makedirs(os.path.dirname(AFS_CATALOG_FILE), exist_ok=True)
        with open(AFS_CATALOG_FILE, 'w', encoding='utf-8') as f:
            f.write("Номер_снимка|Описание|Деревни\n")
        self.catalog = []
        self.villages_by_frame = {}
    
    def _save(self):
        try:
            with open(AFS_CATALOG_FILE, 'w', encoding='utf-8') as f:
                f.write("Номер_снимка|Описание|Деревни\n")
                for item in self.catalog:
                    frame = item['frame']
                    description = item.get('description', '')
                    if description is None:
                        description = ''
                    description = str(description).replace('|', '\\|').replace('\n', ' ')
                    
                    villages = self.villages_by_frame.get(frame, [])
                    villages_str = ','.join(villages) if villages else ''
                    
                    f.write(f"{frame}|{description}|{villages_str}\n")
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
                'villages': result.get('villages', []),
                'has_np': True
            })
            logger.info(f"  📸 Снимок с НП: {result.get('photo_num', '')} - {len(result.get('villages', []))} деревень")
        
        for frame_data in frames_without_np:
            all_frames.append({
                'frame': frame_data.get('frame', ''),
                'description': frame_data.get('description', ''),
                'villages': [],
                'has_np': False
            })
        
        logger.info(f"📁 СОЗДАНИЕ КАТАЛОГА АФС из {len(all_frames)} снимков")
        
        for frame_data in all_frames:
            frame = frame_data['frame']
            description = frame_data['description']
            villages = frame_data['villages']
            has_np = frame_data['has_np']
            
            if not frame:
                continue
            
            if frame in existing_frames:
                stats['duplicates'] += 1
                logger.info(f"  ⚠️ Снимок {frame} уже существует, пропущен")
                continue
            
            self.catalog.append({
                'frame': frame,
                'description': description
            })
            if villages:
                self.villages_by_frame[frame] = villages
            stats['added'] += 1
            existing_frames.add(frame)
            
            if has_np:
                stats['with_np'] += 1
                logger.info(f"  ✅ Добавлен снимок с НП: {frame} ({len(villages)} деревень)")
                if villages:
                    logger.info(f"      📍 Деревни: {', '.join(villages[:10])}")
            else:
                stats['without_np'] += 1
                logger.info(f"  ✅ Добавлен снимок БЕЗ НП: {frame}")
        
        stats['total'] = len(self.catalog)
        self._save()
        
        logger.info(f"✅ Создание каталога АФС завершено: добавлено {stats['added']}, с НП: {stats['with_np']}, без НП: {stats['without_np']}")
        
        return stats
    
    def add_from_kml_results(self, results: List[Dict], frames_without_np: List[Dict]) -> Dict:
        """Дополняет каталог АФС новыми снимками из результатов KML"""
        stats = {'added': 0, 'updated': 0, 'duplicates': 0, 'with_np': 0, 'without_np': 0, 'total': 0}
        
        existing_frames = {item['frame'] for item in self.catalog}
        
        all_frames = []
        for result in results:
            all_frames.append({
                'frame': result.get('photo_num', ''),
                'description': result.get('description', ''),
                'villages': result.get('villages', []),
                'has_np': True
            })
        
        for frame_data in frames_without_np:
            all_frames.append({
                'frame': frame_data.get('frame', ''),
                'description': frame_data.get('description', ''),
                'villages': [],
                'has_np': False
            })
        
        logger.info(f"📁 ДОПОЛНЕНИЕ КАТАЛОГА АФС из {len(all_frames)} снимков")
        
        for frame_data in all_frames:
            frame = frame_data['frame']
            description = frame_data['description']
            villages = frame_data['villages']
            has_np = frame_data['has_np']
            
            if not frame:
                continue
            
            if frame in existing_frames:
                # Проверяем, нужно ли обновить описание
                existing_item = next((item for item in self.catalog if item['frame'] == frame), None)
                if existing_item and existing_item.get('description') != description and description:
                    existing_item['description'] = description
                    stats['updated'] += 1
                    logger.info(f"  📝 Обновлено описание для {frame}")
                else:
                    stats['duplicates'] += 1
                    logger.info(f"  ⚠️ Снимок {frame} уже существует, пропущен")
                continue
            
            self.catalog.append({
                'frame': frame,
                'description': description
            })
            if villages:
                self.villages_by_frame[frame] = villages
            stats['added'] += 1
            existing_frames.add(frame)
            
            if has_np:
                stats['with_np'] += 1
                logger.info(f"  ✅ Добавлен снимок с НП: {frame} ({len(villages)} деревень)")
            else:
                stats['without_np'] += 1
                logger.info(f"  ✅ Добавлен снимок БЕЗ НП: {frame}")
        
        stats['total'] = len(self.catalog)
        self._save()
        
        logger.info(f"✅ Дополнение каталога АФС завершено: добавлено {stats['added']}, обновлено {stats['updated']}")
        
        return stats
    
    def replace_with_kml_results(self, results: List[Dict], frames_without_np: List[Dict]) -> Dict:
        """Заменяет каталог АФС новыми снимками из результатов KML"""
        removed = len(self.catalog)
        
        # Очищаем текущий каталог
        self.catalog = []
        self.villages_by_frame = {}
        
        stats = {'added': 0, 'with_np': 0, 'without_np': 0, 'removed': removed, 'total': 0}
        
        all_frames = []
        for result in results:
            all_frames.append({
                'frame': result.get('photo_num', ''),
                'description': result.get('description', ''),
                'villages': result.get('villages', []),
                'has_np': True
            })
        
        for frame_data in frames_without_np:
            all_frames.append({
                'frame': frame_data.get('frame', ''),
                'description': frame_data.get('description', ''),
                'villages': [],
                'has_np': False
            })
        
        logger.info(f"📁 ЗАМЕНА КАТАЛОГА АФС на {len(all_frames)} снимков")
        
        for frame_data in all_frames:
            frame = frame_data['frame']
            description = frame_data['description']
            villages = frame_data['villages']
            has_np = frame_data['has_np']
            
            if not frame:
                continue
            
            self.catalog.append({
                'frame': frame,
                'description': description
            })
            if villages:
                self.villages_by_frame[frame] = villages
            stats['added'] += 1
            
            if has_np:
                stats['with_np'] += 1
                logger.info(f"  ✅ Добавлен снимок с НП: {frame} ({len(villages)} деревень)")
            else:
                stats['without_np'] += 1
                logger.info(f"  ✅ Добавлен снимок БЕЗ НП: {frame}")
        
        stats['total'] = len(self.catalog)
        self._save()
        
        logger.info(f"✅ Замена каталога АФС завершена: добавлено {stats['added']}, удалено {stats['removed']}")
        
        return stats
    
    def compare_with_catalog(self, other_catalog: List[Dict]) -> Dict:
        """
        Сравнивает текущий каталог с другим каталогом
        Возвращает словарь с ключами: 'new', 'missing', 'different'
        """
        current_frames = {item['frame']: item for item in self.catalog}
        other_frames = {item['frame']: item for item in other_catalog}
        
        new_frames = []
        missing_frames = []
        different_frames = []
        
        # Находим новые снимки (есть в другом, нет в текущем)
        for frame, item in other_frames.items():
            if frame not in current_frames:
                new_frames.append(frame)
        
        # Находим отсутствующие снимки (есть в текущем, нет в другом)
        for frame, item in current_frames.items():
            if frame not in other_frames:
                missing_frames.append(frame)
        
        # Находим снимки с разными описаниями
        for frame, current_item in current_frames.items():
            if frame in other_frames:
                other_item = other_frames[frame]
                if current_item.get('description') != other_item.get('description'):
                    different_frames.append({
                        'frame': frame,
                        'current_desc': current_item.get('description', ''),
                        'other_desc': other_item.get('description', '')
                    })
        
        logger.info(f"📊 Сравнение каталогов: новые={len(new_frames)}, отсутствуют={len(missing_frames)}, различаются={len(different_frames)}")
        
        return {
            'new': new_frames,
            'missing': missing_frames,
            'different': different_frames
        }
    
    def merge_with_catalog(self, other_catalog: List[Dict]) -> Dict:
        """
        Объединяет текущий каталог с другим каталогом
        Добавляет новые снимки, обновляет описания существующих
        """
        stats = {'added': 0, 'updated': 0, 'duplicates': 0, 'total': 0}
        
        existing_frames = {item['frame'] for item in self.catalog}
        
        logger.info(f"📁 ОБЪЕДИНЕНИЕ КАТАЛОГОВ: {len(other_catalog)} снимков для добавления")
        
        for item in other_catalog:
            frame = item.get('frame', '')
            description = item.get('description', '')
            villages = item.get('villages', [])
            
            if not frame:
                continue
            
            if frame in existing_frames:
                # Обновляем описание, если оно изменилось
                existing_item = next((i for i in self.catalog if i['frame'] == frame), None)
                if existing_item and existing_item.get('description') != description and description:
                    existing_item['description'] = description
                    stats['updated'] += 1
                    logger.info(f"  📝 Обновлено описание для {frame}")
                else:
                    stats['duplicates'] += 1
                continue
            
            self.catalog.append({
                'frame': frame,
                'description': description
            })
            if villages:
                self.villages_by_frame[frame] = villages
            stats['added'] += 1
            existing_frames.add(frame)
            logger.info(f"  ✅ Добавлен снимок: {frame}")
        
        stats['total'] = len(self.catalog)
        self._save()
        
        logger.info(f"✅ Объединение каталогов завершено: добавлено {stats['added']}, обновлено {stats['updated']}")
        
        return stats
    
    def get_catalog(self) -> List[Dict]:
        return self.catalog.copy()
    
    def get_catalog_text(self, with_descriptions: bool = False, page: int = 1, per_page: int = 50) -> Tuple[str, int, int]:
        if not self.catalog:
            return "📭 Каталог АФС пуст", 0, 1
        
        total_pages = (len(self.catalog) + per_page - 1) // per_page
        page = max(1, min(page, total_pages))
        
        start = (page - 1) * per_page
        end = start + per_page
        items = self.catalog[start:end]
        
        text = f"📁 <b>Каталог АФС</b> (всего: {len(self.catalog)} снимков, страница {page}/{total_pages})\n\n"
        
        for i, item in enumerate(items, start + 1):
            frame = item['frame']
            villages = self.villages_by_frame.get(frame, [])
            text += f"{i}. {frame}"
            if with_descriptions and item.get('description'):
                text += f"\n   📝 {item['description']}"
            if villages:
                villages_preview = ', '.join(villages[:5])
                if len(villages) > 5:
                    villages_preview += f"... и ещё {len(villages)-5}"
                text += f"\n   📍 Деревни: {villages_preview}"
            text += "\n"
        
        return text, total_pages, page
    
    def get_statistics(self) -> Dict:
        total = len(self.catalog)
        with_description = sum(1 for item in self.catalog if item.get('description') and item['description'].strip())
        with_villages = len(self.villages_by_frame)
        total_village_links = sum(len(v) for v in self.villages_by_frame.values())
        
        recent_items = []
        if self.catalog:
            recent_items = self.catalog[-5:]
        
        desc_lengths = [len(str(item.get('description', ''))) for item in self.catalog if item.get('description')]
        avg_desc_length = sum(desc_lengths) / len(desc_lengths) if desc_lengths else 0
        
        return {
            'total': total,
            'with_description': with_description,
            'without_description': total - with_description,
            'with_villages': with_villages,
            'without_villages': total - with_villages,
            'total_village_links': total_village_links,
            'recent_items': recent_items,
            'avg_description_length': round(avg_desc_length, 0)
        }
    
    def get_photo_details(self, photo_num: str) -> Optional[str]:
        for item in self.catalog:
            if item['frame'] == photo_num:
                return item.get('description', '')
        return None
    
    def get_villages_for_frame(self, photo_num: str) -> List[str]:
        return self.villages_by_frame.get(photo_num, [])
    
    def search_by_village_name(self, village_name: str) -> List[Dict]:
        """Поиск снимков по названию деревни"""
        if not village_name or not self.catalog:
            return []
        
        village_lower = village_name.lower().strip()
        results = []
        
        logger.info(f"🔍 ПОИСК снимков для деревни: {village_name}")
        logger.info(f"   Всего снимков в каталоге: {len(self.catalog)}")
        logger.info(f"   Снимков со связями: {len(self.villages_by_frame)}")
        
        for frame, villages in self.villages_by_frame.items():
            for village in villages:
                if village_lower == village.lower() or village_lower in village.lower():
                    results.append({
                        'frame': frame,
                        'description': self.get_photo_details(frame),
                        'villages': villages
                    })
                    logger.info(f"  ✅ Найден снимок: {frame} (содержит деревню {village})")
                    break
        
        logger.info(f"📊 Найдено {len(results)} снимков для деревни '{village_name}'")
        if not results:
            logger.info("   Снимки в каталоге (первые 10):")
            for i, (frame, villages) in enumerate(list(self.villages_by_frame.items())[:10]):
                logger.info(f"      {i+1}. {frame}: {len(villages)} деревень")
        
        return results
    
    def search_by_frame_name(self, frame_name: str) -> List[Dict]:
        if not frame_name or not self.catalog:
            return []
        
        frame_lower = frame_name.lower().strip()
        results = []
        
        logger.info(f"🔍 ПОИСК снимка по названию: {frame_name}")
        
        if '-' in frame_lower and len(frame_lower.split('-')) == 2:
            suffix = frame_lower
            for item in self.catalog:
                frame = item['frame'].lower()
                if frame.endswith(suffix):
                    results.append({
                        'frame': item['frame'],
                        'description': item.get('description', ''),
                        'villages': self.villages_by_frame.get(item['frame'], [])
                    })
                    logger.info(f"  ✅ Найден снимок: {item['frame']}")
        
        for item in self.catalog:
            frame = item['frame'].lower()
            if frame == frame_lower or frame_lower in frame:
                if not any(r['frame'] == item['frame'] for r in results):
                    results.append({
                        'frame': item['frame'],
                        'description': item.get('description', ''),
                        'villages': self.villages_by_frame.get(item['frame'], [])
                    })
                    logger.info(f"  ✅ Найден снимок: {item['frame']}")
        
        logger.info(f"📊 Найдено {len(results)} снимков по названию '{frame_name}'")
        return results
    
    def export_to_txt(self, filename: str = None) -> str:
        from config import EXPORT_DIR
        
        if not filename:
            filename = f"afs_catalog_{time.strftime('%Y%m%d_%H%M%S')}.txt"
        
        os.makedirs(EXPORT_DIR, exist_ok=True)
        file_path = os.path.join(EXPORT_DIR, filename)
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write("КАТАЛОГ АЭРОФОТОСНИМКОВ (АФС)\n")
                f.write("=" * 80 + "\n")
                f.write(f"Дата создания: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Всего снимков: {len(self.catalog)}\n")
                f.write("=" * 80 + "\n\n")
                
                for i, item in enumerate(self.catalog, 1):
                    frame = item['frame']
                    villages = self.villages_by_frame.get(frame, [])
                    f.write(f"{i}. {frame}\n")
                    if item.get('description'):
                        f.write(f"\n   Описание:\n")
                        f.write(f"   {'-' * 76}\n")
                        f.write(f"   {item['description']}\n")
                        f.write(f"   {'-' * 76}\n")
                    if villages:
                        f.write(f"\n   Населенные пункты в кадре ({len(villages)}):\n")
                        for v in villages:
                            f.write(f"   • {v}\n")
                    f.write("\n")
        except Exception as e:
            logger.error(f"Ошибка экспорта: {e}")
            return None
        
        return file_path
    
    def clear(self) -> int:
        removed = len(self.catalog)
        self.catalog = []
        self.villages_by_frame = {}
        self._save()
        logger.info(f"🗑️ Каталог АФС очищен, удалено {removed} снимков")
        return removed
    
    def is_empty(self) -> bool:
        return len(self.catalog) == 0