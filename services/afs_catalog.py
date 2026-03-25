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
        """
        Создает каталог АФС из ВСЕХ результатов обработки KML
        (включая снимки без населенных пунктов)
        
        Args:
            results: список снимков с населенными пунктами
            frames_without_np: список снимков без населенных пунктов
        
        Returns:
            словарь со статистикой
        """
        stats = {'added': 0, 'duplicates': 0, 'total': 0, 'with_np': 0, 'without_np': 0}
        
        existing_frames = {item['frame'] for item in self.catalog}
        
        # Собираем все снимки из results (с НП)
        all_frames = []
        for result in results:
            all_frames.append({
                'frame': result.get('photo_num', ''),
                'description': result.get('description', ''),
                'has_np': True
            })
        
        # Добавляем снимки без НП
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
                logger.info(f"  ⚠️ Снимок {frame} уже существует, пропущен")
                continue
            
            self.catalog.append({
                'frame': frame,
                'description': description
            })
            stats['added'] += 1
            existing_frames.add(frame)
            
            if has_np:
                stats['with_np'] += 1
                logger.info(f"  ✅ Добавлен снимок с НП: {frame} (описание: {description[:50] if description else 'нет'})")
            else:
                stats['without_np'] += 1
                logger.info(f"  ✅ Добавлен снимок БЕЗ НП: {frame} (описание: {description[:50] if description else 'нет'})")
        
        stats['total'] = len(self.catalog)
        self._save()
        
        logger.info(f"✅ Создание каталога АФС завершено: добавлено {stats['added']}, из них с НП: {stats['with_np']}, без НП: {stats['without_np']}, пропущено дубликатов: {stats['duplicates']}")
        
        return stats
    
    def add_from_kml_results(self, results: List[Dict], frames_without_np: List[Dict]) -> Dict:
        """Дополняет существующий каталог из ВСЕХ результатов обработки KML"""
        stats = {'added': 0, 'updated': 0, 'duplicates': 0, 'total': 0, 'with_np': 0, 'without_np': 0}
        
        existing = {item['frame']: item for item in self.catalog}
        
        # Собираем все снимки из results (с НП)
        all_frames = []
        for result in results:
            all_frames.append({
                'frame': result.get('photo_num', ''),
                'description': result.get('description', ''),
                'has_np': True
            })
        
        # Добавляем снимки без НП
        for frame_data in frames_without_np:
            all_frames.append({
                'frame': frame_data.get('frame', ''),
                'description': frame_data.get('description', ''),
                'has_np': False
            })
        
        logger.info(f"📁 ДОПОЛНЕНИЕ КАТАЛОГА АФС из {len(all_frames)} снимков (с НП: {len(results)}, без НП: {len(frames_without_np)})")
        
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
                    logger.info(f"  🔄 Обновлено описание для {frame}")
                else:
                    stats['duplicates'] += 1
                    logger.info(f"  ⚠️ Снимок {frame} уже существует, пропущен")
            else:
                self.catalog.append({
                    'frame': frame,
                    'description': description
                })
                stats['added'] += 1
                existing[frame] = self.catalog[-1]
                
                if has_np:
                    stats['with_np'] += 1
                    logger.info(f"  ✅ Добавлен снимок с НП: {frame}")
                else:
                    stats['without_np'] += 1
                    logger.info(f"  ✅ Добавлен снимок БЕЗ НП: {frame}")
        
        stats['total'] = len(self.catalog)
        self._save()
        
        logger.info(f"✅ Дополнение каталога АФС завершено: добавлено {stats['added']}, обновлено {stats['updated']}, пропущено {stats['duplicates']}")
        
        return stats
    
    def replace_with_kml_results(self, results: List[Dict], frames_without_np: List[Dict]) -> Dict:
        """Заменяет существующий каталог новыми данными из ВСЕХ результатов KML"""
        old_count = len(self.catalog)
        self.catalog = []
        
        # Собираем все снимки из results (с НП)
        all_frames = []
        for result in results:
            all_frames.append({
                'frame': result.get('photo_num', ''),
                'description': result.get('description', ''),
                'has_np': True
            })
        
        # Добавляем снимки без НП
        for frame_data in frames_without_np:
            all_frames.append({
                'frame': frame_data.get('frame', ''),
                'description': frame_data.get('description', ''),
                'has_np': False
            })
        
        logger.info(f"📁 ЗАМЕНА КАТАЛОГА АФС: {old_count} старых -> {len(all_frames)} новых")
        
        stats = {'with_np': 0, 'without_np': 0}
        
        for frame_data in all_frames:
            frame = frame_data['frame']
            description = frame_data['description']
            has_np = frame_data['has_np']
            
            if frame:
                self.catalog.append({
                    'frame': frame,
                    'description': description
                })
                if has_np:
                    stats['with_np'] += 1
                    logger.info(f"  ✅ Добавлен снимок с НП: {frame}")
                else:
                    stats['without_np'] += 1
                    logger.info(f"  ✅ Добавлен снимок БЕЗ НП: {frame}")
        
        result_stats = {
            'added': len(self.catalog),
            'removed': old_count,
            'total': len(self.catalog),
            'with_np': stats['with_np'],
            'without_np': stats['without_np']
        }
        
        self._save()
        
        logger.info(f"✅ Замена каталога АФС завершено: добавлено {result_stats['added']}, удалено {result_stats['removed']}, из них с НП: {result_stats['with_np']}, без НП: {result_stats['without_np']}")
        
        return result_stats
    
    def merge_with_catalog(self, new_catalog: List[Dict]) -> Dict:
        """Сливает текущий каталог с другим каталогом"""
        stats = {'added': 0, 'updated': 0, 'duplicates': 0, 'total': 0}
        
        existing = {item['frame']: item for item in self.catalog}
        
        logger.info(f"📁 СЛИЯНИЕ КАТАЛОГОВ АФС: текущий {len(self.catalog)}, добавляется {len(new_catalog)}")
        
        for item in new_catalog:
            frame = item.get('frame', '')
            description = item.get('description', '')
            if description is None:
                description = ''
            
            if not frame:
                continue
            
            if frame in existing:
                if existing[frame].get('description', '') != description and description:
                    existing[frame]['description'] = description
                    stats['updated'] += 1
                    logger.info(f"  🔄 Обновлено описание для {frame}")
                else:
                    stats['duplicates'] += 1
            else:
                self.catalog.append({
                    'frame': frame,
                    'description': description
                })
                stats['added'] += 1
                existing[frame] = self.catalog[-1]
                logger.info(f"  ✅ Добавлен снимок: {frame}")
        
        stats['total'] = len(self.catalog)
        self._save()
        
        logger.info(f"✅ Слияние каталогов АФС завершено: добавлено {stats['added']}, обновлено {stats['updated']}")
        
        return stats
    
    def compare_with_catalog(self, other_catalog: List[Dict]) -> Dict:
        """Сравнивает текущий каталог с другим каталогом"""
        current = {item['frame']: item for item in self.catalog}
        other = {item['frame']: item for item in other_catalog}
        
        diff = {
            'new': [],
            'missing': [],
            'different': []
        }
        
        for frame in other:
            if frame not in current:
                diff['new'].append(frame)
        
        for frame in current:
            if frame not in other:
                diff['missing'].append(frame)
        
        for frame in current:
            if frame in other:
                current_desc = current[frame].get('description', '') or ''
                other_desc = other[frame].get('description', '') or ''
                if current_desc != other_desc:
                    diff['different'].append({
                        'frame': frame,
                        'current': current_desc[:100],
                        'other': other_desc[:100]
                    })
        
        logger.info(f"📊 Сравнение каталогов: новые {len(diff['new'])}, отсутствуют {len(diff['missing'])}, различаются {len(diff['different'])}")
        
        return diff
    
    def get_catalog(self) -> List[Dict]:
        """Возвращает копию каталога"""
        return self.catalog.copy()
    
    def get_catalog_text(self, with_descriptions: bool = False, page: int = 1, per_page: int = 50) -> Tuple[str, int, int]:
        """Возвращает текстовое представление каталога с пагинацией"""
        if not self.catalog:
            return "📭 Каталог АФС пуст", 0, 1
        
        total_pages = (len(self.catalog) + per_page - 1) // per_page
        page = max(1, min(page, total_pages))
        
        start = (page - 1) * per_page
        end = start + per_page
        items = self.catalog[start:end]
        
        text = f"📁 <b>Каталог АФС</b> (всего: {len(self.catalog)} снимков, страница {page}/{total_pages})\n\n"
        
        for i, item in enumerate(items, start + 1):
            text += f"{i}. {item['frame']}"
            if with_descriptions and item.get('description'):
                desc_preview = str(item['description'])[:100].replace('\n', ' ')
                if len(str(item['description'])) > 100:
                    desc_preview += "..."
                text += f"\n   📝 {desc_preview}"
            text += "\n"
        
        return text, total_pages, page
    
    def export_to_txt(self, filename: str = None) -> str:
        """Экспортирует каталог в TXT файл"""
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
                    f.write(f"{i}. {item['frame']}\n")
                    if item.get('description'):
                        f.write(f"\n   Описание:\n")
                        f.write(f"   {'-' * 76}\n")
                        desc_lines = str(item['description']).split('\n')
                        for line in desc_lines:
                            f.write(f"   {line}\n")
                        f.write(f"   {'-' * 76}\n")
                    f.write("\n")
        except Exception as e:
            logger.error(f"Ошибка экспорта каталога: {e}")
            return None
        
        return file_path
    
    def clear(self) -> int:
        """Очищает каталог"""
        removed = len(self.catalog)
        self.catalog = []
        self._save()
        logger.info(f"🗑️ Каталог АФС очищен, удалено {removed} снимков")
        return removed
    
    def is_empty(self) -> bool:
        """Проверяет, пуст ли каталог"""
        return len(self.catalog) == 0
    
    def get_statistics(self) -> Dict:
        """Возвращает расширенную статистику каталога"""
        total = len(self.catalog)
        with_description = sum(1 for item in self.catalog if item.get('description'))
        
        # Собираем последние 5 добавленных снимков
        recent_items = []
        if self.catalog:
            recent_items = self.catalog[-5:]
        
        # Собираем статистику по длине описаний
        desc_lengths = [len(str(item.get('description', ''))) for item in self.catalog if item.get('description')]
        avg_desc_length = sum(desc_lengths) / len(desc_lengths) if desc_lengths else 0
        
        return {
            'total': total,
            'with_description': with_description,
            'without_description': total - with_description,
            'recent_items': recent_items,
            'avg_description_length': round(avg_desc_length, 0)
        }