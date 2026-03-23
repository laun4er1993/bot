# services/village_db.py
import os
import csv
import time
import logging
from typing import List, Dict, Optional, Tuple

from config import VILLAGES_FILE
from api_sources import AVAILABLE_DISTRICTS

logger = logging.getLogger(__name__)


class VillageDatabase:
    """База данных населенных пунктов - сохраняется в TXT файле между перезапусками"""
    
    def __init__(self, txt_path: str = VILLAGES_FILE):
        self.txt_path = txt_path
        self.villages: List[Dict] = []
        self.villages_by_name: Dict[str, List[Dict]] = {}
        self.villages_by_district: Dict[str, List[Dict]] = {}
        self.stats = {'total': 0, 'with_coords': 0, 'last_update': None, 'source_file': None}
        self._load()
    
    def _load(self):
        """Загружает базу из TXT файла"""
        if not os.path.exists(self.txt_path):
            self._create_empty()
            return
        try:
            with open(self.txt_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
                if not lines:
                    self._create_empty()
                    return
                
                for line in lines[1:]:
                    line = line.strip()
                    if not line:
                        continue
                    
                    parts = line.split()
                    if len(parts) >= 5:
                        name = parts[0]
                        village_type = parts[1]
                        lat = parts[2] if parts[2] != '-' else ''
                        lon = parts[3] if parts[3] != '-' else ''
                        district = parts[4]
                        
                        if len(parts) > 5:
                            name = ' '.join(parts[:-4])
                            village_type = parts[-4]
                            lat = parts[-3] if parts[-3] != '-' else ''
                            lon = parts[-2] if parts[-2] != '-' else ''
                            district = parts[-1]
                        
                        village = {
                            'name': name,
                            'type': village_type,
                            'lat': lat,
                            'lon': lon,
                            'district': district
                        }
                        self.villages.append(village)
            
            self._build_indexes()
            logger.info(f"✅ Загружено {self.stats['total']} населенных пунктов из TXT")
        except Exception as e:
            logger.error(f"❌ Ошибка загрузки: {e}")
            self._create_empty()
    
    def _build_indexes(self):
        """Перестраивает индексы"""
        self.villages_by_name.clear()
        self.villages_by_district.clear()
        with_coords = 0
        
        for v in self.villages:
            name_lower = v['name'].lower()
            if name_lower not in self.villages_by_name:
                self.villages_by_name[name_lower] = []
            self.villages_by_name[name_lower].append(v)
            
            district = v.get('district', '')
            if district:
                if district not in self.villages_by_district:
                    self.villages_by_district[district] = []
                self.villages_by_district[district].append(v)
            
            if v.get('lat') and v.get('lon') and v['lat'].strip() and v['lon'].strip():
                with_coords += 1
        
        self.stats['total'] = len(self.villages)
        self.stats['with_coords'] = with_coords
    
    def _create_empty(self):
        """Создает пустую базу данных"""
        os.makedirs(os.path.dirname(self.txt_path), exist_ok=True)
        with open(self.txt_path, 'w', encoding='utf-8') as f:
            f.write("Название Тип Широта Долгота Район\n")
        self.villages = []
        self.villages_by_name = {}
        self.villages_by_district = {}
        self.stats = {'total': 0, 'with_coords': 0, 'last_update': None, 'source_file': None}
    
    def _save(self):
        """Сохраняет базу в TXT файл"""
        if not self.villages:
            return
        
        with open(self.txt_path, 'w', encoding='utf-8') as f:
            f.write("Название Тип Широта Долгота Район\n")
            for v in self.villages:
                lat = v.get('lat', '') if v.get('lat') else '-'
                lon = v.get('lon', '') if v.get('lon') else '-'
                f.write(f"{v['name']} {v['type']} {lat} {lon} {v['district']}\n")
    
    def add_village(self, village: Dict) -> Tuple[bool, str]:
        name = village.get('name', '').strip()
        if not name:
            return False, "Название не указано"
        
        name_lower = name.lower()
        if name_lower in self.villages_by_name:
            return False, f"Населенный пункт '{name}' уже существует в каталоге"
        
        if not village.get('type'):
            village['type'] = 'деревня'
        if not village.get('district'):
            village['district'] = ''
        if not village.get('lat'):
            village['lat'] = ''
        if not village.get('lon'):
            village['lon'] = ''
        
        self.villages.append(village)
        
        name_lower = village['name'].lower()
        if name_lower not in self.villages_by_name:
            self.villages_by_name[name_lower] = []
        self.villages_by_name[name_lower].append(village)
        
        district = village.get('district', '')
        if district:
            if district not in self.villages_by_district:
                self.villages_by_district[district] = []
            self.villages_by_district[district].append(village)
        
        if village.get('lat') and village.get('lon') and village['lat'].strip() and village['lon'].strip():
            self.stats['with_coords'] += 1
        
        self.stats['total'] = len(self.villages)
        self._save()
        
        return True, f"Населенный пункт '{name}' добавлен"
    
    def add_villages_batch(self, villages: List[Dict]) -> Dict:
        stats = {'added': 0, 'duplicates': 0, 'errors': 0, 'villages': []}
        
        for village in villages:
            name = village.get('name', '').strip()
            if not name:
                stats['errors'] += 1
                continue
            
            name_lower = name.lower()
            if name_lower in self.villages_by_name:
                stats['duplicates'] += 1
                continue
            
            if not village.get('type'):
                village['type'] = 'деревня'
            if not village.get('district'):
                village['district'] = ''
            if not village.get('lat'):
                village['lat'] = ''
            if not village.get('lon'):
                village['lon'] = ''
            
            self.villages.append(village)
            stats['villages'].append(village)
            stats['added'] += 1
            
            name_lower = village['name'].lower()
            if name_lower not in self.villages_by_name:
                self.villages_by_name[name_lower] = []
            self.villages_by_name[name_lower].append(village)
            
            district = village.get('district', '')
            if district:
                if district not in self.villages_by_district:
                    self.villages_by_district[district] = []
                self.villages_by_district[district].append(village)
        
        self.stats['total'] = len(self.villages)
        self.stats['with_coords'] = sum(1 for v in self.villages if v.get('lat') and v.get('lon') and v['lat'].strip() and v['lon'].strip())
        self._save()
        
        return stats
    
    def remove_district(self, district: str) -> Tuple[int, int]:
        if district not in self.villages_by_district:
            return 0, 0
        
        removed_villages = self.villages_by_district[district]
        removed_count = len(removed_villages)
        
        self.villages = [v for v in self.villages if v.get('district', '') != district]
        
        self._build_indexes()
        self._save()
        
        return removed_count, sum(1 for v in removed_villages if v.get('lat') and v.get('lon') and v['lat'].strip() and v['lon'].strip())
    
    def clear_all(self) -> int:
        removed_count = len(self.villages)
        self.villages = []
        self.villages_by_name.clear()
        self.villages_by_district.clear()
        self.stats = {'total': 0, 'with_coords': 0, 'last_update': None, 'source_file': None}
        self._save()
        return removed_count
    
    def get_villages_by_district(self, district: str) -> List[Dict]:
        return self.villages_by_district.get(district, [])
    
    def get_districts(self) -> List[str]:
        return sorted(self.villages_by_district.keys())
    
    def search(self, query: str) -> List[Dict]:
        if not query:
            return []
        query_lower = query.lower().strip()
        results = []
        seen = set()
        if query_lower in self.villages_by_name:
            for v in self.villages_by_name[query_lower]:
                if v['name'] not in seen:
                    results.append(v)
                    seen.add(v['name'])
        for name, villages in self.villages_by_name.items():
            if query_lower in name and name != query_lower:
                for v in villages:
                    if v['name'] not in seen:
                        results.append(v)
                        seen.add(v['name'])
        return results
    
    def get_stats(self) -> Dict:
        return self.stats.copy()
    
    def export_to_txt(self, filename: str = None) -> str:
        from config import EXPORT_DIR
        if not filename:
            filename = f"населенные_пункты_{time.strftime('%Y%m%d_%H%M%S')}.txt"
        
        os.makedirs(EXPORT_DIR, exist_ok=True)
        file_path = os.path.join(EXPORT_DIR, filename)
        
        with open(file_path, 'w', encoding='cp1251', newline='') as f:
            f.write("Название Тип Широта Долгота Район\n")
            for v in self.villages:
                lat = v.get('lat', '') if v.get('lat') else '-'
                lon = v.get('lon', '') if v.get('lon') else '-'
                f.write(f"{v['name']} {v['type']} {lat} {lon} {v['district']}\n")
        
        return file_path