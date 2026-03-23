# services/kml_processor.py
import os
import time
import logging
from typing import List, Dict, Tuple, Optional
from bs4 import BeautifulSoup
from shapely.geometry import Polygon, Point

from config import EXPORT_DIR, KML_MARGIN_M

logger = logging.getLogger(__name__)


class KMLProcessor:
    """Обработчик KML файлов для поиска населенных пунктов в кадрах"""
    
    def __init__(self, village_db, photos_db):
        self.village_db = village_db
        self.photos_db = photos_db
    
    def process_kml_file(self, kml_path: str, margin_m: float = KML_MARGIN_M) -> Dict:
        """Обрабатывает KML файл и возвращает подробный результат"""
        with open(kml_path, 'r', encoding='utf-8') as f:
            soup = BeautifulSoup(f.read(), 'xml')
        
        results = []
        frames_without_np = []
        district_counter = {}
        
        for placemark in soup.find_all('Placemark'):
            name_elem = placemark.find('name')
            if not name_elem or not name_elem.text.startswith('Frame-'):
                continue
            
            photo_num = name_elem.text.replace('Frame-', '')
            description = self.photos_db.get_photo_details(photo_num)
            
            polygon = placemark.find('Polygon')
            if not polygon:
                frames_without_np.append({'frame': photo_num, 'description': description})
                continue
                
            coords_elem = polygon.find('coordinates')
            if not coords_elem:
                frames_without_np.append({'frame': photo_num, 'description': description})
                continue
            
            coordinates = self._parse_coords(coords_elem.text.strip())
            if not coordinates:
                frames_without_np.append({'frame': photo_num, 'description': description})
                continue
            
            result = self._process_polygon(photo_num, coordinates, margin_m, description)
            
            if result['village_count'] > 0:
                results.append(result)
                for village in result['villages_with_district']:
                    district = village['district']
                    district_counter[district] = district_counter.get(district, 0) + 1
            else:
                frames_without_np.append({'frame': photo_num, 'description': description})
        
        results.sort(key=lambda x: x['village_count'], reverse=True)
        
        top_frames = [
            {'frame': r['photo_num'], 'count': r['village_count'], 'description': r.get('description')}
            for r in results[:10]
        ]
        
        district_stats = sorted(
            [{'district': d, 'count': c} for d, c in district_counter.items()],
            key=lambda x: x['count'],
            reverse=True
        )
        
        stats = {
            'total_frames': len(results) + len(frames_without_np),
            'frames_with_np': len(results),
            'frames_without_np': len(frames_without_np),
            'total_relations': sum(r['village_count'] for r in results),
            'avg_np_per_frame': round(sum(r['village_count'] for r in results) / len(results), 2) if results else 0
        }
        
        return {
            'results': results,
            'stats': stats,
            'frames_without_np': frames_without_np,
            'district_stats': district_stats,
            'top_frames': top_frames
        }
    
    def _parse_coords(self, coords_text: str) -> List[Tuple[float, float]]:
        coords = []
        for point in coords_text.strip().split():
            parts = point.split(',')
            if len(parts) >= 2:
                coords.append((float(parts[1]), float(parts[0])))
        return coords
    
    def _process_polygon(self, photo_num: str, coordinates: List[Tuple[float, float]], margin_m: float, description: str = None) -> Dict:
        margin_deg = margin_m / 111000
        lats = [c[0] for c in coordinates]
        lons = [c[1] for c in coordinates]
        bbox = (min(lats) - margin_deg, max(lats) + margin_deg,
                min(lons) - margin_deg, max(lons) + margin_deg)
        
        polygon = Polygon([(lon, lat) for lat, lon in coordinates])
        buffered = polygon.buffer(margin_deg)
        
        villages_in_photo = []
        villages_with_district = []
        
        for v in self.village_db.villages:
            if not v.get('lat') or not v.get('lon'):
                continue
            try:
                lat = float(v['lat'])
                lon = float(v['lon'])
                if bbox[0] <= lat <= bbox[1] and bbox[2] <= lon <= bbox[3]:
                    if buffered.contains(Point(lon, lat)):
                        villages_in_photo.append(v['name'])
                        villages_with_district.append({
                            'name': v['name'],
                            'type': v.get('type', 'деревня'),
                            'district': v.get('district', ''),
                            'lat': v['lat'],
                            'lon': v['lon']
                        })
            except:
                continue
        
        return {
            'photo_num': photo_num,
            'villages': villages_in_photo,
            'villages_with_district': villages_with_district,
            'village_count': len(villages_in_photo),
            'description': description
        }
    
    def generate_report(self, data: Dict, filename: str) -> str:
        os.makedirs(EXPORT_DIR, exist_ok=True)
        
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        report_filename = f"kml_report_{timestamp}.txt"
        file_path = os.path.join(EXPORT_DIR, report_filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write("=" * 100 + "\n")
            f.write("ОТЧЕТ ПО ОБРАБОТКЕ KML ФАЙЛА\n")
            f.write("=" * 100 + "\n")
            f.write(f"Дата обработки: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Исходный файл: {filename}\n\n