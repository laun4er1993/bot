# services/kml_processor.py
import os
import time
import math
import logging
from typing import List, Dict, Tuple, Optional
from bs4 import BeautifulSoup
from shapely.geometry import Polygon, Point
from shapely.validation import make_valid

from config import EXPORT_DIR, KML_MARGIN_M, KML_USE_INTERSECTS, KML_CACHE_POLYGONS

logger = logging.getLogger(__name__)


class KMLProcessor:
    """Обработчик KML файлов для поиска населенных пунктов в кадрах"""
    
    def __init__(self, village_db, photos_db):
        self.village_db = village_db
        self.photos_db = photos_db
        self.polygon_cache: Dict[str, Polygon] = {} if KML_CACHE_POLYGONS else None
    
    def _meters_to_degrees(self, lat: float, meters: float) -> float:
        """
        Конвертирует метры в градусы с учетом широты.
        
        Args:
            lat: широта в градусах
            meters: расстояние в метрах
        
        Returns:
            расстояние в градусах
        """
        # 1 градус широты ≈ 111.32 км
        lat_deg_per_meter = 1.0 / 111320.0
        
        # Для долготы коэффициент зависит от широты
        # Используем среднее значение для простоты
        return meters * lat_deg_per_meter
    
    def _parse_coords(self, coords_text: str) -> List[Tuple[float, float]]:
        """
        Парсит координаты из KML.
        
        Формат KML: долгота, широта, высота (lon, lat, alt)
        Возвращает: список кортежей (широта, долгота) для удобства работы
        
        Args:
            coords_text: строка с координатами из KML
        
        Returns:
            список координат в формате (широта, долгота)
        """
        coords = []
        for point in coords_text.strip().split():
            parts = point.split(',')
            if len(parts) >= 2:
                try:
                    lon = float(parts[0])
                    lat = float(parts[1])
                    coords.append((lat, lon))
                except ValueError:
                    logger.warning(f"Ошибка парсинга координат: {point}")
                    continue
        return coords
    
    def _create_buffered_polygon(self, photo_num: str, coordinates: List[Tuple[float, float]], margin_m: float) -> Optional[Polygon]:
        """
        Создает буферизованный полигон для кадра.
        
        Args:
            photo_num: номер снимка (для кэша)
            coordinates: список координат в формате (широта, долгота)
            margin_m: буфер в метрах
        
        Returns:
            буферизованный полигон Shapely или None при ошибке
        """
        # Проверяем кэш
        if KML_CACHE_POLYGONS and self.polygon_cache and photo_num in self.polygon_cache:
            logger.debug(f"  Используем кэшированный полигон для {photo_num}")
            return self.polygon_cache[photo_num]
        
        try:
            # Вычисляем среднюю широту для точной конвертации буфера
            avg_lat = sum(lat for lat, _ in coordinates) / len(coordinates)
            margin_deg = self._meters_to_degrees(avg_lat, margin_m)
            
            # Создаем полигон в формате (долгота, широта) для Shapely
            polygon_points = [(lon, lat) for lat, lon in coordinates]
            polygon = Polygon(polygon_points)
            
            # Проверяем валидность полигона
            if not polygon.is_valid:
                logger.warning(f"  Некорректный полигон для {photo_num}, исправляем...")
                polygon = make_valid(polygon)
                if polygon.is_empty:
                    logger.error(f"  Не удалось исправить полигон для {photo_num}")
                    return None
            
            # Создаем буферизованный полигон
            buffered = polygon.buffer(margin_deg)
            
            # Кэшируем
            if KML_CACHE_POLYGONS and self.polygon_cache is not None:
                self.polygon_cache[photo_num] = buffered
            
            return buffered
            
        except Exception as e:
            logger.error(f"  Ошибка создания полигона для {photo_num}: {e}")
            return None
    
    def _get_bbox(self, coordinates: List[Tuple[float, float]], margin_m: float) -> Tuple[float, float, float, float]:
        """
        Вычисляет ограничивающий прямоугольник (bbox) для быстрой фильтрации.
        
        Args:
            coordinates: список координат в формате (широта, долгота)
            margin_m: буфер в метрах
        
        Returns:
            кортеж (min_lat, max_lat, min_lon, max_lon)
        """
        lats = [lat for lat, _ in coordinates]
        lons = [lon for _, lon in coordinates]
        
        avg_lat = sum(lats) / len(lats)
        margin_deg = self._meters_to_degrees(avg_lat, margin_m)
        
        return (
            min(lats) - margin_deg,
            max(lats) + margin_deg,
            min(lons) - margin_deg,
            max(lons) + margin_deg
        )
    
    def _point_in_polygon(self, lat: float, lon: float, buffered_polygon: Polygon, bbox: Tuple[float, float, float, float]) -> bool:
        """
        Проверяет, находится ли точка внутри полигона.
        
        Args:
            lat: широта точки
            lon: долгота точки
            buffered_polygon: буферизованный полигон
            bbox: ограничивающий прямоугольник для быстрой фильтрации
        
        Returns:
            True если точка внутри полигона
        """
        # Быстрая фильтрация по bbox
        if not (bbox[0] <= lat <= bbox[1] and bbox[2] <= lon <= bbox[3]):
            return False
        
        # Точная проверка
        point = Point(lon, lat)
        
        if KML_USE_INTERSECTS:
            # intersects возвращает True для точек на границе
            return buffered_polygon.intersects(point)
        else:
            # contains возвращает True только для точек внутри
            return buffered_polygon.contains(point)
    
    def _process_polygon(self, photo_num: str, coordinates: List[Tuple[float, float]], margin_m: float, description: str = None) -> Dict:
        """
        Обрабатывает полигон и находит населенные пункты внутри.
        
        Args:
            photo_num: номер снимка
            coordinates: список координат в формате (широта, долгота)
            margin_m: буфер в метрах
            description: описание снимка
        
        Returns:
            словарь с результатами обработки
        """
        # Вычисляем bbox для быстрой фильтрации
        bbox = self._get_bbox(coordinates, margin_m)
        
        # Создаем буферизованный полигон
        buffered_polygon = self._create_buffered_polygon(photo_num, coordinates, margin_m)
        if buffered_polygon is None:
            return {
                'photo_num': photo_num,
                'villages': [],
                'villages_with_district': [],
                'village_count': 0,
                'description': description,
                'error': True
            }
        
        # Поиск населенных пунктов
        villages_in_photo = []
        villages_with_district = []
        
        for v in self.village_db.villages:
            if not v.get('lat') or not v.get('lon'):
                continue
            
            try:
                lat = float(v['lat'])
                lon = float(v['lon'])
                
                if self._point_in_polygon(lat, lon, buffered_polygon, bbox):
                    villages_in_photo.append(v['name'])
                    villages_with_district.append({
                        'name': v['name'],
                        'type': v.get('type', 'деревня'),
                        'district': v.get('district', ''),
                        'lat': v['lat'],
                        'lon': v['lon']
                    })
            except Exception as e:
                logger.debug(f"  Ошибка проверки точки {v['name']}: {e}")
                continue
        
        return {
            'photo_num': photo_num,
            'villages': villages_in_photo,
            'villages_with_district': villages_with_district,
            'village_count': len(villages_in_photo),
            'description': description,
            'error': False
        }
    
    def process_kml_file(self, kml_path: str, margin_m: float = KML_MARGIN_M) -> Dict:
        """
        Обрабатывает KML файл и возвращает подробный результат.
        
        Args:
            kml_path: путь к KML файлу
            margin_m: буфер в метрах для расширения границ кадра
        
        Returns:
            словарь с результатами обработки
        """
        # Очищаем кэш перед обработкой нового файла
        if KML_CACHE_POLYGONS and self.polygon_cache is not None:
            self.polygon_cache.clear()
            logger.debug("Кэш полигонов очищен")
        
        try:
            with open(kml_path, 'r', encoding='utf-8') as f:
                soup = BeautifulSoup(f.read(), 'xml')
        except Exception as e:
            logger.error(f"Ошибка чтения KML файла: {e}")
            return {
                'results': [],
                'stats': {'total_frames': 0, 'frames_with_np': 0, 'frames_without_np': 0, 'total_relations': 0, 'avg_np_per_frame': 0},
                'frames_without_np': [],
                'district_stats': [],
                'top_frames': [],
                'error': str(e)
            }
        
        results = []
        frames_without_np = []
        district_counter = {}
        errors = []
        
        for placemark in soup.find_all('Placemark'):
            name_elem = placemark.find('name')
            if not name_elem or not name_elem.text.startswith('Frame-'):
                continue
            
            photo_num = name_elem.text.replace('Frame-', '')
            description = self.photos_db.get_photo_details(photo_num)
            
            # Поиск полигона
            polygon_elem = placemark.find('Polygon')
            if not polygon_elem:
                frames_without_np.append({'frame': photo_num, 'description': description, 'error': 'no_polygon'})
                continue
            
            coords_elem = polygon_elem.find('coordinates')
            if not coords_elem:
                frames_without_np.append({'frame': photo_num, 'description': description, 'error': 'no_coordinates'})
                continue
            
            # Парсим координаты
            coordinates = self._parse_coords(coords_elem.text.strip())
            if len(coordinates) < 3:
                frames_without_np.append({'frame': photo_num, 'description': description, 'error': 'invalid_coordinates'})
                continue
            
            # Обрабатываем полигон
            result = self._process_polygon(photo_num, coordinates, margin_m, description)
            
            if result.get('error'):
                errors.append({'frame': photo_num, 'error': result.get('error')})
                frames_without_np.append({'frame': photo_num, 'description': description, 'error': 'polygon_error'})
                continue
            
            if result['village_count'] > 0:
                results.append(result)
                for village in result['villages_with_district']:
                    district = village['district']
                    if district:
                        district_counter[district] = district_counter.get(district, 0) + 1
            else:
                frames_without_np.append({'frame': photo_num, 'description': description})
        
        # Сортируем результаты по количеству НП (по убыванию)
        results.sort(key=lambda x: x['village_count'], reverse=True)
        
        # Топ-10 снимков по количеству НП
        top_frames = [
            {'frame': r['photo_num'], 'count': r['village_count'], 'description': r.get('description')}
            for r in results[:10]
        ]
        
        # Статистика по районам
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
            'avg_np_per_frame': round(sum(r['village_count'] for r in results) / len(results), 2) if results else 0,
            'errors': len(errors)
        }
        
        logger.info(f"Обработка KML завершена: {stats['total_frames']} кадров, {stats['frames_with_np']} с НП, {stats['total_relations']} связей")
        
        return {
            'results': results,
            'stats': stats,
            'frames_without_np': frames_without_np,
            'district_stats': district_stats,
            'top_frames': top_frames,
            'errors': errors
        }
    
    def generate_report(self, data: Dict, filename: str) -> str:
        """
        Генерирует TXT отчет по результатам обработки KML.
        
        Args:
            data: словарь с результатами обработки (из process_kml_file)
            filename: имя исходного KML файла
        
        Returns:
            путь к созданному TXT файлу
        """
        os.makedirs(EXPORT_DIR, exist_ok=True)
        
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        report_filename = f"kml_report_{timestamp}.txt"
        file_path = os.path.join(EXPORT_DIR, report_filename)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            # Заголовок
            f.write("=" * 100 + "\n")
            f.write("ОТЧЕТ ПО ОБРАБОТКЕ KML ФАЙЛА\n")
            f.write("=" * 100 + "\n")
            f.write(f"Дата обработки: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Исходный файл: {filename}\n")
            f.write(f"Буфер: {KML_MARGIN_M} м\n")
            f.write(f"Метод проверки: {'intersects' if KML_USE_INTERSECTS else 'contains'}\n\n")
            
            # Общая статистика
            stats = data['stats']
            f.write("=" * 100 + "\n")
            f.write("ОБЩАЯ СТАТИСТИКА\n")
            f.write("=" * 100 + "\n")
            f.write(f"Всего обработано снимков: {stats['total_frames']}\n")
            f.write(f"Снимков с населенными пунктами: {stats['frames_with_np']}\n")
            f.write(f"Снимков без населенных пунктов: {stats['frames_without_np']}\n")
            f.write(f"Всего связей (НП в кадрах): {stats['total_relations']}\n")
            if stats['frames_with_np'] > 0:
                f.write(f"Среднее количество НП на кадр: {stats['avg_np_per_frame']}\n")
            if stats.get('errors', 0) > 0:
                f.write(f"Ошибок при обработке: {stats['errors']}\n")
            f.write("\n")
            
            # Топ снимков
            if data['top_frames']:
                f.write("=" * 100 + "\n")
                f.write("ТОП-10 СНИМКОВ ПО КОЛИЧЕСТВУ НП\n")
                f.write("=" * 100 + "\n")
                for i, frame in enumerate(data['top_frames'][:10], 1):
                    f.write(f"{i}. {frame['frame']}: {frame['count']} населенных пунктов\n")
                    if frame.get('description'):
                        f.write(f"\n   📝 ПОЛНОЕ ОПИСАНИЕ СНИМКА:\n")
                        f.write(f"   {'=' * 80}\n")
                        # Разбиваем длинное описание на строки
                        desc_lines = frame['description'].split('\n')
                        for line in desc_lines:
                            f.write(f"   {line}\n")
                        f.write(f"   {'=' * 80}\n")
                f.write("\n")
            
            # Статистика по районам
            if data['district_stats']:
                f.write("=" * 100 + "\n")
                f.write("СТАТИСТИКА ПО РАЙОНАМ\n")
                f.write("=" * 100 + "\n")
                total = stats['total_relations']
                for district in data['district_stats']:
                    percent = (district['count'] / total * 100) if total > 0 else 0
                    f.write(f"{district['district']} район: {district['count']} НП ({percent:.1f}%)\n")
                f.write("\n")
            
            # Детальный список по каждому снимку с НП
            if data['results']:
                f.write("=" * 100 + "\n")
                f.write("ДЕТАЛЬНЫЙ СПИСОК ПО КАЖДОМУ СНИМКУ\n")
                f.write("=" * 100 + "\n\n")
                
                for result in data['results']:
                    f.write("-" * 100 + "\n")
                    f.write(f"Снимок: {result['photo_num']}\n")
                    f.write(f"Количество НП: {result['village_count']}\n")
                    
                    if result.get('description'):
                        f.write(f"\n📝 ПОЛНОЕ ОПИСАНИЕ СНИМКА:\n")
                        f.write(f"{'=' * 80}\n")
                        desc_lines = result['description'].split('\n')
                        for line in desc_lines:
                            f.write(f"{line}\n")
                        f.write(f"{'=' * 80}\n\n")
                    
                    if result['villages_with_district']:
                        f.write("Населенные пункты в кадре:\n")
                        for i, v in enumerate(result['villages_with_district'], 1):
                            coords = f"{v['lat']}, {v['lon']}" if v['lat'] and v['lon'] else "координаты не указаны"
                            f.write(f"  {i}. {v['name']} ({v['type']}, {v['district']} район)\n")
                            f.write(f"     Координаты: {coords}\n")
                    f.write("\n")
            
            # Снимки без населенных пунктов
            if data['frames_without_np']:
                f.write("=" * 100 + "\n")
                f.write(f"СНИМКИ БЕЗ НАСЕЛЕННЫХ ПУНКТОВ ({len(data['frames_without_np'])} шт.)\n")
                f.write("=" * 100 + "\n")
                for frame in data['frames_without_np']:
                    f.write(f"• {frame['frame']}")
                    if frame.get('error'):
                        f.write(f" [ОШИБКА: {frame['error']}]")
                    if frame.get('description'):
                        f.write(f"\n  📝 ПОЛНОЕ ОПИСАНИЕ:\n")
                        f.write(f"  {'-' * 80}\n")
                        desc_lines = frame['description'].split('\n')
                        for line in desc_lines:
                            f.write(f"  {line}\n")
                        f.write(f"  {'-' * 80}\n")
                    else:
                        f.write("\n")
                f.write("\n")
            
            # Ошибки
            if data.get('errors'):
                f.write("=" * 100 + "\n")
                f.write("ОШИБКИ ПРИ ОБРАБОТКЕ\n")
                f.write("=" * 100 + "\n")
                for err in data['errors']:
                    f.write(f"• {err['frame']}: {err['error']}\n")
                f.write("\n")
            
            # Параметры обработки
            f.write("=" * 100 + "\n")
            f.write("ПАРАМЕТРЫ ОБРАБОТКИ\n")
            f.write("=" * 100 + "\n")
            f.write(f"Буфер: {KML_MARGIN_M} м\n")
            f.write(f"Метод проверки: {'intersects (точки на границе считаются внутри)' if KML_USE_INTERSECTS else 'contains (только точки строго внутри)'}\n")
            f.write(f"Кэширование полигонов: {'включено' if KML_CACHE_POLYGONS else 'отключено'}\n\n")
            
            # Конец отчета
            f.write("=" * 100 + "\n")
            f.write("КОНЕЦ ОТЧЕТА\n")
            f.write("=" * 100 + "\n")
        
        logger.info(f"Отчет сохранен: {file_path}")
        return file_path
    
    def clear_cache(self):
        """Очищает кэш полигонов"""
        if self.polygon_cache is not None:
            self.polygon_cache.clear()
            logger.debug("Кэш полигонов очищен")