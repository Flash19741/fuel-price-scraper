import requests
import csv
import io
from .base import BaseScraper
from db.supabase_client import upsert_station, upsert_price


class ItalyScraper(BaseScraper):
    """
    Собирает цены АЗС Италии с carburanti.mise.gov.it
    Правительственный сайт отдаёт CSV файлы с разделителем ';'
    """
    
    def __init__(self, client):
        super().__init__(client)
        self.country = "IT"
        self.currency = "EUR"
        # Два CSV: один с данными АЗС, другой с ценами
        self.stations_url = "https://www.mimit.gov.it/images/exportCSV/anagrafica_impianti_attivi.csv"
        self.prices_url   = "https://www.mimit.gov.it/images/exportCSV/prezzo_alle_8.csv"
    
    def scrape(self):
        print(f"[IT] Начинаем сбор данных Италии...")
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        # Шаг 1: Загружаем список АЗС
        print("[IT] Загружаем список АЗС...")
        r_stations = requests.get(self.stations_url, headers=headers, timeout=120)
        r_stations.raise_for_status()
        
        # Шаг 2: Загружаем цены
        print("[IT] Загружаем цены...")
        r_prices = requests.get(self.prices_url, headers=headers, timeout=120)
        r_prices.raise_for_status()
        
        # Шаг 3: Парсим АЗС
        stations_dict = self._parse_stations(r_stations.content)
        print(f"[IT] Распарсено {len(stations_dict)} АЗС")
        
        # Шаг 4: Парсим и применяем цены
        self._parse_and_apply_prices(r_prices.content, stations_dict)
        
        print(f"[IT] Готово: {self.stations_count} АЗС, {self.prices_count} цен")
    
    def _parse_stations(self, content: bytes) -> dict:
        """
        Парсит CSV с АЗС.
        Возвращает словарь {source_id: station_dict}
        """
        stations = {}
        
        # Итальянские файлы в кодировке latin-1
        text = content.decode("latin-1")
        
        # Пропускаем первую строку (она содержит дату обновления)
        lines = text.split("\n")
        csv_text = "\n".join(lines[1:])  # Берём начиная со второй строки
        
        reader = csv.DictReader(io.StringIO(csv_text), delimiter=";")
        
        for row in reader:
            source_id = row.get("idImpianto", "").strip()
            if not source_id:
                continue
            
            brand = row.get("Gestore", "").strip()
            lat_str = row.get("Latitudine", "").strip().replace(",", ".")
            lng_str = row.get("Longitudine", "").strip().replace(",", ".")
            
            try:
                lat = float(lat_str) if lat_str else None
                lng = float(lng_str) if lng_str else None
            except ValueError:
                lat, lng = None, None
            
            stations[source_id] = {
                "country": self.country,
                "brand": brand,
                "name": row.get("Bandiera", brand).strip(),
                "address": row.get("Indirizzo", "").strip(),
                "city": row.get("Comune", "").strip(),
                "latitude": lat,
                "longitude": lng,
                "logo_url": self.get_brand_logo(brand),
                "source_id": source_id
            }
        
        return stations
    
    def _parse_and_apply_prices(self, content: bytes, stations_dict: dict):
        """Парсит CSV с ценами и сохраняет в БД вместе со станциями."""
        
        text = content.decode("latin-1")
        lines = text.split("\n")
        csv_text = "\n".join(lines[1:])
        
        reader = csv.DictReader(io.StringIO(csv_text), delimiter=";")
        
        # Словарь: source_id → station_id в БД
        saved_stations = {}
        
        for row in reader:
            source_id = row.get("idImpianto", "").strip()
            fuel_name = row.get("descCarburante", "").strip().lower()
            price_str = row.get("prezzo", "").strip().replace(",", ".")
            
            if not source_id or not price_str:
                continue
            
            # Сохраняем АЗС если ещё не сохранили
            if source_id not in saved_stations:
                station_data = stations_dict.get(source_id)
                if station_data:
                    station_id = upsert_station(self.client, station_data)
                    saved_stations[source_id] = station_id
                    self.stations_count += 1
                else:
                    continue
            
            station_id = saved_stations[source_id]
            
            # Маппинг итальянских названий топлива
            fuel_map = {
                "benzina":        "gasoline_95",
                "gasolio":        "diesel",
                "gpl":            "lpg",
                "metano":         "cng",  # Сжатый природный газ
                "super":          "gasoline_95",
                "diesel+":        "diesel_premium",
                "blue diesel":    "diesel_premium",
            }
            
            fuel_type = None
            for key, ftype in fuel_map.items():
                if key in fuel_name:
                    fuel_type = ftype
                    break
            
            if fuel_type:
                try:
                    price = float(price_str)
                    if price > 0:
                        upsert_price(
                            self.client, station_id,
                            fuel_type, price, self.currency
                        )
                        self.prices_count += 1
                except ValueError:
                    pass