import requests
import math
from .base import BaseScraper
from db.supabase_client import upsert_station, upsert_price


class MoldovaScraper(BaseScraper):

    def __init__(self, client):
        super().__init__(client)
        self.country = "MD"
        self.currency = "MDL"
        self.api_url = "https://api.ecarburanti.anre.md/public/"

    def epsg3857_to_wgs84(self, x: float, y: float):
        lon = (x / 20037508.34) * 180.0
        lat = math.degrees(
            2 * math.atan(math.exp((y / 20037508.34) * math.pi)) - math.pi / 2
        )
        return round(lat, 6), round(lon, 6)

    def scrape(self):
        print(f"[MD] Начинаем сбор данных Молдовы...")

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Origin": "https://ecarburanti.anre.md",
            "Referer": "https://ecarburanti.anre.md/"
        }

        response = requests.get(self.api_url, headers=headers, timeout=30)
        response.raise_for_status()

        stations = response.json()
        print(f"[MD] Получено {len(stations)} записей от API")

        for station_data in stations:
            self._process_station(station_data)

        print(f"[MD] Готово: {self.stations_count} АЗС, {self.prices_count} цен")

    def _process_station(self, data: dict):
        x = data.get("x")
        y = data.get("y")
        lat, lon = self.epsg3857_to_wgs84(x, y) if x and y else (None, None)

        if lat is None or lon is None:
            return

        # Уникальный ID = координаты (решает проблему дублей по idno)
        source_id = f"{lat}_{lon}"
        print(f"[MD DEBUG] source_id = {source_id}")

        brand = data.get("station_name", "Unknown").strip()
        city = data.get("lev1", "") or data.get("bua", "") or ""
        region = data.get("lev2", "") or ""

        station = {
            "country": self.country,
            "brand": brand,
            "name": data.get("company_name", brand).strip(),
            "address": data.get("fullstreet", "") or "",
            "city": f"{city}, {region}".strip(", "),
            "latitude": lat,
            "longitude": lon,
            "logo_url": self.get_brand_logo(brand),
            "source_id": source_id
        }

        station_id = upsert_station(self.client, station)
        self.stations_count += 1

        fuel_map = {
            "diesel":   "diesel",
            "gasoline": "gasoline_95",
            "gpl":      "lpg",
        }

        for field, fuel_type in fuel_map.items():
            price = data.get(field)
            if price and float(price) > 0:
                upsert_price(
                    self.client, station_id,
                    fuel_type, float(price), self.currency
                )
                self.prices_count += 1
