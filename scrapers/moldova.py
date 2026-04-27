import requests
from .base import BaseScraper
from db.supabase_client import upsert_station, upsert_price


class MoldovaScraper(BaseScraper):
    """
    Собирает цены АЗС Молдовы с сайта ecarburanti.anre.md
    Сайт использует React и загружает данные через API.
    """
    
    def __init__(self, client):
        super().__init__(client)
        self.country = "MD"
        self.currency = "MDL"
        # API endpoint который отдаёт все АЗС с ценами
        self.api_url = "https://ecarburanti.anre.md/api/stations"
    
    def scrape(self):
        print(f"[MD] Начинаем сбор данных Молдовы...")
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Referer": "https://ecarburanti.anre.md/"
        }
        
        response = requests.get(self.api_url, headers=headers, timeout=30)
        response.raise_for_status()  # Ошибка если статус не 200
        
        stations = response.json()
        print(f"[MD] Получено {len(stations)} АЗС")
        
        for station_data in stations:
            self._process_station(station_data)
        
        print(f"[MD] Готово: {self.stations_count} АЗС, {self.prices_count} цен")
    
    def _process_station(self, data: dict):
        """Обрабатывает одну АЗС из ответа API."""
        
        brand = data.get("brand", "Unknown")
        source_id = str(data.get("id", ""))
        
        # Формируем словарь станции для БД
        station = {
            "country": self.country,
            "brand": brand,
            "name": data.get("name", brand),
            "address": data.get("address", ""),
            "city": data.get("city", ""),
            "latitude": data.get("lat"),
            "longitude": data.get("lng"),
            "logo_url": self.get_brand_logo(brand),
            "source_id": source_id
        }
        
        # Сохраняем в БД, получаем ID
        station_id = upsert_station(self.client, station)
        self.stations_count += 1
        
        # Обрабатываем цены на разные виды топлива
        # Структура может отличаться — подстраиваемся под реальный API
        fuel_map = {
            "petrol":    "gasoline_95",
            "diesel":    "diesel",
            "lpg":       "lpg",
            "premium":   "gasoline_98",
        }
        
        prices = data.get("prices", {})
        for source_key, fuel_type in fuel_map.items():
            price_value = prices.get(source_key)
            if price_value and float(price_value) > 0:
                upsert_price(
                    self.client, station_id,
                    fuel_type, float(price_value), self.currency
                )
                self.prices_count += 1