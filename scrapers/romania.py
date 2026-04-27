import requests
from lxml import etree
from .base import BaseScraper
from db.supabase_client import upsert_station, upsert_price


class RomaniaScraper(BaseScraper):
    """
    Собирает цены АЗС Румынии с monitorulpreturilor.info
    Сайт предоставляет данные в формате XML через API.
    """
    
    def __init__(self, client):
        super().__init__(client)
        self.country = "RO"
        self.currency = "RON"
        self.api_url = "https://monitorulpreturilor.info/api/v1/stations/xml"
    
    def scrape(self):
        print(f"[RO] Начинаем сбор данных Румынии...")
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/xml, text/xml, */*",
        }
        
        response = requests.get(self.api_url, headers=headers, timeout=60)
        response.raise_for_status()
        
        # Парсим XML
        root = etree.fromstring(response.content)
        station_elements = root.findall(".//station")
        
        print(f"[RO] Получено {len(station_elements)} АЗС")
        
        for station_el in station_elements:
            self._process_station(station_el)
        
        print(f"[RO] Готово: {self.stations_count} АЗС, {self.prices_count} цен")
    
    def _get_text(self, element, tag: str) -> str:
        """Безопасно получает текст из XML тега."""
        el = element.find(tag)
        return el.text.strip() if el is not None and el.text else ""
    
    def _process_station(self, el):
        """Обрабатывает один XML элемент станции."""
        
        brand = self._get_text(el, "brand")
        source_id = el.get("id", "") or self._get_text(el, "id")
        
        lat = self._get_text(el, "latitude") or self._get_text(el, "lat")
        lng = self._get_text(el, "longitude") or self._get_text(el, "lng")
        
        station = {
            "country": self.country,
            "brand": brand,
            "name": self._get_text(el, "name") or brand,
            "address": self._get_text(el, "address"),
            "city": self._get_text(el, "city"),
            "latitude": float(lat) if lat else None,
            "longitude": float(lng) if lng else None,
            "logo_url": self.get_brand_logo(brand),
            "source_id": source_id
        }
        
        station_id = upsert_station(self.client, station)
        self.stations_count += 1
        
        # Ищем цены в дочерних элементах
        fuel_map = {
            "benzina":  "gasoline_95",
            "motorina": "diesel",
            "gpl":      "lpg",
            "premium":  "gasoline_98",
        }
        
        prices_el = el.find("prices")
        if prices_el is not None:
            for fuel_el in prices_el:
                fuel_key = fuel_el.tag.lower()
                fuel_type = fuel_map.get(fuel_key)
                
                if fuel_type and fuel_el.text:
                    try:
                        price = float(fuel_el.text.replace(",", "."))
                        if price > 0:
                            upsert_price(
                                self.client, station_id,
                                fuel_type, price, self.currency
                            )
                            self.prices_count += 1
                    except ValueError:
                        pass  # Пропускаем если не число