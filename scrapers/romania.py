import requests
import time
from lxml import etree
from .base import BaseScraper
from db.supabase_client import upsert_station, upsert_price

NS = "http://schemas.datacontract.org/2004/07/pmonsvc.Models.Protos"

def xt(el, tag):
    """Получает текст из XML тега с namespace."""
    found = el.find(f"{{{NS}}}{tag}")
    return found.text.strip() if found is not None and found.text else ""

class RomaniaScraper(BaseScraper):

    def __init__(self, client):
        super().__init__(client)
        self.country = "RO"
        self.currency = "RON"
        self.base_url = "https://monitorulpreturilor.info/pmonsvc/Gas/GetGasItemsByLatLon"
        self.fuel_categories = {
            "11": "gasoline_95",
            "12": "gasoline_98",
            "13": "diesel",
            "14": "lpg",
            "15": "diesel_premium",
        }
        self.buffer = 5000

    def _generate_grid(self):
        """Шаг 0.06 градуса ≈ 6км. Границы Румынии: lat 43.62–48.27, lon 20.26–29.74"""
        points = []
        lat = 43.62
        while lat <= 48.27:
            lon = 20.26
            while lon <= 29.74:
                points.append((round(lat, 3), round(lon, 3)))
                lon = round(lon + 0.06, 3)
            lat = round(lat + 0.06, 3)
        return points

    def scrape(self):
        print(f"[RO] Начинаем сбор данных Румынии...")
        grid = self._generate_grid()
        print(f"[RO] Сетка: {len(grid)} точек")

        all_stations = {}
        all_prices = {}

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/xml, text/xml, */*",
            "Referer": "https://monitorulpreturilor.info/",
        }

        total = len(grid)

        for cat_id, fuel_type in self.fuel_categories.items():
            print(f"[RO] Категория {fuel_type}...")
            found_in_cat = 0

            for i, (lat, lon) in enumerate(grid):
                if i % 500 == 0:
                    print(f"[RO]   точка {i}/{total}...")

                try:
                    time.sleep(0.1)
                    params = {
                        "lat": lat,
                        "lon": lon,
                        "buffer": self.buffer,
                        "CSVGasCatalogProductIds": cat_id,
                        "OrderBy": "dist"
                    }
                    r = requests.get(self.base_url, params=params, headers=headers, timeout=15)

                    if r.status_code != 200:
                        continue

                    # Парсим XML
                    root = etree.fromstring(r.content)

                    # Станции
                    for st in root.findall(f".//{{{NS}}}GasStation"):
                        sid = xt(st, "Id")
                        if sid and sid not in all_stations:
                            all_stations[sid] = st
                            found_in_cat += 1

                    # Цены
                    for pr in root.findall(f".//{{{NS}}}GasProduct"):
                        sid = xt(pr, "Stationid")
                        price_str = xt(pr, "Price")
                        if sid and price_str:
                            try:
                                price = float(price_str)
                                if sid not in all_prices:
                                    all_prices[sid] = {}
                                if fuel_type not in all_prices[sid]:
                                    all_prices[sid][fuel_type] = price
                                else:
                                    all_prices[sid][fuel_type] = min(all_prices[sid][fuel_type], price)
                            except ValueError:
                                pass

                except Exception as e:
                    continue

            print(f"[RO]   Новых АЗС: {found_in_cat}")

        print(f"[RO] Всего уникальных АЗС: {len(all_stations)}")

        for sid, st_el in all_stations.items():
            try:
                self._save_station(sid, st_el, all_prices.get(sid, {}))
            except Exception as e:
                print(f"[RO] Ошибка станции {sid}: {e}")

        print(f"[RO] Готово: {self.stations_count} АЗС, {self.prices_count} цен")

    def _save_station(self, sid, st_el, prices):
        network_el = st_el.find(f"{{{NS}}}Network")
        brand = xt(network_el, "Id") if network_el is not None else "Unknown"
        brand_name = xt(network_el, "n") if network_el is not None else brand
        logo_el = network_el.find(f"{{{NS}}}Logo") if network_el is not None else None
        logo = xt(logo_el, "Logouri") if logo_el is not None else self.get_brand_logo(brand)

        addr_el = st_el.find(f"{{{NS}}}Addr")
        address = xt(addr_el, "Addrstring") if addr_el is not None else ""
        loc_el = addr_el.find(f"{{{NS}}}Location") if addr_el is not None else None
        lat = float(xt(loc_el, "Lat")) if loc_el is not None and xt(loc_el, "Lat") else None
        lon = float(xt(loc_el, "Lon")) if loc_el is not None and xt(loc_el, "Lon") else None

        station = {
            "country": self.country,
            "brand": brand,
            "name": xt(st_el, "n") or brand_name,
            "address": address,
            "city": "",
            "latitude": lat,
            "longitude": lon,
            "logo_url": logo if logo else self.get_brand_logo(brand),
            "source_id": sid
        }

        station_id = upsert_station(self.client, station)
        self.stations_count += 1

        for fuel_type, price in prices.items():
            if price > 0:
                upsert_price(self.client, station_id, fuel_type, price, self.currency)
                self.prices_count += 1
