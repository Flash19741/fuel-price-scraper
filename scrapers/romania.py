import requests
import time
from .base import BaseScraper
from db.supabase_client import upsert_station, upsert_price


class RomaniaScraper(BaseScraper):

    def __init__(self, client):
        super().__init__(client)
        self.country = "RO"
        self.currency = "RON"
        self.base_url = "https://monitorulpreturilor.info/pmonsvc/Gas/GetGasItemsByLatLon"

        self.fuel_categories = {
            "11": "gasoline_95",
            "12": "gasoline_98",
            "21": "diesel",
            "31": "lpg",
            "22": "diesel_premium",
        }

        self.buffer = 5000

    def _generate_grid(self):
        """
        Шаг 0.07 градуса ≈ 7км — перекрывает радиус 5км.
        Румыния: lat 43.6–48.3, lon 20.2–29.7
        """
        points = []
        lat = 43.6
        while lat <= 48.3:
            lon = 20.2
            while lon <= 29.7:
                points.append((round(lat, 2), round(lon, 2)))
                lon += 0.07
            lat += 0.07
        return points

    def scrape(self):
        print(f"[RO] Начинаем сбор данных Румынии...")

        grid = self._generate_grid()
        print(f"[RO] Сетка: {len(grid)} точек")

        # ВРЕМЕННАЯ ДИАГНОСТИКА — удалим после отладки
        test_params = {
            "lat": 44.43,
            "lon": 26.10,
            "buffer": 5000,
            "CSVGasCatalogProductIds": "11",
            "OrderBy": "dist"
        }
        test_headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://monitorulpreturilor.info/",
        }
        test_r = requests.get(self.base_url, params=test_params, headers=test_headers, timeout=15)
        print(f"[RO DEBUG] Status: {test_r.status_code}")
        print(f"[RO DEBUG] Headers: {dict(test_r.headers)}")
        print(f"[RO DEBUG] Body (first 500): {test_r.text[:500]}")
        return  # останавливаемся после диагностики

        all_stations = {}
        all_prices = {}

        total_points = len(grid)

        for cat_id, fuel_type in self.fuel_categories.items():
            print(f"[RO] Категория {fuel_type}...")
            found_in_cat = 0

            for i, (lat, lon) in enumerate(grid):
                if i % 500 == 0:
                    print(f"[RO]   точка {i}/{total_points}...")

                try:
                    time.sleep(0.3)

                    params = {
                        "lat": lat,
                        "lon": lon,
                        "buffer": self.buffer,
                        "CSVGasCatalogProductIds": cat_id,
                        "OrderBy": "dist"
                    }
                    headers = {
                        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                        "Accept": "application/json, text/javascript, */*; q=0.01",
                        "Accept-Language": "ro-RO,ro;q=0.9,en;q=0.8",
                        "X-Requested-With": "XMLHttpRequest",
                        "Referer": "https://monitorulpreturilor.info/",
                        "Origin": "https://monitorulpreturilor.info",
                    }
                    r = requests.get(
                        self.base_url, params=params,
                        headers=headers, timeout=15
                    )

                    if r.status_code != 200:
                        continue

                    data = r.json()

                    for st in data.get("Stations", []):
                        sid = st.get("id")
                        if sid and sid not in all_stations:
                            all_stations[sid] = st
                            found_in_cat += 1

                    for pr in data.get("Products", []):
                        sid = pr.get("stationid")
                        price = pr.get("price")
                        if sid and price:
                            if sid not in all_prices:
                                all_prices[sid] = {}
                            if fuel_type not in all_prices[sid]:
                                all_prices[sid][fuel_type] = float(price)
                            else:
                                all_prices[sid][fuel_type] = min(
                                    all_prices[sid][fuel_type], float(price)
                                )

                except Exception as e:
                    try:
                        print(f"[RO] Ошибка ({lat},{lon}): {e} | Ответ: {r.text[:200]}")
                    except Exception:
                        print(f"[RO] Ошибка ({lat},{lon}): {e}")
                    continue

            print(f"[RO]   Новых АЗС в категории: {found_in_cat}")

        print(f"[RO] Всего уникальных АЗС: {len(all_stations)}")

        for sid, st_data in all_stations.items():
            try:
                self._save_station(sid, st_data, all_prices.get(sid, {}))
            except Exception as e:
                print(f"[RO] Ошибка станции {sid}: {e}")

        print(f"[RO] Готово: {self.stations_count} АЗС, {self.prices_count} цен")

    def _save_station(self, sid: str, st: dict, prices: dict):
        network = st.get("network", {})
        brand = network.get("name", "Unknown")
        logo = network.get("logo", {}).get("logouri") or self.get_brand_logo(brand)

        addr = st.get("addr", {})
        loc = addr.get("location", {})
        lat = loc.get("Lat")
        lon = loc.get("Lon")

        station = {
            "country": self.country,
            "brand": network.get("id", brand),
            "name": st.get("name", brand),
            "address": addr.get("addrstring", ""),
            "city": "",
            "latitude": lat,
            "longitude": lon,
            "logo_url": logo,
            "source_id": sid
        }

        station_id = upsert_station(self.client, station)
        self.stations_count += 1

        for fuel_type, price in prices.items():
            if price > 0:
                upsert_price(
                    self.client, station_id,
                    fuel_type, price, self.currency
                )
                self.prices_count += 1
