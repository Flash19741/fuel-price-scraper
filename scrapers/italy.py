import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from .base import BaseScraper
from db.supabase_client import upsert_station, upsert_price


class ItalyScraper(BaseScraper):

    def __init__(self, client):
        super().__init__(client)
        self.country = "IT"
        self.currency = "EUR"

        self.api_base = "https://carburanti.mise.gov.it/ospzApi"

        self.fuel_types = {
            "1": "gasoline_95",
            "2": "diesel",
            "3": "cng",
            "4": "lpg",
        }

        # Радиус 5 км — не упираемся в лимит 343 АЗС на запрос
        self.radius = 5

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Referer": "https://carburanti.mise.gov.it/",
            "Origin": "https://carburanti.mise.gov.it",
        }

    def _generate_grid(self):
        """
        Сетка точек по территории Италии.
        Шаг 0.08 градуса ≈ 8 км — чуть меньше диаметра круга радиусом 5 км,
        поэтому круги перекрываются и не оставляют пробелов.
        Италия: lat 36.6–47.1, lon 6.6–18.5
        Итого примерно 4700 точек.
        """
        points = []
        lat = 36.6
        while lat <= 47.1:
            lon = 6.6
            while lon <= 18.5:
                points.append((round(lat, 3), round(lon, 3)))
                lon = round(lon + 0.08, 3)
            lat = round(lat + 0.08, 3)
        return points

    def _fetch_one(self, lat, lon, fuel_type_id):
        """
        Один POST-запрос — возвращает список АЗС вокруг точки (lat, lon).
        """
        url = f"{self.api_base}/search/zone"
        body = {
            "points": [{"lat": lat, "lng": lon}],
            "fuelType": fuel_type_id,
            "radius": self.radius
        }
        try:
            r = requests.post(url, json=body, headers=self.headers, timeout=15)
            if r.status_code == 200:
                return r.json().get("results", [])
        except Exception:
            pass
        return []

    def scrape(self):
        print("[IT] Начинаем сбор данных Италии...")

        grid = self._generate_grid()
        print(f"[IT] Сетка: {len(grid)} точек, радиус {self.radius} км")

        all_stations = {}  # sid -> данные станции
        all_prices = {}    # sid -> {fuel_type -> min_price}

        for fuel_type_id, fuel_type in self.fuel_types.items():
            print(f"[IT] Топливо: {fuel_type}...")
            found_new = 0

            with ThreadPoolExecutor(max_workers=20) as executor:
                futures = {
                    executor.submit(self._fetch_one, lat, lon, fuel_type_id): (lat, lon)
                    for lat, lon in grid
                }

                done = 0
                for future in as_completed(futures):
                    done += 1
                    if done % 500 == 0:
                        print(f"[IT]   {done}/{len(grid)} точек обработано...")

                    for st in future.result():
                        sid = str(st.get("id", ""))
                        if not sid:
                            continue

                        if sid not in all_stations:
                            all_stations[sid] = st
                            found_new += 1

                        for fuel in st.get("fuels", []):
                            price = fuel.get("price")
                            if price and float(price) > 0:
                                if sid not in all_prices:
                                    all_prices[sid] = {}
                                if fuel_type not in all_prices[sid]:
                                    all_prices[sid][fuel_type] = float(price)
                                else:
                                    all_prices[sid][fuel_type] = min(
                                        all_prices[sid][fuel_type], float(price)
                                    )

            print(f"[IT]   Новых АЗС: {found_new}")

        print(f"[IT] Всего уникальных АЗС: {len(all_stations)}")

        for sid, st_data in all_stations.items():
            try:
                self._save_station(sid, st_data, all_prices.get(sid, {}))
            except Exception as e:
                print(f"[IT] Ошибка станции {sid}: {e}")

        print(f"[IT] Готово: {self.stations_count} АЗС, {self.prices_count} цен")

    def _save_station(self, sid, st, prices):
        brand = st.get("brand", "Unknown") or "Unknown"
        loc = st.get("location", {})
        lat = loc.get("lat")
        lon = loc.get("lng")

        station = {
            "country": self.country,
            "brand": brand,
            "name": st.get("name", brand) or brand,
            "address": st.get("address", "") or "",
            "city": "",
            "latitude": lat,
            "longitude": lon,
            "logo_url": self.get_brand_logo(brand),
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
