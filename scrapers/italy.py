import requests
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from .base import BaseScraper
from db.supabase_client import upsert_station, upsert_price


class ItalyScraper(BaseScraper):

    def __init__(self, client):
        super().__init__(client)
        self.country = "IT"
        self.currency = "EUR"
        self.api_base = "https://carburanti.mise.gov.it/ospzApi"

        # Соответствие fuelId из API → наше внутреннее название
        self.fuel_map = {
            1: "gasoline_95",
            2: "diesel",
            3: "cng",
            4: "lpg",
        }

        # Радиус 15 км — перекрытие между точками сетки ~5 км
        self.radius = 15

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Referer": "https://carburanti.mise.gov.it/",
            "Origin": "https://carburanti.mise.gov.it",
        }

    def _generate_grid(self):
        """
        Шаг 0.25 градуса ≈ 25 км. Радиус 15 км — круги перекрываются на 5 км.
        Италия: lat 36.6–47.1, lon 6.6–18.5
        Итого ~1600 точек.
        """
        points = []
        lat = 36.6
        while lat <= 47.1:
            lon = 6.6
            while lon <= 18.5:
                points.append((round(lat, 2), round(lon, 2)))
                lon = round(lon + 0.25, 2)
            lat = round(lat + 0.25, 2)
        return points

    def _fetch_one(self, lat, lon):
        """
        Один POST-запрос к API Италии.
        Возвращает список АЗС или [] при ошибке.
        При ошибке 429 (слишком много запросов) — делает паузу и повторяет.
        """
        url = f"{self.api_base}/search/zone"
        body = {
            "points": [{"lat": lat, "lng": lon}],
            "fuelType": "1",
            "radius": self.radius
        }

        for attempt in range(3):
            try:
                r = requests.post(url, json=body, headers=self.headers, timeout=15)

                if r.status_code == 429:
                    # Сервер говорит "слишком много запросов" — ждём и повторяем
                    wait = 2 ** attempt  # 1, 2, 4 сек
                    print(f"[IT] Лимит запросов (429), ждём {wait}с...")
                    time.sleep(wait)
                    continue

                if r.status_code == 200:
                    return r.json().get("results", [])

            except Exception:
                if attempt < 2:
                    time.sleep(1)

        return []

    def scrape(self):
        print("[IT] Начинаем сбор данных Италии...")
        grid = self._generate_grid()
        print(f"[IT] Сетка: {len(grid)} точек, радиус {self.radius} км")

        all_stations = {}   # source_id -> данные станции
        all_prices = {}     # source_id -> {fuel_type -> min_price}

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {
                executor.submit(self._fetch_one, lat, lon): (lat, lon)
                for lat, lon in grid
            }

            done = 0
            for future in as_completed(futures):
                done += 1
                if done % 200 == 0:
                    print(f"[IT] {done}/{len(grid)} точек обработано, АЗС: {len(all_stations)}...")

                for st in future.result():
                    sid = str(st.get("id", ""))
                    if not sid:
                        continue

                    # Сохраняем данные станции (только при первой встрече)
                    if sid not in all_stations:
                        all_stations[sid] = st

                    # Берём все цены из поля fuels
                    for fuel in st.get("fuels", []):
                        fuel_id = fuel.get("fuelId")
                        fuel_type = self.fuel_map.get(fuel_id)
                        if not fuel_type:
                            continue

                        price = fuel.get("price")
                        if price and float(price) > 0:
                            if sid not in all_prices:
                                all_prices[sid] = {}
                            # Сохраняем минимальную цену из дублирующихся ответов
                            if fuel_type not in all_prices[sid]:
                                all_prices[sid][fuel_type] = float(price)
                            else:
                                all_prices[sid][fuel_type] = min(
                                    all_prices[sid][fuel_type], float(price)
                                )

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

        # Пытаемся получить город из разных возможных полей API
        city = (
            st.get("municipality") or
            st.get("city") or
            st.get("town") or
            ""
        )

        station = {
            "country": self.country,
            "brand": brand,
            "name": st.get("name", brand) or brand,
            "address": st.get("address", "") or "",
            "city": city,
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
