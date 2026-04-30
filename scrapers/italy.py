import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from .base import BaseScraper
from db.supabase_client import upsert_station, upsert_price


class ItalyScraper(BaseScraper):

    def __init__(self, client):
        super().__init__(client)
        self.country = "IT"
        self.currency = "EUR"

        # Базовый адрес API — все запросы начинаются с него
        self.api_base = "https://carburanti.mise.gov.it/ospzApi"

        # Словарь: код вида топлива → наше внутреннее название
        self.fuel_types = {
            "1": "gasoline_95",
            "2": "diesel",
            "3": "cng",
            "4": "lpg",
        }

        # Радиус поиска в км вокруг центра города
        self.radius = 10

        # Заголовки для всех запросов — имитируем браузер
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Referer": "https://carburanti.mise.gov.it/",
            "Origin": "https://carburanti.mise.gov.it",
        }

    # ------------------------------------------------------------------
    # ШАГИ 1-3: Получаем географию (регионы → провинции → города)
    # ------------------------------------------------------------------

    def _get_regions(self):
        """
        Шаг 1: Получаем список всех регионов Италии.
        Возвращает список словарей, например:
        [{"id": 1, "name": "Piemonte"}, {"id": 2, "name": "Valle d'Aosta"}, ...]
        """
        url = f"{self.api_base}/registry/region"
        try:
            r = requests.get(url, headers=self.headers, timeout=15)
            if r.status_code == 200:
                data = r.json()
                print(f"[DEBUG] Регионы: {data}")  # ← добавь эту строку
                return data
        except Exception as e:
            print(f"[IT] Ошибка при получении регионов: {e}")
        return []

    def _get_provinces(self, region_id):
        """
        Шаг 2: Получаем список провинций для одного региона.
        region_id — число, например 1 для Piemonte.
        Возвращает список, например:
        [{"id": "TO", "name": "Torino"}, {"id": "VC", "name": "Vercelli"}, ...]
        """
        url = f"{self.api_base}/registry/province?regionId={region_id}"
        try:
            r = requests.get(url, headers=self.headers, timeout=15)
            if r.status_code == 200:
                data = r.json()
                print(f"[DEBUG] Пример провинции: {data[0]}")  # ← добавь эту строку
                return data
        except Exception as e:
            print(f"[IT] Ошибка при получении провинций региона {region_id}: {e}")
        return []

    def _get_towns(self, province_code):
        """
        Шаг 3: Получаем список городов для одной провинции.
        province_code — строка, например "TO" для Torino.
        Возвращает список, например:
        [{"id": 1001, "name": "Torino", "lat": 45.07, "lng": 7.68}, ...]
        """
        url = f"{self.api_base}/registry/town?province={province_code}"
        try:
            r = requests.get(url, headers=self.headers, timeout=15)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            print(f"[IT] Ошибка при получении городов провинции {province_code}: {e}")
        return []

    # ------------------------------------------------------------------
    # ШАГИ 4: Поиск АЗС вокруг конкретного города
    # ------------------------------------------------------------------

    def _fetch_stations_near_town(self, lat, lon, fuel_type_id):
        """
        Шаг 4: Один POST-запрос к /search/zone для конкретной точки (города).
        Возвращает список АЗС из поля "results".
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

    # ------------------------------------------------------------------
    # ГЛАВНЫЙ МЕТОД: scrape() — запускается планировщиком
    # ------------------------------------------------------------------

    def scrape(self):
        print("[IT] Начинаем сбор данных Италии...")

        # --- Шаг 1: Получаем все регионы ---
        regions = self._get_regions()
        if not regions:
            print("[IT] Не удалось получить регионы! Прерываем.")
            return
        print(f"[IT] Найдено регионов: {len(regions)}")

        # --- Шаги 2-3: Собираем все города Италии ---
        all_towns = []  # список кортежей (lat, lon)

        for region in regions:
            region_id = region.get("id")
            provinces = self._get_provinces(region_id)

            for province in provinces:
                # Код провинции может быть в поле "id", "code" или "sigla" —
                # зависит от API. Попробуем несколько вариантов:
                pcode = province.get("sigla") or province.get("code") or province.get("id")
                towns = self._get_towns(pcode)

                for town in towns:
                    lat = town.get("lat") or town.get("latitude")
                    lon = town.get("lng") or town.get("lon") or town.get("longitude")
                    if lat and lon:
                        all_towns.append((float(lat), float(lon)))

        # Убираем дубли координат (округляем до 3 знаков)
        all_towns = list(set(
            (round(lat, 3), round(lon, 3)) for lat, lon in all_towns
        ))

        print(f"[IT] Всего уникальных городов: {len(all_towns)}")

        # --- Шаг 4: По каждому городу и каждому виду топлива ищем АЗС ---
        all_stations = {}   # sid -> данные станции
        all_prices = {}     # sid -> {fuel_type -> min_price}

        for fuel_type_id, fuel_type in self.fuel_types.items():
            print(f"[IT] Вид топлива: {fuel_type}...")
            found_new = 0

            # Запускаем параллельно — 20 потоков одновременно
            with ThreadPoolExecutor(max_workers=20) as executor:
                futures = {
                    executor.submit(
                        self._fetch_stations_near_town, lat, lon, fuel_type_id
                    ): (lat, lon)
                    for lat, lon in all_towns
                }

                done = 0
                for future in as_completed(futures):
                    done += 1
                    if done % 500 == 0:
                        print(f"[IT]   {done}/{len(all_towns)} городов обработано...")

                    results = future.result()

                    for st in results:
                        sid = str(st.get("id", ""))
                        if not sid:
                            continue

                        # Сохраняем данные станции (только один раз)
                        if sid not in all_stations:
                            all_stations[sid] = st
                            found_new += 1

                        # Сохраняем цену (берём минимальную если встречается несколько раз)
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

        # --- Шаг 5: Сохраняем всё в базу данных ---
        for sid, st_data in all_stations.items():
            try:
                self._save_station(sid, st_data, all_prices.get(sid, {}))
            except Exception as e:
                print(f"[IT] Ошибка при сохранении станции {sid}: {e}")

        print(f"[IT] Готово: {self.stations_count} АЗС, {self.prices_count} цен")

    # ------------------------------------------------------------------
    # Сохранение одной станции и её цен в Supabase
    # ------------------------------------------------------------------

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
