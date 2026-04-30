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

        self.radius = 10

        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Referer": "https://carburanti.mise.gov.it/",
            "Origin": "https://carburanti.mise.gov.it",
        }

    def _get_regions(self):
        url = f"{self.api_base}/registry/region"
        try:
            r = requests.get(url, headers=self.headers, timeout=15)
            if r.status_code == 200:
                return r.json().get("results", [])
        except Exception as e:
            print(f"[IT] Ошибка при получении регионов: {e}")
        return []

    def _get_provinces(self, region_id):
        url = f"{self.api_base}/registry/province?regionId={region_id}"
        try:
            r = requests.get(url, headers=self.headers, timeout=15)
            if r.status_code == 200:
                return r.json().get("results", [])
        except Exception as e:
            print(f"[IT] Ошибка при получении провинций региона {region_id}: {e}")
        return []

    def _get_towns(self, province_code):
        url = f"{self.api_base}/registry/town?province={province_code}"
        try:
            r = requests.get(url, headers=self.headers, timeout=15)
            if r.status_code == 200:
                return r.json().get("results", [])
        except Exception as e:
            print(f"[IT] Ошибка при получении городов провинции {province_code}: {e}")
        return []

    def _search_by_town(self, town_id, province_code, fuel_type_id, fuel_type):
        """
        Ищем АЗС в конкретном городе по его названию и коду провинции.
        Возвращает два словаря: stations и prices.
        """
        stations = {}
        prices = {}

        url = f"{self.api_base}/search/zone"
        body = {
            "town": town_id,
            "province": province_code,
            "fuelType": fuel_type_id,
            "radius": self.radius
        }
        try:
            r = requests.post(url, json=body, headers=self.headers, timeout=15)
            if r.status_code != 200:
                return stations, prices

            results = r.json().get("results", [])

            for st in results:
                sid = str(st.get("id", ""))
                if not sid:
                    continue

                stations[sid] = st

                for fuel in st.get("fuels", []):
                    price = fuel.get("price")
                    if price and float(price) > 0:
                        if sid not in prices:
                            prices[sid] = {}
                        if fuel_type not in prices[sid]:
                            prices[sid][fuel_type] = float(price)
                        else:
                            prices[sid][fuel_type] = min(prices[sid][fuel_type], float(price))

        except Exception:
            pass

        return stations, prices

    def scrape(self):
        print("[IT] Начинаем сбор данных Италии...")
        # ВРЕМЕННЫЙ ТЕСТ — удалить после проверки
import json
url = f"{self.api_base}/search/zone"
body = {"town": "Altino", "province": "CH", "fuelType": "1", "radius": 10}
r = requests.post(url, json=body, headers=self.headers, timeout=15)
print(f"[DEBUG TEST] status={r.status_code}")
print(f"[DEBUG TEST] response={json.dumps(r.json(), indent=2)[:1000]}")
return  # останавливаем после теста

        # Шаг 1: получаем все регионы
        regions = self._get_regions()
        if not regions:
            print("[IT] Не удалось получить регионы! Прерываем.")
            return
        print(f"[IT] Найдено регионов: {len(regions)}")

        all_stations = {}
        all_prices = {}

        # Шаги 2-5: регионы → провинции → города → АЗС
        for region in regions:
            region_id = region.get("id")
            provinces = self._get_provinces(region_id)

            for province in provinces:
                pcode = province.get("id")
                towns = self._get_towns(pcode)
                print(f"[IT] Провинция {pcode}: {len(towns)} городов")

                for fuel_type_id, fuel_type in self.fuel_types.items():

                    with ThreadPoolExecutor(max_workers=10) as executor:
                        futures = {
                            executor.submit(
                                self._search_by_town,
                                town.get("id"), pcode, fuel_type_id, fuel_type
                            ): town
                            for town in towns if town.get("id")
                        }

                        for future in as_completed(futures):
                            stations, prices = future.result()

                            for sid, st in stations.items():
                                if sid not in all_stations:
                                    all_stations[sid] = st

                            for sid, price_dict in prices.items():
                                if sid not in all_prices:
                                    all_prices[sid] = {}
                                for ft, price in price_dict.items():
                                    if ft not in all_prices[sid]:
                                        all_prices[sid][ft] = price
                                    else:
                                        all_prices[sid][ft] = min(all_prices[sid][ft], price)

        print(f"[IT] Всего уникальных АЗС: {len(all_stations)}")

        # Шаг 6: сохраняем в базу данных
        for sid, st_data in all_stations.items():
            try:
                self._save_station(sid, st_data, all_prices.get(sid, {}))
            except Exception as e:
                print(f"[IT] Ошибка при сохранении станции {sid}: {e}")

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
