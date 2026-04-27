import requests
from .base import BaseScraper
from db.supabase_client import upsert_station, upsert_price


class RomaniaScraper(BaseScraper):
    """
    API возвращает АЗС в радиусе вокруг точки.
    Покрываем всю Румынию сеткой из ~20 точек с радиусом 80км каждая.
    Станции и цены приходят отдельно — соединяем по stationid.
    """

    def __init__(self, client):
        super().__init__(client)
        self.country = "RO"
        self.currency = "RON"
        self.base_url = "https://monitorulpreturilor.info/pmonsvc/Gas/GetGasItemsByLatLon"

        # Категории топлива на сайте
        # 11=benzina 95, 12=benzina 98, 13=motorina, 14=GPL, 15=motorina premium
        self.fuel_categories = {
            "11": "gasoline_95",
            "12": "gasoline_98",
            "13": "diesel",
            "14": "lpg",
            "15": "diesel_premium",
        }

        # Сетка точек покрывающая всю Румынию (lat, lon)
        # Радиус 80км = 80000м — каждая точка покрывает ~160x160км
        self.grid_points = [
            (48.2, 22.5), (48.2, 25.5), (48.2, 28.0),
            (46.5, 21.5), (46.5, 24.0), (46.5, 26.5), (46.5, 29.0),
            (45.0, 22.5), (45.0, 25.0), (45.0, 27.5),
            (44.5, 23.5), (44.5, 26.0), (44.5, 28.5),
            (43.8, 22.5), (43.8, 25.5), (43.8, 28.0),
        ]
        self.buffer = 80000  # радиус в метрах

    def scrape(self):
        print(f"[RO] Начинаем сбор данных Румынии...")

        # Словарь всех найденных станций: station_id -> данные
        all_stations = {}
        # Словарь цен: station_id -> список {fuel_type, price}
        all_prices = {}

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Referer": "https://monitorulpreturilor.info/",
        }

        # Запрашиваем каждую категорию топлива по каждой точке сетки
        for cat_id, fuel_type in self.fuel_categories.items():
            print(f"[RO] Собираем {fuel_type} (cat {cat_id})...")

            for lat, lon in self.grid_points:
                try:
                    params = {
                        "lat": lat,
                        "lon": lon,
                        "buffer": self.buffer,
                        "CSVGasCatalogProductIds": cat_id,
                        "OrderBy": "dist"
                    }
                    r = requests.get(
                        self.base_url, params=params,
                        headers=headers, timeout=30
                    )
                    r.raise_for_status()
                    data = r.json()

                    # Сохраняем станции
                    for st in data.get("Stations", []):
                        sid = st.get("id")
                        if sid and sid not in all_stations:
                            all_stations[sid] = st

                    # Сохраняем цены
                    for pr in data.get("Products", []):
                        sid = pr.get("stationid")
                        price = pr.get("price")
                        if sid and price:
                            if sid not in all_prices:
                                all_prices[sid] = {}
                            # Берём минимальную цену если несколько продуктов одной категории
                            if fuel_type not in all_prices[sid]:
                                all_prices[sid][fuel_type] = float(price)
                            else:
                                all_prices[sid][fuel_type] = min(
                                    all_prices[sid][fuel_type], float(price)
                                )

                except Exception as e:
                    print(f"[RO] Ошибка точки ({lat},{lon}) cat {cat_id}: {e}")
                    continue

        print(f"[RO] Найдено уникальных АЗС: {len(all_stations)}")

        # Сохраняем в БД
        for sid, st_data in all_stations.items():
            try:
                self._save_station(sid, st_data, all_prices.get(sid, {}))
            except Exception as e:
                print(f"[RO] Ошибка сохранения станции {sid}: {e}")

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
            "city": "",  # API не даёт город отдельно
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
