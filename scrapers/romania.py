import requests
import time
from lxml import etree
from concurrent.futures import ThreadPoolExecutor, as_completed
from .base import BaseScraper
from db.supabase_client import upsert_station, upsert_price

NS = "http://schemas.datacontract.org/2004/07/pmonsvc.Models.Protos"


def xt(el, tag):
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
            "21": "diesel",
            "31": "lpg",
            "22": "diesel_premium",
        }
        self.buffer = 5000

    def _generate_grid(self):
        points = []
        lat = 43.62
        while lat <= 48.27:
            lon = 20.26
            while lon <= 29.74:
                points.append((round(lat, 3), round(lon, 3)))
                lon = round(lon + 0.06, 3)
            lat = round(lat + 0.06, 3)
        return points

    def _fetch_one(self, lat, lon, cat_id):
        """
        Один запрос к API — возвращает (stations, prices) или ([], []).
        При ошибке 429 (слишком много запросов) — делает паузу и повторяет.
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/xml, text/xml, */*",
            "Referer": "https://monitorulpreturilor.info/",
        }
        params = {
            "lat": lat,
            "lon": lon,
            "buffer": self.buffer,
            "CSVGasCatalogProductIds": cat_id,
            "OrderBy": "dist"
        }

        for attempt in range(3):
            try:
                r = requests.get(self.base_url, params=params,
                                 headers=headers, timeout=15)

                if r.status_code == 429:
                    wait = 2 ** attempt  # 1, 2, 4 сек
                    print(f"[RO] Лимит запросов (429), ждём {wait}с...")
                    time.sleep(wait)
                    continue

                if r.status_code != 200:
                    return [], []

                root = etree.fromstring(r.content)
                stations = root.findall(f".//{{{NS}}}GasStation")
                products = root.findall(f".//{{{NS}}}GasProduct")
                return stations, products

            except Exception:
                if attempt < 2:
                    time.sleep(1)

        return [], []

    def scrape(self):
        print(f"[RO] Начинаем сбор данных Румынии...")
        grid = self._generate_grid()
        print(f"[RO] Сетка: {len(grid)} точек")

        all_stations = {}
        all_prices = {}

        def fetch_point(lat, lon):
            """Для одной точки запрашиваем все категории топлива параллельно."""
            point_stations = {}
            point_prices = {}

            # Уменьшено с 5 до 3 потоков — снижаем риск бана по IP
            with ThreadPoolExecutor(max_workers=3) as ex:
                futs = {
                    ex.submit(self._fetch_one, lat, lon, cat_id): (cat_id, fuel_type)
                    for cat_id, fuel_type in self.fuel_categories.items()
                }
                for f in as_completed(futs):
                    cat_id, fuel_type = futs[f]
                    stations_els, products_els = f.result()

                    for st in stations_els:
                        sid = xt(st, "Id")
                        if sid and sid not in point_stations:
                            point_stations[sid] = st

                    for pr in products_els:
                        sid = xt(pr, "Stationid")
                        price_str = xt(pr, "Price")
                        if sid and price_str:
                            try:
                                price = float(price_str)
                                if sid not in point_prices:
                                    point_prices[sid] = {}
                                if fuel_type not in point_prices[sid]:
                                    point_prices[sid][fuel_type] = price
                                else:
                                    point_prices[sid][fuel_type] = min(
                                        point_prices[sid][fuel_type], price
                                    )
                            except ValueError:
                                pass

            return point_stations, point_prices

        # Уменьшено с 20 до 10 потоков — снижаем риск бана по IP
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {
                executor.submit(fetch_point, lat, lon): (lat, lon)
                for lat, lon in grid
            }

            done = 0
            for future in as_completed(futures):
                done += 1
                if done % 1000 == 0:
                    print(f"[RO] {done}/{len(grid)} точек обработано, АЗС: {len(all_stations)}...")

                point_stations, point_prices = future.result()

                for sid, st in point_stations.items():
                    if sid not in all_stations:
                        all_stations[sid] = st

                for sid, price_dict in point_prices.items():
                    if sid not in all_prices:
                        all_prices[sid] = {}
                    for fuel_type, price in price_dict.items():
                        if fuel_type not in all_prices[sid]:
                            all_prices[sid][fuel_type] = price
                        else:
                            all_prices[sid][fuel_type] = min(
                                all_prices[sid][fuel_type], price
                            )

        print(f"[RO] Всего уникальных АЗС: {len(all_stations)}")

        for sid, st_el in all_stations.items():
            try:
                self._save_station(sid, st_el, all_prices.get(sid, {}))
            except Exception as e:
                print(f"[RO] Ошибка станции {sid}: {e}")

        print(f"[RO] Готово: {self.stations_count} АЗС, {self.prices_count} цен")

    def _save_station(self, sid, st_el, prices):
        network_el = st_el.find(f"{{{NS}}}Network")

        # Исправлено: берём Name сети, а не числовой Id
        if network_el is not None:
            brand = xt(network_el, "Name") or xt(network_el, "Id") or "Unknown"
        else:
            brand = "Unknown"

        logo_el = network_el.find(f"{{{NS}}}Logo") if network_el is not None else None
        logo = xt(logo_el, "Logouri") if logo_el is not None else None

        addr_el = st_el.find(f"{{{NS}}}Addr")
        address = xt(addr_el, "Addrstring") if addr_el is not None else ""

        loc_el = addr_el.find(f"{{{NS}}}Location") if addr_el is not None else None
        lat = float(xt(loc_el, "Lat")) if loc_el is not None and xt(loc_el, "Lat") else None
        lon = float(xt(loc_el, "Lon")) if loc_el is not None and xt(loc_el, "Lon") else None

        station = {
            "country": self.country,
            "brand": brand,
            "name": xt(st_el, "Name") or brand,
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
