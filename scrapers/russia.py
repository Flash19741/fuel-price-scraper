"""
Парсеры российских сетей АЗС: Роснефть и Татнефть.

Роснефть: API найден — https://rosneft-azs.ru/front-api/stations
  Отдаёт ВСЕ АЗС одним запросом (~3000+ станций по всей России).
  Структура ответа:
  {
    "data": {
      "regions": [{"code": 77, "name": "Москва"}, ...],
      "fuels":   [{"code": "ai95", "name": "АИ-95"}, ...],
      "stations": [
        {
          "id": 54857,
          "name": "Курганнефтепродукт",
          "номер": "АЗС 2",          ← номер станции (тоже бывает "number")
          "brand": "rosneft",
          "region": 45,              ← числовой код региона
          "address": "...",
          "coordinate": {"lat": 55.458804, "lng": 65.365697},
          "currency": "RUB",
          "fuels":  [{"code": "ai92", "price": 60.5}, ...],  ← иногда поле называется "топливо"
          "services": [...]
        }, ...
      ]
    }
  }

  ВАЖНО: в одном ответе некоторые станции используют ключ "fuels",
  другие — "топливо" (кириллица). Код обрабатывает оба варианта.

Татнефть: API endpoint ещё не найден.
  Инструкция по поиску — в методе scrape() класса TatneftScraper.
"""

import requests
import time
from .base import BaseScraper
from db.supabase_client import upsert_station, upsert_price


# ─────────────────────────────────────────────────────────────────────────────
# МАППИНГ ТОПЛИВА РОСНЕФТИ → наши стандартные названия
# ─────────────────────────────────────────────────────────────────────────────
# "fora" = премиальные марки топлива Роснефти (Pulsar)
# Мы сохраняем их как отдельный тип, чтобы приложение могло их показать
ROSNEFT_FUEL_MAP = {
    "ai92":         "gasoline_92",
    "ai95":         "gasoline_95",
    "ai98":         "gasoline_98",
    "ai100":        "gasoline_100",
    "diesel":       "diesel",
    "дизель":       "diesel",       # встречается написание кириллицей
    "gaz":          "lpg",
    "газ":          "lpg",          # кириллица
    "methane":      "cng",
    "метан":        "cng",          # кириллица
    # Премиальные марки Pulsar / ATUM
    "ai95_fora":    "gasoline_95_premium",
    "ai100_fora":   "gasoline_100_premium",
    "diesel_fora":  "diesel_premium",
    "ai92_atum":    "gasoline_92_premium",
    "ai95_atum":    "gasoline_95_premium",
}


# ─────────────────────────────────────────────────────────────────────────────
# РОСНЕФТЬ
# ─────────────────────────────────────────────────────────────────────────────

class RosneftScraper(BaseScraper):
    """
    Парсер сети АЗС Роснефть.
    Один GET-запрос возвращает все ~3000 станций по России сразу.
    """

    # Реальный API endpoint, найденный через DevTools
    API_URL = "https://rosneft-azs.ru/front-api/stations"

    def __init__(self, client):
        super().__init__(client)
        self.country = "RU"
        self.currency = "RUB"
        self.brand = "Роснефть"

        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            # Сайт проверяет Referer — без него может вернуть 403
            "Referer": "https://rosneft-azs.ru/stations",
            "Origin":  "https://rosneft-azs.ru",
        }

    def _load_stations(self) -> tuple[list, dict]:
        """
        Загружает все АЗС одним запросом.
        Возвращает (список станций, словарь регионов {код: название}).
        """
        for attempt in range(3):
            try:
                r = requests.get(
                    self.API_URL,
                    headers=self.headers,
                    timeout=60   # большой таймаут — ответ может быть ~2-3 МБ
                )

                if r.status_code == 429:
                    wait = 2 ** attempt
                    print(f"[RU/Роснефть] Лимит запросов (429), ждём {wait}с...")
                    time.sleep(wait)
                    continue

                r.raise_for_status()
                data = r.json()

                # Ответ обёрнут в {"data": {...}}
                inner = data.get("data", data)

                stations = inner.get("stations", [])

                # Строим словарь регионов: {45: "Курганская область", ...}
                regions = {
                    reg["code"]: reg["name"]
                    for reg in inner.get("regions", [])
                    if "code" in reg and "name" in reg
                }

                return stations, regions

            except Exception as e:
                if attempt == 2:
                    raise
                print(f"[RU/Роснефть] Ошибка загрузки (попытка {attempt+1}/3): {e}")
                time.sleep(2)

        return [], {}

    def _get_fuel_list(self, station: dict) -> list:
        """
        Получает список топлива из объекта станции.
        API непоследователен: иногда ключ "fuels", иногда "топливо".
        Проверяем оба варианта.
        """
        fuels = station.get("fuels")
        if isinstance(fuels, list) and fuels:
            return fuels

        fuels = station.get("топливо")
        if isinstance(fuels, list) and fuels:
            return fuels

        return []

    def _get_city(self, station: dict, regions: dict) -> str:
        """
        Пытается определить город/регион АЗС.
        API не отдаёт город напрямую — берём название региона.
        """
        region_code = station.get("region")
        if region_code and region_code in regions:
            return regions[region_code]
        return ""

    def scrape(self):
        print(f"[RU/Роснефть] Загружаем список АЗС...")

        stations_raw, regions = self._load_stations()
        print(f"[RU/Роснефть] Получено {len(stations_raw)} АЗС из API")

        # Для отладки — показываем первую станцию
        if stations_raw:
            first = stations_raw[0]
            print(f"[RU/Роснефть] Пример первой АЗС: id={first.get('id')}, "
                  f"адрес={first.get('address', '')[:50]}, "
                  f"топливо: {self._get_fuel_list(first)}")

        for st in stations_raw:
            try:
                self._save_station(st, regions)
            except Exception as e:
                print(f"[RU/Роснефть] Ошибка станции id={st.get('id')}: {e}")

        print(f"[RU/Роснефть] Готово: {self.stations_count} АЗС, "
              f"{self.prices_count} цен")

    def _save_station(self, st: dict, regions: dict):
        sid = str(st.get("id", ""))
        if not sid:
            return

        coord = st.get("coordinate", {})
        lat = coord.get("lat")
        lon = coord.get("lng")
        if not lat or not lon:
            return

        # Номер АЗС — бывает ключ "номер" (кириллица) или "number"
        number = st.get("номер") or st.get("number") or ""

        # Название: "Курганнефтепродукт АЗС 2"
        org_name = st.get("name", self.brand)
        full_name = f"{org_name} {number}".strip() if number else org_name

        station = {
            "country":   self.country,
            "brand":     self.brand,
            "name":      full_name,
            "address":   st.get("address", "") or "",
            "city":      self._get_city(st, regions),
            "latitude":  float(lat),
            "longitude": float(lon),
            "logo_url":  self.get_brand_logo(self.brand),
            # Префикс "rosneft_" защищает от пересечения ID с другими сетями
            "source_id": f"rosneft_{sid}",
        }

        station_id = upsert_station(self.client, station)
        self.stations_count += 1

        # Сохраняем цены
        for fuel_item in self._get_fuel_list(st):
            code = fuel_item.get("code", "")
            # Цена — ключ "price" или "цена" (кириллица)
            price_raw = fuel_item.get("price") or fuel_item.get("цена")

            fuel_type = ROSNEFT_FUEL_MAP.get(code)
            if not fuel_type or price_raw is None:
                continue

            try:
                price = float(str(price_raw).replace(",", ".").replace(" ", ""))
                if price > 0:
                    upsert_price(self.client, station_id,
                                 fuel_type, price, self.currency)
                    self.prices_count += 1
            except (ValueError, TypeError):
                pass


# ─────────────────────────────────────────────────────────────────────────────
# ТАТНЕФТЬ
# ─────────────────────────────────────────────────────────────────────────────

# TODO: заменить на реальный URL после поиска через DevTools
# Ссылка на Яндекс Метрику которую ты прислал — это не API АЗС,
# а аналитика кликов. Нужно найти XHR/Fetch запрос с данными станций.
TATNEFT_API_URL = "https://api.gs.tatneft.ru/api/v2/azs/"  # предположительный URL

TATNEFT_FUEL_MAP = {
    "АИ-92":     "gasoline_92",
    "АИ-95":     "gasoline_95",
    "АИ-98":     "gasoline_98",
    "АИ-100":    "gasoline_100",
    "ДТ":        "diesel",
    "дизель":    "diesel",
    "СУГ":       "lpg",
    "Метан":     "cng",
    "КПГ":       "cng",
    # Английские варианты на случай если API использует их
    "ai92":      "gasoline_92",
    "ai95":      "gasoline_95",
    "ai98":      "gasoline_98",
    "diesel":    "diesel",
    "lpg":       "lpg",
    "cng":       "cng",
}


class TatneftScraper(BaseScraper):
    """
    Парсер сети АЗС Татнефть (~900 станций, преимущественно Татарстан).

    КАК НАЙТИ API ENDPOINT (нужно сделать один раз):
    1. Открой https://azs.tatneft.ru/locator в Chrome
    2. F12 → Network → очисти (🚫) → обнови страницу F5
    3. Подожди 5-10 секунд пока карта загрузится
    4. В фильтре введи: XHR (или Fetch/XHR)
    5. Ищи запрос который возвращает JSON со списком АЗС
       (ищи по ключевым словам: station, azs, map, list, markers)
    6. Нажми на запрос → Headers → скопируй Request URL
    7. Вставь URL в константу TATNEFT_API_URL выше
    """

    def __init__(self, client):
        super().__init__(client)
        self.country = "RU"
        self.currency = "RUB"
        self.brand = "Татнефть"

        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://azs.tatneft.ru/locator",
            "Origin":  "https://azs.tatneft.ru",
        }

    def _load_stations(self) -> list:
        """
        Загружает список АЗС Татнефти.
        Сайт может требовать cookie с главной страницы — используем сессию.
        """
        session = requests.Session()
        session.headers.update(self.headers)

        # Шаг 1: получаем главную страницу чтобы подхватить cookies
        try:
            session.get("https://azs.tatneft.ru/locator", timeout=30)
        except Exception as e:
            print(f"[RU/Татнефть] Не удалось загрузить стартовую страницу: {e}")
            # Продолжаем — может API работает и без cookies

        # Шаг 2: запрашиваем API
        for attempt in range(3):
            try:
                r = session.get(TATNEFT_API_URL, timeout=60)

                if r.status_code == 429:
                    wait = 2 ** attempt
                    print(f"[RU/Татнефть] Лимит запросов (429), ждём {wait}с...")
                    time.sleep(wait)
                    continue

                if r.status_code == 401:
                    raise RuntimeError(
                        "Сервер вернул 401 (нет доступа). "
                        "Вероятно API_URL неверный или требует авторизацию. "
                        "Найди правильный endpoint через DevTools (инструкция в коде)."
                    )

                r.raise_for_status()
                data = r.json()

                # Пробуем разные варианты структуры ответа
                if isinstance(data, list):
                    return data
                for key in ("data", "stations", "items", "results", "list"):
                    if key in data and isinstance(data[key], list):
                        return data[key]

                # Если структура неизвестна — выводим для отладки
                print(f"[RU/Татнефть] Неизвестная структура: {list(data.keys())[:5]}")
                return []

            except RuntimeError:
                raise  # пробрасываем нашу ошибку без повтора
            except Exception as e:
                if attempt == 2:
                    raise
                print(f"[RU/Татнефть] Ошибка (попытка {attempt+1}/3): {e}")
                time.sleep(2)

        return []

    def scrape(self):
        print(f"[RU/Татнефть] Загружаем список АЗС...")

        try:
            stations_raw = self._load_stations()
        except Exception as e:
            print(f"[RU/Татнефть] ОШИБКА: {e}")
            print(f"[RU/Татнефть] Текущий API URL: {TATNEFT_API_URL}")
            raise

        print(f"[RU/Татнефть] Получено {len(stations_raw)} АЗС")

        if stations_raw:
            print(f"[RU/Татнефть] Пример первой АЗС: {stations_raw[0]}")

        for st in stations_raw:
            try:
                self._save_station(st)
            except Exception as e:
                print(f"[RU/Татнефть] Ошибка станции: {e}")

        print(f"[RU/Татнефть] Готово: {self.stations_count} АЗС, "
              f"{self.prices_count} цен")

    def _save_station(self, st: dict):
        # Координаты — перебираем возможные варианты полей
        lat = (st.get("lat") or st.get("latitude") or
               st.get("coord", {}).get("lat") or
               st.get("coordinate", {}).get("lat"))
        lon = (st.get("lon") or st.get("lng") or st.get("longitude") or
               st.get("coord", {}).get("lon") or
               st.get("coordinate", {}).get("lng"))

        if not lat or not lon:
            return

        sid = str(st.get("id") or st.get("stationId") or st.get("station_id") or "")
        if not sid:
            return

        station = {
            "country":   self.country,
            "brand":     self.brand,
            "name":      st.get("name") or st.get("title") or self.brand,
            "address":   st.get("address") or st.get("addr") or "",
            "city":      st.get("city") or st.get("settlement") or st.get("region") or "",
            "latitude":  float(lat),
            "longitude": float(lon),
            "logo_url":  self.get_brand_logo(self.brand),
            "source_id": f"tatneft_{sid}",
        }

        station_id = upsert_station(self.client, station)
        self.stations_count += 1

        # Цены — перебираем возможные ключи
        fuel_list = (st.get("fuels") or st.get("products") or
                     st.get("prices") or st.get("fuelPrices") or [])

        for item in fuel_list:
            fuel_name = (item.get("type") or item.get("name") or
                         item.get("code") or item.get("fuelType") or "")
            price_raw = item.get("price") or item.get("cost") or item.get("value")
            fuel_type = TATNEFT_FUEL_MAP.get(fuel_name)

            if fuel_type and price_raw:
                try:
                    price = float(str(price_raw).replace(",", ".").replace(" ", ""))
                    if price > 0:
                        upsert_price(self.client, station_id,
                                     fuel_type, price, self.currency)
                        self.prices_count += 1
                except (ValueError, TypeError):
                    pass
