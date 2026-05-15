"""
Парсеры российских сетей АЗС: Роснефть и Татнефть.

══════════════════════════════════════════════════════
РОСНЕФТЬ
  API: https://rosneft-azs.ru/front-api/stations
  Один GET-запрос → все ~3000 АЗС по России.
  Топливо: строковый код "ai92", "ai95", "diesel" и т.д.
  Особенность: часть станций использует кириллицу "топливо"/"цена".

ТАТНЕФТЬ
  API станций:  https://api.gs.tatneft.ru/api/v2/azs/
  API топлива:  https://api.gs.tatneft.ru/api/v2/fuel-types/
  Топливо задаётся числовым fuel_type_id.
  Справочник (проверен на реальных данных):
    29=АИ-92 Танеко, 30=ДТ, 33=Метан, 34=АИ-95, 35=AdBlue (пропускаем),
    36=АИ-92, 37=Газ, 40=АИ-98, 46=ДТ Танеко, 74=АИ-95 Танеко,
    82=АИ-100, 83=ДТ Арктика
  Ответ API: {"status": "success", "data": [...]}
══════════════════════════════════════════════════════
"""

import requests
import time
from .base import BaseScraper
from db.supabase_client import upsert_station, upsert_price


# ─────────────────────────────────────────────────────────────────────────────
# РОСНЕФТЬ — маппинг кодов топлива → наши стандартные названия
# ─────────────────────────────────────────────────────────────────────────────
ROSNEFT_FUEL_MAP = {
    # Обычные марки
    "ai92":        "gasoline_92",
    "ai95":        "gasoline_95",
    "ai98":        "gasoline_98",
    "ai100":       "gasoline_100",
    "diesel":      "diesel",
    "дизель":      "diesel",        # кириллица встречается в реальных данных
    "gaz":         "lpg",
    "газ":         "lpg",           # кириллица
    "methane":     "cng",
    "метан":       "cng",           # кириллица
    # Премиальные марки Pulsar и ATUM
    "ai95_fora":   "gasoline_95_premium",
    "ai100_fora":  "gasoline_100_premium",
    "diesel_fora": "diesel_premium",
    "ai92_atum":   "gasoline_92_premium",
    "ai95_atum":   "gasoline_95_premium",
}


class RosneftScraper(BaseScraper):
    """
    Парсер сети АЗС Роснефть (~3000 станций по всей России).
    Один GET-запрос возвращает все станции сразу.
    """

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
            "Accept":  "application/json, text/plain, */*",
            # Сайт проверяет Referer — без него может вернуть 403
            "Referer": "https://rosneft-azs.ru/stations",
            "Origin":  "https://rosneft-azs.ru",
        }

    def _load(self) -> tuple[list, dict]:
        """
        Загружает все АЗС одним запросом.
        Возвращает (список станций, словарь регионов {код → название}).
        """
        for attempt in range(3):
            try:
                r = requests.get(self.API_URL, headers=self.headers, timeout=60)
                if r.status_code == 429:
                    wait = 2 ** attempt
                    print(f"[RU/Роснефть] 429, ждём {wait}с...")
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                inner = r.json().get("data", {})
                stations = inner.get("stations", [])
                regions = {
                    reg["code"]: reg["name"]
                    for reg in inner.get("regions", [])
                    if "code" in reg and "name" in reg
                }
                return stations, regions
            except Exception as e:
                if attempt == 2:
                    raise
                print(f"[RU/Роснефть] Ошибка (попытка {attempt+1}/3): {e}")
                time.sleep(2)
        return [], {}

    def _fuel_list(self, st: dict) -> list:
        """
        Возвращает список топлива из объекта станции.
        В реальных данных ключ бывает "fuels" или "топливо" (кириллица).
        """
        for key in ("fuels", "топливо"):
            val = st.get(key)
            if isinstance(val, list) and val:
                return val
        return []

    def scrape(self):
        print("[RU/Роснефть] Загружаем список АЗС...")
        stations_raw, regions = self._load()
        print(f"[RU/Роснефть] Получено {len(stations_raw)} АЗС")

        if stations_raw:
            s = stations_raw[0]
            print(f"[RU/Роснефть] Пример: id={s.get('id')}, "
                  f"адрес={str(s.get('address', ''))[:60]}, "
                  f"топливо={self._fuel_list(s)}")

        for st in stations_raw:
            try:
                self._save(st, regions)
            except Exception as e:
                print(f"[RU/Роснефть] Ошибка id={st.get('id')}: {e}")

        print(f"[RU/Роснефть] Готово: {self.stations_count} АЗС, "
              f"{self.prices_count} цен")

    def _save(self, st: dict, regions: dict):
        sid = str(st.get("id", ""))
        coord = st.get("coordinate", {})
        lat, lon = coord.get("lat"), coord.get("lng")
        if not sid or not lat or not lon:
            return

        # Номер АЗС — ключ бывает "номер" (кириллица) или "number"
        number = st.get("номер") or st.get("number") or ""
        org = st.get("name", self.brand)
        full_name = f"{org} {number}".strip() if number else org

        # Город не передаётся напрямую — берём название региона
        region_code = st.get("region")
        city = regions.get(region_code, "") if region_code else ""

        station_id = upsert_station(self.client, {
            "country":   self.country,
            "brand":     self.brand,
            "name":      full_name,
            "address":   st.get("address", "") or "",
            "city":      city,
            "latitude":  float(lat),
            "longitude": float(lon),
            "logo_url":  self.get_brand_logo(self.brand),
            "source_id": f"rosneft_{sid}",
        })
        self.stations_count += 1

        for item in self._fuel_list(st):
            code = item.get("code", "")
            # Цена — ключ "price" или "цена" (кириллица)
            price_raw = item.get("price") or item.get("цена")
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

# Точный маппинг fuel_type_id → наш стандартный код.
# Получен из реального справочника https://api.gs.tatneft.ru/api/v2/fuel-types/
# Структура ответа: {"status":"success","data":{"items":[...],"updated":...}}
# Поле "title" содержит название, "is_taneco"=true означает топливо марки Танеко.
TATNEFT_FUEL_MAP = {
    29: "gasoline_92_premium",  # АИ-92 Танеко  (is_taneco=True)
    30: "diesel",               # ДТ             (обычный)
    33: "cng",                  # Метан / КПГ
    34: "gasoline_95",          # АИ-95          (обычный)
    # 35: AdBlue — это не топливо для автомобиля, пропускаем
    36: "gasoline_92",          # АИ-92          (обычный)
    37: "lpg",                  # Газ / СУГ
    40: "gasoline_98",          # АИ-98
    46: "diesel_premium",       # ДТ Танеко      (is_taneco=True)
    74: "gasoline_95_premium",  # АИ-95 Танеко   (is_taneco=True)
    82: "gasoline_100",         # АИ-100
    83: "diesel_arctic",        # ДТ Арктика Танеко (зимнее)
}

# IDs которые намеренно пропускаем (не виды топлива)
TATNEFT_SKIP_IDS = {35}  # AdBlue — жидкость для катализатора, не топливо


class TatneftScraper(BaseScraper):
    """
    Парсер сети АЗС Татнефть (~900 станций, Татарстан и соседние регионы).

    Структура одной АЗС в ответе API:
    {
      "id": 2,
      "lat": 55.824423,
      "lon": 49.156411,
      "region": "Казань, районы прилегающие к городу",
      "number": 14,
      "address": "Республика Татарстан, г.Казань, Ямашева проспект, 105а/1",
      "currency_code": "rub",
      "actualization_date": 1778781005,
      "fuel": [
        {"fuel_type_id": 46, "price": 74.32, "discount_price": null, ...},
        {"fuel_type_id": 36, "price": 62.10, ...},
        {"fuel_type_id": 37, "price": 26.90, ...},  ← СУГ (газ)
        {"fuel_type_id": 29, "price": 63.10, ...},  ← АИ-92 Танеко
        {"fuel_type_id": 34, "price": 66.20, ...},  ← АИ-95
        {"fuel_type_id": 82, "price": 90.05, ...},  ← АИ-100
      ],
      "connector": [...],   ← зарядки для электромобилей, не используем
      "photos": ["https://gis-media.cloud.tatneftm.ru/..."],
      "owner": "tatneft"
    }
    """

    API_STATIONS   = "https://api.gs.tatneft.ru/api/v2/azs/"
    API_FUEL_TYPES = "https://api.gs.tatneft.ru/api/v2/fuel-types/"

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
            "Accept":  "application/json, text/plain, */*",
            "Referer": "https://azs.tatneft.ru/locator",
            "Origin":  "https://azs.tatneft.ru",
        }

    def _load_fuel_types(self) -> dict[int, str]:
        """
        Загружает актуальный справочник типов топлива из API.
        Возвращает словарь {fuel_type_id: "gasoline_95"}.

        Структура ответа API:
        {
          "status": "success",
          "data": {
            "items": [
              {"id": 34, "title": "АИ-95", "is_taneco": false, ...},
              {"id": 46, "title": "ДТ ",   "is_taneco": true,  ...},
              ...
            ],
            "updated": 1778780703.051342
          }
        }

        Если API недоступен — используем встроенный словарь TATNEFT_FUEL_MAP.
        """
        # Маппинг названий из справочника → наши коды
        # Используем filter_group_title — он чище чем title (нет лишних пробелов)
        name_to_type = {
            "аи 92":         "gasoline_92",
            "аи 92 танеко":  "gasoline_92_premium",
            "аи 95":         "gasoline_95",
            "аи 95 танеко":  "gasoline_95_premium",
            "аи 98":         "gasoline_98",
            "аи 100":        "gasoline_100",
            "дт":            "diesel",
            "дт танеко":     "diesel_premium",
            "дт арктика":    "diesel_arctic",
            "газ":           "lpg",
            "кпг":           "cng",
            "метан":         "cng",
            # adblue — намеренно не добавляем, это не топливо
        }

        try:
            r = requests.get(self.API_FUEL_TYPES, headers=self.headers, timeout=15)
            r.raise_for_status()
            resp = r.json()

            # Ответ обёрнут: {"status":"success","data":{"items":[...]}}
            items = resp.get("data", {}).get("items", [])
            if not items:
                raise ValueError("Пустой список items в ответе справочника")

            result = {}
            unknown = []

            for item in items:
                fid = item.get("id")
                if fid is None or fid in TATNEFT_SKIP_IDS:
                    continue

                # filter_group_title чище: "АИ 95 Танеко" вместо "АИ-95 "
                group = (item.get("filter_group_title") or "").strip().lower()
                title = (item.get("title") or "").strip().lower()

                fuel_type = name_to_type.get(group) or name_to_type.get(title)

                if fuel_type:
                    result[fid] = fuel_type
                else:
                    unknown.append({"id": fid, "title": title, "group": group})

            if unknown:
                print(f"[RU/Татнефть] ⚠ Неизвестные типы топлива в справочнике: {unknown}")
                print(f"[RU/Татнефть]   Добавь их в name_to_type в методе _load_fuel_types()")

            print(f"[RU/Татнефть] Справочник загружен: {len(result)} типов топлива")
            # Дополняем встроенным маппингом — на случай если API вернул не все ID
            combined = {**TATNEFT_FUEL_MAP, **result}
            return combined

        except Exception as e:
            print(f"[RU/Татнефть] Не удалось загрузить справочник: {e}")
            print(f"[RU/Татнефть] Используем встроенный маппинг TATNEFT_FUEL_MAP")
            return TATNEFT_FUEL_MAP.copy()

    def _load_stations(self) -> list:
        """
        Загружает все АЗС одним запросом.
        Ответ: {"status": "success", "data": [...список АЗС...]}
        """
        for attempt in range(3):
            try:
                r = requests.get(self.API_STATIONS, headers=self.headers, timeout=60)
                if r.status_code == 429:
                    wait = 2 ** attempt
                    print(f"[RU/Татнефть] 429, ждём {wait}с...")
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                resp = r.json()

                # Ответ: {"status": "success", "data": [...]}
                data = resp.get("data", resp)
                if isinstance(data, list):
                    return data

                print(f"[RU/Татнефть] Неожиданная структура data: {type(data)}")
                return []

            except Exception as e:
                if attempt == 2:
                    raise
                print(f"[RU/Татнефть] Ошибка загрузки (попытка {attempt+1}/3): {e}")
                time.sleep(2)
        return []

    def scrape(self):
        print("[RU/Татнефть] Загружаем справочник типов топлива...")
        fuel_map = self._load_fuel_types()

        print("[RU/Татнефть] Загружаем список АЗС...")
        stations_raw = self._load_stations()
        print(f"[RU/Татнефть] Получено {len(stations_raw)} АЗС")

        if stations_raw:
            s = stations_raw[0]
            print(f"[RU/Татнефть] Пример: id={s.get('id')}, "
                  f"АЗС №{s.get('number')}, "
                  f"адрес={str(s.get('address', ''))[:60]}")
            # Показываем как расшифруются цены первой АЗС
            for item in s.get("fuel", []):
                fid = item.get("fuel_type_id")
                ft = fuel_map.get(fid, "?")
                print(f"  fuel_type_id={fid} → {ft}: {item.get('price')} руб")

        for st in stations_raw:
            try:
                self._save(st, fuel_map)
            except Exception as e:
                print(f"[RU/Татнефть] Ошибка id={st.get('id')}: {e}")

        print(f"[RU/Татнефть] Готово: {self.stations_count} АЗС, "
              f"{self.prices_count} цен")

    def _save(self, st: dict, fuel_map: dict):
        sid = str(st.get("id", ""))
        lat = st.get("lat")
        lon = st.get("lon")
        if not sid or lat is None or lon is None:
            return

        number = st.get("number", "")
        full_name = f"{self.brand} АЗС {number}".strip() if number else self.brand

        station_id = upsert_station(self.client, {
            "country":   self.country,
            "brand":     self.brand,
            "name":      full_name,
            "address":   st.get("address", "") or "",
            # API отдаёт регион строкой: "Казань, районы прилегающие к городу"
            "city":      (st.get("region") or "").strip(),
            "latitude":  float(lat),
            "longitude": float(lon),
            "logo_url":  self.get_brand_logo(self.brand),
            "source_id": f"tatneft_{sid}",
        })
        self.stations_count += 1

        for item in st.get("fuel", []):
            fid = item.get("fuel_type_id")

            # Пропускаем намеренно исключённые типы (AdBlue)
            if fid in TATNEFT_SKIP_IDS:
                continue

            fuel_type = fuel_map.get(fid)
            if not fuel_type:
                continue

            # Берём скидочную цену если есть, иначе обычную
            price_raw = item.get("discount_price") or item.get("price")
            if price_raw is None:
                continue

            try:
                price = float(price_raw)
                if price > 0:
                    upsert_price(self.client, station_id,
                                 fuel_type, price, self.currency)
                    self.prices_count += 1
            except (ValueError, TypeError):
                pass
