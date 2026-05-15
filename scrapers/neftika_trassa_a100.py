"""
Парсеры: Нефтика (RU), Трасса (RU), А-100 (BY).

══════════════════════════════════════════════════════
НЕФТИКА (neftika.ru)
  ~40 АЗС, Брянская и Смоленская области.
  Всё в HTML: координаты "N 53.278 E 34.385", цены "<strong>Аи-95 59,57</strong>".
  Метод: BeautifulSoup. Готово полностью.

ТРАССА (trassagk.ru, ПАО ЕвроТранс)
  ~58 АЗС, Московский регион.
  Адреса и координаты: HTML-таблица на /locator.
  Цены: HTML карточки АЗС — структура из скриншота:
    <div class="head">98</div>  → название топлива
    <div class="price">95.98</div>  → цена
  Карточки доступны по URL /api/station/?id=<N> или похожему.
  Метод: запрос /locator для списка + запрос карточки каждой АЗС.

А-100 (azs.a-100.by, Беларусь)
  42 АЗС в Беларуси.
  Цены: ОДИНАКОВЫЕ для всех АЗС, берём с главной страницы.
    <div class="price-item__label">АИ-95</div>
    <div class="price-item__price">2,67</div>
  Координаты: страница /set-azs/map-azs/ содержит JS-объект
    с данными всех АЗС (ищем в <script> теге).
  Метод: BeautifulSoup + regex для извлечения JS-данных.
══════════════════════════════════════════════════════
"""

import re
import json
import time
import requests
from bs4 import BeautifulSoup
from .base import BaseScraper
from db.supabase_client import upsert_station, upsert_price


# ─────────────────────────────────────────────────────────────────────────────
# НЕФТИКА
# ─────────────────────────────────────────────────────────────────────────────

NEFTIKA_FUEL_MAP = {
    "аи-95":      "gasoline_95",
    "аи-92":      "gasoline_92",
    "аи-92плюс":  "gasoline_92_premium",
    "аи-92 плюс": "gasoline_92_premium",
    "аи-95плюс":  "gasoline_95_premium",
    "аи-95 плюс": "gasoline_95_premium",
    "аи-98":      "gasoline_98",
    "дтевро":     "diesel",
    "дт евро":    "diesel",
    "дтплюс":     "diesel_premium",
    "дт плюс":    "diesel_premium",
    "дт акция":   "diesel_promo",
    "adblue":     "adblue",
}


class NeftikaScraper(BaseScraper):
    """
    Парсер сети АЗС Нефтика (~40 станций, Брянская и Смоленская обл.).
    Данные встроены прямо в HTML главной страницы.
    """

    URL = "http://www.neftika.ru/"

    def __init__(self, client):
        super().__init__(client)
        self.country = "RU"
        self.currency = "RUB"
        self.brand = "Нефтика"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,*/*",
        }

    def _parse_coord(self, text: str):
        m = re.search(r"N\s*([\d.]+)\s*E\s*([\d.]+)", text, re.I)
        return (float(m.group(1)), float(m.group(2))) if m else (None, None)

    def _parse_price(self, text: str):
        text = text.strip()
        m = re.search(r"([\d]+[.,][\d]+)\s*$", text)
        if not m:
            return None, None
        price_str = m.group(1).replace(",", ".")
        fuel_name = re.sub(r"\s+", " ", text[:m.start()].strip().lower())
        fuel_type = NEFTIKA_FUEL_MAP.get(fuel_name)
        if not fuel_type:
            return None, None
        try:
            return fuel_type, float(price_str)
        except ValueError:
            return None, None

    def scrape(self):
        print(f"[RU/Нефтика] Загружаем {self.URL}...")
        r = requests.get(self.URL, headers=self.headers, timeout=30)
        r.raise_for_status()
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")

        # Находим все h4 с координатами
        coord_headers = soup.find_all("h4", string=re.compile(r"N\s*[\d.]+\s*E", re.I))
        print(f"[RU/Нефтика] Найдено блоков с координатами: {len(coord_headers)}")

        for coord_h4 in coord_headers:
            try:
                self._parse_block(coord_h4)
            except Exception as e:
                print(f"[RU/Нефтика] Ошибка блока: {e}")

        print(f"[RU/Нефтика] Готово: {self.stations_count} АЗС, {self.prices_count} цен")

    def _parse_block(self, coord_h4):
        lat, lon = self._parse_coord(coord_h4.get_text())
        if lat is None:
            return

        # Название — предыдущий h4/h3 без координат
        name_tag = coord_h4.find_previous(["h4", "h3"])
        name = name_tag.get_text(strip=True) if name_tag else self.brand
        if re.search(r"N\s*[\d.]+\s*E", name, re.I):
            name = self.brand

        num_m = re.search(r"[№#]?\s*(\d+)", name)
        source_id = f"neftika_{num_m.group(1)}" if num_m else f"neftika_{lat}_{lon}"

        # Город из ближайшего h3/h4-заголовка раздела
        city = ""
        for prev in coord_h4.find_all_previous(["h3", "h4"]):
            t = prev.get_text(strip=True)
            if re.search(r"^(г\.|город|обл\.|р-н|район)", t, re.I):
                city = t
                break

        station_id = upsert_station(self.client, {
            "country": self.country, "brand": self.brand,
            "name": name, "address": name, "city": city,
            "latitude": lat, "longitude": lon,
            "logo_url": self.get_brand_logo(self.brand),
            "source_id": source_id,
        })
        self.stations_count += 1

        for tag in coord_h4.find_next_siblings():
            if tag.name == "h4" and re.search(r"N\s*[\d.]+\s*E", tag.get_text(), re.I):
                break
            if tag.name == "h3":
                break
            strongs = tag.find_all("strong") if tag.name != "strong" else [tag]
            for strong in strongs:
                text = strong.get_text(strip=True)
                if "цены" in text.lower() and "действительны" in text.lower():
                    continue
                if "временно" in text.lower():
                    continue
                fuel_type, price = self._parse_price(text)
                if fuel_type and price and price > 0:
                    upsert_price(self.client, station_id,
                                 fuel_type, price, self.currency)
                    self.prices_count += 1


# ─────────────────────────────────────────────────────────────────────────────
# ТРАССА
# ─────────────────────────────────────────────────────────────────────────────

TRASSA_FUEL_MAP = {
    # Названия из div.head в карточке АЗС (числа и текст)
    "98":      "gasoline_98",
    "95":      "gasoline_95",
    "92":      "gasoline_92",
    "пс":      "gasoline_95_premium",   # "ПС" = Премиум Супер
    "дт":      "diesel",
    "дизель":  "diesel",
    "эл.":     "electric",              # зарядка электромобиля
    "эл":      "electric",
    "суг":     "lpg",
    "газ":     "lpg",
    "метан":   "cng",
    "adblue":  "adblue",
    # Запасные английские варианты
    "diesel":  "diesel",
    "lpg":     "lpg",
}


class TrassaScraper(BaseScraper):
    """
    Парсер сети АЗС Трасса / ПАО ЕвроТранс (~58 станций, Московский регион).

    Шаг 1: Берём список АЗС (название, адрес, координаты) из HTML-таблицы на /locator.
    Шаг 2: Для каждой АЗС запрашиваем страницу /locator?id=N (или похожую)
            и парсим цены из HTML-карточки.

    Структура карточки АЗС (из скриншота DevTools):
    <div class="mgo-cards mgo-cls">
      <h2 class="section-name">Цены на топливо</h2>
      <a href="#" class="mgo-card">
        <div class="head">98</div>    ← название топлива
        <div class="body">
          <div class="price">95.98</div>  ← цена
        </div>
      </a>
      <a href="#" class="mgo-card active">
        <div class="head">95</div>
        ...
      </a>
    </div>
    """

    LOCATOR_URL = "https://trassagk.ru/locator"

    def __init__(self, client):
        super().__init__(client)
        self.country = "RU"
        self.currency = "RUB"
        self.brand = "Трасса"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,*/*",
            "Referer": "https://trassagk.ru/",
        })

    def _load_stations_html(self) -> list[dict]:
        """Загружает список АЗС из HTML-таблицы на /locator."""
        r = self.session.get(self.LOCATOR_URL, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        stations = []
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cols = row.find_all("td")
                if len(cols) < 4:
                    continue
                name = cols[0].get_text(strip=True)
                address = cols[1].get_text(strip=True)
                lat_str = cols[2].get_text(strip=True)
                lon_str = cols[3].get_text(strip=True)

                # Только АЗС, пропускаем рестораны/ЭЗС/AdBlue-модули
                if any(x in name.lower() for x in ["ресторан", "элзс", "adblue"]):
                    continue
                if "азс" not in name.lower() and "трасса" not in name.lower():
                    continue

                try:
                    lat, lon = float(lat_str), float(lon_str)
                except ValueError:
                    continue

                num_m = re.search(r"№\s*(\d+)", name)
                number = num_m.group(1) if num_m else ""

                stations.append({
                    "source_id": f"trassa_{number}" if number else f"trassa_{lat}_{lon}",
                    "number": number,
                    "name": name,
                    "address": address,
                    "lat": lat,
                    "lon": lon,
                })
        return stations

    def _fetch_prices(self, number: str) -> dict:
        """
        Запрашивает страницу конкретной АЗС и парсит цены.

        Трасса использует карту на основе MGO (map.geo.by).
        При клике на маркер карта делает AJAX-запрос к /locator?id=N
        или загружает данные через JS. Пробуем несколько вариантов URL.

        Структура HTML карточки (из скриншота):
        <a class="mgo-card">
          <div class="head">95</div>       ← марка топлива
          <div class="body">
            <div class="price">70.68</div> ← цена
          </div>
        </a>
        """
        if not number:
            return {}

        # Пробуем разные варианты URL для получения данных АЗС
        candidate_urls = [
            f"https://trassagk.ru/locator/?id={number}",
            f"https://trassagk.ru/locator?id={number}",
            f"https://trassagk.ru/api/station/?id={number}",
            f"https://trassagk.ru/api/azs/{number}/",
        ]

        for url in candidate_urls:
            try:
                r = self.session.get(url, timeout=15)
                if r.status_code != 200:
                    continue

                # Пробуем JSON ответ
                try:
                    data = r.json()
                    prices = self._extract_prices_from_json(data)
                    if prices:
                        return prices
                except Exception:
                    pass

                # Пробуем HTML карточку
                prices = self._extract_prices_from_html(r.text)
                if prices:
                    return prices

            except Exception:
                continue

        return {}

    def _extract_prices_from_html(self, html: str) -> dict:
        """
        Парсит цены из HTML карточки АЗС.
        Структура: <a class="mgo-card"><div class="head">95</div>
                   <div class="price">70.68</div></a>
        """
        soup = BeautifulSoup(html, "html.parser")
        prices = {}

        # Ищем блок "Цены на топливо"
        cards_block = soup.find(class_="mgo-cards")
        if not cards_block:
            # Ищем любые карточки с топливом
            cards_block = soup

        for card in cards_block.find_all(class_="mgo-card"):
            head = card.find(class_="head")
            price_el = card.find(class_="price")
            if not head or not price_el:
                continue

            fuel_name = head.get_text(strip=True).lower()
            price_str = price_el.get_text(strip=True).replace(",", ".")

            fuel_type = TRASSA_FUEL_MAP.get(fuel_name)
            if not fuel_type:
                continue
            try:
                price = float(price_str)
                if price > 0:
                    prices[fuel_type] = price
            except ValueError:
                continue

        return prices

    def _extract_prices_from_json(self, data) -> dict:
        """Пробует извлечь цены из JSON ответа (если API вернёт JSON)."""
        prices = {}
        fuel_list = None

        if isinstance(data, list):
            fuel_list = data
        elif isinstance(data, dict):
            for key in ("fuels", "prices", "fuel", "products"):
                if key in data and isinstance(data[key], list):
                    fuel_list = data[key]
                    break

        if not fuel_list:
            return {}

        for item in fuel_list:
            name = (item.get("name") or item.get("type") or item.get("head") or "").lower()
            price_raw = item.get("price") or item.get("cost") or item.get("value")
            fuel_type = TRASSA_FUEL_MAP.get(name)
            if fuel_type and price_raw:
                try:
                    prices[fuel_type] = float(str(price_raw).replace(",", "."))
                except ValueError:
                    pass
        return prices

    def scrape(self):
        print(f"[RU/Трасса] Загружаем список АЗС из HTML...")
        stations = self._load_stations_html()
        print(f"[RU/Трасса] Найдено {len(stations)} АЗС")

        for st in stations:
            try:
                # Запрашиваем цены для этой АЗС
                prices = self._fetch_prices(st["number"])
                if not prices:
                    print(f"[RU/Трасса] АЗС №{st['number']}: цены не найдены")

                station_id = upsert_station(self.client, {
                    "country":   self.country,
                    "brand":     self.brand,
                    "name":      st["name"],
                    "address":   st["address"],
                    "city":      "Московский регион",
                    "latitude":  st["lat"],
                    "longitude": st["lon"],
                    "logo_url":  self.get_brand_logo(self.brand),
                    "source_id": st["source_id"],
                })
                self.stations_count += 1

                for fuel_type, price in prices.items():
                    upsert_price(self.client, station_id,
                                 fuel_type, price, self.currency)
                    self.prices_count += 1

                # Пауза между запросами — не нагружаем сервер
                time.sleep(0.3)

            except Exception as e:
                print(f"[RU/Трасса] Ошибка {st.get('name')}: {e}")

        print(f"[RU/Трасса] Готово: {self.stations_count} АЗС, {self.prices_count} цен")

        if self.prices_count == 0:
            print(f"[RU/Трасса] ⚠ Цены не загрузились.")
            print(f"[RU/Трасса]   Нужно уточнить URL карточки АЗС через DevTools:")
            print(f"[RU/Трасса]   1. Открой trassagk.ru/locator → кликни на АЗС на карте")
            print(f"[RU/Трасса]   2. F12 → Network → Fetch/XHR → найди запрос который")
            print(f"[RU/Трасса]      вернул HTML карточки с ценами")
            print(f"[RU/Трасса]   3. Добавь URL в candidate_urls в _fetch_prices()")


# ─────────────────────────────────────────────────────────────────────────────
# А-100 (БЕЛАРУСЬ)
# ─────────────────────────────────────────────────────────────────────────────

A100_FUEL_MAP = {
    "аи-98":    "gasoline_98",
    "аи-95":    "gasoline_95",
    "аи-92":    "gasoline_92",
    "дт евро":  "diesel",
    "дт":       "diesel",
    "газ":      "lpg",
    "adblue":   "adblue",
}


class A100Scraper(BaseScraper):
    """
    Парсер сети АЗС А-100 (42 АЗС, Беларусь).

    ЦЕНЫ одинаковые для всех АЗС — берём с главной страницы:
      <div class="price-item__label">АИ-95</div>
      <div class="price-item__price">2,67</div>

    КООРДИНАТЫ и АДРЕСА: страница карты /set-azs/map-azs/ содержит
    JS-инициализацию карты с данными всех АЗС в <script> теге.
    Ищем паттерн: var stations = [...] или JSON.parse('[...]')
    или data-атрибуты на div#map.

    Если JS-данные не найдены — берём адреса из HTML-таблицы (без координат)
    и сохраняем АЗС только с адресом, без lat/lon.
    """

    HOME_URL = "https://azs.a-100.by/"
    MAP_URL  = "https://azs.a-100.by/set-azs/map-azs/"

    def __init__(self, client):
        super().__init__(client)
        self.country = "BY"
        self.currency = "BYN"
        self.brand = "А-100"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,*/*",
            "Referer": "https://azs.a-100.by/",
        }

    # ── Цены с главной страницы ───────────────────────────────────────────────

    def _fetch_prices(self) -> dict:
        """
        Берёт актуальные цены с главной страницы А-100.
        Цены одинаковые для всех АЗС сети.

        Структура HTML:
        <div class="price-item__label">АИ-95</div>
        <div class="price-item__price">2,67</div>
        """
        r = requests.get(self.HOME_URL, headers=self.headers, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        prices = {}
        # Ищем блок с ценами (класс price-block или price-map-block)
        price_block = soup.find(class_=re.compile(r"price-block|price-map-block"))
        if not price_block:
            print("[BY/А-100] ⚠ Блок цен не найден на главной странице!")
            return {}

        items = price_block.find_all(class_=re.compile(r"price-item|price-block__li"))
        for item in items:
            label = item.find(class_=re.compile(r"label|name"))
            price_el = item.find(class_=re.compile(r"price(?!-)"))  # price, но не price-item
            if not label or not price_el:
                continue

            fuel_name = label.get_text(strip=True).lower()
            # Убираем подсказку (tooltip) из названия
            fuel_name = re.sub(r"\s+", " ", fuel_name).strip()

            price_str = price_el.get_text(strip=True).replace(",", ".")
            fuel_type = A100_FUEL_MAP.get(fuel_name)

            if fuel_type:
                try:
                    price = float(price_str)
                    if price > 0:
                        prices[fuel_type] = price
                except ValueError:
                    pass

        print(f"[BY/А-100] Цены с главной: {prices}")
        return prices

    # ── Координаты АЗС ────────────────────────────────────────────────────────

    def _fetch_stations(self) -> list[dict]:
        """
        Загружает список АЗС со страницы карты.

        Пробует несколько способов найти координаты:
        1. JSON в <script> теге — самый надёжный
        2. data-атрибуты на элементах карты
        3. HTML-таблица (адреса без координат) — запасной вариант
        """
        r = requests.get(self.MAP_URL, headers=self.headers, timeout=30)
        r.raise_for_status()
        html = r.text
        soup = BeautifulSoup(html, "html.parser")

        # ── Способ 1: JSON в <script> тегах ──────────────────────────────────
        stations = self._try_extract_from_scripts(soup)
        if stations:
            print(f"[BY/А-100] Координаты найдены в JS: {len(stations)} АЗС")
            return stations

        # ── Способ 2: data-атрибуты на элементах ─────────────────────────────
        stations = self._try_extract_from_data_attrs(soup)
        if stations:
            print(f"[BY/А-100] Координаты найдены в data-атрибутах: {len(stations)} АЗС")
            return stations

        # ── Способ 3: HTML-таблица (адреса без координат) ─────────────────────
        print(f"[BY/А-100] ⚠ Координаты не найдены. Сохраняем АЗС только с адресами.")
        return self._extract_from_html_table(soup)

    def _try_extract_from_scripts(self, soup) -> list[dict]:
        """Ищет JSON с данными АЗС внутри <script> тегов."""
        for script in soup.find_all("script"):
            text = script.string or ""
            if not text or "lat" not in text.lower():
                continue

            # Паттерны JS: var stations = [...], BX.message({...}), window.stations = [...]
            patterns = [
                r'(?:var\s+stations|window\.stations)\s*=\s*(\[.+?\])\s*[;,]',
                r'BX\.message\s*\(\s*(\{.+?\})\s*\)',
                r'"stations"\s*:\s*(\[.+?\])',
                r'stations\s*:\s*(\[.+?\])',
            ]

            for pattern in patterns:
                m = re.search(pattern, text, re.DOTALL)
                if not m:
                    continue
                try:
                    data = json.loads(m.group(1))
                    if isinstance(data, list) and data:
                        return self._normalize_station_list(data)
                    elif isinstance(data, dict):
                        for key in ("stations", "azs", "items"):
                            if key in data and isinstance(data[key], list):
                                return self._normalize_station_list(data[key])
                except (json.JSONDecodeError, KeyError):
                    continue

        return []

    def _try_extract_from_data_attrs(self, soup) -> list[dict]:
        """Ищет координаты в data-атрибутах HTML-элементов."""
        stations = []
        # data-lat / data-lng или data-coords
        for el in soup.find_all(attrs={"data-lat": True}):
            try:
                lat = float(el.get("data-lat") or el.get("data-latitude", 0))
                lon = float(el.get("data-lng") or el.get("data-lon") or
                            el.get("data-longitude", 0))
                if lat and lon:
                    num = el.get("data-id") or el.get("data-num") or ""
                    addr = el.get("data-address") or el.get("title") or ""
                    stations.append({
                        "number": str(num),
                        "address": addr,
                        "lat": lat,
                        "lon": lon,
                    })
            except (ValueError, TypeError):
                continue

        return stations

    def _extract_from_html_table(self, soup) -> list[dict]:
        """
        Запасной вариант: берём адреса из HTML-таблицы.
        Координат нет — АЗС сохранятся без lat/lon.
        """
        stations = []
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cols = row.find_all("td")
                if len(cols) < 2:
                    continue
                number_text = cols[0].get_text(strip=True)
                address = cols[1].get_text(strip=True) if len(cols) > 1 else ""
                num_m = re.search(r"(\d+)", number_text)
                if num_m:
                    stations.append({
                        "number": num_m.group(1),
                        "address": address,
                        "lat": None,
                        "lon": None,
                    })
        return stations

    def _normalize_station_list(self, raw: list) -> list[dict]:
        """Нормализует список АЗС из разных форматов JS."""
        result = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            lat = item.get("lat") or item.get("latitude") or item.get("LATITUDE")
            lon = (item.get("lon") or item.get("lng") or
                   item.get("longitude") or item.get("LONGITUDE"))
            num = (str(item.get("id") or item.get("ID") or
                       item.get("num") or item.get("number") or ""))
            addr = (item.get("address") or item.get("ADDRESS") or
                    item.get("addr") or "")
            if lat and lon:
                result.append({
                    "number": num,
                    "address": addr,
                    "lat": float(lat),
                    "lon": float(lon),
                })
        return result

    # ── Основной метод ────────────────────────────────────────────────────────

    def scrape(self):
        print(f"[BY/А-100] Загружаем цены с главной страницы...")
        prices = self._fetch_prices()

        print(f"[BY/А-100] Загружаем список АЗС...")
        stations = self._fetch_stations()
        print(f"[BY/А-100] Получено {len(stations)} АЗС")

        if not stations:
            raise RuntimeError(
                "Не удалось получить список АЗС А-100. "
                "Сайт использует JavaScript-карту — нужно найти API вручную. "
                "Открой /set-azs/map-azs/ → F12 → Network → Fetch/XHR"
            )

        for st in stations:
            try:
                number = st.get("number", "")
                station_id = upsert_station(self.client, {
                    "country":   self.country,
                    "brand":     self.brand,
                    "name":      f"{self.brand} №{number}" if number else self.brand,
                    "address":   st.get("address", ""),
                    "city":      "Беларусь",
                    "latitude":  st.get("lat"),   # может быть None
                    "longitude": st.get("lon"),   # может быть None
                    "logo_url":  self.get_brand_logo(self.brand),
                    "source_id": f"a100_{number}" if number else f"a100_{st.get('lat')}_{st.get('lon')}",
                })
                self.stations_count += 1

                # Применяем общие цены к каждой АЗС
                for fuel_type, price in prices.items():
                    upsert_price(self.client, station_id,
                                 fuel_type, price, self.currency)
                    self.prices_count += 1

            except Exception as e:
                print(f"[BY/А-100] Ошибка АЗС №{st.get('number')}: {e}")

        print(f"[BY/А-100] Готово: {self.stations_count} АЗС, {self.prices_count} цен")

        if self.stations_count > 0 and not stations[0].get("lat"):
            print(f"[BY/А-100] ⚠ АЗС сохранены без координат.")
            print(f"[BY/А-100]   Чтобы добавить координаты — найди API карты через DevTools.")
