"""
Парсеры: Нефтика (RU), Трасса (RU), А-100 (BY).
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

    Сайт старый (2016), данные встроены в HTML главной страницы.

    ОСОБЕННОСТИ HTML (найдены при отладке):

    1. Координаты могут стоять либо в ОТДЕЛЬНОМ h4:
         <h4>ААЗС №16 ул.Речная</h4>
         <h4>N 53.278785 E 34.385325</h4>
       ЛИБО В ТОЙ ЖЕ СТРОКЕ что и название:
         <h4>ААЗС №33 г. Новозыбков ул. Мичурина  N 52.526853 E 31.961448</h4>

    2. Цены иногда разбиты на несколько <strong>:
         <strong>Аи-92 плюс 55</strong><strong>,17</strong>
       Поэтому нельзя парсить каждый <strong> отдельно —
       нужно сначала склеить текст соседних тегов.

    3. Некоторые h4 содержат только координаты без названия.

    РЕШЕНИЕ: парсим ВСЕ h4 у которых есть координаты (в любом месте строки),
    а цены собираем из ВСЕГО текста блока (склеивая соседние strong-теги).
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

    def _extract_coord(self, text: str):
        """Ищет координаты в строке. Возвращает (lat, lon) или (None, None)."""
        m = re.search(r"N\s*([\d.]+)\s*E\s*([\d.]+)", text, re.I)
        return (float(m.group(1)), float(m.group(2))) if m else (None, None)

    def _extract_name(self, text: str) -> str:
        """Убирает координаты из строки названия, возвращает чистое имя."""
        clean = re.sub(r"N\s*[\d.]+\s*E\s*[\d.]+", "", text, flags=re.I)
        return clean.strip(" \t\n,.")

    def _collect_price_text(self, start_tag) -> str:
        """
        Собирает весь текст цен начиная с тега start_tag до следующего блока АЗС.

        Проблема: цена "55,17" может быть разбита на два <strong>:
          <strong>Аи-92 плюс 55</strong><strong>,17</strong>
        Решение: склеиваем текст всех соседних strong-тегов идущих подряд,
        а потом уже ищем в склеенной строке топливо+цену.
        """
        lines = []
        current_line = ""

        for tag in start_tag.find_next_siblings():
            # Стоп — следующий блок АЗС (h4 с координатами или h3 раздела)
            if tag.name == "h3":
                break
            if tag.name == "h4":
                # h4 с координатами — новая АЗС
                if re.search(r"N\s*[\d.]+\s*E", tag.get_text(), re.I):
                    break
                # h4 без координат — тоже новая АЗС (название следующей)
                break

            # Собираем все strong внутри тега
            strongs = tag.find_all("strong") if tag.name != "strong" else [tag]
            if not strongs:
                # Тег без strong — если была накоплена строка, сохраняем
                if current_line.strip():
                    lines.append(current_line.strip())
                    current_line = ""
                continue

            for strong in strongs:
                text = strong.get_text()
                # Если текст начинается с запятой или цифры — склеиваем с предыдущим
                if current_line and re.match(r"^[,.\d]", text.strip()):
                    current_line += text
                else:
                    if current_line.strip():
                        lines.append(current_line.strip())
                    current_line = text

        if current_line.strip():
            lines.append(current_line.strip())

        return "\n".join(lines)

    def _parse_prices_from_text(self, text: str) -> list[tuple]:
        """
        Парсит цены из многострочного текста.
        Возвращает список (fuel_type, price).

        Формат строки: "Аи-95 59,57" или "ДТ Акция 59,77"
        """
        results = []
        for line in text.split("\n"):
            line = line.strip()
            if not line:
                continue
            # Пропускаем служебные строки
            if "цены" in line.lower() and "действительны" in line.lower():
                continue
            if "временно" in line.lower():
                continue

            # Ищем число в конце строки
            m = re.search(r"([\d]+[.,][\d]+)\s*$", line)
            if not m:
                continue

            price_str = m.group(1).replace(",", ".")
            fuel_raw = re.sub(r"\s+", " ", line[:m.start()].strip().lower())
            fuel_type = NEFTIKA_FUEL_MAP.get(fuel_raw)
            if not fuel_type:
                continue

            try:
                price = float(price_str)
                if price > 0:
                    results.append((fuel_type, price))
            except ValueError:
                pass

        return results

    def scrape(self):
        print(f"[RU/Нефтика] Загружаем {self.URL}...")
        r = requests.get(self.URL, headers=self.headers, timeout=30)
        r.raise_for_status()
        r.encoding = "utf-8"
        soup = BeautifulSoup(r.text, "html.parser")

        # Находим ВСЕ h4 у которых ЕСТЬ координаты (в любом месте текста)
        all_h4 = soup.find_all("h4")
        coord_tags = [
            tag for tag in all_h4
            if re.search(r"N\s*[\d.]+\s*E\s*[\d.]+", tag.get_text(), re.I)
        ]
        print(f"[RU/Нефтика] Найдено h4 с координатами: {len(coord_tags)}")

        # Словарь source_id → данные станции (чтобы не дублировать)
        seen_ids = set()

        for h4 in coord_tags:
            try:
                full_text = h4.get_text(strip=True)
                lat, lon = self._extract_coord(full_text)
                if lat is None:
                    continue

                name = self._extract_name(full_text)

                # Если название пустое — берём предыдущий h4 (там может быть имя)
                if not name or len(name) < 3:
                    prev_h4 = h4.find_previous("h4")
                    if prev_h4:
                        prev_text = prev_h4.get_text(strip=True)
                        if not re.search(r"N\s*[\d.]+\s*E", prev_text, re.I):
                            name = prev_text

                if not name:
                    name = self.brand

                # source_id из номера АЗС
                num_m = re.search(r"[№#№]?\s*(\d+)", name)
                source_id = f"neftika_{num_m.group(1)}" if num_m else f"neftika_{lat}_{lon}"

                if source_id in seen_ids:
                    continue
                seen_ids.add(source_id)

                # Город — ближайший h3/h4 заголовок раздела выше
                city = ""
                for prev in h4.find_all_previous(["h3", "h4"]):
                    t = prev.get_text(strip=True)
                    if re.search(r"N\s*[\d.]+\s*E", t, re.I):
                        continue
                    if re.search(r"(г\.|город|обл\.|р-н|район|пгт\.)", t, re.I):
                        city = t
                        break

                station_id = upsert_station(self.client, {
                    "country":   self.country,
                    "brand":     self.brand,
                    "name":      name,
                    "address":   name,
                    "city":      city,
                    "latitude":  lat,
                    "longitude": lon,
                    "logo_url":  self.get_brand_logo(self.brand),
                    "source_id": source_id,
                })
                self.stations_count += 1

                # Собираем цены — идём по siblings от этого h4
                price_text = self._collect_price_text(h4)
                for fuel_type, price in self._parse_prices_from_text(price_text):
                    upsert_price(self.client, station_id,
                                 fuel_type, price, self.currency)
                    self.prices_count += 1

            except Exception as e:
                print(f"[RU/Нефтика] Ошибка блока '{h4.get_text()[:40]}': {e}")

        print(f"[RU/Нефтика] Готово: {self.stations_count} АЗС, {self.prices_count} цен")


# ─────────────────────────────────────────────────────────────────────────────
# ТРАССА
# ─────────────────────────────────────────────────────────────────────────────

TRASSA_FUEL_MAP = {
    "98":      "gasoline_98",
    "95":      "gasoline_95",
    "92":      "gasoline_92",
    "пс":      "gasoline_95_premium",
    "дт":      "diesel",
    "дизель":  "diesel",
    "эл.":     "electric",
    "эл":      "electric",
    "суг":     "lpg",
    "газ":     "lpg",
    "метан":   "cng",
    "adblue":  "adblue",
    "diesel":  "diesel",
}


class TrassaScraper(BaseScraper):
    """
    Парсер сети АЗС Трасса / ПАО ЕвроТранс (~58 станций, Московский регион).

    Адреса + координаты: HTML-таблица на /locator.
    Цены: HTML карточки АЗС — структура из DevTools:
      <a class="mgo-card">
        <div class="head">95</div>
        <div class="body"><div class="price">70.68</div></div>
      </a>

    ПРОБЛЕМА TIMEOUT: trassagk.ru блокирует запросы с серверов GitHub Actions
    по IP-адресу. Решение — добавить User-Agent браузера и задержки.
    Если timeout повторяется — сайт нужно парсить через прокси или
    исключить из автоматического запуска (запускать вручную).
    """

    LOCATOR_URL = "https://trassagk.ru/locator"

    def __init__(self, client):
        super().__init__(client)
        self.country = "RU"
        self.currency = "RUB"
        self.brand = "Трасса"
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,*/*;q=0.9",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://trassagk.ru/",
            "Connection": "keep-alive",
        })

    def _load_stations_html(self) -> list[dict]:
        """Загружает список АЗС из HTML-таблицы на /locator."""
        # Увеличенный timeout — сайт медленный
        r = self.session.get(self.LOCATOR_URL, timeout=60)
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
        Запрашивает HTML-карточку АЗС и парсит цены.
        Пробует несколько вариантов URL.
        """
        if not number:
            return {}

        candidate_urls = [
            f"https://trassagk.ru/locator/?id={number}",
            f"https://trassagk.ru/locator?id={number}",
            f"https://trassagk.ru/api/station/?id={number}",
            f"https://trassagk.ru/api/azs/{number}/",
        ]

        for url in candidate_urls:
            try:
                r = self.session.get(url, timeout=20)
                if r.status_code != 200:
                    continue

                # Пробуем JSON
                try:
                    data = r.json()
                    prices = self._extract_prices_json(data)
                    if prices:
                        return prices
                except Exception:
                    pass

                # Пробуем HTML
                prices = self._extract_prices_html(r.text)
                if prices:
                    return prices

            except Exception:
                continue

        return {}

    def _extract_prices_html(self, html: str) -> dict:
        """
        Парсит цены из HTML-карточки.
        Структура: <a class="mgo-card">
                     <div class="head">95</div>
                     <div class="body"><div class="price">70.68</div></div>
                   </a>
        """
        soup = BeautifulSoup(html, "html.parser")
        prices = {}

        for card in soup.find_all(class_="mgo-card"):
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
                pass
        return prices

    def _extract_prices_json(self, data) -> dict:
        """Извлекает цены из JSON ответа."""
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
            name = (item.get("name") or item.get("head") or item.get("type") or "").lower()
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

        try:
            stations = self._load_stations_html()
        except requests.exceptions.ConnectTimeout:
            print(f"[RU/Трасса] ⚠ Timeout — сайт trassagk.ru недоступен с серверов GitHub.")
            print(f"[RU/Трасса]   Возможные причины:")
            print(f"[RU/Трасса]   1. Сайт блокирует IP-адреса GitHub Actions")
            print(f"[RU/Трасса]   2. Временные проблемы с сайтом")
            print(f"[RU/Трасса]   Решение: добавить повтор через 5 мин или исключить из автозапуска.")
            raise
        except Exception as e:
            print(f"[RU/Трасса] Ошибка загрузки: {e}")
            raise

        print(f"[RU/Трасса] Найдено {len(stations)} АЗС")

        for st in stations:
            try:
                prices = self._fetch_prices(st["number"])

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

                time.sleep(0.3)

            except Exception as e:
                print(f"[RU/Трасса] Ошибка {st.get('name')}: {e}")

        print(f"[RU/Трасса] Готово: {self.stations_count} АЗС, {self.prices_count} цен")

        if self.stations_count > 0 and self.prices_count == 0:
            print(f"[RU/Трасса] ⚠ АЗС сохранены без цен.")
            print(f"[RU/Трасса]   Нужно уточнить URL карточки через DevTools:")
            print(f"[RU/Трасса]   Открой trassagk.ru/locator → кликни АЗС → F12 → Network")
            print(f"[RU/Трасса]   Найди Fetch/XHR запрос который вернул HTML с ценами")


# ─────────────────────────────────────────────────────────────────────────────
# А-100 (БЕЛАРУСЬ)
# ─────────────────────────────────────────────────────────────────────────────

A100_FUEL_MAP = {
    "аи-98":    "gasoline_98",
    "аи-95":    "gasoline_95",
    "аи-92":    "gasoline_92",
    "дт евро":  "diesel",
    "дт":       "diesel",
    # "газ" — отдельная обработка: текст содержит tooltip с описанием,
    # поэтому ищем по началу строки
    "adblue":   "adblue",
}


class A100Scraper(BaseScraper):
    """
    Парсер сети АЗС А-100 (42 АЗС, Беларусь).

    Цены одинаковые для всех АЗС — берём с главной страницы.
    Координаты — из JavaScript на странице карты.
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

    def _fetch_prices(self) -> dict:
        """
        Парсит цены с главной страницы.

        Структура HTML:
          <div class="price-item__label">АИ-95</div>
          <div class="price-item__price">2,67</div>

        ИСПРАВЛЕНИЕ: поле "Газ" содержит <span class="tooltip..."> внутри label,
        поэтому get_text() возвращает "Газ Углеводородный сжиженный газ...".
        Берём только первое слово/фразу до пробела.
        """
        r = requests.get(self.HOME_URL, headers=self.headers, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")

        prices = {}
        for item in soup.find_all(class_=re.compile(r"price-item(?!__)")):
            label_el = item.find(class_=re.compile(r"price-item__label"))
            price_el  = item.find(class_=re.compile(r"price-item__price"))
            if not label_el or not price_el:
                continue

            # Берём только прямой текстовый узел label (без tooltip-спана)
            # Метод: ищем первый NavigableString внутри label
            label_text = ""
            for child in label_el.children:
                # NavigableString — это текстовый узел (не тег)
                if hasattr(child, "strip"):
                    label_text = child.strip()
                    if label_text:
                        break

            if not label_text:
                label_text = label_el.get_text(separator=" ").strip()
                # Берём только первую часть до возможного tooltip-текста
                label_text = label_text.split("\n")[0].strip()

            fuel_name = label_text.lower().strip()
            price_str = price_el.get_text(strip=True).replace(",", ".")

            # Газ — особый случай
            if fuel_name.startswith("газ"):
                fuel_type = "lpg"
            else:
                fuel_type = A100_FUEL_MAP.get(fuel_name)

            if not fuel_type:
                continue

            try:
                price = float(price_str)
                if price > 0:
                    prices[fuel_type] = price
            except ValueError:
                pass

        print(f"[BY/А-100] Цены: {prices}")
        return prices

    def _fetch_stations(self) -> list[dict]:
        """
        Загружает список АЗС со страницы карты.
        Пробует 3 способа найти координаты.
        """
        r = requests.get(self.MAP_URL, headers=self.headers, timeout=30)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        html = r.text

        # Способ 1: JSON в <script> тегах
        stations = self._from_scripts(soup)
        if stations:
            print(f"[BY/А-100] Координаты из JS: {len(stations)} АЗС")
            return stations

        # Способ 2: data-атрибуты
        stations = self._from_data_attrs(soup)
        if stations:
            print(f"[BY/А-100] Координаты из data-атрибутов: {len(stations)} АЗС")
            return stations

        # Способ 3: ищем lat/lon прямо в тексте скриптов regex-ом
        stations = self._from_script_regex(html)
        if stations:
            print(f"[BY/А-100] Координаты из regex в JS: {len(stations)} АЗС")
            return stations

        # Запасной: HTML-таблица без координат
        print(f"[BY/А-100] ⚠ Координаты не найдены. Берём адреса из HTML.")
        return self._from_html_table(soup)

    def _from_scripts(self, soup) -> list[dict]:
        """Ищет JSON массив с данными АЗС в тегах <script>."""
        for script in soup.find_all("script"):
            text = script.string or ""
            if not text or len(text) < 100:
                continue
            if not ("lat" in text or "latitude" in text or "LATITUDE" in text):
                continue

            # Разные паттерны JS инициализации
            patterns = [
                r'(?:var\s+\w*[Ss]tation\w*|window\.\w*[Ss]tation\w*)\s*=\s*(\[.+?\])\s*[;,]',
                r'"(?:stations|items|azs|points|markers)"\s*:\s*(\[.+?\])',
                r'(?:stations|items|points|markers)\s*:\s*(\[.+?\])',
                r'BX\.message\s*\(\s*(\{.+?\})\s*\)',
                # Bitrix CMS (сайт А-100 скорее всего на Bitrix)
                r'arResult\s*=\s*(\[.+?\])',
                r'STATIONS\s*=\s*(\[.+?\])',
            ]

            for pattern in patterns:
                for m in re.finditer(pattern, text, re.DOTALL):
                    try:
                        data = json.loads(m.group(1))
                        if isinstance(data, list) and len(data) > 5:
                            result = self._normalize(data)
                            if result:
                                return result
                        elif isinstance(data, dict):
                            for key in ("stations", "azs", "items", "STATIONS"):
                                if key in data and isinstance(data[key], list):
                                    result = self._normalize(data[key])
                                    if result:
                                        return result
                    except (json.JSONDecodeError, KeyError):
                        continue
        return []

    def _from_data_attrs(self, soup) -> list[dict]:
        """Ищет координаты в data-атрибутах элементов."""
        stations = []
        for el in soup.find_all(attrs={"data-lat": True}):
            try:
                lat = float(el.get("data-lat", 0))
                lon = float(el.get("data-lng") or el.get("data-lon") or
                            el.get("data-longitude", 0))
                if lat and lon:
                    stations.append({
                        "number": str(el.get("data-id") or el.get("data-num") or ""),
                        "address": el.get("data-address") or el.get("title") or "",
                        "lat": lat, "lon": lon,
                    })
            except (ValueError, TypeError):
                continue
        return stations

    def _from_script_regex(self, html: str) -> list[dict]:
        """
        Ищет пары координат напрямую в тексте JS через regex.
        Ищем паттерн: "lat":53.xxx,"lon":27.xxx или похожие.
        """
        # Ищем все объекты с lat+lon в одной строке
        pattern = re.compile(
            r'"(?:lat|latitude|LAT)"\s*:\s*([\d.]+)'
            r'.*?'
            r'"(?:lon|lng|longitude|LON|LNG)"\s*:\s*([\d.]+)',
            re.DOTALL
        )
        stations = []
        for m in pattern.finditer(html):
            try:
                lat = float(m.group(1))
                lon = float(m.group(2))
                # Фильтруем координаты Беларуси: lat 51-56, lon 23-33
                if 51 < lat < 56 and 23 < lon < 33:
                    stations.append({"number": "", "address": "", "lat": lat, "lon": lon})
            except ValueError:
                continue

        # Убираем дубликаты по координатам
        seen = set()
        unique = []
        for st in stations:
            key = (round(st["lat"], 4), round(st["lon"], 4))
            if key not in seen:
                seen.add(key)
                unique.append(st)

        return unique

    def _from_html_table(self, soup) -> list[dict]:
        """Запасной вариант: адреса из HTML-таблицы без координат."""
        stations = []
        for table in soup.find_all("table"):
            for row in table.find_all("tr"):
                cols = row.find_all("td")
                if len(cols) < 2:
                    continue
                num_m = re.search(r"(\d+)", cols[0].get_text())
                if num_m:
                    stations.append({
                        "number": num_m.group(1),
                        "address": cols[1].get_text(strip=True) if len(cols) > 1 else "",
                        "lat": None, "lon": None,
                    })
        return stations

    def _normalize(self, raw: list) -> list[dict]:
        """Нормализует сырые данные АЗС из JS."""
        result = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            lat = (item.get("lat") or item.get("latitude") or
                   item.get("LAT") or item.get("LATITUDE"))
            lon = (item.get("lon") or item.get("lng") or
                   item.get("longitude") or item.get("LON") or
                   item.get("LNG") or item.get("LONGITUDE"))
            if lat and lon:
                result.append({
                    "number": str(item.get("id") or item.get("ID") or
                                  item.get("num") or item.get("NUMBER") or ""),
                    "address": (item.get("address") or item.get("ADDRESS") or
                                item.get("addr") or ""),
                    "lat": float(lat),
                    "lon": float(lon),
                })
        return result

    def scrape(self):
        print(f"[BY/А-100] Загружаем цены...")
        prices = self._fetch_prices()

        print(f"[BY/А-100] Загружаем список АЗС...")
        stations = self._fetch_stations()
        print(f"[BY/А-100] Получено {len(stations)} АЗС")

        if not stations:
            raise RuntimeError("Не удалось получить список АЗС А-100.")

        for st in stations:
            try:
                number = st.get("number", "")
                sid = f"a100_{number}" if number else f"a100_{st.get('lat')}_{st.get('lon')}"

                station_id = upsert_station(self.client, {
                    "country":   self.country,
                    "brand":     self.brand,
                    "name":      f"{self.brand} №{number}" if number else self.brand,
                    "address":   st.get("address", ""),
                    "city":      "Беларусь",
                    "latitude":  st.get("lat"),
                    "longitude": st.get("lon"),
                    "logo_url":  self.get_brand_logo(self.brand),
                    "source_id": sid,
                })
                self.stations_count += 1

                for fuel_type, price in prices.items():
                    upsert_price(self.client, station_id,
                                 fuel_type, price, self.currency)
                    self.prices_count += 1

            except Exception as e:
                print(f"[BY/А-100] Ошибка АЗС №{st.get('number')}: {e}")

        print(f"[BY/А-100] Готово: {self.stations_count} АЗС, {self.prices_count} цен")

        no_coords = sum(1 for s in stations if not s.get("lat"))
        if no_coords:
            print(f"[BY/А-100] ⚠ {no_coords} АЗС без координат.")
            print(f"[BY/А-100]   Открой /set-azs/map-azs/ → F12 → Network → Fetch/XHR")
            print(f"[BY/А-100]   Найди запрос с координатами → пришли URL")
