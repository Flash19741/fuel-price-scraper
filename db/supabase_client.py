import os
import time
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")


def get_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Не найдены SUPABASE_URL или SUPABASE_KEY!")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def _retry(func, attempts=3):
    """
    Вспомогательная функция: пытается выполнить func() до 3 раз.
    Если все попытки провалились — пробрасывает последнюю ошибку.
    Между попытками ждёт 1, затем 2 секунды.
    """
    last_error = None
    for attempt in range(attempts):
        try:
            return func()
        except Exception as e:
            last_error = e
            if attempt < attempts - 1:
                wait = 2 ** attempt  # 1 сек, потом 2 сек
                print(f"[DB] Ошибка (попытка {attempt + 1}/{attempts}): {e}. Повтор через {wait}с...")
                time.sleep(wait)
    raise last_error


def upsert_station(client: Client, station: dict) -> int:
    """
    Сохраняет АЗС в БД. Если такая АЗС уже есть (по country + source_id) —
    обновляет данные. Если нет — создаёт новую запись.
    Один запрос к БД вместо двух (было: SELECT + INSERT/UPDATE).

    ВАЖНО: в Supabase таблица stations должна иметь UNIQUE constraint
    на колонки (country, source_id). Как создать — см. README.
    """
    def do_upsert():
        result = client.table("stations").upsert(
            station,
            on_conflict="country,source_id"
        ).execute()
        return result.data[0]["id"]

    return _retry(do_upsert)


def upsert_price(client: Client, station_id: int, fuel_type: str,
                 price: float, currency: str):
    """
    Обновляет текущую цену топлива на АЗС.
    Если цена изменилась — дополнительно записывает в историю цен.
    """
    # Шаг 1: читаем старую цену (чтобы понять, изменилась ли)
    def get_old():
        return client.table("fuel_prices").select("price").eq(
            "station_id", station_id
        ).eq("fuel_type", fuel_type).execute()

    old = _retry(get_old)
    old_price = float(old.data[0]["price"]) if old.data else None

    # Шаг 2: сохраняем актуальную цену (upsert по station_id + fuel_type)
    def do_upsert():
        client.table("fuel_prices").upsert({
            "station_id": station_id,
            "fuel_type": fuel_type,
            "price": price,
            "currency": currency,
            "updated_at": "now()"
        }, on_conflict="station_id,fuel_type").execute()

    _retry(do_upsert)

    # Шаг 3: если цена изменилась (или её раньше не было) — пишем в историю
    if old_price is None or abs(old_price - price) > 0.0001:
        def write_history():
            client.table("price_history").insert({
                "station_id": station_id,
                "fuel_type": fuel_type,
                "price": price,
                "currency": currency
            }).execute()

        _retry(write_history)


def log_scrape(client: Client, country: str) -> int:
    """Создаёт запись о начале сбора данных по стране. Возвращает ID записи."""
    def do_insert():
        result = client.table("scrape_logs").insert({
            "country": country,
            "status": "running"
        }).execute()
        return result.data[0]["id"]

    return _retry(do_insert)


def finish_log(client: Client, log_id: int, stations: int,
               prices: int, error: str = None):
    """Обновляет запись лога: фиксирует результат и время окончания."""
    def do_update():
        client.table("scrape_logs").update({
            "finished_at": "now()",
            "stations_found": stations,
            "prices_updated": prices,
            "status": "error" if error else "success",
            "error_message": error
        }).eq("id", log_id).execute()

    _retry(do_update)
