import os
from supabase import create_client, Client

# Читаем URL и ключ из переменных окружения (секреты GitHub)
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def get_client() -> Client:
    """Создаёт и возвращает клиент Supabase."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Не найдены SUPABASE_URL или SUPABASE_KEY в переменных окружения!")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def upsert_station(client: Client, station: dict) -> int:
    """
    Добавляет АЗС или обновляет если уже есть.
    Возвращает ID станции в базе данных.
    """
    result = client.table("stations").upsert(
        station,
        on_conflict="country,source_id"  # Если такая страна+ID уже есть — обновляем
    ).execute()
    
    return result.data[0]["id"]


def upsert_price(client: Client, station_id: int, fuel_type: str, 
                 price: float, currency: str):
    """
    Обновляет текущую цену топлива на АЗС.
    Если цена изменилась — добавляет запись в историю.
    """
    # Получаем старую цену (если есть)
    old = client.table("fuel_prices").select("price").eq(
        "station_id", station_id
    ).eq("fuel_type", fuel_type).execute()
    
    old_price = float(old.data[0]["price"]) if old.data else None
    
    # Обновляем текущую цену
    client.table("fuel_prices").upsert({
        "station_id": station_id,
        "fuel_type": fuel_type,
        "price": price,
        "currency": currency,
        "updated_at": "now()"
    }, on_conflict="station_id,fuel_type").execute()
    
    # Если цена изменилась (или это первый раз) — записываем в историю
    if old_price is None or abs(old_price - price) > 0.0001:
        client.table("price_history").insert({
            "station_id": station_id,
            "fuel_type": fuel_type,
            "price": price,
            "currency": currency
        }).execute()


def log_scrape(client: Client, country: str) -> int:
    """Создаёт запись о начале сбора данных. Возвращает ID лога."""
    result = client.table("scrape_logs").insert({
        "country": country,
        "status": "running"
    }).execute()
    return result.data[0]["id"]


def finish_log(client: Client, log_id: int, stations: int, 
               prices: int, error: str = None):
    """Обновляет запись лога по завершении."""
    client.table("scrape_logs").update({
        "finished_at": "now()",
        "stations_found": stations,
        "prices_updated": prices,
        "status": "error" if error else "success",
        "error_message": error
    }).eq("id", log_id).execute()