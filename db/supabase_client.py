import os
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

def get_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("Не найдены SUPABASE_URL или SUPABASE_KEY!")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def upsert_station(client: Client, station: dict) -> int:
    result = client.table("stations").upsert(
        station,
        on_conflict="country,source_id"
    ).execute()
    return result.data[0]["id"]


def upsert_price(client: Client, station_id: int, fuel_type: str,
                 price: float, currency: str):
    old = client.table("fuel_prices").select("price").eq(
        "station_id", station_id
    ).eq("fuel_type", fuel_type).execute()

    old_price = float(old.data[0]["price"]) if old.data else None

    client.table("fuel_prices").upsert({
        "station_id": station_id,
        "fuel_type": fuel_type,
        "price": price,
        "currency": currency,
        "updated_at": "now()"
    }, on_conflict="station_id,fuel_type").execute()

    if old_price is None or abs(old_price - price) > 0.0001:
        client.table("price_history").insert({
            "station_id": station_id,
            "fuel_type": fuel_type,
            "price": price,
            "currency": currency
        }).execute()


def log_scrape(client: Client, country: str) -> int:
    result = client.table("scrape_logs").insert({
        "country": country,
        "status": "running"
    }).execute()
    return result.data[0]["id"]


def finish_log(client: Client, log_id: int, stations: int,
               prices: int, error: str = None):
    client.table("scrape_logs").update({
        "finished_at": "now()",
        "stations_found": stations,
        "prices_updated": prices,
        "status": "error" if error else "success",
        "error_message": error
    }).eq("id", log_id).execute()
