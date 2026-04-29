import sys
from db.supabase_client import get_client, log_scrape, finish_log
from scrapers.moldova import MoldovaScraper
from scrapers.romania import RomaniaScraper
from scrapers.italy import ItalyScraper


def run_scraper(scraper_class, client, country_name):
    """
    Запускает один скрапер с логированием и обработкой ошибок.
    Даже если один скрапер упал — остальные продолжат работу.
    """
    log_id = log_scrape(client, country_name)
    
    try:
        scraper = scraper_class(client)
        scraper.scrape()
        
        finish_log(
            client, log_id,
            stations=scraper.stations_count,
            prices=scraper.prices_count
        )
        print(f"✅ {country_name}: успешно завершён")
        
    except Exception as e:
        error_msg = str(e)
        print(f"❌ {country_name}: ОШИБКА — {error_msg}")
        finish_log(client, log_id, stations=0, prices=0, error=error_msg)


def main():
    print("=" * 50)
    print("🚀 Запуск сборщика цен на топливо")
    print("=" * 50)
    
    # Подключаемся к базе данных
    try:
        client = get_client()
        print("✅ Подключение к Supabase успешно")
    except Exception as e:
        print(f"❌ Не удалось подключиться к Supabase: {e}")
        sys.exit(1)
    
    # Запускаем скраперы по очереди
    scrapers = [
        (MoldovaScraper, "MD"),
        (RomaniaScraper, "RO"),
        (ItalyScraper,   "IT"),
    ]
    
    for scraper_class, country in scrapers:
        print(f"\n--- {country} ---")
        run_scraper(scraper_class, client, country)
    
    print("\n" + "=" * 50)
    print("✅ Все скраперы завершили работу")
    print("=" * 50)


if __name__ == "__main__":
    main()
