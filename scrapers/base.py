from abc import ABC, abstractmethod
from supabase import Client


class BaseScraper(ABC):
    """
    Базовый класс для всех скраперов.
    Каждый скрапер страны наследует этот класс и реализует метод scrape().
    """
    
    def __init__(self, client: Client):
        self.client = client        # Клиент базы данных
        self.country = ""           # Код страны, напр. 'MD'
        self.currency = ""          # Валюта, напр. 'MDL'
        self.stations_count = 0     # Счётчик обработанных АЗС
        self.prices_count = 0       # Счётчик обновлённых цен
    
    @abstractmethod
    def scrape(self):
        """
        Главный метод — собирает данные и сохраняет в БД.
        Каждый скрапер обязан его реализовать.
        """
        pass
    
    def get_brand_logo(self, brand: str) -> str:
        """
        Возвращает URL логотипа по названию бренда.
        Логотипы берём из общедоступных CDN.
        """
        # Словарь известных брендов → URL логотипа
        logos = {
            "petrom":    "https://upload.wikimedia.org/wikipedia/commons/thumb/0/06/Petrom_logo.svg/200px-Petrom_logo.svg.png",
            "lukoil":    "https://upload.wikimedia.org/wikipedia/commons/thumb/e/e4/Lukoil_logo.svg/200px-Lukoil_logo.svg.png",
            "shell":     "https://upload.wikimedia.org/wikipedia/commons/thumb/8/8c/Shell_Global_logo.svg/200px-Shell_Global_logo.svg.png",
            "bp":        "https://upload.wikimedia.org/wikipedia/commons/thumb/c/c3/BP_Helios_logo.svg/200px-BP_Helios_logo.svg.png",
            "total":     "https://upload.wikimedia.org/wikipedia/commons/thumb/0/0e/Logo_TotalEnergies.svg/200px-Logo_TotalEnergies.svg.png",
            "rompetrol": "https://upload.wikimedia.org/wikipedia/commons/thumb/f/f0/Rompetrol_logo.svg/200px-Rompetrol_logo.svg.png",
            "socar":     "https://upload.wikimedia.org/wikipedia/commons/thumb/a/a5/SOCAR_logo.svg/200px-SOCAR_logo.svg.png",
            "eni":       "https://upload.wikimedia.org/wikipedia/commons/thumb/4/46/ENI_logo.svg/200px-ENI_logo.svg.png",
            "agip":      "https://upload.wikimedia.org/wikipedia/commons/thumb/4/46/ENI_logo.svg/200px-ENI_logo.svg.png",
            "q8":        "https://upload.wikimedia.org/wikipedia/commons/thumb/3/3c/Q8_logo.svg/200px-Q8_logo.svg.png",
        }
        
        brand_lower = brand.lower().strip()
        
        # Ищем частичное совпадение
        for key, url in logos.items():
            if key in brand_lower:
                return url
        
        return None  # Логотип неизвестен