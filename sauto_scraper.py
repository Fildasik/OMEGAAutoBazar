import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

# -----------------------------------------------------------
# 1) TŘÍDA CSVManager: stará se o složku data/ a o CSV soubor
# -----------------------------------------------------------

class CSVManager:
    def __init__(self, csv_file):
        """
        Konstruktor třídy CSVManager.
        csv_file: cesta k CSV souboru, např. "data/auta_sauto.csv"
        """
        self.csv_file = csv_file
        # Pokud neexistuje adresář, ve kterém csv_file je, vytvoříme ho
        os.makedirs(os.path.dirname(self.csv_file), exist_ok=True)

    def load_existing_urls(self):
        """
        Načte existující URL z CSV, pokud soubor existuje.
        Vrátí množinu URL.
        """
        if os.path.exists(self.csv_file):
            try:
                df = pd.read_csv(self.csv_file)
                if "URL" in df.columns:
                    return set(df["URL"].unique())
            except Exception as e:
                print(f"Chyba při načítání existujících dat: {e}")
        return set()

    def save_ads(self, ads):
        """
        Uloží nové záznamy (ads) do CSV souboru.
        Pokud CSV už existuje, spojí staré a nové záznamy a odstraní duplicity.
        """
        if not ads:
            return

        df_new = pd.DataFrame(ads)
        if os.path.exists(self.csv_file):
            df_existing = pd.read_csv(self.csv_file)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined.drop_duplicates(subset=["URL"], inplace=True)
        else:
            df_combined = df_new

        df_combined.to_csv(self.csv_file, index=False)

# -----------------------------------------------------------
# 2) HLAVNÍ LOGIKA SCRAPERU
# -----------------------------------------------------------

CSV_FILE = "../data/auta_sauto.csv"
MAX_ADS = 50
NUM_PAGES = 5

# Vytvoříme instanci správce CSV
csv_manager = CSVManager(CSV_FILE)
# Načteme existující URL, abychom zabránili duplicitám
existing_urls = csv_manager.load_existing_urls()

def is_complete(ad):
    required_keys = ["Značka", "Model", "Rok", "Najeté km", "Cena", "Palivo", "Převodovka", "Výkon (kW)"]
    for key in required_keys:
        if ad.get(key, "Nezjištěno") == "Nezjištěno":
            return False
    return True

def print_ad(ad):
    print("========================================")
    print(f"URL: {ad.get('URL')}")
    for key, value in ad.items():
        if key != "URL":
            print(f"{key}: {value}")
    print("========================================\n")

def clean_numeric(value):
    # Odstraní text "kW" a přebytečné mezery
    return value.replace("kW", "").strip()

def get_listing_links_from_pages(base_url, num_pages=5):
    """
    Projde num_pages stránek s inzeráty (?page=1..num_pages)
    a z každé stránky vytáhne odkazy na detail inzerátu.
    """
    all_links = []
    headers = {"User-Agent": "Mozilla/5.0"}
    for page in range(1, num_pages + 1):
        url = f"{base_url}?page={page}"
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
        except Exception as e:
            print(f"Chyba při načítání listingu {url}: {e}")
            continue

        soup = BeautifulSoup(response.text, "html.parser")
        cars = soup.find_all("a", class_="sds-surface sds-surface--clickable sds-surface--00 c-item__link")
        for car in cars:
            href = car.get("href")
            if href:
                full_url = href if href.startswith("http") else "https://www.sauto.cz" + href
                all_links.append(full_url)
        time.sleep(0.5)

    return list(set(all_links))

def parse_sauto_detail(url):
    details = {
        "URL": url,
        "Značka": "Nezjištěno",
        "Model": "Nezjištěno",
        "Rok": "Nezjištěno",
        "Najeté km": "Nezjištěno",
        "Cena": "Nezjištěno",
        "Palivo": "Nezjištěno",
        "Převodovka": "Nezjištěno",
        "Výkon (kW)": "Nezjištěno"
    }
    # Kontrola duplicity
    if url in existing_urls:
        return None

    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f"Chyba při načítání detailu {url}: {e}")
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    # 1) Titulek: pokusíme se najít značku a model
    title_el = soup.find("h1", class_="c-item-title")
    if title_el:
        title_text = title_el.get_text(strip=True)
        if "," in title_text:
            parts = title_text.split(",", 1)
            details["Značka"] = parts[0].strip()
            details["Model"] = parts[1].strip()
        else:
            parts = title_text.split(" ", 1)
            if len(parts) == 2:
                details["Značka"] = parts[0].strip()
                details["Model"] = parts[1].strip()

    # 2) Příklad – staticky vyplníme některé údaje
    #    Ve skutečnosti bys je měl vyčíst z HTML podle tagů a class.
    details["Rok"] = "2015"
    details["Najeté km"] = "80000"
    details["Cena"] = "250000"
    details["Palivo"] = "Benzín"
    details["Převodovka"] = "Manuál"
    # 3) Výkon
    raw_power = soup.find("span", class_="power")
    if raw_power:
        details["Výkon (kW)"] = clean_numeric(raw_power.get_text(strip=True))

    time.sleep(0.5)

    if is_complete(details):
        print_ad(details)
        return details
    else:
        return None

if __name__ == "__main__":
    base_url = "https://www.sauto.cz/inzerce/osobni"  # Příklad URL
    links = get_listing_links_from_pages(base_url, NUM_PAGES)
    links = links[:MAX_ADS]
    print(f"Nalezeno {len(links)} odkazů (limit {MAX_ADS}).")

    ads = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(parse_sauto_detail, url): url for url in links}
        for future in as_completed(future_to_url):
            ad = future.result()
            if ad:
                ads.append(ad)

    # Uložíme do CSV přes naši třídu CSVManager
    csv_manager.save_ads(ads)
