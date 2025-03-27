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
        csv_file: cesta k CSV souboru, např. "data/auta_aaaauto.csv"
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
                # Pokud by v CSV nebyl sloupec URL (teď tam nebude),
                # pak stačí vrátit prázdnou množinu.
                if "URL" in df.columns:
                    return set(df["URL"].unique())
            except Exception as e:
                print(f"Chyba při načítání existujících dat: {e}")
        return set()

    def save_ads(self, ads):
        """
        Uloží nové záznamy (ads) do CSV souboru.
        Pokud CSV už existuje, spojí staré a nové záznamy a odstraní duplicity
        podle hodnot sloupců, které jsou v ads.
        """
        if not ads:
            return

        df_new = pd.DataFrame(ads)
        if os.path.exists(self.csv_file):
            df_existing = pd.read_csv(self.csv_file)
            # Sloupec URL už v ads nebude, takže klidně vynecháme deduplikaci
            # podle URL. Můžeš ale zvolit jiný klíč, pokud ho tam máš.
            # Pro ukázku deduplikaci vypneme (nebo deduplikuj podle 'Model', atd.)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            # df_combined.drop_duplicates(subset=["Model", "Rok"], inplace=True)
        else:
            df_combined = df_new

        df_combined.to_csv(self.csv_file, index=False)

# -----------------------------------------------------------
# 2) HLAVNÍ LOGIKA SCRAPERU
# -----------------------------------------------------------

CSV_FILE = "data/auta_aaaauto.csv"
MAX_ADS = 1000
NUM_PAGES = 100

csv_manager = CSVManager(CSV_FILE)
existing_urls = csv_manager.load_existing_urls()

def is_complete(ad):
    required_keys = ["Značka", "Model", "Rok", "Najeté km", "Cena", "Palivo", "Převodovka", "Výkon (kW)"]
    for key in required_keys:
        if ad.get(key, "Nezjištěno") == "Nezjištěno":
            return False
    return True

def print_ad_no_url(ad):
    """
    Vypíše inzerát do terminálu "hezky pod sebou" bez URL.
    """
    print("========================================")
    for key, value in ad.items():
        print(f"{key}: {value}")
    print("========================================\n")

def clean_numeric(value):
    return value.replace("kW", "").strip()

import re

def fetch_url(url, session, max_retries=3, timeout=30):
    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response
        except Exception as e:
            print(f"Chyba při načítání {url} (pokus {attempt}/{max_retries}): {e}")
            time.sleep(0.5)
    return None

import requests

session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0"
})

def get_listing_links_from_pages(base_url, num_pages=5, session=None):
    all_links = []
    for page in range(1, num_pages+1):
        url = f"{base_url}?page={page}"
        try:
            response = session.get(url, timeout=10)
            response.raise_for_status()
        except Exception as e:
            print(f"Chyba při načítání listingu {url}: {e}")
            continue
        soup = BeautifulSoup(response.text, "html.parser")
        links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "car.html" in href:
                if not href.startswith("http"):
                    href = "https://www.aaaauto.cz" + href
                links.append(href)
        all_links.extend(links)
        time.sleep(0.5)
    return list(set(all_links))

def parse_aaaauto_detail(url, session):
    """
    Vrací slovník BEZ klíče 'URL'.
    """
    # Tohle si držíme jen kvůli kontrole duplicity
    if url in existing_urls:
        return None

    response = fetch_url(url, session)
    if not response:
        return None

    soup = BeautifulSoup(response.text, "html.parser")

    # Připravíme si slovník s daty (bez URL)
    details_no_url = {
        "Značka": "Nezjištěno",
        "Model": "Nezjištěno",
        "Rok": "Nezjištěno",
        "Najeté km": "Nezjištěno",
        "Cena": "Nezjištěno",
        "Palivo": "Nezjištěno",
        "Převodovka": "Nezjištěno",
        "Výkon (kW)": "Nezjištěno"
    }

    # 1) Cena
    price_el = soup.find("strong", class_="carCard__price-value carCard__price-value--big textGrey notranslate")
    if price_el:
        price_text = price_el.get_text(" ", strip=True)
        price_text = price_text.replace("Kč", "").replace("\xa0", "").replace(" ", "").strip()
        details_no_url["Cena"] = price_text

    # 2) Další údaje
    li_tags = soup.find_all("li")
    found_automat = False
    for li in li_tags:
        text = li.get_text(" ", strip=True).lower()
        strong = li.find("strong")
        if strong:
            value = strong.get_text(strip=True)
            if "značka" in text:
                details_no_url["Značka"] = value
            elif "model" in text:
                details_no_url["Model"] = value
            elif "rok" in text:
                details_no_url["Rok"] = value
            elif "tachometr" in text:
                details_no_url["Najeté km"] = value
            elif "palivo" in text:
                details_no_url["Palivo"] = value
            elif "výkon" in text:
                details_no_url["Výkon (kW)"] = clean_numeric(value)

        if "automat" in text:
            found_automat = True

    if found_automat:
        details_no_url["Převodovka"] = "Automat"
    else:
        details_no_url["Převodovka"] = "Manuál"

    time.sleep(0.5)

    if is_complete(details_no_url):
        # Hezky vypíšeme do terminálu
        print_ad_no_url(details_no_url)
        return details_no_url
    else:
        return None

if __name__ == "__main__":
    base_url = "https://www.aaaauto.cz/ojete-vozy/"  # Příklad URL
    links = get_listing_links_from_pages(base_url, NUM_PAGES, session)
    # Omezíme počet odkazů
    links = links[:MAX_ADS]
    print(f"Nalezeno {len(links)} odkazů (limit {MAX_ADS}).")

    ads = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        future_to_url = {executor.submit(parse_aaaauto_detail, url, session): url for url in links}
        for future in as_completed(future_to_url):
            ad = future.result()
            if ad:
                ads.append(ad)

    # Uložíme do CSV (už bez sloupce URL)
    csv_manager.save_ads(ads)
