import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
<<<<<<< HEAD
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
=======
from concurrent.futures import ThreadPoolExecutor, as_completed

# Aby se dlouhé texty (např. URL) v konzoli nezkracovaly:
pd.set_option('display.max_colwidth', None)

headers = {
    "User-Agent": "Mozilla/5.0"
}

def get_listing_links(listing_url):
    response = requests.get(listing_url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")
    cars = soup.find_all("a", class_="sds-surface sds-surface--clickable sds-surface--00 c-item__link")
    links = []
    for car in cars:
        href = car.get("href")
        if not href:
            continue
        full_url = href if href.startswith("http") else "https://www.sauto.cz" + href
        links.append(full_url)
    return links

def parse_sauto_detail(url):
    # Defaultní hodnoty
    brand = "Nezjištěno"
    model = "Nezjištěno"
    year_val = "Nezjištěno"
    mileage_val = "Nezjištěno"
    price_val = "Nezjištěno"
    fuel_val = "Nezjištěno"
    gearbox_val = "Nezjištěno"
    power_kw = "Nezjištěno"

    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, "html.parser")

    # ------------------------------------------------------------
    # Základní údaje z URL (fallback)
    # ------------------------------------------------------------
    fallback_brand = None
    fallback_model = None
    try:
        url_parts = url.split('/detail/')[1].split('/')
        if len(url_parts) >= 2:
            fallback_brand = url_parts[0].capitalize()
            fallback_model = url_parts[1].capitalize()
    except Exception:
        pass

    # ------------------------------------------------------------
    # 1) Značka a Model z titulku (<h1 class="c-item-title">)
    # ------------------------------------------------------------
>>>>>>> bb487f5469bd694df803453a80042a5bee964868
    title_el = soup.find("h1", class_="c-item-title")
    if title_el:
        title_text = title_el.get_text(strip=True)
        if "," in title_text:
            parts = title_text.split(",", 1)
<<<<<<< HEAD
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
=======
            if fallback_brand and fallback_model:
                brand = fallback_brand
                model = fallback_model
                extra = parts[1].strip()
                if extra:
                    model = model + ", " + extra
            else:
                brand = parts[0].strip()
                model = parts[1].strip()
        else:
            parts = title_text.split(" ", 1)
            if len(parts) == 2:
                brand = parts[0].strip()
                model = parts[1].strip()
            else:
                brand = title_text.strip()
    else:
        if fallback_brand:
            brand = fallback_brand
        if fallback_model:
            model = fallback_model

    # ------------------------------------------------------------
    # 2) Rok a Najeté km z <span class="c-a-basic-info__subtitle-info">
    # Např.: "Ojeté, 5/2014, 136 000 km" nebo "Ojeté, 2022, 21 500 km"
    # ------------------------------------------------------------
    subinfo_el = soup.find("span", class_="c-a-basic-info__subtitle-info")
    if subinfo_el:
        subinfo_text = subinfo_el.get_text(" ", strip=True)
        subinfo_text = subinfo_text.replace("Ojeté", "").replace("Nové", "").strip()
        parts = subinfo_text.split(",")
        for p in parts:
            p_clean = p.strip()
            # Zkusíme formát "5/2014"
            if "/" in p_clean and year_val == "Nezjištěno":
                splitted = p_clean.split("/")
                if len(splitted) == 2:
                    try:
                        year_val = int(splitted[1].strip())
                    except:
                        pass
            # Pokud je to jen rok jako číslo (např. "2022")
            if year_val == "Nezjištěno" and p_clean.isdigit():
                val = int(p_clean)
                if 1900 < val < 2100:
                    year_val = val
            # Najeté km
            if "km" in p_clean.lower() and mileage_val == "Nezjištěno":
                p_clean2 = (p_clean.lower()
                            .replace("km", "")
                            .replace("\xa0", "")
                            .replace(" ", "")
                            .strip())
                try:
                    mileage_val = int(p_clean2)
                except:
                    pass

    # ------------------------------------------------------------
    # 3) Cena (z <div class="c-a-basic-info__price"> nebo fallback <span class="c-basic-info__price">)
    # ------------------------------------------------------------
    price_el = soup.find("div", class_="c-a-basic-info__price")
    if price_el:
        price_txt = price_el.get_text(strip=True)
        price_txt = (price_txt
                     .replace("Kč", "")
                     .replace("\xa0", "")
                     .replace(" ", "")
                     .strip())
        if price_txt:
            price_val = price_txt
    else:
        price_el = soup.find("span", class_="c-basic-info__price")
        if price_el:
            price_txt = price_el.get_text(strip=True)
            price_txt = (price_txt
                         .replace("Kč", "")
                         .replace("\xa0", "")
                         .replace(" ", "")
                         .strip())
            if price_txt:
                price_val = price_txt

    # ------------------------------------------------------------
    # 4) Palivo, Převodovka, Výkon (kW) – z tiles (<li class="c-car-properties__tile">)
    # ------------------------------------------------------------
    tiles = soup.find_all("li", class_="c-car-properties__tile")
    for tile in tiles:
        label_div = tile.find("div", class_="c-car-properties__tile-label")
        value_div = tile.find("div", class_="c-car-properties__tile-value")
        if not label_div or not value_div:
            continue
        label_txt = label_div.get_text(strip=True)
        value_txt = value_div.get_text(strip=True)
        if "Palivo" in label_txt and fuel_val == "Nezjištěno":
            fuel_val = value_txt.strip()
        elif "Převodovka" in label_txt and gearbox_val == "Nezjištěno":
            gearbox_val = value_txt.strip()
        elif "Výkon" in label_txt and power_kw == "Nezjištěno":
            pwr = value_txt.replace("kW", "").replace("\xa0", "").strip()
            if pwr:
                power_kw = pwr

    return {
        "URL": url,
        "Značka": brand,
        "Model": model,
        "Rok": year_val,
        "Najeté km": mileage_val,
        "Cena": price_val,
        "Palivo": fuel_val,
        "Převodovka": gearbox_val,
        "Výkon (kW)": power_kw
    }

def scrape_sauto_one_page(listing_url, max_workers=10):
    links = get_listing_links(listing_url)
    print(f"Našel jsem {len(links)} inzerátů na stránce: {listing_url}")
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_link = {executor.submit(parse_sauto_detail, link): link for link in links}
        for future in as_completed(future_to_link):
            link = future_to_link[future]
            try:
                car_data = future.result()
                print(f"  URL:          {car_data['URL']}")
                print(f"  Značka:       {car_data['Značka']}")
                print(f"  Model:        {car_data['Model']}")
                print(f"  Rok:          {car_data['Rok']}")
                print(f"  Najeté km:    {car_data['Najeté km']}")
                print(f"  Cena:         {car_data['Cena']}")
                print(f"  Palivo:       {car_data['Palivo']}")
                print(f"  Převodovka:   {car_data['Převodovka']}")
                print(f"  Výkon (kW):   {car_data['Výkon (kW)']}")
                print("-" * 60)
                results.append(car_data)
            except Exception as e:
                print(f"  Chyba při parsování detailu {link}: {e}")
    return results

def scrape_sauto_min_inzeraty(listing_url_base, min_inzeraty=500, max_pages=50, max_workers=10):
    all_results = []
    page = 1
    while page <= max_pages:
        page_url = listing_url_base if page == 1 else f"{listing_url_base}?page={page}"
        print(f"\n==== SCRAPUJI STRÁNKU č.{page}: {page_url} ====")
        page_results = scrape_sauto_one_page(page_url, max_workers=max_workers)
        if not page_results:
            print("Žádné další inzeráty – končím.")
            break
        all_results.extend(page_results)
        print(f"Aktuálně nasbíráno {len(all_results)} inzerátů.\n")
        if len(all_results) >= min_inzeraty:
            print(f"Dosaženo {min_inzeraty} inzerátů – končím.")
            break
        page += 1
        time.sleep(2)
    df = pd.DataFrame(all_results)
    # Při ukládání do CSV nechceme mít sloupec URL
    if "URL" in df.columns:
        df.drop(columns=["URL"], inplace=True)
    df.to_csv("auta_sauto.csv", index=False, encoding="utf-8-sig")
    print(f"\nHotovo! Uloženo {len(df)} záznamů do 'auta_sauto.csv'.")
    return df

if __name__ == "__main__":
    base_url = "https://www.sauto.cz/inzerce/osobni"
    # Nastav minimální počet inzerátů – zde je příklad s 45
    df = scrape_sauto_min_inzeraty(base_url, min_inzeraty=200, max_pages=20, max_workers=500)
    print("\nNáhled na prvních 5 řádků:")
    print(df.head())
>>>>>>> bb487f5469bd694df803453a80042a5bee964868
