import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
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
    title_el = soup.find("h1", class_="c-item-title")
    if title_el:
        title_text = title_el.get_text(strip=True)
        if "," in title_text:
            parts = title_text.split(",", 1)
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
    df = scrape_sauto_min_inzeraty(base_url, min_inzeraty=45, max_pages=1, max_workers=500)
    print("\nNáhled na prvních 5 řádků:")
    print(df.head())
