import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

pd.set_option('display.max_colwidth', None)

headers = {
    "User-Agent": "Mozilla/5.0"
}

def get_listing_links(listing_url):
    """
    Získá odkazy na detaily aut z jedné stránky Sauto (kde je seznam inzerátů)
    a odstraní duplicitní URL.
    """
    try:
        response = requests.get(listing_url, headers=headers, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f"Chyba při načítání listingu {listing_url}: {e}")
        return []

    soup = BeautifulSoup(response.text, "html.parser")
    cars = soup.find_all("a", class_="sds-surface sds-surface--clickable sds-surface--00 c-item__link")
    links = []
    for car in cars:
        href = car.get("href")
        if not href:
            continue
        full_url = href if href.startswith("http") else "https://www.sauto.cz" + href
        links.append(full_url)
    # Odstraníme duplicity URL
    return list(set(links))

def parse_sauto_detail(url):
    """
    Stáhne a naparsuje detail inzerátu z Sauto.
    Vrací inzerát jako slovník s klíči: URL, Značka, Model, Rok, Najeté km, Cena, Palivo, Převodovka, Výkon (kW).
    """
    # Defaultní hodnoty
    brand = "Nezjištěno"
    model = "Nezjištěno"
    year_val = "Nezjištěno"
    mileage_val = "Nezjištěno"
    price_val = "Nezjištěno"
    fuel_val = "Nezjištěno"
    gearbox_val = "Nezjištěno"
    power_kw = "Nezjištěno"

    # Fallback z URL (pokud se nepodaří nic najít přímo na stránce)
    fallback_brand = None
    fallback_model = None
    try:
        url_parts = url.split('/detail/')[1].split('/')
        if len(url_parts) >= 2:
            fallback_brand = url_parts[0].capitalize()
            fallback_model = url_parts[1].capitalize()
    except Exception:
        pass

    # Stáhneme stránku
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f"Chyba při načítání detailu {url}: {e}")
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

    soup = BeautifulSoup(response.text, "html.parser")

    # 1) Značka a Model z titulku
    title_el = soup.find("h1", class_="c-item-title")
    if title_el:
        title_text = title_el.get_text(strip=True)
        if "," in title_text:
            parts = title_text.split(",", 1)
            if fallback_brand and fallback_model:
                brand = fallback_brand
                model = fallback_model
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

    # Pokud se v modelu vyskytuje čárka, ořízneme ji
    if isinstance(model, str) and "," in model:
        model = model.split(",", 1)[0].strip()

    # 2) Rok a Najeté km z <span class="c-a-basic-info__subtitle-info">
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
                        rok_cislo = int(splitted[1].strip())
                        if 1900 < rok_cislo < 2100:
                            year_val = rok_cislo
                    except:
                        pass
            # Pokud je to jen rok (např. "2022")
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

    # 3) Cena
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

    # 4) Palivo, Převodovka, Výkon (kW) – z tiles (<li class="c-car-properties__tile">)
    tiles = soup.find_all("li", class_="c-car-properties__tile")
    tile_data = {}
    for tile in tiles:
        label_div = tile.find("div", class_="c-car-properties__tile-label")
        value_div = tile.find("div", class_="c-car-properties__tile-value")
        if not label_div or not value_div:
            continue
        label_txt = label_div.get_text(strip=True)
        value_txt = value_div.get_text(strip=True)
        normalized_label = label_txt.strip()

        # Pokud je to "Výkon", ukládáme nejvyšší hodnotu (pokud jich je víc)
        if normalized_label == "Výkon":
            try:
                current_val = int(tile_data.get("Výkon", "0").replace("kW", "").replace(" ", ""))
            except:
                current_val = 0
            try:
                new_val = int(value_txt.replace("kW", "").replace(" ", ""))
            except:
                new_val = 0
            if new_val > current_val:
                tile_data["Výkon"] = value_txt
        else:
            if normalized_label not in tile_data:
                tile_data[normalized_label] = value_txt

    fuel_val = tile_data.get("Palivo", "Nezjištěno")
    gearbox_val = tile_data.get("Převodovka", "Nezjištěno")
    power_kw = tile_data.get("Výkon", "Nezjištěno")

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
    """
    Zpracuje jednu stránku Sauto, najde odkazy na auta a paralelně naparsuje jejich detaily.
    """
    links = get_listing_links(listing_url)
    print(f"Na stránce '{listing_url}' nalezeno {len(links)} inzerátů.")
    results = []
    seen = set()  # pokud chceme deduplikovat

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(parse_sauto_detail, link): link for link in links}
        for future in as_completed(future_to_url):
            link = future_to_url[future]
            try:
                car_data = future.result()
                # Dedup klíč (můžete vypnout, pokud nechcete)
                key = (
                    car_data["Značka"],
                    car_data["Model"],
                    car_data["Rok"],
                    car_data["Najeté km"],
                    car_data["Cena"],
                    car_data["Palivo"],
                    car_data["Převodovka"],
                    car_data["Výkon (kW)"]
                )
                if key in seen:
                    continue
                seen.add(key)
                results.append(car_data)

                # Formátovaný výpis včetně URL
                print("URL:        ", car_data["URL"])
                print("Značka:     ", car_data["Značka"])
                print("Model:      ", car_data["Model"])
                print("Rok:        ", car_data["Rok"])
                print("Najeté km:  ", car_data["Najeté km"])
                print("Cena:       ", f"{car_data['Cena']} Kč" if car_data["Cena"] != "Nezjištěno" else "Nezjištěno")
                print("Palivo:     ", car_data["Palivo"])
                print("Převodovka: ", car_data["Převodovka"])
                print("Výkon (kW): ", car_data["Výkon (kW)"])
                print("-" * 60)

            except Exception as e:
                print(f"Chyba při parsování detailu {link}: {e}")

    return results

def scrape_sauto_min_inzeraty(listing_url_base,
                              min_inzeraty=50,
                              max_pages=2,
                              max_workers=10):
    """
    Prochází více stránek (až max_pages) a sbírá inzeráty.
    Pokud se nasbírá min_inzeraty, končí.
    Výsledná data se uloží do CSV, ale sloupec URL odstraníme.
    """
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

    # Vytvoříme DataFrame
    df = pd.DataFrame(all_results)

    # Sloupec URL nechceme v CSV, proto ho před uložením odstraníme
    if "URL" in df.columns:
        df.drop(columns=["URL"], inplace=True)

    df.to_csv("auta_sauto.csv", index=False, encoding="utf-8-sig")
    print(f"\nHotovo! Uloženo {len(df)} záznamů do 'auta_sauto.csv'.")
    return df

if __name__ == "__main__":
    base_url = "https://www.sauto.cz/inzerce/osobni"
    df = scrape_sauto_min_inzeraty(
        listing_url_base=base_url,
        min_inzeraty=10000,    # pro rychlejší test
        max_pages=20000000,        # pro rychlejší test
        max_workers=5
    )

    print("\nNáhled na prvních 5 řádků:")
    print(df.head())
