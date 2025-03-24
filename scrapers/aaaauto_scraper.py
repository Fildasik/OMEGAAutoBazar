import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
import concurrent.futures

pd.set_option('display.max_colwidth', None)

# Vytvoříme persistentní session s hlavičkou
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0"
})

def fetch_url(url, session, max_retries=3, timeout=30):
    """
    Načte URL s opakovanými pokusy, aby se minimalizovaly chyby s timeoutem.
    Používá persistentní session.
    """
    for attempt in range(1, max_retries + 1):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response
        except Exception as e:
            print(f"Chyba při načítání {url} (pokus {attempt}/{max_retries}): {e}")
            if attempt < max_retries:
                time.sleep(1)  # kratší čekání pro rychlejší běh
    return None

def get_listing_links(listing_url, session):
    """
    Načte stránku s inzeráty a najde všechny odkazy obsahující 'car.html'.
    Odstraní duplicity.
    """
    response = fetch_url(listing_url, session)
    if not response:
        return []
    soup = BeautifulSoup(response.text, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "car.html" in href:
            if not href.startswith("http"):
                href = "https://www.aaaauto.cz" + href
            links.append(href)
    return list(set(links))

def parse_aaaauto_detail(url, session):
    """
    Načte detail inzerátu a extrahuje údaje:
      - Značka, Model, Rok, Najeté km, Cena, Palivo, Převodovka, Výkon (kW).
    Používá fetch_url se session.
    """
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

    response = fetch_url(url, session)
    if not response:
        return details

    soup = BeautifulSoup(response.text, "html.parser")

    # 1) Cena – hlavní prvek
    price_el = soup.find("strong", class_="carCard__price-value carCard__price-value--big textGrey notranslate")
    if price_el:
        price_text = price_el.get_text(" ", strip=True)
        price_text = price_text.replace("Kč", "").replace("\xa0", "").replace(" ", "").strip()
        details["Cena"] = price_text

    # 2) Data z <li> elementů
    li_tags = soup.find_all("li")
    for li in li_tags:
        text = li.get_text(" ", strip=True)
        strong = li.find("strong")
        if not strong:
            continue
        value = strong.get_text(strip=True)
        if "Značka" in text:
            details["Značka"] = value
        elif "Model" in text:
            details["Model"] = value
        elif "Rok" in text:
            details["Rok"] = value
        elif "Tachometr" in text:
            details["Najeté km"] = value.replace("km", "").strip()
        elif "Palivo" in text:
            details["Palivo"] = value
        elif "Převodovka" in text:
            if "automat" in value.lower():
                details["Převodovka"] = "Automat"
            else:
                details["Převodovka"] = "Manuál"
        elif "Výkon" in text:
            details["Výkon (kW)"] = value.replace("kW", "").strip()

    # 3) Fallback – pokud některá data chybí, zkusíme další prvky
    if details["Značka"] == "Nezjištěno" or details["Model"] == "Nezjištěno" or details["Rok"] == "Nezjištěno":
        h1_el = soup.find("h1", class_="h2 mb5 notranslate")
        if h1_el:
            span_el = h1_el.find("span", class_="regular")
            if span_el:
                span_text = span_el.get_text(" ", strip=True)
                parts = span_text.split(",")
                if len(parts) >= 2:
                    if details["Model"] == "Nezjištěno":
                        details["Model"] = parts[0].strip()
                    if details["Rok"] == "Nezjištěno":
                        details["Rok"] = parts[1].strip()
                elif len(parts) == 1 and details["Model"] == "Nezjištěno":
                    details["Model"] = parts[0].strip()
                brand_text = h1_el.get_text(" ", strip=True).replace(span_text, "").strip()
                if brand_text and details["Značka"] == "Nezjištěno":
                    details["Značka"] = brand_text
        tr_tags = soup.find_all("tr")
        for tr in tr_tags:
            th = tr.find("th")
            td = tr.find("td")
            if th and td:
                header = th.get_text(" ", strip=True)
                value = td.get_text(" ", strip=True)
                if "Rok uvedení" in header and details["Rok"] == "Nezjištěno":
                    details["Rok"] = value
                elif "Tachometr" in header and details["Najeté km"] == "Nezjištěno":
                    details["Najeté km"] = value.replace("km", "").strip()
                elif "Převodovka" in header and details["Převodovka"] == "Nezjištěno":
                    if "automat" in value.lower():
                        details["Převodovka"] = "Automat"
                    else:
                        details["Převodovka"] = "Manuál"
                elif "Palivo" in header and details["Palivo"] == "Nezjištěno":
                    details["Palivo"] = value

    if details["Převodovka"] == "Nezjištěno":
        details["Převodovka"] = "Manuál"

    return details

def scrape_aaaauto_one_page(listing_url, session, max_workers=20):
    print(f"Scrapování stránky: {listing_url}")
    links = get_listing_links(listing_url, session)
    print(f"Nalezeno {len(links)} odkazů...")
    results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(parse_aaaauto_detail, link, session): link for link in links}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                data = future.result()
                results.append(data)
                print("------------------------------------------------------------")
                print(f"URL:        {data['URL']}")
                print(f"Značka:     {data['Značka']}")
                print(f"Model:      {data['Model']}")
                print(f"Rok:        {data['Rok']}")
                print(f"Najeté km:  {data['Najeté km']}")
                print(f"Cena:       {data['Cena']} Kč")
                print(f"Palivo:     {data['Palivo']}")
                print(f"Převodovka: {data['Převodovka']}")
                print(f"Výkon (kW): {data['Výkon (kW)']}")
            except Exception as exc:
                print(f"Chyba při zpracování detailu {url}: {exc}")
    return results

def scrape_aaaauto(min_inzeraty=50, max_pages=2, max_workers=20):
    base_url = "https://www.aaaauto.cz/ojete-vozy/"
    all_results = []
    page = 1
    while page <= max_pages:
        url = base_url if page == 1 else f"https://www.aaaauto.cz/ojete-vozy/#!&page={page}"
        print(f"\nZpracovávám stránku {page}: {url}")
        page_results = scrape_aaaauto_one_page(url, session, max_workers=max_workers)
        if not page_results:
            print("Žádné další inzeráty – končím.")
            break
        all_results.extend(page_results)
        print(f"Celkem nasbíráno: {len(all_results)} inzerátů.\n")
        if len(all_results) >= min_inzeraty:
            print(f"Dosaženo {min_inzeraty} inzerátů – končím.")
            break
        page += 1
        time.sleep(1)
    df = pd.DataFrame(all_results)
    # Odstraníme URL sloupec před uložením do CSV
    if "URL" in df.columns:
        df.drop(columns=["URL"], inplace=True)
    df.to_csv("auta_aaaauto.csv", index=False, encoding="utf-8-sig")
    print(f"\nUloženo {len(df)} záznamů do 'auta_aaaauto.csv'.")
    return df

if __name__ == "__main__":
    # Nastavte min_inzeraty a max_pages podle potřeby
    df = scrape_aaaauto(min_inzeraty=10000, max_pages=10000, max_workers=20)
    print("\nNáhled na prvních 5 řádků:")
    print(df.head())
