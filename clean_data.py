import pandas as pd


def normalize_transmission(val):
    """
    Sjednotí převodovku na "Automat", "Manuál" nebo "Nezjištěno".
    """
    if pd.isna(val):
        return "Nezjištěno"
    val_lower = str(val).lower().strip()
    if "automat" in val_lower:
        return "Automat"
    elif "manuál" in val_lower or "manuální" in val_lower or "stupňů" in val_lower:
        return "Manuál"
    return "Nezjištěno"


def normalize_fuel(val):
    """
    Sjednotí palivo na základní kategorie.
    """
    if pd.isna(val):
        return "Nezjištěno"
    val_lower = str(val).lower().strip()
    if "benz" in val_lower:
        return "Benzín"
    elif "naft" in val_lower or "diesel" in val_lower:
        return "Nafta"
    elif "hybrid" in val_lower:
        return "Hybrid"
    elif "elekt" in val_lower:
        return "Elektro"
    return "Nezjištěno"


def load_and_merge_data(file1, file2):
    df1 = pd.read_csv(file1)
    df2 = pd.read_csv(file2)
    df_all = pd.concat([df1, df2], ignore_index=True)
    print("Původní počet záznamů:", len(df_all))
    return df_all


def clean_data(df):
    # Normalizace převodovky a paliva
    if "Převodovka" in df.columns:
        df["Převodovka"] = df["Převodovka"].apply(normalize_transmission)
    if "Palivo" in df.columns:
        df["Palivo"] = df["Palivo"].apply(normalize_fuel)

    # Převod sloupců na numerické hodnoty
    numeric_cols = ["Rok", "Najeté km", "Cena", "Výkon (kW)"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Odstranění řádků s chybějícími hodnotami v klíčových sloupcích
    must_have_cols = ["Značka", "Model", "Rok", "Najeté km", "Cena", "Palivo", "Převodovka", "Výkon (kW)"]
    existing_cols = [c for c in must_have_cols if c in df.columns]
    df.dropna(subset=existing_cols, inplace=True)

    # Odstranění duplicit – podle všech klíčových sloupců
    df.drop_duplicates(subset=must_have_cols, keep="first", inplace=True)

    # Odstranění řádků, kde některý z klíčových atributů obsahuje "Nezjištěno"
    df = df[~df[must_have_cols].apply(lambda row: row.astype(str).str.contains("Nezjištěno").any(), axis=1)]

    print("Po čištění a filtrování počet záznamů:", len(df))
    return df


def main():
    # Načtení a sloučení dat – uprav názvy souborů podle tvých CSV
    df_all = load_and_merge_data("auta_sauto.csv", "auta_aaaauto.csv")

    # Vyčištění dat (převedení na čísla, odstranění null, odstranění řádků s "Nezjištěno")
    df_clean = clean_data(df_all)

    # Uložení vyčištěných dat do nového CSV (finální dataset obsahuje jen atributy)
    df_clean.to_csv("auta_cleaned.csv", index=False, encoding="utf-8-sig")
    print("Hotovo! Vyčištěná data jsou uložena v 'auta_cleaned.csv'.")

    # Pro kontrolu zobrazíme prvních pár řádků
    print(df_clean.head())


if __name__ == "__main__":
    main()
