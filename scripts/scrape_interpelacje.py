#!/usr/bin/env python3
"""
Scraper interpelacji i zapytań radnych z BIP Lublin.

Źródło: https://bip.lublin.eu/rada-miasta-lublin/

Interpelacje i zapytania to dokumenty dostępne w BIP,
zazwyczaj w formie artykułów z załącznikami (PDF).

Użycie:
  python3 scrape_interpelacje.py [--output docs/interpelacje.json]
                                 [--kadencja IX]
                                 [--fetch-details]
                                 [--debug]

UWAGA: Uruchom lokalnie — sandbox Cowork blokuje domeny
"""

import argparse
import json
import os
import re
import sys
import time

try:
    import requests
except ImportError:
    print("Wymagany moduł: pip install requests")
    sys.exit(1)

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("Wymagany moduł: pip install beautifulsoup4")
    sys.exit(1)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL = "https://bip.lublin.eu"

KADENCJE = {
    "IX":   {"label": "IX kadencja (2024–2029)"},
    "VIII": {"label": "VIII kadencja (2018–2024)"},
}

HEADERS = {
    "User-Agent": "Radoskop/1.0 (https://lublin.radoskop.pl; kontakt@radoskop.pl)",
    "Accept": "text/html",
}

DELAY = 0.5


# ---------------------------------------------------------------------------
# Scraping — list page
# ---------------------------------------------------------------------------

def fetch_page(session, url, debug=False):
    """Pobiera stronę HTML."""
    if debug:
        print(f"  [DEBUG] GET {url}")

    resp = session.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


def parse_interpelacje_list(html, debug=False):
    """Parsuje stronę listy interpelacji.

    Struktura może się różnić w zależności od BIP.
    Szukamy artykułów zawierających słowa "interpelacja" lub "zapytanie".
    """
    soup = BeautifulSoup(html, "html.parser")
    records = []

    # Szukamy linków do artykułów z interpelacjami
    for a in soup.find_all("a", href=True):
        text = a.get_text(strip=True)
        href = a["href"]

        # Szukamy artykułów z słowem "interpelacja" lub "zapytanie" w tytule
        text_lower = text.lower()
        if not ("interpelacja" in text_lower or "zapytanie" in text_lower or
                "wniosek" in text_lower):
            continue

        if not href.startswith("http"):
            href = f"{BASE_URL}{href}" if href.startswith("/") else f"{BASE_URL}/{href}"

        # Określ typ
        typ = "interpelacja"
        if "zapytanie" in text_lower:
            typ = "zapytanie"
        elif "wniosek" in text_lower:
            typ = "wniosek"

        record = {
            "przedmiot": text,
            "bip_url": href,
            "typ": typ,
            "radny": "",
            "status": "",
        }
        records.append(record)

    if debug:
        print(f"  [DEBUG] Parsed {len(records)} records from list page")

    return records


def fetch_detail(session, bip_url, debug=False):
    """Pobiera szczegóły interpelacji z jej strony."""
    if not bip_url:
        return {}

    if debug:
        print(f"  [DEBUG] GET {bip_url}")

    try:
        resp = session.get(bip_url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        detail = {}

        # Szukamy danych w tabelach lub paragrafach
        for row in soup.find_all("tr"):
            th = row.find("th")
            td = row.find("td")
            if not th or not td:
                continue

            label = th.get_text(strip=True).lower()
            val = td.get_text(strip=True)

            if "typ" in label:
                detail["typ_full"] = val
            elif "nr" in label or "numer" in label:
                detail["nr_sprawy"] = val
            elif "data" in label and "wytworzenia" in label:
                detail["data_wplywu"] = parse_date(val)

        # Szukamy załączników
        attachments = []
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            text = a.get_text(strip=True)
            if ".pdf" in href.lower() or "attachment" in href.lower():
                full_url = f"{BASE_URL}{href}" if href.startswith("/") else href
                attachments.append({"nazwa": text, "url": full_url})

                text_lower = text.lower()
                if "odpowied" in text_lower:
                    detail["odpowiedz_url"] = full_url
                elif not detail.get("tresc_url"):
                    detail["tresc_url"] = full_url

        if attachments:
            detail["zalaczniki"] = attachments

        return detail
    except Exception as e:
        if debug:
            print(f"  [DEBUG] Error fetching detail {bip_url}: {e}")
        return {}


def parse_date(raw):
    """Konwertuje datę na format YYYY-MM-DD."""
    if not raw:
        return ""
    raw = raw.strip()
    # DD.MM.YYYY lub DD.MM.YYYY HH:MM
    m = re.match(r"(\d{2})\.(\d{2})\.(\d{4})", raw)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    # YYYY-MM-DD
    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", raw)
    if m:
        return raw[:10]
    return raw


# ---------------------------------------------------------------------------
# Category classification
# ---------------------------------------------------------------------------

CATEGORIES = {
    "transport": ["transport", "komunikacj", "autobus", "tramwaj", "drog", "ulic", "rondo",
                  "chodnik", "przejści", "parkow", "rower", "ścieżk", "mpk", "przystank",
                  "sygnaliz", "skrzyżow"],
    "infrastruktura": ["infrastru", "remont", "naprawa", "budow", "inwesty", "moderniz",
                       "oświetl", "kanalizacj", "wodociąg", "nawierzch", "most"],
    "bezpieczeństwo": ["bezpiecz", "straż", "policj", "monitoring", "kradzież", "wandal",
                       "przestęp", "patrol"],
    "edukacja": ["szkoł", "edukacj", "przedszkol", "żłob", "nauczyc", "kształc",
                 "oświat", "uczni"],
    "zdrowie": ["zdrow", "szpital", "leczni", "medyc", "lekarz", "przychodni",
                "ambulat"],
    "środowisko": ["środowisk", "zieleń", "drzew", "park ", "recykl", "odpady",
                   "śmieci", "klimat", "ekolog", "powietrz", "smog", "hałas"],
    "mieszkalnictwo": ["mieszka", "lokal", "zasob", "czynsz", "wspólnot", "kamieni",
                       "dewelop", "budynek"],
    "kultura": ["kultur", "bibliotek", "muzeum", "teatr", "koncert", "festiwal",
                "zabytek", "zabytk"],
    "sport": ["sport", "boisko", "stadion", "basen", "siłowni", "hala sport",
              "rekrea"],
    "pomoc społeczna": ["społeczn", "pomoc", "bezdomn", "senior", "niepełnospr",
                        "opiek", "zasiłk"],
    "budżet": ["budżet", "finansow", "wydatk", "dotacj", "środki", "pieniąd",
               "podatk"],
    "administracja": ["administrac", "urzęd", "pracowni", "regulam", "organizac",
                      "procedur", "biurokrac"],
}


def classify_category(przedmiot):
    """Klasyfikuje kategorię interpelacji na podstawie przedmiotu."""
    if not przedmiot:
        return "inne"
    text = przedmiot.lower()
    for cat, keywords in CATEGORIES.items():
        for kw in keywords:
            if kw in text:
                return cat
    return "inne"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def scrape(kadencje, output_path, fetch_details=True, debug=False):
    """Główna funkcja scrapowania interpelacji."""
    session = requests.Session()
    all_records = []

    # Główne ścieżki do interpelacji w BIP Lublin
    INTERPELACJE_URLS = [
        f"{BASE_URL}/rada-miasta-lublin/ix-kadencja/interpelacje-i-zapytania/",
        f"{BASE_URL}/rada-miasta-lublin/interpelacje/",
        f"{BASE_URL}/rada-miasta-lublin/",
    ]

    print(f"\n=== Pobieranie interpelacji i zapytań ===")

    for url in INTERPELACJE_URLS:
        print(f"\nPróbuję: {url}")
        try:
            html = fetch_page(session, url, debug=debug)
            records = parse_interpelacje_list(html, debug=debug)
            if records:
                print(f"  → znaleziono {len(records)} rekordów")
                all_records.extend(records)
        except Exception as e:
            print(f"  BŁĄD: {e}")

    if not all_records:
        print("\nUWAGA: Nie znaleziono interpelacji na żadnej stronie!")
        all_records = []

    print(f"\nPobrano: {len(all_records)} rekordów")

    # Optionally fetch details for each record
    if fetch_details and all_records:
        print(f"\nPobieram szczegóły ({len(all_records)} rekordów)...")
        for i, rec in enumerate(all_records):
            bip_url = rec.get("bip_url", "")
            if not bip_url:
                continue
            detail = fetch_detail(session, bip_url, debug=debug)
            if detail:
                rec.update({k: v for k, v in detail.items() if v})
            if (i + 1) % 50 == 0:
                print(f"  Szczegóły: {i+1}/{len(all_records)}")
            time.sleep(0.3)

    # Classify categories and normalize fields
    for rec in all_records:
        rec["kategoria"] = classify_category(rec.get("przedmiot", ""))

        # Normalize status
        status = rec.get("status", "").lower()
        rec["odpowiedz_status"] = status

        # Ensure consistent output fields
        rec.setdefault("data_wplywu", "")
        rec.setdefault("data_odpowiedzi", "")
        rec.setdefault("tresc_url", "")
        rec.setdefault("odpowiedz_url", "")
        rec.setdefault("nr_sprawy", "")

    # Sort by newest first
    all_records.sort(
        key=lambda x: x.get("data_wplywu", "") or x.get("bip_url", ""),
        reverse=True,
    )

    # Stats
    interp = sum(1 for r in all_records if r.get("typ") == "interpelacja")
    zap = sum(1 for r in all_records if r.get("typ") == "zapytanie")
    answered = sum(1 for r in all_records if "udzielono" in r.get("odpowiedz_status", ""))
    print(f"\n=== Podsumowanie ===")
    print(f"Interpelacje: {interp}")
    print(f"Zapytania:    {zap}")
    print(f"Z odpowiedzią: {answered}")
    print(f"Razem:        {len(all_records)}")

    # Save
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)

    size_kb = os.path.getsize(output_path) / 1024
    print(f"\nZapisano: {output_path} ({size_kb:.1f} KB)")
    print(f"Gotowe: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Scraper interpelacji i zapytań radnych z BIP Lublin"
    )
    parser.add_argument(
        "--output", default="docs/interpelacje.json",
        help="Ścieżka do pliku wyjściowego (domyślnie: docs/interpelacje.json)"
    )
    parser.add_argument(
        "--kadencja", default="IX",
        help="Kadencja: IX, VIII lub 'all' (domyślnie: IX)"
    )
    parser.add_argument(
        "--skip-details", action="store_true",
        help="Pomiń pobieranie szczegółów (szybciej, ale brak dat i załączników)"
    )
    parser.add_argument(
        "--debug", action="store_true",
        help="Włącz szczegółowe logowanie"
    )
    args = parser.parse_args()

    if args.kadencja.lower() == "all":
        kadencje = list(KADENCJE.keys())
    else:
        kadencje = [k.strip() for k in args.kadencja.split(",")]

    scrape(
        kadencje=kadencje,
        output_path=args.output,
        fetch_details=not args.skip_details,
        debug=args.debug,
    )


if __name__ == "__main__":
    main()
