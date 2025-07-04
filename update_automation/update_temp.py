import urllib.request
import re
import numpy as np
import pandas as pd
import time
import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

MAX_WORKERS = 3  # Number of concurrent downloads

def from_html_to_dict(html_station_string):
    result = re.findall(r'VALUES\[\d+\] = new Array\("\d+","\d+/\d+/\d+","\d+.\d+","\d*.*\d*"\)', html_station_string)
    station_day = {}
    for entry in result:
        parts = entry.split('"')
        date = parts[3]
        value = float(parts[5]) if parts[5] else None
        station_day[date] = value
    return station_day

def fetch_single_station(stazione):
    retries = 1
    while True:
        try:
            url = f"http://www.sir.toscana.it/monitoraggio/dettaglio.php?id={stazione}&title=&type=termo_men"
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})

            with urllib.request.urlopen(req) as fp:
                content_type = fp.headers.get('Content-Type', '')
                if 'image' in content_type.lower():
                    print(f"⚠️  Station {stazione} returned an image, skipping.")
                    return stazione, ""  # Return empty HTML to indicate skip
                html = fp.read().decode("utf8")
            return stazione, html
        # Skip also if exeption in terms of 500 internal server error occurs, retry for all other exceptions
        except Exception as e:
            if "500" in str(e):
                print(f"⚠️  Station {stazione} encountered a 500 error, skipping.")
                return stazione, ""  # Return empty HTML to indicate skip
            wait = retries * 10
            print(f"❌ Error for {stazione}: {e} — retrying in {wait}s")
            time.sleep(wait)

def fetch_station_data_parallel(station_ids):
    htmls = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_single_station, st): st for st in station_ids}
        for future in tqdm.tqdm(as_completed(futures), total=len(futures), desc="Fetching stations"):
            stazione, html = future.result()
            htmls[stazione] = html
    return htmls

def main():
    # Read the cleaned station metadata
    stazioni = pd.read_csv("assets/stazioni.csv", sep=";")
    stazioni = stazioni.drop_duplicates(subset="IDStazione")
    stazioni.set_index("IDStazione", inplace=True)

    # Fetch data in parallel
    htmls = fetch_station_data_parallel(stazioni.index)

    # Parse all HTMLs
    dati_stazioni = {id_: from_html_to_dict(htmls[id_]) for id_ in htmls}

    # Assemble final dataframe
    dati_pandas = pd.DataFrame.from_dict(dati_stazioni, orient="index")
    dati_completi = pd.concat([stazioni, dati_pandas], axis=1)
    dati_completi.reset_index(inplace=True)  # Bring IDStazione back as a column
    dati_completi = dati_completi.drop(columns=[
        'Fiume', 'Provincia', 'Comune', 'StazioneExtra',
        'Strumento', 'QuotaTerra', 'IDSensoreRete'
    ], errors='ignore')
    dati_completi.fillna(0, inplace=True)

    # Save result
    dati_completi.to_csv("assets/temp_completi.csv", encoding="utf8", index=False)
    print("✅ File saved to assets/temp_completi.csv")

if __name__ == "__main__":
    main()
