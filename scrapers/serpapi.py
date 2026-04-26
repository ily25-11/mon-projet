import requests
import pandas as pd
import hashlib
from datetime import datetime
import os

API_KEY = "TA_CLE_SERPAPI"
OUTPUT_PATH = "/opt/airflow/data/offres_serpapi.csv"

QUERIES = [
    "data engineer france",
    "data scientist france",
    "data analyst france",
]

def scraper_serpapi():

    offres = []

    for q in QUERIES:

        params = {
            "engine": "google_jobs",
            "q": q,
            "hl": "fr",
            "api_key": API_KEY
        }

        url = "https://serpapi.com/search"

        resp = requests.get(url, params=params)

        if resp.status_code != 200:
            print("Erreur API")
            continue

        jobs = resp.json().get("jobs_results", [])

        for job in jobs:
            titre = job.get("title", "N/A")
            entreprise = job.get("company_name") or "Non spécifiée"
            lien = job.get("related_links", [{}])[0].get("link", "")

            hash_id = hashlib.sha1(
                f"{titre}|{entreprise}|{lien}".encode()
            ).hexdigest()[:16]

            offres.append({
                "hash_id": hash_id,
                "titre": titre,
                "entreprise": entreprise,
                "lieu": job.get("location", "N/A"),
                "secteur": "Informatique / Tech",
                "description": "",
                "lien": lien,
                "date_scraping": datetime.now().strftime("%Y-%m-%d"),
                "source": "serpapi",
            })

    df = pd.DataFrame(offres)
    df.drop_duplicates(subset=["hash_id"], inplace=True)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"SerpAPI : {len(df)} offres")

    return df.to_dict("records")


def scraper_serpapi_task():
    offres = scraper_serpapi()
    return len(offres)