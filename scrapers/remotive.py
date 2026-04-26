import requests
import pandas as pd
import hashlib
from datetime import datetime
import os

OUTPUT_PATH = "/opt/airflow/data/offres_remotive.csv"

def scraper_remotive():

    url = "https://remotive.com/api/remote-jobs?search=data"
    resp = requests.get(url)

    data = resp.json().get("jobs", [])
    offres = []

    for job in data:
        titre = job.get("title", "N/A")
        entreprise = job.get("company_name") or "Non spécifiée"
        lien = job.get("url")

        hash_id = hashlib.sha1(f"{titre}|{entreprise}|{lien}".encode()).hexdigest()[:16]

        offres.append({
            "hash_id": hash_id,
            "titre": titre,
            "entreprise": entreprise,
            "lieu": job.get("candidate_required_location", "Remote"),
            "secteur": "Informatique / Tech",
            "description": (job.get("description") or "")[:400],
            "lien": lien,
            "date_scraping": datetime.now().strftime("%Y-%m-%d"),
            "source": "remotive",
        })

    df = pd.DataFrame(offres)
    df.drop_duplicates(subset=["hash_id"], inplace=True)

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"Remotive : {len(df)} offres")

    return df.to_dict("records")
def scraper_remotive_task():
    offres = scraper_remotive()
    return len(offres)