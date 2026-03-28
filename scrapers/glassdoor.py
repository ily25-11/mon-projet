import requests
import pandas as pd
import os
from datetime import datetime

API_KEY = "8ade4281d2msh542f283bb88cd93p1ce916jsn8589dbf9385d"

def scraper_glassdoor(poste="data scientist"):
    print(f"🔍 Démarrage JSearch Glassdoor : {poste}")

    offres = []
    headers = {
        "X-RapidAPI-Key": API_KEY,
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com"
    }

    for page in range(1, 4):
        try:
            params = {
                "query": poste,
                "page": str(page),
                "num_pages": "1",
                "date_posted": "month",
                "country": "fr",
                "language": "fr"
            }

            response = requests.get(
                "https://jsearch.p.rapidapi.com/search",
                headers=headers,
                params=params,
                timeout=30
            )

            print(f"📡 Page {page} - Status : {response.status_code}")
            jobs = response.json().get("data", [])

            for job in jobs:
                offres.append({
                    "titre": job.get("job_title", "N/A"),
                    "entreprise": job.get("employer_name", "N/A"),
                    "lieu": job.get("job_city", "N/A"),
                    "salaire_min": job.get("job_min_salary", "N/A"),
                    "salaire_max": job.get("job_max_salary", "N/A"),
                    "remote": job.get("job_is_remote", False),
                    "description": job.get("job_description", "N/A")[:200],
                    "lien": job.get("job_apply_link", "N/A"),
                    "date_scraping": datetime.now().strftime("%Y-%m-%d"),
                    "source": "glassdoor"
                })

            print(f"✅ Page {page} : {len(jobs)} offres récupérées")

        except Exception as e:
            print(f"❌ Erreur page {page} : {e}")
            continue

    os.makedirs("/opt/airflow/data", exist_ok=True)

    if offres:
        df = pd.DataFrame(offres)
        chemin = "/opt/airflow/data/offres_glassdoor.csv"
        df.to_csv(chemin, index=False, encoding="utf-8")
        print(f"💾 {len(offres)} offres sauvegardées dans {chemin}")
    else:
        print("❌ Aucune offre récupérée")

    return offres


if __name__ == "__main__":
    offres = scraper_glassdoor("data scientist")
    print(f"\n📊 Total : {len(offres)} offres")
    if offres:
        print("\nAperçu des 3 premières offres :")
        for o in offres[:3]:
            print(f"  - {o['titre']} | {o['entreprise']} | {o['lieu']}")