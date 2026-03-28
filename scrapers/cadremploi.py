import requests
import pandas as pd
import os
from datetime import datetime

def scraper_cadremploi(poste="data scientist"):
    print(f"🔍 Démarrage Arbeitnow API : {poste}")

    offres = []

    for page in range(1, 4):
        try:
            url = "https://www.arbeitnow.com/api/job-board-api"
            params = {
                "search": poste,
                "page": page
            }

            response = requests.get(url, params=params, timeout=30)
            print(f"📡 Page {page} - Status : {response.status_code}")

            jobs = response.json().get("data", [])

            for job in jobs:
                offres.append({
                    "titre": job.get("title", "N/A"),
                    "entreprise": job.get("company_name", "N/A"),
                    "lieu": job.get("location", "N/A"),
                    "remote": job.get("remote", False),
                    "description": job.get("description", "N/A")[:200],
                    "lien": job.get("url", "N/A"),
                    "tags": ", ".join(job.get("tags", [])),
                    "date_scraping": datetime.now().strftime("%Y-%m-%d"),
                    "source": "arbeitnow"
                })

            print(f"✅ Page {page} : {len(jobs)} offres récupérées")

        except Exception as e:
            print(f"❌ Erreur page {page} : {e}")
            continue

    os.makedirs("/opt/airflow/data", exist_ok=True)

    if offres:
        df = pd.DataFrame(offres)
        chemin = "/opt/airflow/data/offres_cadremploi.csv"
        df.to_csv(chemin, index=False, encoding="utf-8")
        print(f"💾 {len(offres)} offres sauvegardées dans {chemin}")
    else:
        print("❌ Aucune offre récupérée")

    return offres


if __name__ == "__main__":
    offres = scraper_cadremploi("data scientist")
    print(f"\n📊 Total : {len(offres)} offres")
    if offres:
        print("\nAperçu des 3 premières offres :")
        for o in offres[:3]:
            print(f"  - {o['titre']} | {o['entreprise']} | {o['lieu']}")