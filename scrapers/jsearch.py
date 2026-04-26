import os
import time
import hashlib
import requests
import pandas as pd
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────────────────────────────────

API_KEY = os.getenv("RAPIDAPI_KEY", "")
OUTPUT_DIR = "/opt/airflow/data"

POSTES = [
    "data scientist",
    "data engineer",
    "data analyst",
]

BASE_URL = "https://jsearch.p.rapidapi.com/search"


# ─── SCRAPER PRINCIPAL ────────────────────────────────────────────────────────

def scraper_jsearch(pages_max=5):

    if not API_KEY:
        raise EnvironmentError("❌ RAPIDAPI_KEY manquante")

    headers = {
        "X-RapidAPI-Key": API_KEY,
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
    }

    toutes = []

    for poste in POSTES:
        print(f"\n🔎 Recherche : {poste}")

        for page in range(1, pages_max + 1):

            params = {
                "query": poste,
                "page": str(page),
                "country": "fr",
                "date_posted": "month",
            }

            try:
                resp = requests.get(
                    BASE_URL,
                    headers=headers,
                    params=params,
                    timeout=15,
                )

                # 🔴 DEBUG IMPORTANT
                if resp.status_code == 429:
                    print("⚠️ Quota atteint JSearch")
                    return toutes

                if resp.status_code != 200:
                    print(f"\n❌ HTTP {resp.status_code}")
                    print(resp.text[:400])
                    break

                data = resp.json().get("data", [])

                # ⚠️ NE PAS BREAK → juste skip
                if not data:
                    print(f"Page {page} vide")
                    continue

                for job in data:

                    titre = job.get("job_title", "N/A")
                    entreprise = job.get("employer_name")

                    # ✅ FIX entreprise NULL
                    if not entreprise or str(entreprise).strip() == "":
                        entreprise = "Non spécifiée"

                    lien = job.get("job_apply_link", "N/A")

                    hash_id = hashlib.sha1(
                        f"{titre}|{entreprise}|{lien}".encode()
                    ).hexdigest()[:16]

                    toutes.append({
                        "hash_id": hash_id,
                        "titre": titre,
                        "entreprise": entreprise,
                        "lieu": job.get("job_city") or job.get("job_country", "N/A"),
                        "pays": job.get("job_country", "N/A"),
                        "secteur": "Informatique / Tech",
                        "salaire_min": job.get("job_min_salary"),
                        "salaire_max": job.get("job_max_salary"),
                        "salaire_devise": job.get("job_salary_currency", "EUR"),
                        "remote": job.get("job_is_remote", False),
                        "contrat": job.get("job_employment_type", "N/A"),
                        "description": (job.get("job_description") or "")[:400],
                        "lien": lien,
                        "date_offre": (job.get("job_posted_at_datetime_utc") or "")[:10],
                        "date_scraping": datetime.now().strftime("%Y-%m-%d"),
                        "source": "jsearch",
                        "poste_recherche": poste,
                    })

                print(f"Page {page} → {len(data)} offres")
                time.sleep(1)

            except requests.Timeout:
                print(f"⏱️ Timeout page {page}, on continue")
                continue
            except Exception as e:
                print(f"❌ Erreur : {e}")
                break

    # ─── POST TRAITEMENT ──────────────────────────────────────────────────────

    if not toutes:
        print("❌ Aucune donnée récupérée JSearch")
        return []

    df = pd.DataFrame(toutes)

    avant = len(df)
    df.drop_duplicates(subset=["hash_id"], inplace=True)

    print(f"\n📊 Deduplication : {avant} → {len(df)}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = f"{OUTPUT_DIR}/offres_jsearch.csv"
    df.to_csv(path, index=False, encoding="utf-8")

    print(f"💾 Sauvegarde : {len(df)} offres → {path}")

    return df.to_dict("records")


# ─── AIRFLOW TASK ────────────────────────────────────────────────────────────

def scraper_jsearch_task():
    offres = scraper_jsearch(pages_max=5)
    print(f"JSearch task : {len(offres)} offres")
    return len(offres)

def scraper_jsearch_multi_postes(pages_max=5):
    return scraper_jsearch(pages_max)
# ─── TEST LOCAL ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("API KEY:", "OK" if API_KEY else "EMPTY")

    data = scraper_jsearch(pages_max=3)

    print(f"\nTOTAL : {len(data)} offres")

    for o in data[:3]:
        print(f"- {o['titre']} | {o['entreprise']} | {o['lieu']}")
