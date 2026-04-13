import os
import time
import requests
import pandas as pd
from datetime import datetime


API_KEY = os.getenv("RAPIDAPI_KEY", "")
OUTPUT_DIR = "/opt/airflow/data"

POSTES = [
    "data scientist France",
    "data engineer France",
    "data analyst France",
    "machine learning engineer",
]


def scraper_jsearch_task(
    poste: str = "data scientist",
    pages_max: int = 5,
    source_name: str = "jsearch",
) -> list[dict]:
    if not API_KEY:
        raise EnvironmentError("Variable RAPIDAPI_KEY manquante")

    print(f"JSearch — recherche : {poste}")
    offres = []
    headers = {
        "X-RapidAPI-Key":  API_KEY,
        "X-RapidAPI-Host": "jsearch.p.rapidapi.com",
    }

    for page in range(1, pages_max + 1):
        params = {
            "query":       poste,
            "page":        str(page),
            "num_pages":   "1",
            "country":     "fr",          # ← corrigé ("all" est invalide)
            "date_posted": "month",       # ← offres du mois, pas "all" qui donne du vieux
        }

        try:
            resp = requests.get(
                "https://jsearch.p.rapidapi.com/search",
                headers=headers,
                params=params,
                timeout=20,
            )

            # Quota dépassé — on arrête proprement
            if resp.status_code == 429:
                print(f"⚠️  Quota JSearch dépassé à la page {page}. Arrêt.")
                break

            # Autre erreur HTTP
            if resp.status_code != 200:
                print(f"❌ Erreur HTTP {resp.status_code} page {page} : {resp.text[:200]}")
                break

            data = resp.json().get("data", [])
            if not data:
                print(f"  Page {page} : aucune donnée, arrêt.")
                break

            for job in data:
                # Récupérer le max de champs disponibles
                offres.append({
                    "titre":         job.get("job_title", "N/A"),
                    "entreprise":    job.get("employer_name", "N/A"),
                    "lieu":          job.get("job_city") or job.get("job_country", "N/A"),
                    "pays":          job.get("job_country", "N/A"),
                    "salaire_min":   job.get("job_min_salary"),
                    "salaire_max":   job.get("job_max_salary"),
                    "salaire_devise": job.get("job_salary_currency", "EUR"),
                    "remote":        job.get("job_is_remote", False),
                    "contrat":       job.get("job_employment_type", "N/A"),
                    "description":   (job.get("job_description") or "")[:500],
                    "lien":          job.get("job_apply_link", "N/A"),
                    "date_offre":    (job.get("job_posted_at_datetime_utc") or "")[:10],
                    "date_scraping": datetime.now().strftime("%Y-%m-%d"),
                    "source":        source_name,
                })

            print(f"  Page {page} : {len(data)} offres récupérées")
            time.sleep(1)  # ← pause pour ne pas saturer le quota

        except requests.Timeout:
            print(f"  Page {page} : timeout, on passe à la suivante")
            continue
        except Exception as e:
            print(f"  Page {page} : erreur inattendue — {e}")
            break

    # Sauvegarde CSV
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    if offres:
        chemin = f"{OUTPUT_DIR}/offres_{source_name}.csv"
        pd.DataFrame(offres).to_csv(chemin, index=False, encoding="utf-8")
        print(f"💾 {len(offres)} offres sauvegardées → {chemin}")
    else:
        print("❌ Aucune offre JSearch récupérée")

    return offres


def scraper_jsearch_multi_postes(pages_max: int = 5) -> list[dict]:
    """
    Lance JSearch sur plusieurs postes et déduplique le résultat.
    À appeler depuis le DAG à la place de scraper_jsearch_task().
    """
    toutes = []
    for poste in POSTES:
        offres = scraper_jsearch_task(poste, pages_max=pages_max, source_name="jsearch")
        toutes.extend(offres)
        time.sleep(2)  # pause entre les postes

    if toutes:
        df = pd.DataFrame(toutes)
        avant = len(df)
        df.drop_duplicates(subset=["titre", "entreprise"], keep="first", inplace=True)
        print(f"JSearch multi-postes : {avant} → {len(df)} offres uniques")
        chemin = f"{OUTPUT_DIR}/offres_jsearch.csv"
        df.to_csv(chemin, index=False, encoding="utf-8")
        return df.to_dict("records")

    return []


if __name__ == "__main__":
    offres = scraper_jsearch_multi_postes(pages_max=3)
    print(f"\nTotal : {len(offres)} offres")
    for o in offres[:3]:
        print(f"  - {o['titre']} | {o['entreprise']} | {o['lieu']}")