import os
import time
import hashlib
import requests
import pandas as pd
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────────────────────────────────

APP_ID  = os.getenv("ADZUNA_APP_ID", "")
APP_KEY = os.getenv("ADZUNA_APP_KEY", "")

BASE_URL         = "https://api.adzuna.com/v1/api/jobs"
COUNTRY          = "fr"
OUTPUT_PATH      = "/opt/airflow/data/offres_adzuna.csv"
RESULTS_PER_PAGE = 50

CATEGORIES = ["it-jobs"]

KEYWORDS_PAR_CATEGORIE = {
    "it-jobs": [
        "data engineer",
        "data analyst",
        "data scientist",
    ],
}

# ─── AUTH ─────────────────────────────────────────────────────────────────────

def _check_credentials():
    if not APP_ID or not APP_KEY:
        raise EnvironmentError("❌ ADZUNA_APP_ID ou ADZUNA_APP_KEY manquant")


# ─── SCRAPER PAGE ─────────────────────────────────────────────────────────────

def scraper_adzuna_page(categorie, mot_cle, page=1):

    params = {
        "app_id": APP_ID,
        "app_key": APP_KEY,
        "results_per_page": RESULTS_PER_PAGE,
        "what": mot_cle,
        "sort_by": "date",
    }

    url = f"{BASE_URL}/{COUNTRY}/search/{page}"

    try:
        resp = requests.get(url, params=params, timeout=15)

        # DEBUG important
        if resp.status_code != 200:
            print(f"\n❌ Adzuna ERROR {resp.status_code}")
            print(resp.text[:500])
            return []

        data = resp.json()
        jobs = data.get("results", [])

        offres = []

        for job in jobs:
            titre = job.get("title", "N/A")
            url_offre = job.get("redirect_url", "N/A")

            hash_id = hashlib.sha1(
                f"{titre}|{url_offre}".encode()
            ).hexdigest()[:16]

            entreprise = job.get("company", {}).get("display_name")

            # FIX entreprise NULL
            if not entreprise or entreprise.strip() == "":
                entreprise = "Non spécifiée"

            offres.append({
                "hash_id": hash_id,
                "titre": titre,
                "entreprise": entreprise,
                "lieu": job.get("location", {}).get("display_name", "N/A"),
                "region": _extract_region(job),
                "salaire_min": job.get("salary_min"),
                "salaire_max": job.get("salary_max"),
                "salaire_brut": _format_salaire(
                    job.get("salary_min"),
                    job.get("salary_max")
                ),
                "remote": _detect_remote(job),
                "contrat": job.get("contract_time", "N/A"),
                "contrat_type": job.get("contract_type", "N/A"),
                "categorie": categorie,
                "secteur": "Informatique / Tech",
                "description": (job.get("description") or "")[:500],
                "lien": url_offre,
                "tags": mot_cle,
                "date_creation": (job.get("created") or "")[:10],
                "date_scraping": datetime.now().strftime("%Y-%m-%d"),
                "source": "adzuna",
                "poste_recherche": mot_cle,
            })

        print(f"✅ {mot_cle} p.{page} → {len(offres)} offres")
        return offres

    except Exception as e:
        print(f"❌ Erreur Adzuna : {e}")
        return []


# ─── SCRAPER CATÉGORIE ────────────────────────────────────────────────────────

def scraper_adzuna_categorie(categorie, keywords, pages_par_keyword=2):

    toutes = []

    for mot_cle in keywords:
        for page in range(1, pages_par_keyword + 1):

            offres = scraper_adzuna_page(categorie, mot_cle, page)

            if not offres:
                break

            toutes.extend(offres)
            time.sleep(1)

    return toutes


# ─── UTILITAIRES ──────────────────────────────────────────────────────────────

def _extract_region(job):
    locs = job.get("location", {}).get("area", [])
    if len(locs) >= 2:
        return locs[1]
    return "N/A"


def _format_salaire(sal_min, sal_max):
    if sal_min and sal_max:
        return f"{int(sal_min):,} – {int(sal_max):,} €"
    if sal_min:
        return f"{int(sal_min):,} €"
    return ""


def _detect_remote(job):
    text = ((job.get("description") or "") + job.get("title", "")).lower()
    return any(k in text for k in [
        "remote", "télétravail", "teletravail", "hybrid", "hybride"
    ])


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def scraper_adzuna():

    _check_credentials()

    print("\n🚀 Lancement Adzuna")

    toutes = []

    for cat in CATEGORIES:
        keywords = KEYWORDS_PAR_CATEGORIE[cat]
        offres = scraper_adzuna_categorie(cat, keywords)
        toutes.extend(offres)

    if not toutes:
        print("⚠️ Aucune donnée récupérée")
        return []

    df = pd.DataFrame(toutes)

    avant = len(df)
    df.drop_duplicates(subset=["hash_id"], inplace=True)

    print(f"📊 Dedup : {avant} → {len(df)}")

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print(f"💾 Sauvegardé : {len(df)} offres")

    return df.to_dict("records")


def scraper_adzuna_task():
    offres = scraper_adzuna()
    return len(offres)


# ─── TEST ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("APP_ID:", APP_ID)
    print("APP_KEY:", "OK" if APP_KEY else "EMPTY")

    data = scraper_adzuna()
    print(f"\nTOTAL: {len(data)} offres")