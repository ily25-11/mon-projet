"""
Scraper The Muse API
====================
API publique — AUCUNE CLÉ REQUISE pour les offres publiques.
Documentation : https://www.themuse.com/developers/api/v2

✅ VERSION IT/DATA UNIQUEMENT — conforme au projet Job Intelligent
"""

import os
import re
import time
import hashlib
import requests
import pandas as pd
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────────────────────────────────

BASE_URL    = "https://www.themuse.com/api/public/jobs"
OUTPUT_PATH = "/opt/airflow/data/offres_themuse.csv"
PAGE_SIZE   = 20  # max The Muse

# ✅ Uniquement les catégories IT/Data
CATEGORIES = [
    "Computer and IT",
    "Data and Analytics",
    "Software Engineer",
    "Product",
]

# Mapping catégorie → secteur
SECTEUR_MAP = {
    "Computer and IT":    "Informatique / Tech",
    "Data and Analytics": "Informatique / Tech",
    "Software Engineer":  "Informatique / Tech",
    "Product":            "Informatique / Tech",
}


# ─── SCRAPING ─────────────────────────────────────────────────────────────────

def scraper_themuse_categorie(
    categorie: str,
    pages_max: int = 10,
) -> list[dict]:
    """Scrape toutes les offres d'une catégorie The Muse."""
    offres = []

    for page in range(1, pages_max + 1):
        params = {
            "category":   categorie,
            "page":       page,
            "descending": "true",
        }

        try:
            resp = requests.get(BASE_URL, params=params, timeout=20)

            if resp.status_code == 429:
                print(f"  [TheMuse] Rate limit page {page}. Pause 30s...")
                time.sleep(30)
                continue

            if resp.status_code != 200:
                print(f"  [TheMuse] HTTP {resp.status_code} page {page}")
                break

            data    = resp.json()
            jobs    = data.get("results", [])
            total_p = data.get("page_count", 1)

            if not jobs:
                break

            for job in jobs:
                titre   = job.get("name", "N/A")
                company = job.get("company", {}).get("name", "N/A")
                lien    = job.get("refs", {}).get("landing_page", "N/A")

                hash_id = hashlib.sha1(
                    f"{titre}|{company}|{lien}".encode()
                ).hexdigest()[:16]

                locations = job.get("locations", [])
                lieu      = locations[0].get("name", "N/A") if locations else "N/A"

                contents = job.get("contents", "")
                desc     = _strip_html(contents)[:500] if contents else "N/A"

                levels = job.get("levels", [])
                niveau = levels[0].get("name", "N/A") if levels else "N/A"

                job_type = job.get("type", "full-time")

                offres.append({
                    "hash_id":         hash_id,
                    "titre":           titre,
                    "entreprise":      company,
                    "lieu":            lieu,
                    "region":          _extract_region(lieu),
                    "salaire_min":     None,
                    "salaire_max":     None,
                    "salaire_brut":    "",
                    "remote":          _detect_remote_themuse(lieu, desc),
                    "contrat":         job_type,
                    "contrat_type":    job_type,
                    "categorie":       categorie,
                    "secteur":         SECTEUR_MAP.get(categorie, "Informatique / Tech"),
                    "description":     desc,
                    "lien":            lien,
                    "tags":            categorie,
                    "niveau":          niveau,
                    "date_creation":   (job.get("publication_date") or "")[:10],
                    "date_scraping":   datetime.now().strftime("%Y-%m-%d"),
                    "source":          "themuse",
                    "poste_recherche": categorie,
                })

            print(f"  [TheMuse] '{categorie}' p.{page}/{total_p} : {len(jobs)} offres")

            if page >= total_p:
                break

            time.sleep(0.5)

        except Exception as e:
            print(f"  [TheMuse] Erreur page {page} : {e}")
            break

    return offres


# ─── UTILITAIRES ──────────────────────────────────────────────────────────────

def _strip_html(html: str) -> str:
    clean = re.sub(r"<[^>]+>", " ", html)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean


def _extract_region(lieu: str) -> str:
    if not lieu or lieu == "N/A":
        return "N/A"
    parts = [p.strip() for p in lieu.split(",")]
    return parts[-1] if len(parts) >= 2 else lieu


def _detect_remote_themuse(lieu: str, desc: str) -> bool:
    lieu_l = lieu.lower()
    desc_l = desc.lower()
    kws    = ["remote", "télétravail", "teletravail", "à distance",
              "anywhere", "flexible", "hybrid"]
    return any(k in lieu_l or k in desc_l for k in kws)


# ─── FONCTION PRINCIPALE ──────────────────────────────────────────────────────

def scraper_themuse(
    categories: list[str] | None = None,
    pages_max: int = 10,
) -> list[dict]:
    """
    Point d'entrée principal — compatible DAG Airflow.
    ✅ Scrape uniquement les catégories IT/Data.
    """
    if categories is None:
        categories = CATEGORIES

    print(f"The Muse — démarrage pour {len(categories)} catégories IT/Data")
    toutes = []

    for cat in categories:
        print(f"\nCategorie : {cat}")
        offres = scraper_themuse_categorie(cat, pages_max=pages_max)
        toutes.extend(offres)
        print(f"  → {len(offres)} offres")
        time.sleep(1.5)

    if toutes:
        df = pd.DataFrame(toutes)
        avant = len(df)
        df.drop_duplicates(subset=["hash_id"], keep="first", inplace=True)
        print(f"\nDeduplication The Muse : {avant} → {len(df)} offres uniques")

        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
        print(f"Sauvegarde : {len(df)} offres → {OUTPUT_PATH}")
        return df.to_dict("records")

    print("Aucune offre The Muse récupérée.")
    return []


def scraper_themuse_task():
    """Wrapper Airflow."""
    offres = scraper_themuse()
    print(f"The Muse task : {len(offres)} offres")
    return len(offres)


# ─── TEST LOCAL ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    offres = scraper_themuse(
        categories=["Data and Analytics", "Software Engineer"],
        pages_max=3,
    )
    print(f"\nTotal : {len(offres)} offres")
    for o in offres[:3]:
        print(f"  - [{o['secteur']}] {o['titre']} | {o['entreprise']} | {o['lieu']}")