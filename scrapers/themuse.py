"""
Scraper The Muse API
====================
API publique — AUCUNE CLÉ REQUISE pour les offres publiques.
Documentation : https://www.themuse.com/developers/api/v2

Volume estimé : ~1 000-3 000 offres (base internationale, beaucoup de FR/EU)
Catégories : 50+ secteurs disponibles natifs

Avantages :
  - Zéro inscription
  - Multi-secteurs natifs
  - Bonne structuration (level, category, company_size)
  - International avec filtre location possible
"""

import os
import time
import hashlib
import requests
import pandas as pd
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────────────────────────────────

BASE_URL    = "https://www.themuse.com/api/public/jobs"
OUTPUT_PATH = "/opt/airflow/data/offres_themuse.csv"
PAGE_SIZE   = 20  # max The Muse

# Catégories natives The Muse (couvrent tous les domaines)
CATEGORIES = [
    "Computer and IT",
    "Data and Analytics",
    "Software Engineer",
    "Product",
    "Design and UX",
    "Marketing and PR",
    "Sales",
    "Finance",
    "Accounting",
    "HR and Recruiting",
    "Legal",
    "Operations",
    "Project Management",
    "Healthcare",
    "Education",
    "Engineering",
    "Science",
    "Social Media",
    "Customer Service",
    "Business Development",
    "Consulting",
    "Real Estate",
    "Retail",
    "Media and Journalism",
    "Environmental",
    "Nonprofit",
    "Administration",
    "Logistics",
    "Hospitality",
    "Architecture",
]

# Niveaux d'expérience disponibles
LEVELS = ["Entry Level", "Mid Level", "Senior Level", "Management", "Internship"]

# Mapping catégorie → secteur lisible en français
SECTEUR_MAP = {
    "Computer and IT":        "Informatique / Tech",
    "Data and Analytics":     "Informatique / Tech",
    "Software Engineer":      "Informatique / Tech",
    "Product":                "Informatique / Tech",
    "Design and UX":          "Design / UX",
    "Marketing and PR":       "Marketing / Communication",
    "Sales":                  "Commercial / Ventes",
    "Finance":                "Finance / Comptabilité",
    "Accounting":             "Finance / Comptabilité",
    "HR and Recruiting":      "Ressources Humaines",
    "Legal":                  "Juridique",
    "Operations":             "Management / Opérations",
    "Project Management":     "Management / Opérations",
    "Healthcare":             "Santé / Médical",
    "Education":              "Education / Formation",
    "Engineering":            "Ingénierie",
    "Science":                "R&D / Scientifique",
    "Social Media":           "Marketing / Communication",
    "Customer Service":       "Service Client",
    "Business Development":   "Commercial / Ventes",
    "Consulting":             "Conseil",
    "Real Estate":            "Immobilier",
    "Retail":                 "Commerce / Distribution",
    "Media and Journalism":   "Médias / Journalisme",
    "Environmental":          "Environnement",
    "Nonprofit":              "Associatif / ONG",
    "Administration":         "Administration",
    "Logistics":              "Logistique / Supply Chain",
    "Hospitality":            "Hôtellerie / Restauration",
    "Architecture":           "Architecture / BTP",
}


# ─── SCRAPING ─────────────────────────────────────────────────────────────────

def scraper_themuse_categorie(
    categorie: str,
    pages_max: int = 10,
) -> list[dict]:
    """
    Scrape toutes les offres d'une catégorie The Muse.
    The Muse pagine par 'page' (commence à 1).
    """
    offres = []

    for page in range(1, pages_max + 1):
        params = {
            "category": categorie,
            "page":     page,
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

            data      = resp.json()
            jobs      = data.get("results", [])
            total_p   = data.get("page_count", 1)

            if not jobs:
                break

            for job in jobs:
                titre   = job.get("name", "N/A")
                company = job.get("company", {}).get("name", "N/A")
                lien    = job.get("refs", {}).get("landing_page", "N/A")

                # Hash unique
                hash_id = hashlib.sha1(
                    f"{titre}|{company}|{lien}".encode()
                ).hexdigest()[:16]

                # Localisation
                locations = job.get("locations", [])
                lieu = locations[0].get("name", "N/A") if locations else "N/A"

                # Contenu
                contents = job.get("contents", "")
                desc = _strip_html(contents)[:500] if contents else "N/A"

                # Niveau
                levels = job.get("levels", [])
                niveau = levels[0].get("name", "N/A") if levels else "N/A"

                # Type contrat
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
                    "secteur":         SECTEUR_MAP.get(categorie, "Autre"),
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
    """Supprime les balises HTML basiques."""
    import re
    clean = re.sub(r"<[^>]+>", " ", html)
    clean = re.sub(r"\s+", " ", clean).strip()
    return clean


def _extract_region(lieu: str) -> str:
    """Tente d'extraire une région ou pays depuis le lieu."""
    if not lieu or lieu == "N/A":
        return "N/A"
    parts = [p.strip() for p in lieu.split(",")]
    if len(parts) >= 2:
        return parts[-1]  # dernier élément = pays/région généralement
    return lieu


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

    Args:
        categories: catégories à scraper (défaut : CATEGORIES global)
        pages_max: pages max par catégorie

    Returns:
        Liste de dicts offres
    """
    if categories is None:
        categories = CATEGORIES

    print(f"The Muse — démarrage pour {len(categories)} catégories")
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
        categories=["Data and Analytics", "Finance", "Marketing and PR"],
        pages_max=3,
    )
    print(f"\nTotal : {len(offres)} offres")
    for o in offres[:3]:
        print(f"  - [{o['secteur']}] {o['titre']} | {o['entreprise']} | {o['lieu']}")