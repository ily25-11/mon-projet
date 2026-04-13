"""
Scraper Adzuna API
==================
Inscription gratuite : https://developer.adzuna.com
  → Create App → récupérer APP_ID et APP_KEY
  → Quota gratuit : 250 requêtes/jour (~5 000-10 000 offres)

Variables d'environnement :
  ADZUNA_APP_ID=<votre app_id>
  ADZUNA_APP_KEY=<votre app_key>

Avantages :
  - Tous les secteurs (IT, finance, santé, marketing, BTP, etc.)
  - Couvre France + international (pays configurable)
  - Salaires inclus dans l'API
  - Gratuit jusqu'à 250 req/jour
"""

import os
import time
import hashlib
import requests
import pandas as pd
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────────────────────────────────

APP_ID  = os.getenv("ADZUNA_APP_ID", "")
APP_KEY = os.getenv("ADZUNA_APP_KEY", "")

BASE_URL    = "https://api.adzuna.com/v1/api/jobs"
COUNTRY     = "fr"         # fr, gb, us, de, etc.
OUTPUT_PATH = "/opt/airflow/data/offres_adzuna.csv"
RESULTS_PER_PAGE = 50      # max Adzuna

# ─── SECTEURS COUVERTS ────────────────────────────────────────────────────────
# Adzuna supporte les catégories natives — on couvre un max de secteurs.
# Liste complète : https://api.adzuna.com/v1/api/jobs/fr/categories
CATEGORIES = [
    "it-jobs",
    "accounting-finance-jobs",
    "engineering-jobs",
    "healthcare-nursing-jobs",
    "sales-jobs",
    "marketing-jobs",
    "hr-jobs",
    "legal-jobs",
    "logistics-warehouse-jobs",
    "education-jobs",
    "scientific-qa-jobs",
    "social-work-jobs",
    "retail-jobs",
    "hospitality-catering-jobs",
    "consultancy-jobs",
    "graduate-jobs",
]

# Mots-clés supplémentaires par catégorie pour maximiser les résultats
KEYWORDS_PAR_CATEGORIE = {
    "it-jobs": [
        "data scientist", "data engineer", "data analyst",
        "machine learning", "devops", "développeur python",
        "cloud architect", "cybersécurité", "IA générative",
    ],
    "accounting-finance-jobs": [
        "analyste financier", "contrôleur de gestion",
        "comptable", "auditeur", "risk manager", "trésorier",
    ],
    "engineering-jobs": [
        "ingénieur mécanique", "ingénieur électrique",
        "chef de projet", "ingénieur R&D", "automatisme",
    ],
    "healthcare-nursing-jobs": [
        "infirmier", "médecin", "pharmacien",
        "kinésithérapeute", "technicien médical",
    ],
    "sales-jobs": [
        "commercial", "account manager", "business developer",
        "directeur commercial", "ingénieur commercial",
    ],
    "marketing-jobs": [
        "chef de produit", "traffic manager", "SEO",
        "brand manager", "content manager", "growth hacker",
    ],
    "hr-jobs": [
        "RH", "recruteur", "HRBP", "chargé de formation",
        "responsable paie",
    ],
    "legal-jobs": [
        "juriste", "avocat", "compliance officer",
        "responsable juridique", "paralegal",
    ],
    "logistics-warehouse-jobs": [
        "supply chain", "logisticien", "responsable entrepôt",
        "chef de dépôt", "approvisionneur",
    ],
    "education-jobs": [
        "formateur", "enseignant", "consultant pédagogique",
        "responsable formation",
    ],
}


# ─── AUTHENTIFICATION & VÉRIFICATION ─────────────────────────────────────────

def _check_credentials():
    if not APP_ID or not APP_KEY:
        raise EnvironmentError(
            "Variables ADZUNA_APP_ID et ADZUNA_APP_KEY manquantes.\n"
            "Inscription gratuite sur https://developer.adzuna.com"
        )


# ─── SCRAPING PAR CATÉGORIE + MOT-CLÉ ────────────────────────────────────────

def scraper_adzuna_page(
    categorie: str,
    mot_cle: str,
    page: int = 1,
) -> list[dict]:
    """Récupère une page de résultats Adzuna."""

    params = {
        "app_id":          APP_ID,
        "app_key":         APP_KEY,
        "results_per_page": RESULTS_PER_PAGE,
        "what":            mot_cle,
        "where":           "France",
        "content-type":    "application/json",
        "sort_by":         "date",
    }

    url = f"{BASE_URL}/{COUNTRY}/search/{page}"

    try:
        resp = requests.get(url, params=params, timeout=20)

        if resp.status_code == 401:
            raise PermissionError("Credentials Adzuna invalides (401)")
        if resp.status_code == 429:
            print(f"  [Adzuna] Quota dépassé. Pause 60s...")
            time.sleep(60)
            return []
        if resp.status_code != 200:
            print(f"  [Adzuna] HTTP {resp.status_code} — {resp.text[:200]}")
            return []

        data  = resp.json()
        jobs  = data.get("results", [])
        total = data.get("count", 0)

        offres = []
        for job in jobs:
            sal   = job.get("salary_min"), job.get("salary_max")
            titre = job.get("title", "N/A")
            url_offre = job.get("redirect_url", "N/A")

            # Hash unique pour déduplication croisée (titre + url)
            hash_id = hashlib.sha1(
                f"{titre}|{url_offre}".encode()
            ).hexdigest()[:16]

            offres.append({
                "hash_id":          hash_id,
                "titre":            titre,
                "entreprise":       job.get("company", {}).get("display_name", "N/A"),
                "lieu":             job.get("location", {}).get("display_name", "N/A"),
                "region":           _extract_region(job),
                "salaire_min":      sal[0],
                "salaire_max":      sal[1],
                "salaire_brut":     _format_salaire(sal[0], sal[1]),
                "remote":           _detect_remote(job),
                "contrat":          job.get("contract_time", "N/A"),
                "contrat_type":     job.get("contract_type", "N/A"),
                "categorie":        categorie,
                "secteur":          _map_secteur(categorie),
                "description":      (job.get("description") or "")[:500],
                "lien":             url_offre,
                "tags":             mot_cle,
                "date_creation":    (job.get("created") or "")[:10],
                "date_scraping":    datetime.now().strftime("%Y-%m-%d"),
                "source":           "adzuna",
                "poste_recherche":  mot_cle,
            })

        print(f"  [Adzuna] {categorie} / '{mot_cle}' p.{page} : "
              f"{len(jobs)} offres (total dispo : {total})")

        return offres

    except PermissionError:
        raise
    except Exception as e:
        print(f"  [Adzuna] Erreur inattendue : {e}")
        return []


def scraper_adzuna_categorie(
    categorie: str,
    keywords: list[str],
    pages_par_keyword: int = 3,
) -> list[dict]:
    """Scrape plusieurs mots-clés dans une catégorie."""
    toutes = []

    for mot_cle in keywords:
        for page in range(1, pages_par_keyword + 1):
            offres = scraper_adzuna_page(categorie, mot_cle, page)
            toutes.extend(offres)
            if not offres:
                break
            time.sleep(0.8)  # respecter le rate limit

    return toutes


# ─── UTILITAIRES ──────────────────────────────────────────────────────────────

def _extract_region(job: dict) -> str:
    """Extrait la région depuis la localisation Adzuna."""
    locs = job.get("location", {}).get("area", [])
    # area = ['France', 'Île-de-France', 'Paris'] typiquement
    if len(locs) >= 2:
        return locs[1]
    return "N/A"


def _format_salaire(sal_min, sal_max) -> str:
    if sal_min and sal_max:
        return f"{int(sal_min):,} – {int(sal_max):,} € annuel"
    if sal_min:
        return f"{int(sal_min):,} € annuel"
    return ""


def _detect_remote(job: dict) -> bool:
    desc = (job.get("description") or "").lower()
    titre = (job.get("title") or "").lower()
    kws = ["télétravail", "teletravail", "remote", "à distance",
           "full remote", "hybrid", "hybride"]
    return any(k in desc or k in titre for k in kws)


def _map_secteur(categorie: str) -> str:
    """Mappe la catégorie Adzuna vers un secteur lisible."""
    mapping = {
        "it-jobs":                   "Informatique / Tech",
        "accounting-finance-jobs":   "Finance / Comptabilité",
        "engineering-jobs":          "Ingénierie",
        "healthcare-nursing-jobs":   "Santé / Médical",
        "sales-jobs":                "Commercial / Ventes",
        "marketing-jobs":            "Marketing / Communication",
        "hr-jobs":                   "Ressources Humaines",
        "legal-jobs":                "Juridique",
        "logistics-warehouse-jobs":  "Logistique / Supply Chain",
        "education-jobs":            "Education / Formation",
        "scientific-qa-jobs":        "R&D / Scientifique",
        "social-work-jobs":          "Social / Médico-social",
        "retail-jobs":               "Commerce / Distribution",
        "hospitality-catering-jobs": "Hôtellerie / Restauration",
        "consultancy-jobs":          "Conseil",
        "graduate-jobs":             "Jeunes diplômés",
    }
    return mapping.get(categorie, "Autre")


# ─── FONCTION PRINCIPALE ──────────────────────────────────────────────────────

def scraper_adzuna(
    categories: list[str] | None = None,
    pages_par_keyword: int = 3,
) -> list[dict]:
    """
    Point d'entrée principal — compatible avec le DAG Airflow.

    Args:
        categories: liste de catégories Adzuna (défaut : CATEGORIES global)
        pages_par_keyword: pages à scraper par mot-clé (défaut : 3)

    Returns:
        Liste de toutes les offres sous forme de dicts
    """
    _check_credentials()

    if categories is None:
        categories = CATEGORIES

    print(f"Adzuna — démarrage pour {len(categories)} catégories")
    toutes = []

    for cat in categories:
        keywords = KEYWORDS_PAR_CATEGORIE.get(cat, [cat.replace("-jobs", "")])
        print(f"\nCategorie : {cat} ({len(keywords)} mots-clés)")
        offres = scraper_adzuna_categorie(cat, keywords, pages_par_keyword)
        toutes.extend(offres)
        print(f"  → {len(offres)} offres pour {cat}")
        time.sleep(1)

    if toutes:
        df = pd.DataFrame(toutes)
        avant = len(df)
        df.drop_duplicates(subset=["hash_id"], keep="first", inplace=True)
        print(f"\nDeduplication Adzuna : {avant} → {len(df)} offres uniques")

        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
        print(f"Sauvegarde : {len(df)} offres → {OUTPUT_PATH}")
        return df.to_dict("records")

    print("Aucune offre Adzuna récupérée.")
    return []


def scraper_adzuna_task():
    """Wrapper Airflow — retourne le nombre d'offres."""
    offres = scraper_adzuna()
    print(f"Adzuna task : {len(offres)} offres")
    return len(offres)


# ─── TEST LOCAL ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    offres = scraper_adzuna(
        categories=["it-jobs", "accounting-finance-jobs", "marketing-jobs"],
        pages_par_keyword=2,
    )
    print(f"\nTotal : {len(offres)} offres")
    if offres:
        for o in offres[:3]:
            sal = o.get("salaire_brut") or "N/A"
            print(f"  - [{o['secteur']}] {o['titre']} | {o['entreprise']} | {sal}")