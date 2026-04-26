import os
import hashlib
import pandas as pd
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────────────────────────────────

INPUT_POSTINGS  = "/opt/airflow/data/job_postings.csv"
INPUT_SKILLS    = "/opt/airflow/data/job_skills.csv"
OUTPUT_PATH     = "/opt/airflow/data/offres_kaggle_linkedin.csv"

KEYWORDS_IT = [
    "data engineer", "data scientist", "data analyst", "data architect",
    "machine learning", "deep learning", "mlops", "dataops", "big data",
    "business intelligence", "bi developer", "analytics", "python developer",
    "cloud engineer", "devops", "ai engineer", "nlp", "computer vision",
]

# ─── FILTRE IT ────────────────────────────────────────────────────────────────

def est_it(titre):
    if not titre:
        return False
    titre_lower = str(titre).lower()
    return any(kw in titre_lower for kw in KEYWORDS_IT)


def _detect_remote(titre, contrat):
    text = f"{titre} {contrat}".lower()
    return any(k in text for k in ["remote", "hybrid", "télétravail", "work from home"])


# ─── NORMALISATION ────────────────────────────────────────────────────────────

def scraper_kaggle_linkedin():

    # 1. Chargement job_postings.csv
    if not os.path.exists(INPUT_POSTINGS):
        print(f"❌ Fichier introuvable : {INPUT_POSTINGS}")
        return []

    print(f"📂 Chargement job_postings.csv...")
    df = pd.read_csv(INPUT_POSTINGS, low_memory=False)
    print(f"   → {len(df)} lignes brutes")

    # 2. Filtre IT
    df = df[df["job_title"].apply(est_it)].copy()
    print(f"   → {len(df)} lignes après filtre IT")

    # 3. Merge avec job_skills si disponible
    if os.path.exists(INPUT_SKILLS):
        print(f"📂 Chargement job_skills.csv...")
        df_skills = pd.read_csv(INPUT_SKILLS, low_memory=False)
        df = df.merge(df_skills, on="job_link", how="left")
        print(f"   → Merge skills OK")
    else:
        df["job_skills"] = None
        print(f"⚠️  job_skills.csv introuvable, skills ignorés")

    # 4. Normalisation vers le format standard
    offres = []

    for _, row in df.iterrows():

        titre      = str(row.get("job_title",      "N/A")).strip()
        entreprise = str(row.get("company",         "Non spécifiée")).strip()
        lieu       = str(row.get("job_location",    "N/A")).strip()
        lien       = str(row.get("job_link",        "N/A")).strip()
        contrat    = str(row.get("job_type",        "N/A")).strip()
        niveau     = str(row.get("job_level",       "N/A")).strip()
        pays       = str(row.get("search_country",  "N/A")).strip()
        date_vue   = str(row.get("first_seen",      ""))[:10]
        skills     = str(row.get("job_skills",      ""))

        if not entreprise or entreprise in ("nan", ""):
            entreprise = "Non spécifiée"

        hash_id = hashlib.sha1(
            f"{titre}|{entreprise}|{lien}".encode()
        ).hexdigest()[:16]

        offres.append({
            "hash_id":              hash_id,
            "titre":                titre,
            "entreprise":           entreprise,
            "lieu":                 lieu,
            "region":               pays,
            "salaire_min":          None,
            "salaire_max":          None,
            "salaire_brut":         None,
            "salaire_annuel_estime": None,
            "remote":               _detect_remote(titre, contrat),
            "contrat":              contrat,
            "contrat_type":         contrat,
            "categorie":            "it-jobs",
            "secteur":              "Informatique / Tech",
            "description":          skills[:500] if skills and skills != "nan" else None,
            "lien":                 lien,
            "tags":                 str(row.get("search_position", "data"))[:100],
            "rome_code":            None,
            "niveau":               niveau,
            "date_creation":        date_vue,
            "date_scraping":        datetime.now().strftime("%Y-%m-%d"),
            "source":               "kaggle_linkedin",
            "poste_recherche":      titre,
        })

    if not offres:
        print("⚠️ Aucune offre après normalisation")
        return []

    df_out = pd.DataFrame(offres)

    avant = len(df_out)
    df_out.drop_duplicates(subset=["hash_id"], inplace=True)
    print(f"📊 Dedup : {avant} → {len(df_out)}")

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df_out.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
    print(f"💾 Sauvegardé : {len(df_out)} offres → {OUTPUT_PATH}")

    return df_out.to_dict("records")


# ─── AIRFLOW TASK ─────────────────────────────────────────────────────────────

def scraper_kaggle_linkedin_task():
    offres = scraper_kaggle_linkedin()
    return len(offres)


# ─── TEST LOCAL ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    data = scraper_kaggle_linkedin()
    print(f"\nTOTAL : {len(data)} offres")
    if data:
        print("\nExemple :")
        for k, v in list(data[0].items()):
            print(f"  {k}: {v}")