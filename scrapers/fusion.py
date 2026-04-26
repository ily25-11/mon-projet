import os
import hashlib
import pandas as pd
import numpy as np
from datetime import datetime, date

DATA_DIR    = "/opt/airflow/data"
OUTPUT_PATH = f"{DATA_DIR}/offres_all.csv"

SOURCES = {
    "france_travail":   f"{DATA_DIR}/offres_france_travail.csv",
    "arbeitnow":        f"{DATA_DIR}/offres_cadremploi.csv",
    "themuse":          f"{DATA_DIR}/offres_themuse.csv",
    "remotive":         f"{DATA_DIR}/offres_remotive.csv",
    "serpapi":          f"{DATA_DIR}/offres_serpapi.csv",
    "kaggle_linkedin":  f"{DATA_DIR}/offres_kaggle_linkedin.csv",  # ✅ NOUVEAU
}

COLONNES_STANDARD = [
    "hash_id", "titre", "entreprise", "lieu", "region",
    "salaire_min", "salaire_max", "salaire_brut", "salaire_annuel_estime",
    "remote", "contrat", "contrat_type", "categorie", "secteur",
    "description", "lien", "tags", "rome_code", "niveau",
    "date_creation", "date_scraping", "source", "poste_recherche",
]

# ─── CHARGEMENT ───────────────────────────────────────────────────────────────

def charger_source(nom, chemin):
    """
    Charge un CSV source.
    - Ignore les fichiers manquants
    - Pour kaggle_linkedin : pas de vérification de date (fichier statique)
    - Pour les autres : ignore les CSV d'un run précédent (données périmées)
    """
    if not os.path.exists(chemin):
        print(f"[fusion] ⚠️  manquant        : {nom}")
        return None

    # Le dataset Kaggle est statique, pas besoin de vérifier la date
    if nom != "kaggle_linkedin":
        mtime = datetime.fromtimestamp(os.path.getmtime(chemin)).date()
        if mtime < date.today():
            print(f"[fusion] ⚠️  périmé ({mtime})  : {nom} — ignoré")
            return None

    try:
        df = pd.read_csv(chemin, low_memory=False)
    except Exception as e:
        print(f"[fusion] ❌ erreur lecture    : {nom} — {e}")
        return None

    if df.empty:
        print(f"[fusion] ⚠️  vide             : {nom}")
        return None

    # Force la colonne source
    df["source"] = nom

    # Génère hash_id si absent
    if "hash_id" not in df.columns:
        df["hash_id"] = df.apply(
            lambda r: hashlib.sha1(
                f"{r.get('titre', '')}|{r.get('entreprise', '')}|{r.get('lien', '')}".encode()
            ).hexdigest()[:16],
            axis=1,
        )

    mtime_str = datetime.fromtimestamp(os.path.getmtime(chemin)).date()
    print(f"[fusion] ✅ chargé ({mtime_str})    : {nom} — {len(df)} lignes")
    return df


# ─── NORMALISATION DES COLONNES ───────────────────────────────────────────────

def normaliser_colonnes(df):
    """Ajoute les colonnes manquantes avec None pour homogénéiser les DataFrames."""
    for col in COLONNES_STANDARD:
        if col not in df.columns:
            df[col] = None
    return df[COLONNES_STANDARD]


# ─── NETTOYAGE GÉNÉRAL ────────────────────────────────────────────────────────

def nettoyer_dataframe(df):
    df = df.replace({"nan": None, "N/A": None, "": None})
    df = df.replace({np.nan: None})

    df["entreprise"] = df["entreprise"].fillna("Non spécifiée")
    df["secteur"]    = df["secteur"].fillna("Autre")
    df["remote"]     = df["remote"].fillna(False)

    df["remote"] = df["remote"].apply(
        lambda x: True if str(x).lower() in ("true", "1", "oui") else False
    )

    df["description"] = df["description"].apply(
        lambda x: str(x)[:500] if x else None
    )

    return df


# ─── NETTOYAGE SALAIRES ───────────────────────────────────────────────────────

def nettoyer_salaires(df):
    df["salaire_min"] = pd.to_numeric(df["salaire_min"], errors="coerce")
    df["salaire_max"] = pd.to_numeric(df["salaire_max"], errors="coerce")

    df.loc[df["salaire_min"] < 8_000,   "salaire_min"] = None
    df.loc[df["salaire_max"] < 8_000,   "salaire_max"] = None
    df.loc[df["salaire_max"] > 500_000, "salaire_max"] = None

    df["salaire_annuel_estime"] = df[["salaire_min", "salaire_max"]].mean(axis=1)

    return df


# ─── DÉDUPLICATION ────────────────────────────────────────────────────────────

def dedupliquer(df):
    avant = len(df)

    df = df.drop_duplicates(subset=["hash_id"])
    df = df.drop_duplicates(subset=["titre", "lien"], keep="first")

    print(f"[fusion] dedup : {avant} → {len(df)} (supprimé {avant - len(df)})")
    return df


# ─── FILTRE IT ────────────────────────────────────────────────────────────────

def filtre_it(df):
    pattern = (
        r"data\s*(engineer|scientist|analyst|architect|steward|manager)|"
        r"machine\s*learning|deep\s*learning|mlops|dataops|"
        r"big\s*data|analytics|bi\s*developer|business\s*intelligence|"
        r"devops|cloud\s*engineer|python\s*developer|"
        r"ingénieur\s*données|développeur\s*data|"
        r"senior\s*machine\s*learning|ai\s*engineer|nlp|computer\s*vision"
    )

    mask = df["titre"].str.contains(pattern, case=False, na=False, regex=True)

    avant = len(df)
    df = df[mask]
    print(f"[fusion] filtre IT : {avant} → {len(df)}")

    return df


# ─── RAPPORT ──────────────────────────────────────────────────────────────────

def afficher_rapport(df):
    print("\n" + "=" * 52)
    print(f"  RAPPORT FUSION — {date.today()}")
    print("=" * 52)
    for source, count in df["source"].value_counts().items():
        print(f"  {source:<22}: {count:>6} offres")
    print("-" * 52)
    print(f"  TOTAL FINAL           : {len(df):>6} offres")
    print("=" * 52)


# ─── POINT D'ENTRÉE ───────────────────────────────────────────────────────────

def fusionner_offres():
    print("\n===== FUSION =====")

    dfs = []

    for nom, path in SOURCES.items():
        df = charger_source(nom, path)
        if df is not None and not df.empty:
            df = normaliser_colonnes(df)
            dfs.append(df)

    if not dfs:
        raise Exception("❌ Aucune source valide pour aujourd'hui")

    df_all = pd.concat(dfs, ignore_index=True)
    print(f"\n[fusion] Total brut (avant dedup) : {len(df_all)}")

    df_all = nettoyer_salaires(df_all)
    df_all = dedupliquer(df_all)
    df_all = nettoyer_dataframe(df_all)
    df_all = filtre_it(df_all)

    os.makedirs(DATA_DIR, exist_ok=True)
    df_all.to_csv(OUTPUT_PATH, index=False)

    afficher_rapport(df_all)

    return df_all


def fusionner_offres_task():
    df = fusionner_offres()
    return len(df)


def sauvegarder_en_db_task():
    from save_to_db import sauvegarder_en_db
    return sauvegarder_en_db()