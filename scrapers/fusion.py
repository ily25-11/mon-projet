import os
import re
import hashlib
import pandas as pd
import numpy as np
from datetime import datetime

DATA_DIR = "/opt/airflow/data"
OUTPUT_PATH = f"{DATA_DIR}/offres_all.csv"

SOURCES = {
    "france_travail": f"{DATA_DIR}/offres_france_travail.csv",
    "arbeitnow":      f"{DATA_DIR}/offres_cadremploi.csv",
    "adzuna":         f"{DATA_DIR}/offres_adzuna.csv",
    "themuse":        f"{DATA_DIR}/offres_themuse.csv",
    "jsearch":        f"{DATA_DIR}/offres_jsearch.csv",
    "remotive":       f"{DATA_DIR}/offres_remotive.csv",
    "serpapi":        f"{DATA_DIR}/offres_serpapi.csv",
}

# ─────────────────────────────────────────────

def charger_source(nom, chemin):
    if not os.path.exists(chemin):
        print(f"[fusion] ❌ manquant : {chemin}")
        return None

    try:
        df = pd.read_csv(chemin, low_memory=False)
        print(f"[fusion] {nom} : {len(df)} lignes")
    except Exception as e:
        print(f"[fusion] ❌ erreur lecture {nom} : {e}")
        return None

    df["source"] = nom

    if "hash_id" not in df.columns:
        df["hash_id"] = df.apply(
            lambda r: hashlib.sha1(
                f"{r.get('titre','')}|{r.get('entreprise','')}|{r.get('lien','')}".encode()
            ).hexdigest()[:16],
            axis=1
        )

    return df


# ─────────────────────────────────────────────

def normaliser_colonnes(df):
    colonnes = [
        "hash_id", "titre", "entreprise", "lieu", "secteur",
        "salaire_min", "salaire_max", "remote",
        "description", "lien", "date_scraping", "source"
    ]

    for col in colonnes:
        if col not in df.columns:
            df[col] = None

    return df


# ─────────────────────────────────────────────

def nettoyer_dataframe(df):

    df = df.replace({"nan": None, "N/A": None, "": None})
    df = df.replace({np.nan: None})

    df["entreprise"] = df["entreprise"].fillna("Non spécifiée")

    df["description"] = df["description"].apply(
        lambda x: str(x)[:500] if x else None
    )

    df["remote"] = df["remote"].fillna(False)
    df["remote"] = df["remote"].apply(
        lambda x: True if str(x).lower() in ("true","1","oui") else False
    )

    df["secteur"] = df["secteur"].fillna("Autre")

    return df


# ─────────────────────────────────────────────

def nettoyer_salaires(df):

    df["salaire_min"] = pd.to_numeric(df.get("salaire_min"), errors="coerce")
    df["salaire_max"] = pd.to_numeric(df.get("salaire_max"), errors="coerce")

    df.loc[df["salaire_min"] < 8000, "salaire_min"] = None
    df.loc[df["salaire_max"] < 8000, "salaire_max"] = None

    df["salaire_annuel_estime"] = df[["salaire_min","salaire_max"]].mean(axis=1)

    return df


# ─────────────────────────────────────────────

def dedupliquer(df):

    avant = len(df)

    df = df.drop_duplicates(subset=["hash_id"])

    print(f"[fusion] dedup : {avant} → {len(df)}")

    return df


# ─────────────────────────────────────────────

def filtre_it(df):

    mask = (
        df["secteur"] == "Informatique / Tech"
    ) | (
        df["titre"].str.contains(
            "data|engineer|developer|devops|ml|ai",
            case=False,
            na=False
        )
    )

    avant = len(df)
    df = df[mask]

    print(f"[fusion] filtre IT : {avant} → {len(df)}")

    return df


# ─────────────────────────────────────────────

def fusionner_offres():

    print("\n===== FUSION =====")

    dfs = []

    for nom, path in SOURCES.items():
        df = charger_source(nom, path)

        if df is not None and len(df) > 0:
            df = normaliser_colonnes(df)
            dfs.append(df)

    if not dfs:
        raise Exception("❌ aucune source valide")

    print("\nSources chargées :")
    for d in dfs:
        print("-", d["source"].iloc[0], len(d))

    df_all = pd.concat(dfs, ignore_index=True)

    print("\nTotal brut :", len(df_all))

    df_all = nettoyer_salaires(df_all)
    df_all = dedupliquer(df_all)
    df_all = nettoyer_dataframe(df_all)
    df_all = filtre_it(df_all)

    os.makedirs(DATA_DIR, exist_ok=True)
    df_all.to_csv(OUTPUT_PATH, index=False)

    print("\nTop entreprises :")
    print(df_all["entreprise"].value_counts().head(10))

    print("\nSources :")
    print(df_all["source"].value_counts())

    print("\nTOTAL FINAL :", len(df_all))

    return df_all


# ─────────────────────────────────────────────

def fusionner_offres_task():
    df = fusionner_offres()
    return len(df)


# ─────────────────────────────────────────────

def sauvegarder_en_db_task():
    from save_to_db import sauvegarder_en_db
    return sauvegarder_en_db()