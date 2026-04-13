"""
fusion.py — Moteur de fusion et nettoyage des offres
======================================================
Ce module :
  1. Charge tous les CSV sources (france_travail, arbeitnow, adzuna, themuse, jsearch)
  2. Unifie les colonnes dans un schéma commun
  3. Déduplique par hash_id (titre+url) — plus robuste que titre+entreprise
  4. Nettoie et normalise les salaires
  5. Catégorise les offres sans secteur dans un secteur standardisé
  6. Sauvegarde offres_all.csv + injecte en PostgreSQL
  7. ✅ NOUVEAU : lit offres_transformees.csv si disponible (après transform.py)

Schéma unifié de sortie :
  hash_id, titre, titre_normalise, entreprise, lieu, region, pays,
  salaire_min, salaire_max, salaire_brut, salaire_annuel_estime,
  remote, contrat, contrat_type, secteur, categorie, competences_extraites,
  description, lien, tags, niveau, rome_code,
  date_creation, date_scraping, source, poste_recherche
"""

import os
import re
import hashlib
import pandas as pd
import numpy as np
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────────────────────────────────

DATA_DIR          = "/opt/airflow/data"
OUTPUT_PATH       = f"{DATA_DIR}/offres_all.csv"
OUTPUT_TRANSFORME = f"{DATA_DIR}/offres_transformees.csv"  # ← produit par transform.py

# Fichiers sources attendus
SOURCES = {
    "france_travail": f"{DATA_DIR}/offres_france_travail.csv",
    "arbeitnow":      f"{DATA_DIR}/offres_cadremploi.csv",
    "adzuna":         f"{DATA_DIR}/offres_adzuna.csv",
    "themuse":        f"{DATA_DIR}/offres_themuse.csv",
    "jsearch":        f"{DATA_DIR}/offres_jsearch.csv",
}

# Schéma cible — toutes les colonnes attendues dans offres_all.csv
SCHEMA = [
    "hash_id",
    "titre",
    "titre_normalise",        # ← produit par transform.py
    "entreprise",
    "lieu",
    "region",
    "pays",
    "salaire_min",
    "salaire_max",
    "salaire_brut",
    "salaire_annuel_estime",
    "remote",
    "contrat",
    "contrat_type",
    "secteur",
    "categorie",
    "competences_extraites",  # ← produit par transform.py
    "description",
    "lien",
    "tags",
    "niveau",
    "rome_code",
    "date_creation",
    "date_scraping",
    "source",
    "poste_recherche",
]

# ─── MAPPING SECTEUR FALLBACK ─────────────────────────────────────────────────

SECTEUR_KEYWORDS = {
    "Informatique / Tech": [
        "data", "python", "java", "développeur", "developer", "devops",
        "cloud", "ia", "ai", "machine learning", "sql", "engineer",
        "software", "sre", "scrum", "agile", "tech", "it ", " it",
        "cybersécurité", "réseau", "infrastructure",
    ],
    "Finance / Comptabilité": [
        "finance", "comptabl", "audit", "trésor", "contrôleur",
        "analyste financier", "risk", "banque", "assurance",
        "gestion de patrimoine", "analyst", "portfolio",
    ],
    "Marketing / Communication": [
        "marketing", "communication", "seo", "sem", "content",
        "brand", "media", "digital", "social media", "rédact",
        "traffic", "growth",
    ],
    "Commercial / Ventes": [
        "commercial", "vente", "sales", "account", "business develop",
        "ingénieur commercial", "technico-commercial", "key account",
    ],
    "Ressources Humaines": [
        "rh", "ressources humaines", "recrutement", "recruteur",
        "talent", "paie", "formation", "hrbp",
    ],
    "Ingénierie": [
        "ingénieur", "ingenieur", "engineer", "mécanique", "électrique",
        "automatisme", "bureau d'études", "r&d", "process",
        "production", "méthodes",
    ],
    "Santé / Médical": [
        "infirmier", "médecin", "pharmacien", "soignant", "aide-soignant",
        "kiné", "médical", "santé", "clinique", "hôpital", "paramédical",
    ],
    "Juridique": [
        "juriste", "avocat", "droit", "compliance", "juridique",
        "notaire", "legal", "paralegal",
    ],
    "Logistique / Supply Chain": [
        "logistique", "supply chain", "entrepôt", "transport",
        "approvisionn", "stock", "cariste", "import export",
    ],
    "Management / Opérations": [
        "directeur", "manager", "chef de projet", "project manager",
        "responsable", "directrice", "coo", "ceo", "opérations",
    ],
    "Education / Formation": [
        "formateur", "enseignant", "pédagogie", "formation",
        "professeur", "éducation",
    ],
    "Commerce / Distribution": [
        "commerce", "distribution", "acheteur", "achats",
        "approvisionnement", "merchandis", "magasin", "retail",
    ],
    "Conseil": [
        "consultant", "consulting", "conseil", "advisory", "cabinet",
    ],
    "Immobilier / BTP": [
        "immobilier", "btp", "conducteur de travaux", "architecte",
        "bâtiment", "chantier", "génie civil", "travaux",
    ],
    "Hôtellerie / Restauration": [
        "hôtel", "restauration", "cuisine", "chef", "serveur",
        "réception", "hébergement",
    ],
    "R&D / Scientifique": [
        "chercheur", "scientifique", "laboratoire", "recherche",
        "biologie", "chimie", "physique", "biotech",
    ],
}


# ─── CHARGEMENT & NORMALISATION ───────────────────────────────────────────────

def charger_source(nom_source: str, chemin: str) -> pd.DataFrame | None:
    if not os.path.exists(chemin):
        print(f"  [fusion] Fichier manquant : {chemin}")
        return None
    try:
        df = pd.read_csv(chemin, low_memory=False)
        print(f"  [fusion] {nom_source} : {len(df)} lignes chargées")
        df["source"] = nom_source
        if "hash_id" not in df.columns:
            df["hash_id"] = df.apply(
                lambda r: hashlib.sha1(
                    f"{r.get('titre', '')}|{r.get('lien', '')}".encode()
                ).hexdigest()[:16],
                axis=1,
            )
        return df
    except Exception as e:
        print(f"  [fusion] Erreur lecture {nom_source} : {e}")
        return None


def normaliser_colonnes(df: pd.DataFrame, source: str) -> pd.DataFrame:
    rename_maps = {
        "jsearch": {
            "salaire_devise": "salaire_brut",
            "date_offre":     "date_creation",
        },
    }
    if source in rename_maps:
        df = df.rename(columns=rename_maps[source])

    defaults = {
        "hash_id":               "",
        "titre":                 "N/A",
        "titre_normalise":       None,
        "entreprise":            "N/A",
        "lieu":                  "N/A",
        "region":                "N/A",
        "pays":                  "France",
        "salaire_min":           None,
        "salaire_max":           None,
        "salaire_brut":          "",
        "salaire_annuel_estime": None,
        "remote":                False,
        "contrat":               "N/A",
        "contrat_type":          "N/A",
        "secteur":               "Autre",
        "categorie":             "N/A",
        "competences_extraites": None,
        "description":           "",
        "lien":                  "N/A",
        "tags":                  "",
        "niveau":                "N/A",
        "rome_code":             "",
        "date_creation":         "",
        "date_scraping":         datetime.now().strftime("%Y-%m-%d"),
        "source":                source,
        "poste_recherche":       "N/A",
    }

    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default

    return df[[c for c in SCHEMA if c in df.columns] +
               [c for c in df.columns if c not in SCHEMA]]


# ─── NETTOYAGE SALAIRES ───────────────────────────────────────────────────────

def nettoyer_salaires(df: pd.DataFrame) -> pd.DataFrame:
    def parse_libelle(libelle: str):
        if not libelle or pd.isna(libelle):
            return None, None
        libelle  = str(libelle)
        mensuel  = "mensuel" in libelle.lower()
        nombres  = re.findall(r"\d+(?:[,. ]\d+)*", libelle)
        nombres  = [float(n.replace(",", ".").replace(" ", "")) for n in nombres if n]
        if not nombres:
            return None, None
        if mensuel:
            nombres = [n * 12 for n in nombres]
        return (min(nombres), max(nombres)) if len(nombres) > 1 else (nombres[0], nombres[0])

    mask_missing = df["salaire_min"].isna() & df["salaire_brut"].notna()
    if mask_missing.any():
        parsed = df.loc[mask_missing, "salaire_brut"].apply(parse_libelle)
        df.loc[mask_missing, "salaire_min"] = parsed.apply(lambda x: x[0])
        df.loc[mask_missing, "salaire_max"] = parsed.apply(lambda x: x[1])

    df["salaire_min"] = pd.to_numeric(df["salaire_min"], errors="coerce")
    df["salaire_max"] = pd.to_numeric(df["salaire_max"], errors="coerce")

    df.loc[df["salaire_min"] < 8_000,   "salaire_min"] = None
    df.loc[df["salaire_min"] > 500_000, "salaire_min"] = None
    df.loc[df["salaire_max"] < 8_000,   "salaire_max"] = None
    df.loc[df["salaire_max"] > 500_000, "salaire_max"] = None

    df["salaire_annuel_estime"] = df[["salaire_min", "salaire_max"]].mean(axis=1)
    return df


# ─── INFÉRENCE DU SECTEUR ─────────────────────────────────────────────────────

def inferer_secteur(titre: str) -> str:
    if not titre or pd.isna(titre):
        return "Autre"
    titre_lower = str(titre).lower()
    for secteur, keywords in SECTEUR_KEYWORDS.items():
        if any(kw in titre_lower for kw in keywords):
            return secteur
    return "Autre"


def corriger_secteurs(df: pd.DataFrame) -> pd.DataFrame:
    mask = df["secteur"].isin(["Autre", "N/A", "", None]) | df["secteur"].isna()
    if mask.any():
        df.loc[mask, "secteur"] = df.loc[mask, "titre"].apply(inferer_secteur)
        print(f"  [fusion] Secteur inféré pour {mask.sum()} offres")
    return df


# ─── DÉDUPLICATION ────────────────────────────────────────────────────────────

def dedupliquer(df: pd.DataFrame) -> pd.DataFrame:
    avant = len(df)

    df = df.drop_duplicates(subset=["hash_id"], keep="first")

    df["_titre_norm"] = (
        df["titre"].str.lower()
        .str.replace(r"[^\w\s]", "", regex=True)
        .str.strip()
    )
    df["_ent_norm"] = (
        df["entreprise"].str.lower()
        .str.replace(r"[^\w\s]", "", regex=True)
        .str.strip()
    )

    source_priority = {
        "france_travail": 0,
        "adzuna":         1,
        "jsearch":        2,
        "themuse":        3,
        "arbeitnow":      4,
    }
    df["_prio"] = df["source"].map(source_priority).fillna(5)
    df = df.sort_values("_prio")
    df = df.drop_duplicates(subset=["_titre_norm", "_ent_norm"], keep="first")
    df = df.drop(columns=["_titre_norm", "_ent_norm", "_prio"])

    apres = len(df)
    print(f"  [fusion] Deduplication : {avant} → {apres} offres uniques "
          f"({avant - apres} supprimées)")
    return df


# ─── NETTOYAGE GÉNÉRAL ────────────────────────────────────────────────────────

def nettoyer_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace({"nan": None, "N/A": None, "": None})
    df = df.where(pd.notnull(df), None)

    df["description"] = df["description"].apply(
        lambda x: str(x)[:500] if x else None
    )
    df["remote"] = df["remote"].apply(
        lambda x: True if str(x).lower() in ("true", "1", "oui", "yes") else False
    )
    df["titre"] = df["titre"].apply(
        lambda x: re.sub(r"\s+", " ", str(x)).strip() if x else None
    )

    today = datetime.now().strftime("%Y-%m-%d")
    df["date_scraping"] = df["date_scraping"].fillna(today)
    return df


# ─── FONCTION PRINCIPALE FUSION ───────────────────────────────────────────────

def fusionner_offres(sources: dict | None = None) -> pd.DataFrame:
    if sources is None:
        sources = SOURCES

    print("=" * 55)
    print(f"FUSION — démarrage ({len(sources)} sources)")
    print("=" * 55)

    dfs = []
    for nom, chemin in sources.items():
        df = charger_source(nom, chemin)
        if df is not None and len(df) > 0:
            df = normaliser_colonnes(df, nom)
            dfs.append(df)

    if not dfs:
        raise RuntimeError("Aucun fichier CSV trouvé — tous les scrapers ont échoué.")

    df_all = pd.concat(dfs, ignore_index=True)
    print(f"\nTotal brut : {len(df_all)} offres de {len(dfs)} sources")

    print("\n-- Nettoyage salaires --")
    df_all = nettoyer_salaires(df_all)

    print("-- Correction secteurs --")
    df_all = corriger_secteurs(df_all)

    print("-- Deduplication --")
    df_all = dedupliquer(df_all)

    print("-- Nettoyage final --")
    df_all = nettoyer_dataframe(df_all)

    cols_finales = [c for c in SCHEMA if c in df_all.columns]
    df_all = df_all[cols_finales]

    os.makedirs(DATA_DIR, exist_ok=True)
    df_all.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")

    print("\n" + "=" * 55)
    print(f"FUSION TERMINÉE : {len(df_all)} offres uniques")
    print(f"Fichier : {OUTPUT_PATH}")
    print("\nRépartition par source :")
    for src, n in df_all["source"].value_counts().items():
        print(f"  {src:<20} : {n:>5} offres")
    print("\nRépartition par secteur (top 10) :")
    for sec, n in df_all["secteur"].value_counts().head(10).items():
        print(f"  {str(sec):<35} : {n:>5} offres")
    print(f"\nAvec salaire : {df_all['salaire_annuel_estime'].notna().sum()} offres")
    print(f"Remote       : {df_all['remote'].sum()} offres")
    print("=" * 55)

    return df_all


def fusionner_offres_task() -> int:
    df = fusionner_offres()
    return len(df)


# ─── INJECTION POSTGRESQL ─────────────────────────────────────────────────────

def sauvegarder_en_db(df: pd.DataFrame | None = None) -> int:
    import psycopg2

    DB_CONFIG = {
        "host":     os.getenv("POSTGRES_HOST", "postgres"),
        "database": os.getenv("POSTGRES_DB",   "airflow"),
        "user":     os.getenv("POSTGRES_USER",  "airflow"),
        "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
        "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    }

    if df is None:
        # ✅ Priorité au fichier transformé (produit par transform.py)
        fichier = OUTPUT_TRANSFORME if os.path.exists(OUTPUT_TRANSFORME) else OUTPUT_PATH
        print(f"📂 Lecture : {fichier}")
        df = pd.read_csv(fichier, low_memory=False)
        df = df.where(pd.notnull(df), None)

    print(f"PostgreSQL — insertion de {len(df)} offres...")

    conn   = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # ✅ Schéma complet avec titre_normalise et competences_extraites
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS offres_emploi (
            id                    SERIAL PRIMARY KEY,
            hash_id               VARCHAR(32) UNIQUE,
            titre                 VARCHAR(500),
            titre_normalise       VARCHAR(200),
            entreprise            VARCHAR(500),
            lieu                  VARCHAR(500),
            region                VARCHAR(200),
            pays                  VARCHAR(100) DEFAULT 'France',
            salaire_min           FLOAT,
            salaire_max           FLOAT,
            salaire_brut          VARCHAR(300),
            salaire_annuel_estime FLOAT,
            remote                BOOLEAN,
            contrat               VARCHAR(200),
            contrat_type          VARCHAR(100),
            secteur               VARCHAR(200),
            categorie             VARCHAR(500),
            competences_extraites TEXT,
            description           TEXT,
            lien                  TEXT,
            tags                  TEXT,
            niveau                VARCHAR(200),
            rome_code             VARCHAR(10),
            date_creation         DATE,
            date_scraping         DATE,
            source                VARCHAR(100),
            poste_recherche       VARCHAR(300),
            created_at            TIMESTAMP DEFAULT NOW(),
            updated_at            TIMESTAMP DEFAULT NOW()
        )
    """)

    # ✅ Ajouter colonnes si table existe déjà (migration sécurisée)
    for col_sql in [
        "ALTER TABLE offres_emploi ADD COLUMN IF NOT EXISTS titre_normalise VARCHAR(200);",
        "ALTER TABLE offres_emploi ADD COLUMN IF NOT EXISTS competences_extraites TEXT;",
    ]:
        try:
            cursor.execute(col_sql)
        except Exception:
            pass

    conn.commit()

    inseres = mises_a_jour = erreurs = 0

    def clean(val):
        if val is None:
            return None
        s = str(val)
        return None if s in ("nan", "None", "N/A", "") else val

    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO offres_emploi
                  (hash_id, titre, titre_normalise, entreprise, lieu, region, pays,
                   salaire_min, salaire_max, salaire_brut, salaire_annuel_estime,
                   remote, contrat, contrat_type, secteur, categorie,
                   competences_extraites, description, lien, tags, niveau, rome_code,
                   date_creation, date_scraping, source, poste_recherche)
                VALUES
                  (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (hash_id) DO UPDATE SET
                  salaire_min           = EXCLUDED.salaire_min,
                  salaire_max           = EXCLUDED.salaire_max,
                  salaire_annuel_estime = EXCLUDED.salaire_annuel_estime,
                  titre_normalise       = EXCLUDED.titre_normalise,
                  competences_extraites = EXCLUDED.competences_extraites,
                  tags                  = EXCLUDED.tags,
                  updated_at            = NOW()
            """, (
                clean(row.get("hash_id")),
                clean(row.get("titre")),
                clean(row.get("titre_normalise")),
                clean(row.get("entreprise")),
                clean(row.get("lieu")),
                clean(row.get("region")),
                clean(row.get("pays")) or "France",
                row.get("salaire_min"),
                row.get("salaire_max"),
                clean(row.get("salaire_brut")),
                row.get("salaire_annuel_estime"),
                bool(row.get("remote")),
                clean(row.get("contrat")),
                clean(row.get("contrat_type")),
                clean(row.get("secteur")) or "Autre",
                clean(row.get("categorie")),
                clean(row.get("competences_extraites")),
                clean(row.get("description")),
                clean(row.get("lien")),
                clean(row.get("tags")),
                clean(row.get("niveau")),
                clean(row.get("rome_code")),
                clean(row.get("date_creation")),
                clean(row.get("date_scraping")),
                clean(row.get("source")),
                clean(row.get("poste_recherche")),
            ))

            if cursor.rowcount > 0:
                inseres += 1
            else:
                mises_a_jour += 1

        except Exception as e:
            erreurs += 1
            conn.rollback()
            if erreurs <= 3:
                print(f"  ⚠️ Erreur insertion : {e}")
            continue

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ DB : {inseres} insérées | {mises_a_jour} mises à jour | {erreurs} erreurs")
    return inseres + mises_a_jour


def sauvegarder_en_db_task() -> int:
    """Wrapper Airflow — lit offres_transformees.csv en priorité."""
    return sauvegarder_en_db()


# ─── TEST LOCAL ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    df = fusionner_offres()
    print(f"\nPrêt pour insertion : {len(df)} offres")
    print(df[["source", "secteur", "titre", "salaire_annuel_estime"]].head(10).to_string())