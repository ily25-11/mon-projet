"""
save_to_db.py — Lecture du CSV fusionné et insertion en base PostgreSQL
"""
import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

DATA_DIR    = "/opt/airflow/data"
INPUT_PATH  = f"{DATA_DIR}/offres_all.csv"

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "postgres"),
    "database": os.getenv("POSTGRES_DB", "airflow"),
    "user":     os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
}

# ─── CRÉATION TABLE ───────────────────────────────────────────────────────────

SQL_CREATE = """
CREATE TABLE IF NOT EXISTS offres_emploi (
    hash_id              VARCHAR(16)  PRIMARY KEY,
    titre                TEXT,
    entreprise           TEXT,
    lieu                 TEXT,
    region               TEXT,
    salaire_min          FLOAT,
    salaire_max          FLOAT,
    salaire_brut         TEXT,
    salaire_annuel_estime FLOAT,
    remote               BOOLEAN,
    contrat              TEXT,
    contrat_type         TEXT,
    categorie            TEXT,
    secteur              TEXT,
    description          TEXT,
    lien                 TEXT,
    tags                 TEXT,
    rome_code            TEXT,
    niveau               TEXT,
    date_creation        DATE,
    date_scraping        DATE,
    source               TEXT,
    poste_recherche      TEXT
);
"""

# ─── COLONNES À INSÉRER ───────────────────────────────────────────────────────

COLONNES = [
    "hash_id", "titre", "entreprise", "lieu", "region",
    "salaire_min", "salaire_max", "salaire_brut", "salaire_annuel_estime",
    "remote", "contrat", "contrat_type", "categorie", "secteur",
    "description", "lien", "tags", "rome_code", "niveau",
    "date_creation", "date_scraping", "source", "poste_recherche",
]

# ─── NETTOYAGE AVANT INSERTION ────────────────────────────────────────────────

def preparer_ligne(row):
    """Convertit une ligne pandas en tuple propre pour psycopg2."""
    def clean(val):
        if pd.isna(val) or val == "" or val == "nan" or val == "N/A":
            return None
        return val

    return tuple(clean(row.get(col)) for col in COLONNES)

# ─── FONCTION PRINCIPALE ──────────────────────────────────────────────────────

def sauvegarder_en_db():

    # 1. Chargement CSV fusionné
    if not os.path.exists(INPUT_PATH):
        print(f"[save_to_db] ❌ Fichier introuvable : {INPUT_PATH}")
        return 0

    df = pd.read_csv(INPUT_PATH, low_memory=False)

    if df.empty:
        print("[save_to_db] ⚠️  CSV vide, rien à insérer")
        return 0

    print(f"[save_to_db] 📂 {len(df)} offres à insérer")

    # 2. Connexion PostgreSQL
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = False
        cur = conn.cursor()
        print("[save_to_db] ✅ Connexion DB OK")
    except Exception as e:
        print(f"[save_to_db] ❌ Connexion DB échouée : {e}")
        return 0

    try:
        # 3. Création table si elle n'existe pas
        cur.execute(SQL_CREATE)

        # 4. Préparation des données
        lignes = [preparer_ligne(row) for _, row in df.iterrows()]

        # 5. Insertion avec ON CONFLICT DO NOTHING (évite les doublons sur hash_id)
        sql_insert = f"""
            INSERT INTO offres_emploi ({", ".join(COLONNES)})
            VALUES %s
            ON CONFLICT (hash_id) DO NOTHING
        """

        execute_values(cur, sql_insert, lignes, page_size=200)
        conn.commit()

        # 6. Compte réel des lignes insérées
        cur.execute("SELECT COUNT(*) FROM offres_emploi;")
        total_db = cur.fetchone()[0]

        print(f"[save_to_db] ✅ Insertion OK — total en DB : {total_db}")
        return len(lignes)

    except Exception as e:
        conn.rollback()
        print(f"[save_to_db] ❌ Erreur insertion : {e}")
        return 0

    finally:
        cur.close()
        conn.close()


# ─── AIRFLOW TASK ─────────────────────────────────────────────────────────────

def sauvegarder_en_db_task():
    return sauvegarder_en_db()


# ─── TEST LOCAL ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    n = sauvegarder_en_db()
    print(f"\nTOTAL INSÉRÉ : {n}")