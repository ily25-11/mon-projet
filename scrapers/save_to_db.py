import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os

# Connexion PostgreSQL
DB_CONFIG = {
    "host": "postgres",
    "database": "airflow",
    "user": "airflow",
    "password": "airflow",
    "port": 5432
}

def creer_table():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS offres_emploi (
            id SERIAL PRIMARY KEY,
            titre VARCHAR(500),
            entreprise VARCHAR(500),
            lieu VARCHAR(500),
            salaire_min FLOAT,
            salaire_max FLOAT,
            remote BOOLEAN,
            description TEXT,
            lien TEXT,
            tags VARCHAR(500),
            date_scraping DATE,
            source VARCHAR(100),
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(titre, entreprise, source)
        )
    """)

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ Table créée avec succès !")

def sauvegarder_en_db():
    fichier = "/opt/airflow/data/offres_all.csv"

    if not os.path.exists(fichier):
        print("❌ Fichier offres_all.csv introuvable !")
        return 0

    df = pd.read_csv(fichier)
    print(f"📂 {len(df)} offres à insérer...")

    # Nettoyer les valeurs NaN
    df = df.where(pd.notnull(df), None)

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    inseres = 0
    for _, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO offres_emploi 
                (titre, entreprise, lieu, salaire_min, salaire_max, 
                 remote, description, lien, tags, date_scraping, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (titre, entreprise, source) DO NOTHING
            """, (
                row.get("titre"),
                row.get("entreprise"),
                row.get("lieu"),
                row.get("salaire_min"),
                row.get("salaire_max"),
                row.get("remote"),
                row.get("description"),
                row.get("lien"),
                row.get("tags"),
                row.get("date_scraping"),
                row.get("source")
            ))
            inseres += 1
        except Exception as e:
            print(f"⚠️ Erreur ligne : {e}")
            continue

    conn.commit()
    cursor.close()
    conn.close()

    print(f"💾 {inseres} offres insérées dans PostgreSQL !")
    return inseres


if __name__ == "__main__":
    creer_table()
    sauvegarder_en_db()