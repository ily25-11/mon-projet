# build_embeddings.py — lire depuis fact_offres (données propres)
import os, pickle
import pandas as pd
from sentence_transformers import SentenceTransformer
import psycopg2

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB",   "airflow"),
    "user":     os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
}

MODEL_NAME  = "paraphrase-multilingual-MiniLM-L12-v2"
OUTPUT_PATH = "recommender/embeddings.pkl"

def build_embeddings():
    # 1. Lire fact_offres + joins utiles
    conn = psycopg2.connect(**DB_CONFIG)
    query = """
        SELECT
            f.offre_id,
            f.titre_normalise   AS titre,
            f.lien,
            f.remote,
            f.salaire_annuel_estime,
            f.categorie,
            f.poste_recherche,
            e.entreprise,
            l.lieu,
            l.region,
            l.pays,
            c.contrat_type      AS contrat,
            s.source,
            t.date_publication  AS date_creation,
            o.description,
            o.secteur,
            f.salaire_annuel_estime
            STRING_AGG(comp.competence, ', ') AS tags
        FROM fact_offres f
        LEFT JOIN dim_entreprise e ON f.entreprise_id = e.entreprise_id
        LEFT JOIN dim_lieu       l ON f.lieu_id       = l.lieu_id
        LEFT JOIN dim_contrat    c ON f.contrat_id    = c.contrat_id
        LEFT JOIN dim_source     s ON f.source_id     = s.source_id
        LEFT JOIN dim_temps      t ON f.temps_id      = t.temps_id
        LEFT JOIN offre_competence oc   ON f.offre_id = oc.offre_id
        LEFT JOIN offres_emploi o ON o.hash_id = f.hash_id
        LEFT JOIN dim_competence   comp ON oc.competence_id = comp.competence_id
        GROUP BY f.offre_id, f.titre_normalise, f.lien, f.remote,
                 f.salaire_annuel_estime, f.categorie, f.poste_recherche,
                 e.entreprise, l.lieu, l.region, l.pays,
                 c.contrat_type, s.source, t.date_publication
    """
    df = pd.read_sql(query, conn)
    conn.close()
    print(f"✅ {len(df)} offres chargées depuis PostgreSQL")

    # 2. Texte pour l'embedding
    df["text"] = (
        df["titre"].fillna("")         + " " +
        df["poste_recherche"].fillna("") + " " +
        df["tags"].fillna("")          + " " +
        df["categorie"].fillna("")     + " " +
        df["contrat"].fillna("")
    )

    # 3. Encoder
    model = SentenceTransformer(MODEL_NAME)
    print("Génération des embeddings...")
    embeddings = model.encode(
        df["text"].tolist(),
        show_progress_bar=True,
        batch_size=64
    )

    # 4. Sauvegarder
    with open(OUTPUT_PATH, "wb") as f:
        pickle.dump({"embeddings": embeddings, "df": df}, f)

    print(f"✅ Embeddings sauvegardés → {OUTPUT_PATH}")
    print(f"   Shape : {embeddings.shape}")

if __name__ == "__main__":
    build_embeddings()