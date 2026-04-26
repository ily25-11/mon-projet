import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
import pickle

MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"

def build_embeddings(csv_path="data/offres_all.csv", output_path="recommender/embeddings.pkl"):
    model = SentenceTransformer(MODEL_NAME)
    df = pd.read_csv(csv_path)

    print(f"Colonnes disponibles : {df.columns.tolist()}")
    print(f"Nombre d'offres : {len(df)}")

    # Colonnes réelles du CSV : titre, description, tags, lieu, categorie, secteur, contrat
    df["text"] = (
        df.get("titre",       pd.Series([""] * len(df))).fillna("") + " " +
        df.get("description", pd.Series([""] * len(df))).fillna("") + " " +
        df.get("tags",        pd.Series([""] * len(df))).fillna("") + " " +
        df.get("categorie",   pd.Series([""] * len(df))).fillna("") + " " +
        df.get("secteur",     pd.Series([""] * len(df))).fillna("")
    )

    print("Génération des embeddings...")
    embeddings = model.encode(df["text"].tolist(), show_progress_bar=True)

    with open(output_path, "wb") as f:
        pickle.dump({"embeddings": embeddings, "df": df}, f)

    print(f"✅ {len(df)} offres encodées et sauvegardées dans {output_path}")

if __name__ == "__main__":
    build_embeddings()