import pickle
import math
import numpy as np
import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
model = SentenceTransformer(MODEL_NAME)

with open("recommender/embeddings.pkl", "rb") as f:
    data = pickle.load(f)

embeddings = data["embeddings"]
df         = data["df"]

# Valeurs qui sont du MODE DE TRAVAIL, pas du type de contrat
REMOTE_KEYWORDS = {"onsite", "hybrid", "remote", "hybride", "sur site"}

CONTRAT_MAP = {
    "fulltime":       "CDI",
    "full_time":      "CDI",
    "full-time":      "CDI",
    "permanent":      "CDI",
    "parttime":       "Temps partiel",
    "part_time":      "Temps partiel",
    "part-time":      "Temps partiel",
    "contract":       "CDD",
    "contractor":     "Freelance",
    "freelance":      "Freelance",
    "internship":     "Stage",
    "intern":         "Stage",
    "apprenticeship": "Alternance",
    "temporary":      "Intérim",
    "interim":        "Intérim",
    "cdi":            "CDI",
    "cdd":            "CDD",
    "stage":          "Stage",
    "alternance":     "Alternance",
}

def normaliser_contrat(contrat_type, contrat):
    for val in [contrat_type, contrat]:
        if not val or str(val).lower().strip() in ("nan", "none", ""):
            continue
        key = str(val).lower().strip()
        # Ignorer les valeurs qui sont du mode de travail
        if key in REMOTE_KEYWORDS:
            continue
        if key in CONTRAT_MAP:
            return CONTRAT_MAP[key]
        if str(val) in ("CDI","CDD","Stage","Alternance","Freelance","Intérim","Temps partiel"):
            return str(val)
    return "Non renseigné"

def normaliser_remote(contrat_type, remote):
    ct = str(contrat_type).lower().strip() if contrat_type else ""
    if ct == "remote":  return "Remote"
    if ct == "hybrid":  return "Hybride"
    if ct == "onsite":  return "Sur site"
    if remote is True or str(remote).lower() in ("true", "1", "remote"):
        return "Remote"
    return "Sur site"

def clean_value(v):
    if v is None:
        return None
    if isinstance(v, (bool, np.bool_)):
        return bool(v)
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating, float)):
        return None if (math.isnan(float(v)) or math.isinf(float(v))) else float(v)
    if isinstance(v, pd.Timestamp):
        return str(v)[:10]
    return v

def recommend(user_text: str, top_k: int = 10):
    user_embedding = model.encode([user_text])
    scores = cosine_similarity(user_embedding, embeddings)[0]
    top_indices = np.argsort(scores)[::-1][:top_k]

    results = df.iloc[top_indices].copy()
    results["score"] = scores[top_indices].astype(float)

    results["localisation"] = results["lieu"].fillna("Non renseigné")
    results["tags"]         = results["tags"].fillna("")
    results["description"]  = results["description"].fillna("")
    results["lien"]         = results["lien"].fillna("")
    results["lieu"]         = results["lieu"].fillna("Non renseigné")

    results["contrat"] = results.apply(
        lambda r: normaliser_contrat(r.get("contrat_type"), r.get("contrat")), axis=1
    )
    results["remote"] = results.apply(
        lambda r: normaliser_remote(r.get("contrat_type"), r.get("remote")), axis=1
    )

    cols = [
        "titre", "entreprise", "localisation", "lieu",
        "region", "contrat", "remote",
        "tags", "lien", "score", "description",
        "categorie", "secteur", "salaire_annuel_estime",
        "date_creation"
    ]
    available = [c for c in cols if c in results.columns]
    records   = results[available].to_dict(orient="records")
    return [{k: clean_value(v) for k, v in record.items()} for record in records]