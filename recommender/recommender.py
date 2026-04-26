import pickle
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

MODEL_NAME = "paraphrase-multilingual-MiniLM-L12-v2"
model = SentenceTransformer(MODEL_NAME)

with open("recommender/embeddings.pkl", "rb") as f:
    data = pickle.load(f)

embeddings = data["embeddings"]
df = data["df"]

def recommend(user_text: str, top_k: int = 10):
    user_embedding = model.encode([user_text])
    scores = cosine_similarity(user_embedding, embeddings)[0]
    top_indices = np.argsort(scores)[::-1][:top_k]

    results = df.iloc[top_indices].copy()
    results["score"] = scores[top_indices]

    cols = ["titre", "entreprise", "lieu", "contrat", "remote", "tags", "lien", "score"]
    return (
        results[cols]
        .where(results[cols].notna(), other=None)  # ← remplace NaN par None
        .to_dict(orient="records")
    )