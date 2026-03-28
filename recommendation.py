"""
╔══════════════════════════════════════════════════════════╗
║        SYSTÈME DE RECOMMANDATION - JOB INTELLIGENT       ║
║  Mode 1 : TF-IDF + Cosine Similarity (rapide)            ║
║  Mode 2 : Sentence Transformers (NLP avancé, précis)     ║
║  Entrée  : titre de poste OU texte de CV                 ║
╚══════════════════════════════════════════════════════════╝
"""

import pandas as pd
import numpy as np
import psycopg2
import os
import pickle
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# ─────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "database": os.getenv("POSTGRES_DB", "airflow"),
    "user":     os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432))
}

MODEL_PATH = "/opt/airflow/models/tfidf_model.pkl"
os.makedirs("/opt/airflow/models", exist_ok=True)


# ─────────────────────────────────────────
# 1. CHARGEMENT DES DONNÉES
# ─────────────────────────────────────────

def charger_offres_db():
    """Charge les offres depuis PostgreSQL."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        df = pd.read_sql("""
            SELECT id, titre, entreprise, lieu, salaire_min, salaire_max,
                   remote, description, lien, tags, source, date_scraping
            FROM offres_emploi
            ORDER BY date_scraping DESC
        """, conn)
        conn.close()
        print(f"✅ {len(df)} offres chargées depuis PostgreSQL")
        return df
    except Exception as e:
        print(f"⚠️ Erreur DB : {e} — chargement depuis CSV")
        return charger_offres_csv()


def charger_offres_csv():
    """Fallback : charge depuis le CSV fusionné."""
    chemin = "/opt/airflow/data/offres_all.csv"
    if os.path.exists(chemin):
        df = pd.read_csv(chemin)
        print(f"✅ {len(df)} offres chargées depuis CSV")
        return df
    else:
        raise FileNotFoundError("❌ Aucune source de données disponible !")


def preparer_texte(df):
    """Crée un champ texte combiné pour le matching."""
    df = df.copy()
    df["titre"]       = df["titre"].fillna("")
    df["description"] = df["description"].fillna("")
    df["tags"]        = df["tags"].fillna("")
    df["lieu"]        = df["lieu"].fillna("")

    # Titre a plus de poids (répété 3x)
    df["texte_complet"] = (
        df["titre"] + " " + df["titre"] + " " + df["titre"] + " " +
        df["description"] + " " +
        df["tags"]
    )
    return df


# ─────────────────────────────────────────
# 2. MODE TF-IDF (RAPIDE)
# ─────────────────────────────────────────

def construire_modele_tfidf(df, forcer=False):
    """Construit ou charge le modèle TF-IDF."""
    if os.path.exists(MODEL_PATH) and not forcer:
        with open(MODEL_PATH, "rb") as f:
            data = pickle.load(f)
        print("📦 Modèle TF-IDF chargé depuis le disque")
        return data["vectorizer"], data["matrice"]

    print("🔧 Construction du modèle TF-IDF...")
    vectorizer = TfidfVectorizer(
        max_features=10000,
        ngram_range=(1, 2),
        stop_words=None,  # garder les mots français
        min_df=1
    )
    matrice = vectorizer.fit_transform(df["texte_complet"])

    with open(MODEL_PATH, "wb") as f:
        pickle.dump({"vectorizer": vectorizer, "matrice": matrice}, f)

    print(f"✅ Modèle TF-IDF construit ({matrice.shape[0]} offres, {matrice.shape[1]} features)")
    return vectorizer, matrice


def recommander_tfidf(profil_texte, df, vectorizer, matrice, top_n=10,
                      filtre_lieu=None, filtre_remote=None, filtre_source=None):
    """Recommande des offres par TF-IDF + cosine similarity."""

    # Vectoriser le profil utilisateur
    vecteur_profil = vectorizer.transform([profil_texte])
    scores = cosine_similarity(vecteur_profil, matrice).flatten()

    # Trier par score décroissant
    indices = np.argsort(scores)[::-1]
    df_result = df.iloc[indices].copy()
    df_result["score_similarite"] = scores[indices]

    # Filtres optionnels
    if filtre_lieu:
        df_result = df_result[
            df_result["lieu"].str.contains(filtre_lieu, case=False, na=False)
        ]
    if filtre_remote is not None:
        df_result = df_result[df_result["remote"] == filtre_remote]
    if filtre_source:
        df_result = df_result[df_result["source"] == filtre_source]

    # Garder uniquement les scores > 0
    df_result = df_result[df_result["score_similarite"] > 0]

    return df_result.head(top_n)


# ─────────────────────────────────────────
# 3. MODE SENTENCE TRANSFORMERS (NLP AVANCÉ)
# ─────────────────────────────────────────

def recommander_nlp(profil_texte, df, top_n=10,
                    filtre_lieu=None, filtre_remote=None, filtre_source=None):
    """Recommande des offres avec Sentence Transformers (plus précis)."""
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        print("⚠️  sentence-transformers non installé.")
        print("    Installe avec : pip install sentence-transformers")
        return pd.DataFrame()

    print("🧠 Chargement du modèle NLP (première fois = ~30 secondes)...")
    model = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")

    # Encoder les offres et le profil
    textes_offres  = df["texte_complet"].tolist()
    embeddings_offres = model.encode(textes_offres, show_progress_bar=True, batch_size=64)
    embedding_profil  = model.encode([profil_texte])

    # Cosine similarity
    scores = cosine_similarity(embedding_profil, embeddings_offres).flatten()
    indices = np.argsort(scores)[::-1]

    df_result = df.iloc[indices].copy()
    df_result["score_similarite"] = scores[indices]

    # Filtres optionnels
    if filtre_lieu:
        df_result = df_result[
            df_result["lieu"].str.contains(filtre_lieu, case=False, na=False)
        ]
    if filtre_remote is not None:
        df_result = df_result[df_result["remote"] == filtre_remote]
    if filtre_source:
        df_result = df_result[df_result["source"] == filtre_source]

    return df_result.head(top_n)


# ─────────────────────────────────────────
# 4. AFFICHAGE DES RÉSULTATS
# ─────────────────────────────────────────

def afficher_resultats(df_result, mode="tfidf"):
    """Affiche les offres recommandées de façon lisible."""
    if df_result.empty:
        print("❌ Aucune offre trouvée.")
        return

    print(f"\n{'═'*60}")
    print(f"  🎯 TOP {len(df_result)} OFFRES RECOMMANDÉES [{mode.upper()}]")
    print(f"{'═'*60}\n")

    for i, (_, row) in enumerate(df_result.iterrows(), 1):
        score = row.get("score_similarite", 0)
        remote_label = "🏠 Remote" if row.get("remote") else "🏢 Présentiel"

        sal_min = row.get("salaire_min")
        sal_max = row.get("salaire_max")
        salaire = ""
        if pd.notna(sal_min) and pd.notna(sal_max):
            salaire = f"💶 {int(sal_min)}€ - {int(sal_max)}€"
        elif pd.notna(sal_min):
            salaire = f"💶 À partir de {int(sal_min)}€"

        print(f"  [{i:02d}] {'⭐'*min(5, int(score*10))} ({score:.3f})")
        print(f"       📌 {row.get('titre', 'N/A')}")
        print(f"       🏦 {row.get('entreprise', 'N/A')}")
        print(f"       📍 {row.get('lieu', 'N/A')} | {remote_label}")
        if salaire:
            print(f"       {salaire}")
        print(f"       🔗 {row.get('lien', 'N/A')}")
        print(f"       📅 {row.get('date_scraping', 'N/A')} | Source: {row.get('source', 'N/A')}")
        print()


# ─────────────────────────────────────────
# 5. POINT D'ENTRÉE PRINCIPAL
# ─────────────────────────────────────────

def recommander(
    profil,
    mode="tfidf",
    type_profil="titre",
    top_n=10,
    filtre_lieu=None,
    filtre_remote=None,
    filtre_source=None,
    forcer_rebuild=False
):
    """
    Fonction principale de recommandation.

    Paramètres :
    ------------
    profil        : str  — titre de poste OU texte de CV
    mode          : str  — "tfidf" (rapide) ou "nlp" (précis)
    type_profil   : str  — "titre" ou "cv"
    top_n         : int  — nombre d'offres à retourner
    filtre_lieu   : str  — ex: "Paris", "Lyon"
    filtre_remote : bool — True = remote uniquement
    filtre_source : str  — "jsearch", "arbeitnow"
    forcer_rebuild: bool — reconstruire le modèle TF-IDF

    Exemple :
    ---------
    recommander("Data Engineer Python Spark", mode="tfidf", filtre_lieu="Paris")
    recommander(cv_text, mode="nlp", type_profil="cv", top_n=5)
    """

    print(f"\n🚀 Recommandation [{mode.upper()}] | Profil: {type_profil}")
    print(f"   Recherche : {profil[:100]}{'...' if len(profil) > 100 else ''}\n")

    # Charger et préparer les données
    df = charger_offres_db()
    df = preparer_texte(df)

    if mode == "tfidf":
        vectorizer, matrice = construire_modele_tfidf(df, forcer=forcer_rebuild)
        resultats = recommander_tfidf(
            profil, df, vectorizer, matrice, top_n,
            filtre_lieu, filtre_remote, filtre_source
        )

    elif mode == "nlp":
        resultats = recommander_nlp(
            profil, df, top_n,
            filtre_lieu, filtre_remote, filtre_source
        )

    else:
        raise ValueError("mode doit être 'tfidf' ou 'nlp'")

    afficher_resultats(resultats, mode=mode)
    return resultats


# ─────────────────────────────────────────
# 6. EXEMPLES D'UTILISATION
# ─────────────────────────────────────────

if __name__ == "__main__":

    # ── Exemple 1 : Par titre de poste (TF-IDF) ──
    print("=" * 60)
    print("EXEMPLE 1 : Titre de poste → TF-IDF")
    recommander(
        profil="Data Engineer Python Spark AWS",
        mode="tfidf",
        type_profil="titre",
        top_n=5,
        filtre_lieu=None,
    )

    # ── Exemple 2 : Par titre de poste (NLP) ──
    print("=" * 60)
    print("EXEMPLE 2 : Titre de poste → NLP")
    recommander(
        profil="Machine Learning Engineer deep learning",
        mode="nlp",
        type_profil="titre",
        top_n=5
    )

    # ── Exemple 3 : Par CV (TF-IDF) ──
    cv_exemple = """
    Ingénieur Data avec 3 ans d'expérience en Python, SQL, Spark et Airflow.
    J'ai travaillé sur des pipelines de données en temps réel avec Kafka et Flink.
    Expérience en machine learning avec scikit-learn et TensorFlow.
    Certifié AWS Cloud Practitioner. Recherche un poste de Data Engineer senior.
    Compétences : Python, SQL, Spark, Airflow, Kafka, Docker, Kubernetes, AWS, GCP.
    """
    print("=" * 60)
    print("EXEMPLE 3 : CV texte → TF-IDF")
    recommander(
        profil=cv_exemple,
        mode="tfidf",
        type_profil="cv",
        top_n=5
    )

    # ── Exemple 4 : Remote uniquement ──
    print("=" * 60)
    print("EXEMPLE 4 : Remote uniquement → TF-IDF")
    recommander(
        profil="Data Scientist NLP",
        mode="tfidf",
        top_n=5,
        filtre_remote=True
    )