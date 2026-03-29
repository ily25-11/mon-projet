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
import os
import pickle
import warnings
warnings.filterwarnings("ignore")

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sqlalchemy import create_engine

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

NLP_MODEL_NAME = "paraphrase-multilingual-mpnet-base-v2"

# Seuils de similarité minimum
SEUIL_TFIDF = 0.05
SEUIL_NLP   = 0.30

# ✅ Bonus de score par source (arbeitnow = offres souvent hors-sujet)
SOURCE_WEIGHTS = {
    "jsearch":    1.20,   # +20% : offres FR data/tech ciblées
    "glassdoor":  1.15,   # +15% : offres FR vérifiées
    "arbeitnow":  0.75,   # -25% : beaucoup d'offres DE/EN non data
}

# ✅ Mots-clés qui signalent une offre hors-domaine data/tech
MOTS_HORS_DOMAINE = [
    "maçon", "plombier", "électricien", "chauffeur", "cuisinier",
    "serveur", "caissier", "infirmier", "aide-soignant", "boulanger",
    "fachplaner", "datenbankentwickler", "ingenieur gmbh", "beratende",
    "wordpress", "wix", "shopify designer", "community manager",
]


# ─────────────────────────────────────────
# 1. CHARGEMENT DES DONNÉES
# ─────────────────────────────────────────

def get_engine():
    cfg = DB_CONFIG
    url = (
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['database']}"
    )
    return create_engine(url)


def charger_offres_db():
    try:
        engine = get_engine()
        df = pd.read_sql("""
            SELECT id, titre, entreprise, lieu, salaire_min, salaire_max,
                   remote, description, lien, tags, source, date_scraping
            FROM offres_emploi
            ORDER BY date_scraping DESC
        """, engine)
        print(f"✅ {len(df)} offres chargées depuis PostgreSQL")
        return df
    except Exception as e:
        print(f"⚠️ Erreur DB : {e} — chargement depuis CSV")
        return charger_offres_csv()


def charger_offres_csv():
    chemin = "/opt/airflow/data/offres_all.csv"
    if os.path.exists(chemin):
        df = pd.read_csv(chemin)
        print(f"✅ {len(df)} offres chargées depuis CSV")
        return df
    raise FileNotFoundError("❌ Aucune source de données disponible !")


def dedoublonner(df):
    avant = len(df)
    df = df.drop_duplicates(subset=["lien"], keep="first")
    df = df.drop_duplicates(subset=["titre", "entreprise"], keep="first")
    apres = len(df)
    if avant != apres:
        print(f"🧹 {avant - apres} doublons supprimés ({apres} offres uniques)")
    return df.reset_index(drop=True)


def normaliser_remote(df):
    def to_bool(val):
        if pd.isna(val):
            return False
        if isinstance(val, bool):
            return val
        if isinstance(val, (int, float)):
            return bool(val)
        return str(val).strip().lower() in ("true", "1", "t", "yes", "oui")

    df = df.copy()
    df["remote"] = df["remote"].apply(to_bool)
    nb_remote = df["remote"].sum()
    print(f"📊 {nb_remote} offres remote | {len(df) - nb_remote} présentiel")
    return df


def filtrer_hors_domaine(df):
    """
    ✅ NOUVEAU : supprime les offres clairement hors-domaine data/tech.
    Vérifie dans le titre ET la description.
    """
    avant = len(df)
    texte_check = (df["titre"].fillna("") + " " + df["description"].fillna("")).str.lower()

    masque_hors = texte_check.apply(
        lambda t: any(mot in t for mot in MOTS_HORS_DOMAINE)
    )
    df = df[~masque_hors].reset_index(drop=True)
    apres = len(df)
    if avant != apres:
        print(f"🚫 {avant - apres} offres hors-domaine exclues ({apres} restantes)")
    return df


def preparer_texte(df):
    """
    Crée texte_complet pour le matching.
    Titre x3, description x2, tags x1 + tag remote textuel.
    """
    df = df.copy()
    df["titre"]       = df["titre"].fillna("")
    df["description"] = df["description"].fillna("")
    df["tags"]        = df["tags"].fillna("")
    df["lieu"]        = df["lieu"].fillna("")

    df["remote_tag"] = df["remote"].apply(
        lambda r: "remote télétravail full-remote" if r else ""
    )

    df["texte_complet"] = (
        (df["titre"] + " ") * 3 +
        (df["description"] + " ") * 2 +
        df["tags"] + " " +
        df["remote_tag"]
    ).str.strip()

    return df


def diagnostiquer(df):
    print("\n📋 Diagnostique des données :")
    print(f"   Total offres uniques        : {len(df)}")
    print(f"   Lieu renseigné              : {df['lieu'].ne('').sum()} ({df['lieu'].ne('').mean():.0%})")
    print(f"   Description > 100 caractères: {(df['description'].str.len() > 100).sum()}")
    print(f"   Tags renseignés             : {df['tags'].ne('').sum()}")
    print(f"   Offres remote               : {df['remote'].sum()}")
    print(f"   Sources                     : {df['source'].value_counts().to_dict()}\n")


def appliquer_bonus_source(df_result):
    """
    ✅ NOUVEAU : ajuste le score selon la source.
    Pénalise arbeitnow, booste jsearch/glassdoor.
    """
    def bonus(row):
        poids = SOURCE_WEIGHTS.get(row["source"], 1.0)
        return row["score_similarite"] * poids

    df_result = df_result.copy()
    df_result["score_brut"]      = df_result["score_similarite"]
    df_result["score_similarite"] = df_result.apply(bonus, axis=1)
    # Re-trier après ajustement
    df_result = df_result.sort_values("score_similarite", ascending=False)
    return df_result


# ─────────────────────────────────────────
# 2. MODE TF-IDF (RAPIDE)
# ─────────────────────────────────────────

def construire_modele_tfidf(df, forcer=False):
    if os.path.exists(MODEL_PATH) and not forcer:
        with open(MODEL_PATH, "rb") as f:
            data = pickle.load(f)
        print("📦 Modèle TF-IDF chargé depuis le disque")
        return data["vectorizer"], data["matrice"]

    print("🔧 Construction du modèle TF-IDF...")
    vectorizer = TfidfVectorizer(
        max_features=15000,
        ngram_range=(1, 3),
        stop_words=None,
        min_df=1,
        sublinear_tf=True
    )
    matrice = vectorizer.fit_transform(df["texte_complet"])

    with open(MODEL_PATH, "wb") as f:
        pickle.dump({"vectorizer": vectorizer, "matrice": matrice}, f)

    print(f"✅ Modèle TF-IDF construit ({matrice.shape[0]} offres, {matrice.shape[1]} features)")
    return vectorizer, matrice


def recommander_tfidf(profil_texte, df, vectorizer, matrice, top_n=10,
                      filtre_lieu=None, filtre_remote=None, filtre_source=None,
                      appliquer_poids_source=True):
    vecteur_profil = vectorizer.transform([profil_texte])
    scores = cosine_similarity(vecteur_profil, matrice).flatten()

    indices = np.argsort(scores)[::-1]
    df_result = df.iloc[indices].copy()
    df_result["score_similarite"] = scores[indices]

    # Filtres
    if filtre_lieu:
        df_result = df_result[
            df_result["lieu"].str.contains(filtre_lieu, case=False, na=False)
        ]
    if filtre_remote is not None:
        df_result = df_result[df_result["remote"] == filtre_remote]
    if filtre_source:
        df_result = df_result[df_result["source"] == filtre_source]

    # Seuil minimum
    df_result = df_result[df_result["score_similarite"] >= SEUIL_TFIDF]

    # Bonus/malus source
    if appliquer_poids_source:
        df_result = appliquer_bonus_source(df_result)

    if df_result.empty:
        print(f"⚠️  Aucune offre trouvée (seuil={SEUIL_TFIDF}).")
        if filtre_remote:
            print("    → Les offres remote data/NLP sont absentes de la base actuelle.")
            print("    → Conseil : scraper plus de sources avec filtre 'remote' activé.\n")

    return df_result.head(top_n)


# ─────────────────────────────────────────
# 3. MODE SENTENCE TRANSFORMERS (NLP AVANCÉ)
# ─────────────────────────────────────────

def recommander_nlp(profil_texte, df, top_n=10,
                    filtre_lieu=None, filtre_remote=None, filtre_source=None,
                    appliquer_poids_source=True):
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        print("⚠️  sentence-transformers non installé : pip install sentence-transformers")
        return pd.DataFrame()

    print(f"🧠 Chargement du modèle NLP : {NLP_MODEL_NAME}\n")
    model = SentenceTransformer(NLP_MODEL_NAME)

    textes_offres     = df["texte_complet"].tolist()
    embeddings_offres = model.encode(textes_offres, show_progress_bar=True, batch_size=32)
    embedding_profil  = model.encode([profil_texte])

    scores  = cosine_similarity(embedding_profil, embeddings_offres).flatten()
    indices = np.argsort(scores)[::-1]

    df_result = df.iloc[indices].copy()
    df_result["score_similarite"] = scores[indices]

    # Filtres
    if filtre_lieu:
        df_result = df_result[
            df_result["lieu"].str.contains(filtre_lieu, case=False, na=False)
        ]
    if filtre_remote is not None:
        df_result = df_result[df_result["remote"] == filtre_remote]
    if filtre_source:
        df_result = df_result[df_result["source"] == filtre_source]

    # Seuil minimum
    df_result = df_result[df_result["score_similarite"] >= SEUIL_NLP]

    # Bonus/malus source
    if appliquer_poids_source:
        df_result = appliquer_bonus_source(df_result)

    if df_result.empty:
        print(f"⚠️  Aucune offre au-dessus du seuil NLP ({SEUIL_NLP}).")
        print("    → Conseil : réduire SEUIL_NLP ou enrichir les descriptions.\n")

    return df_result.head(top_n)


# ─────────────────────────────────────────
# 4. AFFICHAGE DES RÉSULTATS
# ─────────────────────────────────────────

def afficher_resultats(df_result, mode="tfidf"):
    if df_result.empty:
        print("❌ Aucune offre trouvée.")
        return

    print(f"\n{'═'*60}")
    print(f"  🎯 TOP {len(df_result)} OFFRES RECOMMANDÉES [{mode.upper()}]")
    print(f"{'═'*60}\n")

    for i, (_, row) in enumerate(df_result.iterrows(), 1):
        score        = row.get("score_similarite", 0)
        score_brut   = row.get("score_brut", score)
        remote_label = "🏠 Remote" if row.get("remote") else "🏢 Présentiel"
        lieu         = row.get("lieu") or "Non précisé"

        sal_min = row.get("salaire_min")
        sal_max = row.get("salaire_max")
        salaire = ""
        if pd.notna(sal_min) and pd.notna(sal_max):
            salaire = f"💶 {int(sal_min)}€ - {int(sal_max)}€"
        elif pd.notna(sal_min):
            salaire = f"💶 À partir de {int(sal_min)}€"

        etoiles = "⭐" * min(5, int(score * 10)) if score >= 0.05 else "·"

        # Affiche score ajusté + score brut si différents
        score_str = f"{score:.3f}"
        if abs(score - score_brut) > 0.001:
            score_str += f" (brut: {score_brut:.3f})"

        print(f"  [{i:02d}] {etoiles} ({score_str})")
        print(f"       📌 {row.get('titre', 'N/A')}")
        print(f"       🏦 {row.get('entreprise', 'N/A')}")
        print(f"       📍 {lieu} | {remote_label}")
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
    forcer_rebuild=False,
    diagnostic=False,
    poids_source=True
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
    filtre_source : str  — forcer une source : "jsearch", "arbeitnow", "glassdoor"
    forcer_rebuild: bool — reconstruire le modèle TF-IDF
    diagnostic    : bool — afficher les stats de qualité des données
    poids_source  : bool — appliquer bonus/malus par source (défaut: True)
    """

    print(f"\n🚀 Recommandation [{mode.upper()}] | Profil: {type_profil}")
    print(f"   Recherche : {profil[:100]}{'...' if len(profil) > 100 else ''}\n")

    df = charger_offres_db()
    df = dedoublonner(df)
    df = normaliser_remote(df)
    df = filtrer_hors_domaine(df)   # ✅ filtre les offres hors-domaine
    df = preparer_texte(df)

    if diagnostic:
        diagnostiquer(df)

    if mode == "tfidf":
        vectorizer, matrice = construire_modele_tfidf(df, forcer=forcer_rebuild)
        resultats = recommander_tfidf(
            profil, df, vectorizer, matrice, top_n,
            filtre_lieu, filtre_remote, filtre_source,
            appliquer_poids_source=poids_source
        )
    elif mode == "nlp":
        resultats = recommander_nlp(
            profil, df, top_n,
            filtre_lieu, filtre_remote, filtre_source,
            appliquer_poids_source=poids_source
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
        forcer_rebuild=True,
        type_profil="titre",
        top_n=5,
        diagnostic=True
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