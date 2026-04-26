"""
api.py — FastAPI pour Job Intelligent
Placer à la RACINE de mon-projet/ (même niveau que recommender/)

Lancer :
    uvicorn api:app --host 0.0.0.0 --port 8000 --reload

Dépendances à ajouter dans requirements.txt :
    fastapi
    uvicorn[standard]
    python-multipart
"""

import io
import traceback
import logging
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("jobIA")

# ── Import du vrai moteur de recommandation ──────────────────────────────────
from recommender.recommender import recommend, df  # on importe aussi df pour les stats

# ── Tentative d'import du parser CV (optionnel) ───────────────────────────────
try:
    from recommender.cv_parser import extract_text_from_pdf as parse_cv
    CV_PARSER_AVAILABLE = True
except ImportError:
    CV_PARSER_AVAILABLE = False

# ── Application ──────────────────────────────────────────────────────────────
app = FastAPI(
    title="Job Intelligent API",
    description="Recommandation d'offres d'emploi Data via Sentence Transformers",
    version="1.0.0",
)

# CORS : autorise le fichier HTML local (file://) et localhost
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # restreindre en production
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Schémas ───────────────────────────────────────────────────────────────────
class RecoRequest(BaseModel):
    user_text: str = Field(..., min_length=1, description="Texte libre : poste + compétences")
    top_k: int = Field(10, ge=1, le=50)
    min_score: float = Field(0.0, ge=0.0, le=1.0)


class HealthResponse(BaseModel):
    status: str
    cv_parser: bool


# ── Routes ────────────────────────────────────────────────────────────────────
@app.get("/health", response_model=HealthResponse, tags=["Système"])
def health():
    """Vérifie que l'API est opérationnelle."""
    return {"status": "ok", "cv_parser": CV_PARSER_AVAILABLE}


@app.post("/recommend", tags=["Recommandation"])
def get_recommendations(req: RecoRequest):
    """
    Retourne les offres les plus proches sémantiquement du texte utilisateur.
    - user_text  : combinaison poste + compétences (ou texte extrait du CV)
    - top_k      : nombre d'offres à retourner
    - min_score  : score cosinus minimum (0.0 → 1.0)
    """
    if not req.user_text.strip():
        raise HTTPException(status_code=400, detail="user_text ne peut pas être vide")

    try:
        results = recommend(user_text=req.user_text, top_k=req.top_k)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur moteur : {str(e)}")

    # Filtrage min_score côté API (le recommender ne l'applique pas encore)
    filtered = [r for r in results if r["score"] >= req.min_score]

    return {
        "count": len(filtered),
        "query": req.user_text,
        "results": filtered,
    }


@app.post("/recommend/cv", tags=["Recommandation"])
async def recommend_from_cv(
    file: UploadFile = File(...),
    top_k: int = 10,
    min_score: float = 0.0,
):
    """
    Upload d'un PDF → extraction du texte → recommandation.
    Nécessite que recommender/cv_parser.py soit disponible.
    """
    if not CV_PARSER_AVAILABLE:
        raise HTTPException(
            status_code=501,
            detail="cv_parser non disponible. Installez pdfplumber ou PyMuPDF."
        )

    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Seuls les fichiers PDF sont acceptés")

    try:
        content = await file.read()
        logger.info(f"CV reçu : {file.filename}, taille : {len(content)} bytes")
        cv_text = parse_cv(content)
        logger.info(f"Texte extrait : {len(cv_text)} caractères")
    except Exception as e:
        logger.error(f"Erreur extraction CV :\n{traceback.format_exc()}")
        raise HTTPException(status_code=422, detail=f"Impossible de lire le CV : {str(e)}")

    if not cv_text or len(cv_text.strip()) < 20:
        raise HTTPException(status_code=422, detail="CV trop court ou illisible")

    try:
        results = recommend(user_text=cv_text, top_k=top_k)
        logger.info(f"Recommandation OK : {len(results)} résultats")
    except Exception as e:
        logger.error(f"Erreur recommandation :\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Erreur moteur : {str(e)}")

    filtered = [r for r in results if r["score"] >= min_score]

@app.get("/stats", tags=["Système"])
def get_stats():
    """
    Retourne les statistiques réelles du DataFrame en mémoire :
    total d'offres, répartition par source, date du dernier scraping.
    """
    total = len(df)

    # Répartition par source
    sources_raw = {}
    if "source" in df.columns:
        sources_raw = df["source"].value_counts().to_dict()

    # Normalisation des noms de sources
    SOURCE_LABELS = {
        "france_travail":  "France Travail",
        "francetravail":   "France Travail",
        "arbeitnow":       "Arbeitnow",
        "the_muse":        "The Muse",
        "themuse":         "The Muse",
        "remotive":        "Remotive",
        "adzuna":          "Adzuna",
        "jsearch":         "JSearch",
        "serpapi":         "SerpApi",
        "kaggle":          "Kaggle LinkedIn",
        "kaggle_linkedin": "Kaggle LinkedIn",
        "linkedin":        "Kaggle LinkedIn",
    }

    sources = {}
    for raw_name, count in sources_raw.items():
        label = SOURCE_LABELS.get(str(raw_name).lower().strip(), str(raw_name))
        sources[label] = sources.get(label, 0) + int(count)

    # Date du dernier scraping
    last_update = None
    for col in ["date_scraping", "date_creation"]:
        if col in df.columns:
            try:
                last_update = str(df[col].dropna().max())[:10]
                break
            except Exception:
                pass

    return {
        "total":       total,
        "sources":     sources,
        "last_update": last_update or "N/A",
        "model":       "paraphrase-multilingual-MiniLM-L12-v2",
    }