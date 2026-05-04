"""
api.py — FastAPI pour Job Intelligent
Placer à la RACINE de mon-projet/ (même niveau que recommender/)

Lancer :
    uvicorn api:app --host 0.0.0.0 --port 8000 --reload
"""

import io
import traceback
import logging
from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("jobIA")

from recommender.recommender import recommend, df

try:
    from recommender.cv_parser import extract_text_from_pdf as parse_cv
    CV_PARSER_AVAILABLE = True
except ImportError:
    CV_PARSER_AVAILABLE = False

app = FastAPI(
    title="Job Intelligent API",
    description="Recommandation d'offres d'emploi Data via Sentence Transformers",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class RecoRequest(BaseModel):
    user_text: str = Field(..., min_length=1)
    top_k: int = Field(10, ge=1, le=50)
    min_score: float = Field(0.0, ge=0.0, le=1.0)


class HealthResponse(BaseModel):
    status: str
    cv_parser: bool


@app.get("/health", response_model=HealthResponse, tags=["Système"])
def health():
    return {"status": "ok", "cv_parser": CV_PARSER_AVAILABLE}


@app.post("/recommend", tags=["Recommandation"])
def get_recommendations(req: RecoRequest):
    if not req.user_text.strip():
        raise HTTPException(status_code=400, detail="user_text ne peut pas être vide")
    try:
        results = recommend(user_text=req.user_text, top_k=req.top_k)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur moteur : {str(e)}")

    filtered = [r for r in results if r["score"] >= req.min_score]
    return {
        "count": len(filtered),
        "query": req.user_text,
        "results": filtered,
    }


@app.post("/recommend/cv", tags=["Recommandation"])
async def recommend_from_cv(
    file: UploadFile = File(...),
    top_k: int = Form(10),
    min_score: float = Form(0.0),
):
    if not CV_PARSER_AVAILABLE:
        raise HTTPException(status_code=501, detail="cv_parser non disponible.")

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
    return {                          # ← le return manquait ici
        "count": len(filtered),
        "cv_filename": file.filename,
        "results": filtered,
    }

@app.get("/dashboard", tags=["Dashboard"])
def get_dashboard():
    """Retourne toutes les données pour les graphiques."""
    
    # 1. Offres par source
    sources = {}
    if "source" in df.columns:
        sources = df["source"].value_counts().to_dict()
        SOURCE_LABELS = {
            "france_travail": "France Travail", "francetravail": "France Travail",
            "arbeitnow": "Arbeitnow", "the_muse": "The Muse", "themuse": "The Muse",
            "remotive": "Remotive", "serpapi": "SerpApi",
            "kaggle_linkedin": "Kaggle LinkedIn", "kaggle": "Kaggle LinkedIn",
        }
        sources = {}
        for k, v in df["source"].value_counts().to_dict().items():
            label = SOURCE_LABELS.get(str(k).lower().strip(), str(k))
            sources[label] = sources.get(label, 0) + int(v)

    # 2. Offres par type de contrat
    contrats = {}
    col = "contrat_type" if "contrat_type" in df.columns else "contrat"
    if col in df.columns:
        REMOTE_KW = {"onsite", "hybrid", "remote", "hybride"}
        CONTRAT_MAP = {
            "fulltime": "CDI", "full_time": "CDI", "full-time": "CDI", "permanent": "CDI",
            "parttime": "Temps partiel", "part_time": "Temps partiel",
            "contract": "CDD", "contractor": "Freelance", "freelance": "Freelance",
            "internship": "Stage", "intern": "Stage",
            "apprenticeship": "Alternance", "temporary": "Intérim",
            "cdi": "CDI", "cdd": "CDD", "stage": "Stage",
            "alternance": "Alternance", "interim": "Intérim",
        }
        for val in df[col].dropna():
            key = str(val).lower().strip()
            if key in REMOTE_KW:
                continue
            label = CONTRAT_MAP.get(key, str(val))
            contrats[label] = contrats.get(label, 0) + 1

    # 3. Top 10 postes
    postes = {}
    if "poste_recherche" in df.columns:
        postes = (
            df["poste_recherche"]
            .dropna()
            .loc[lambda s: (s != "") & (s != "Non renseigné")]
            .value_counts()
            .head(10)
            .to_dict()
        )

    # 4. Top 10 compétences
    competences = {}
    if "tags" in df.columns:
        from collections import Counter
        all_tags = []
        for tags in df["tags"].dropna():
            all_tags.extend([t.strip() for t in str(tags).split(",") if t.strip()])
        competences = dict(Counter(all_tags).most_common(10))

    # 5. Offres par mois
    par_mois = {}
    for col in ["date_scraping", "date_creation"]:
        if col in df.columns:
            try:
                dates = pd.to_datetime(df[col], errors="coerce").dropna()
                par_mois = (
                    dates.dt.to_period("M")
                    .astype(str)
                    .value_counts()
                    .sort_index()
                    .to_dict()
                )
                break
            except Exception:
                pass

    # 6. Remote vs Sur site
    remote = {"Remote": 0, "Sur site": 0, "Hybride": 0}
    if "remote" in df.columns:
        for val in df["remote"].dropna():
            if val is True or str(val).lower() in ("true", "1", "remote"):
                remote["Remote"] += 1
            else:
                remote["Sur site"] += 1
    if "contrat_type" in df.columns:
        for val in df["contrat_type"].dropna():
            if str(val).lower() == "hybrid":
                remote["Hybride"] += 1
                remote["Sur site"] = max(0, remote["Sur site"] - 1)

    return {
        "sources":     sources,
        "contrats":    contrats,
        "postes":      postes,
        "competences": competences,
        "par_mois":    par_mois,
        "remote":      remote,
        "total":       len(df),
    }
@app.get("/stats", tags=["Système"])
def get_stats():
    total = len(df)

    sources_raw = {}
    if "source" in df.columns:
        sources_raw = df["source"].value_counts().to_dict()

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