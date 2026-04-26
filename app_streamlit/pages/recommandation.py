import streamlit as st
import pickle, io
import numpy as np
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from icons import ic

st.set_page_config(page_title="Recommandation — Job Intelligent", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
    [data-testid="stSidebar"] { background-color: #0f172a; }
    [data-testid="stSidebar"] * { color: #e2e8f0 !important; }
</style>
""", unsafe_allow_html=True)

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown(f"""
    <div style="display:flex;align-items:center;gap:10px;padding:.5rem 0 1rem;">
        <div style="background:#1D9E75;border-radius:8px;width:32px;height:32px;display:flex;align-items:center;justify-content:center;">
            {ic("target","white",18)}
        </div>
        <span style="font-size:1rem;font-weight:600;">Job Intelligent</span>
    </div>
    """, unsafe_allow_html=True)
    st.divider()
    st.markdown('<p style="font-size:.7rem;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:#94a3b8;margin-bottom:.5rem;">Navigation</p>', unsafe_allow_html=True)
    st.page_link("accueil.py", label="  Accueil")
    st.markdown(f"""
    <div style="background:rgba(255,255,255,.08);border-radius:8px;padding:.5rem .75rem;margin:.3rem 0;display:flex;align-items:center;gap:8px;">
        {ic("search","#1D9E75",16)}
        <span style="font-weight:500;color:#f1f5f9;font-size:.9rem;">Recommandation</span>
    </div>
    """, unsafe_allow_html=True)
    st.page_link("pages/a_propos.py", label="  À propos")
    st.divider()
    st.markdown(f'<p style="font-size:.7rem;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:#94a3b8;margin-bottom:.8rem;">Paramètres</p>', unsafe_allow_html=True)
    top_k     = st.slider("Nombre de résultats", 5, 30, 10, 5)
    score_min = st.slider("Score minimum (%)",   0, 100, 30, 5)
    st.divider()
    st.markdown(f"""
    <div style="font-size:.83rem;display:flex;align-items:center;gap:8px;">
        {ic("check-circle","#1D9E75",14)} MAJ : <strong>26/04/2026</strong>
    </div>
    """, unsafe_allow_html=True)

# ── Cache modèle & données ────────────────────────────────────────────────────
@st.cache_resource(show_spinner="Chargement du modèle…")
def load_model():
    from sentence_transformers import SentenceTransformer
    return SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")

@st.cache_data(show_spinner="Chargement des offres…")
def load_embeddings():
    with open("recommender/embeddings.pkl", "rb") as f:
        data = pickle.load(f)
    return data["embeddings"], data["df"]

def extract_pdf(file_bytes):
    try:
        import pdfplumber
        with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
            return " ".join(p.extract_text() or "" for p in pdf.pages)
    except Exception:
        try:
            import pypdf
            r = pypdf.PdfReader(io.BytesIO(file_bytes))
            return " ".join(p.extract_text() or "" for p in r.pages)
        except Exception as e:
            st.error(f"Impossible de lire le PDF : {e}")
            return ""

def recommend(user_text, embeddings, df, top_k, score_min):
    from sklearn.metrics.pairwise import cosine_similarity
    model = load_model()
    scores  = cosine_similarity(model.encode([user_text]), embeddings)[0]
    top_idx = np.argsort(scores)[::-1][:top_k * 3]
    res     = df.iloc[top_idx].copy()
    res["score"] = scores[top_idx]
    return res[res["score"] >= score_min / 100].head(top_k)

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="display:flex;align-items:center;gap:10px;margin-bottom:.3rem;">
    {ic("search","#0f172a",26)}
    <span style="font-size:2rem;font-weight:700;color:#0f172a;">Recommandation d'offres</span>
</div>
<p style="color:#64748b;font-size:.95rem;">Renseignez votre profil ou importez votre CV pour obtenir les offres les plus adaptées.</p>
""", unsafe_allow_html=True)
st.markdown("<br>", unsafe_allow_html=True)

# ── Layout ────────────────────────────────────────────────────────────────────
col_form, col_res = st.columns([1, 1.6], gap="large")

with col_form:
    st.markdown(f'<p style="font-size:.75rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:#94a3b8;display:flex;align-items:center;gap:6px;">{ic("user","#94a3b8",13)} Votre profil</p>', unsafe_allow_html=True)
    tab_cv, tab_man = st.tabs(["Upload CV (PDF)", "Saisie manuelle"])
    cv_text = man_text = ""

    with tab_cv:
        f = st.file_uploader("Importez votre CV en PDF", type=["pdf"])
        if f:
            cv_text = extract_pdf(f.read())
            if cv_text:
                st.markdown(f"""
                <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;padding:.8rem 1rem;font-size:.85rem;color:#166534;display:flex;align-items:center;gap:8px;margin-top:.5rem;">
                    {ic("check-circle","#1D9E75",16)} CV lu avec succès.
                </div>""", unsafe_allow_html=True)
                with st.expander("Aperçu du texte extrait"):
                    st.text(cv_text[:800] + ("..." if len(cv_text) > 800 else ""))

    with tab_man:
        poste  = st.text_input("Titre de poste recherché", placeholder="ex : Data Engineer, ML Engineer…")
        skills = st.text_area("Compétences", placeholder="ex : Python, Spark, SQL, dbt, Airflow…", height=100)
        ville  = st.text_input("Ville / Région", placeholder="ex : Paris, Lyon, Remote…")
        man_text = f"{poste} {skills} {ville}".strip()

    user_text = (cv_text + " " + man_text).strip()
    st.markdown("<br>", unsafe_allow_html=True)
    run = st.button("Lancer la recommandation", type="primary", use_container_width=True)

# ── Résultats ─────────────────────────────────────────────────────────────────
TAG = "display:inline-block;border-radius:999px;font-size:.72rem;font-weight:600;padding:.15rem .6rem;margin:.15rem .15rem 0 0;border:1px solid;"
TG  = "background:#f0fdf4;color:#166534;border-color:#bbf7d0;"
TP  = "background:#faf5ff;color:#6b21a8;border-color:#e9d5ff;"
TB  = "background:#eff6ff;color:#1e40af;border-color:#bfdbfe;"

with col_res:
    st.markdown(f'<p style="font-size:.75rem;font-weight:600;text-transform:uppercase;letter-spacing:.06em;color:#94a3b8;display:flex;align-items:center;gap:6px;">{ic("bar-chart","#94a3b8",13)} Résultats</p>', unsafe_allow_html=True)

    if run:
        if not user_text:
            st.warning("Renseignez au moins un champ ou uploadez un CV.")
        else:
            try:
                embeddings, df = load_embeddings()
                with st.spinner("Analyse sémantique en cours…"):
                    results = recommend(user_text, embeddings, df, top_k, score_min)

                if results.empty:
                    st.markdown(f"""
                    <div style="text-align:center;padding:3rem;color:#94a3b8;">
                        <div style="display:flex;justify-content:center;margin-bottom:1rem;">{ic("alert-triangle","#cbd5e1",40)}</div>
                        <div style="font-size:.95rem;">Aucune offre trouvée.<br>Baissez le score minimum ou élargissez les critères.</div>
                    </div>""", unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div style="display:flex;align-items:center;gap:8px;margin-bottom:1rem;">
                        {ic("briefcase","#1D9E75",18)}
                        <span style="font-weight:600;color:#0f172a;">{len(results)} offres correspondant à votre profil</span>
                    </div>""", unsafe_allow_html=True)

                    def sg(row, col, default="N/A"):
                        if col not in results.columns: return default
                        v = row[col]
                        return default if (pd.isna(v) or str(v).strip().lower() in ("nan","none","")) else str(v).strip()

                    for _, row in results.iterrows():
                        pct     = int(row["score"] * 100)
                        titre   = sg(row,"titre","Offre sans titre")
                        ent     = sg(row,"entreprise")
                        lieu    = sg(row,"lieu")
                        contrat = sg(row,"contrat_type", sg(row,"contrat",""))
                        remote  = sg(row,"remote","")
                        lien    = sg(row,"lien","#")

                        badges = ""
                        if contrat and contrat != "N/A":
                            badges += f'<span style="{TAG}{TP}">{contrat}</span>'
                        if str(remote).lower() in ("true","1","oui","yes"):
                            badges += f'<span style="{TAG}{TB}">{ic("globe","#1e40af",11)} Remote</span>'
                        raw = sg(row,"tags","")
                        if raw and raw != "N/A":
                            for s in raw.split(",")[:5]:
                                s = s.strip()
                                if s and s.lower() not in ("nan","none"):
                                    badges += f'<span style="{TAG}{TG}">{s}</span>'

                        st.markdown(f"""
                        <div style="background:white;border-radius:12px;padding:1.2rem 1.4rem;border:1px solid #e2e8f0;margin-bottom:.8rem;">
                            <div style="font-size:1rem;font-weight:600;color:#0f172a;">{titre}</div>
                            <div style="display:flex;align-items:center;gap:14px;font-size:.82rem;color:#64748b;margin-top:.35rem;flex-wrap:wrap;">
                                <span style="display:flex;align-items:center;gap:5px;">{ic("briefcase","#94a3b8",13)} {ent}</span>
                                <span style="display:flex;align-items:center;gap:5px;">{ic("map-pin","#94a3b8",13)} {lieu}</span>
                            </div>
                            <div style="margin-top:.55rem;">{badges}</div>
                            <div style="margin-top:.7rem;background:#f1f5f9;border-radius:999px;height:5px;">
                                <div style="height:5px;border-radius:999px;background:linear-gradient(90deg,#1D9E75,#0ea5e9);width:{pct}%;"></div>
                            </div>
                            <div style="display:flex;justify-content:space-between;align-items:center;margin-top:.4rem;">
                                <span style="display:flex;align-items:center;gap:5px;font-size:.78rem;color:#1D9E75;font-weight:600;">
                                    {ic("trending-up","#1D9E75",13)} Matching : {pct}%
                                </span>
                                <a href="{lien}" target="_blank" style="display:flex;align-items:center;gap:4px;font-size:.78rem;color:#0ea5e9;text-decoration:none;font-weight:600;">
                                    Voir l'offre {ic("external-link","#0ea5e9",12)}
                                </a>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)

                    st.markdown("<br>", unsafe_allow_html=True)
                    ecols = [c for c in ["titre","entreprise","lieu","contrat_type","remote","score","lien"] if c in results.columns]
                    csv   = results[ecols].copy()
                    csv["score"] = (csv["score"]*100).round(1).astype(str)+"%"
                    st.download_button(
                        label="Télécharger les résultats (CSV)",
                        data=csv.to_csv(index=False).encode("utf-8"),
                        file_name="recommandations.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
            except FileNotFoundError:
                st.error("embeddings.pkl introuvable — lancez d'abord : python recommender/embeddings.py")
            except Exception as e:
                st.error(f"Erreur : {e}")
    else:
        st.markdown(f"""
        <div style="text-align:center;padding:3rem;color:#94a3b8;">
            <div style="display:flex;justify-content:center;margin-bottom:1rem;">{ic("target","#cbd5e1",48)}</div>
            <div style="font-size:.95rem;">Renseignez votre profil à gauche<br>et lancez la recommandation.</div>
        </div>""", unsafe_allow_html=True)