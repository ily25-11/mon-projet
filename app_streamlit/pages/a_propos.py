import streamlit as st
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from icons import ic

st.set_page_config(page_title="À propos — Job Intelligent", layout="wide", initial_sidebar_state="expanded")

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
    st.page_link("accueil.py",               label="  Accueil")
    st.page_link("pages/recommandation.py",  label="  Recommandation")
    st.markdown(f"""
    <div style="background:rgba(255,255,255,.08);border-radius:8px;padding:.5rem .75rem;margin:.3rem 0;display:flex;align-items:center;gap:8px;">
        {ic("info","#1D9E75",16)}
        <span style="font-weight:500;color:#f1f5f9;font-size:.9rem;">À propos</span>
    </div>
    """, unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="display:flex;align-items:center;gap:10px;margin-bottom:.4rem;">
    {ic("info","#0f172a",26)}
    <span style="font-size:2rem;font-weight:700;color:#0f172a;">À propos du projet</span>
</div>
<p style="font-size:.92rem;color:#334155;line-height:1.8;">
    Job Intelligent est un système de recommandation d'offres d'emploi dans le domaine de la Data, développé dans le cadre d'un projet académique.
</p>
""", unsafe_allow_html=True)

col1, col2 = st.columns([1.2, 1], gap="large")

def section(icon, label):
    return f'<div style="display:flex;align-items:center;gap:8px;margin:2rem 0 .8rem;border-left:4px solid #1D9E75;padding-left:.75rem;">{ic(icon,"#1D9E75",18)}<span style="font-size:1.05rem;font-weight:600;color:#0f172a;">{label}</span></div>'

with col1:
    st.markdown(section("target","Problématique"), unsafe_allow_html=True)
    st.markdown("""
    <p style="font-size:.92rem;color:#334155;line-height:1.8;">
    Le marché de l'emploi Data est fragmenté : les offres sont dispersées sur de nombreuses plateformes
    avec des formats hétérogènes et des intitulés non standardisés. Les candidats peinent à identifier
    rapidement les offres correspondant à leur profil.<br><br>
    <strong>Job Intelligent</strong> centralise ces offres et utilise le <em>matching sémantique</em>
    pour proposer les opportunités les plus pertinentes.
    </p>
    """, unsafe_allow_html=True)

    st.markdown(section("layers","Architecture technique"), unsafe_allow_html=True)
    components = [
        ("activity",   "Pipeline Airflow",         "scraping_pipeline_v2", "Scraping quotidien depuis 7 sources. Déduplication et stockage en base."),
        ("brain",      "Modèle d'embeddings",       "embeddings.py",        "Encode chaque offre en vecteur dense avec sentence-transformers."),
        ("search",     "Moteur de recommandation",  "recommender.py",       "Encode le profil utilisateur et calcule la similarité cosinus."),
        ("file-text",  "Extracteur PDF",            "cv_parser.py",         "Extraction du texte brut d'un CV PDF via pdfplumber/pypdf."),
        ("zap",        "Interface Streamlit",       "accueil.py",           "Application multi-pages : accueil, recommandation, à propos."),
    ]
    for icon, title, module, desc in components:
        st.markdown(f"""
        <div style="background:#f8fafc;border-radius:10px;padding:1rem 1.25rem;border:1px solid #e2e8f0;margin-bottom:.6rem;">
            <div style="display:flex;align-items:center;gap:8px;margin-bottom:.3rem;">
                {ic(icon,"#1D9E75",16)}
                <span style="font-weight:600;font-size:.9rem;color:#0f172a;">{title}</span>
                <code style="font-size:.7rem;background:#e2e8f0;padding:.1rem .4rem;border-radius:4px;color:#475569;">{module}</code>
            </div>
            <div style="font-size:.82rem;color:#64748b;">{desc}</div>
        </div>
        """, unsafe_allow_html=True)

with col2:
    st.markdown(section("cpu","Stack technique"), unsafe_allow_html=True)
    techs = [
        ("Scraping",      ["requests","BeautifulSoup","API REST"]),
        ("Orchestration", ["Apache Airflow","Docker"]),
        ("NLP / ML",      ["sentence-transformers","scikit-learn","numpy"]),
        ("Data",          ["pandas","PostgreSQL","pickle"]),
        ("Interface",     ["Streamlit","pdfplumber"]),
    ]
    for cat, items in techs:
        chips = "".join([f'<span style="display:inline-block;background:#1e293b;color:#e2e8f0;font-size:.72rem;font-weight:600;padding:.2rem .65rem;border-radius:6px;margin:.15rem;font-family:monospace;">{t}</span>' for t in items])
        st.markdown(f'<p style="font-size:.72rem;font-weight:600;color:#94a3b8;margin:.9rem 0 .3rem;text-transform:uppercase;">{cat}</p>{chips}', unsafe_allow_html=True)

    st.markdown(section("globe","Sources de données"), unsafe_allow_html=True)
    sources = [
        ("France Travail","#1D9E75","1 549 offres"),
        ("Arbeitnow",     "#1D9E75","247 offres"),
        ("The Muse",      "#1D9E75","400 offres"),
        ("Remotive",      "#1D9E75","21 offres"),
        ("Adzuna",        "#ef4444","0 offres — à vérifier"),
        ("JSearch",       "#ef4444","0 offres — à vérifier"),
        ("SerpApi",       "#ef4444","0 offres — à vérifier"),
    ]
    for name, color, count in sources:
        ok = color == "#1D9E75"
        st.markdown(f"""
        <div style="display:flex;justify-content:space-between;align-items:center;padding:.45rem 0;border-bottom:.5px solid #e2e8f0;font-size:.85rem;">
            <span style="display:flex;align-items:center;gap:8px;">{ic("check-circle" if ok else "x-circle",color,14)} {name}</span>
            <span style="color:#64748b;">{count}</span>
        </div>
        """, unsafe_allow_html=True)

    st.markdown(section("alert-triangle","Limitations connues"), unsafe_allow_html=True)
    for l in ["3 sources sur 7 à 0 offres (quota API ou clé expirée)","0 insertions en DB au dernier run — bug de fusion","Pas de filtrage par date de publication"]:
        st.markdown(f"""
        <div style="background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:.7rem 1rem;font-size:.85rem;color:#9a3412;margin-bottom:.5rem;display:flex;align-items:flex-start;gap:8px;">
            {ic("alert-triangle","#f97316",15)} {l}
        </div>""", unsafe_allow_html=True)

    st.markdown(section("rocket","Évolutions prévues"), unsafe_allow_html=True)
    for r in ["Feedback utilisateur pour affiner les recommandations","Filtres avancés : contrat, salaire, remote","Alertes email pour les nouvelles offres matchées"]:
        st.markdown(f"""
        <div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:8px;padding:.7rem 1rem;font-size:.85rem;color:#166534;margin-bottom:.5rem;display:flex;align-items:flex-start;gap:8px;">
            {ic("check-circle","#1D9E75",15)} {r}
        </div>""", unsafe_allow_html=True)

st.divider()
st.markdown('<p style="text-align:center;font-size:.8rem;color:#94a3b8;">Job Intelligent · Projet Data · 2026</p>', unsafe_allow_html=True)