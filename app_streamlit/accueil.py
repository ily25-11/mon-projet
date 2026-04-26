import streamlit as st
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from icons import ic

st.set_page_config(page_title="Job Intelligent", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
    [data-testid="stSidebar"] { background-color: #0f172a; }
    [data-testid="stSidebar"] * { color: #e2e8f0 !important; }
    .hero-title {
        font-size: 2.8rem; font-weight: 700;
        background: linear-gradient(135deg, #1D9E75, #0ea5e9);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        line-height: 1.25;
    }
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
    st.markdown(f'<p style="font-size:.7rem;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:#94a3b8;margin-bottom:.5rem;">Navigation</p>', unsafe_allow_html=True)
    st.markdown(f"""
    <div style="background:rgba(255,255,255,.08);border-radius:8px;padding:.5rem .75rem;margin-bottom:.3rem;display:flex;align-items:center;gap:8px;">
        {ic("home","#1D9E75",16)}
        <span style="font-weight:500;color:#f1f5f9;font-size:.9rem;">Accueil</span>
    </div>
    """, unsafe_allow_html=True)
    st.page_link("pages/recommandation.py", label="  Recommandation")
    st.page_link("pages/a_propos.py",       label="  À propos")
    st.divider()
    st.markdown(f"""
    <p style="font-size:.7rem;font-weight:600;text-transform:uppercase;letter-spacing:.08em;color:#94a3b8;margin-bottom:.5rem;">Pipeline</p>
    <div style="font-size:.83rem;display:flex;flex-direction:column;gap:.5rem;">
        <div style="display:flex;align-items:center;gap:8px;">{ic("check-circle","#1D9E75",14)} MAJ : <strong>26/04/2026</strong></div>
        <div style="display:flex;align-items:center;gap:8px;">{ic("package","#94a3b8",14)} Sources actives : <strong>4 / 7</strong></div>
    </div>
    """, unsafe_allow_html=True)

# ── Hero ──────────────────────────────────────────────────────────────────────
col_hero, col_img = st.columns([3, 2], gap="large")
with col_hero:
    st.markdown('<div class="hero-title">Trouvez votre prochain poste Data — intelligemment.</div>', unsafe_allow_html=True)
    st.markdown('<p style="font-size:1.1rem;color:#64748b;margin-top:1rem;line-height:1.8;">Job Intelligent centralise des milliers d\'offres d\'emploi dans la Data et vous propose des recommandations personnalisées grâce à l\'analyse sémantique de votre CV ou profil.</p>', unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("Lancer la recommandation", type="primary"):
        st.switch_page("pages/recommandation.py")
with col_img:
    st.image("https://illustrations.popsy.co/amber/man-with-a-laptop.svg", use_container_width=True)

st.divider()

# ── Stats ─────────────────────────────────────────────────────────────────────
st.markdown(f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:1rem;">{ic("bar-chart","#0f172a",20)}<span style="font-size:1.2rem;font-weight:600;color:#0f172a;margin-left:6px;">Chiffres clés</span></div>', unsafe_allow_html=True)

for col, (icon, color, num, label) in zip(st.columns(4), [
    ("briefcase", "#1D9E75", "4 559",  "Offres uniques"),
    ("layers",    "#0ea5e9", "7",      "Sources agrégées"),
    ("zap",       "#f59e0b", "< 2s",   "Temps de recommandation"),
    ("brain",     "#8b5cf6", "100%",   "Matching sémantique"),
]):
    with col:
        st.markdown(f"""
        <div style="background:white;border-radius:12px;padding:1.4rem;text-align:center;border:1px solid #e2e8f0;">
            <div style="display:flex;justify-content:center;margin-bottom:.6rem;">{ic(icon,color,26)}</div>
            <div style="font-size:2rem;font-weight:700;color:{color};">{num}</div>
            <div style="font-size:.85rem;color:#64748b;margin-top:.2rem;">{label}</div>
        </div>
        """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ── Features ──────────────────────────────────────────────────────────────────
st.markdown(f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:1rem;">{ic("star","#0f172a",20)}<span style="font-size:1.2rem;font-weight:600;color:#0f172a;margin-left:6px;">Fonctionnalités</span></div>', unsafe_allow_html=True)

features = [
    ("file-text",  "#1D9E75", "Upload de CV (PDF)",    "Importez votre CV en PDF. Le système extrait automatiquement vos compétences pour alimenter le moteur de matching."),
    ("zap",        "#f59e0b", "Résultats instantanés",  "Les embeddings sont pré-calculés. La recommandation se fait par similarité cosinus en moins de 2 secondes."),
    ("brain",      "#8b5cf6", "Matching sémantique",    "Utilise paraphrase-multilingual-MiniLM-L12-v2 pour comprendre le sens de votre profil, pas seulement les mots-clés."),
    ("map-pin",    "#ef4444", "Filtrage par ville",     "Affinez les résultats par localisation pour cibler les offres dans votre région ou en remote."),
    ("globe",      "#0ea5e9", "Sources multiples",      "Offres agrégées depuis France Travail, Arbeitnow, The Muse, Remotive, Adzuna, JSearch et SerpApi."),
    ("refresh-cw", "#1D9E75", "Pipeline automatisé",   "Le scraping tourne quotidiennement via Apache Airflow. La base d'offres est toujours à jour."),
]
f1, f2, f3 = st.columns(3)
for i, (icon, color, title, desc) in enumerate(features):
    with [f1, f2, f3][i % 3]:
        st.markdown(f"""
        <div style="background:#f8fafc;border-radius:12px;padding:1.25rem 1.4rem;border-left:4px solid {color};margin-bottom:1rem;">
            <div style="display:flex;align-items:center;gap:8px;margin-bottom:.5rem;">
                {ic(icon,color,18)}
                <span style="font-weight:600;font-size:.95rem;color:#0f172a;">{title}</span>
            </div>
            <div style="font-size:.85rem;color:#64748b;line-height:1.65;">{desc}</div>
        </div>
        """, unsafe_allow_html=True)

st.divider()

# ── Sources ───────────────────────────────────────────────────────────────────
st.markdown(f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:1rem;">{ic("link","#0f172a",20)}<span style="font-size:1.2rem;font-weight:600;color:#0f172a;margin-left:6px;">Sources de données</span></div>', unsafe_allow_html=True)
sources = ["France Travail","Arbeitnow","The Muse","Remotive","Adzuna","JSearch","SerpApi"]
st.markdown(" ".join([
    f'<span style="display:inline-block;background:#e0f2fe;color:#0369a1;font-size:.75rem;font-weight:600;padding:.25rem .75rem;border-radius:999px;margin:.2rem;">{s}</span>'
    for s in sources
]), unsafe_allow_html=True)
st.markdown("<br><br>", unsafe_allow_html=True)