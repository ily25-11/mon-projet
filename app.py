"""
Job Intelligent - Application de Recommandation d'Offres d'Emploi
Interface Streamlit professionnelle

Lancement : streamlit run app.py
"""

import streamlit as st
import pandas as pd
import sys
import os

# Import depuis recommendation.py
sys.path.insert(0, os.path.dirname(__file__))
from recommendation import (
    charger_offres_db,
    preparer_texte,
    construire_modele_tfidf,
    recommander_tfidf,
    recommander_nlp
)

# ─────────────────────────────────────────
# CONFIGURATION PAGE
# ─────────────────────────────────────────

st.set_page_config(
    page_title="Job Intelligent",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─────────────────────────────────────────
# CSS PROFESSIONNEL
# ─────────────────────────────────────────

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

    .main { background-color: #F7F9FC; }

    section[data-testid="stSidebar"] { background-color: #1F4E79; }
    section[data-testid="stSidebar"] * { color: white !important; }
    section[data-testid="stSidebar"] label {
        color: rgba(255,255,255,0.75) !important;
        font-size: 0.78rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.06em;
    }

    .app-header {
        background: linear-gradient(135deg, #1F4E79 0%, #2E75B6 100%);
        border-radius: 14px;
        padding: 2.2rem 2.8rem;
        margin-bottom: 1.8rem;
        color: white;
    }
    .app-header h1 {
        font-size: 2.2rem;
        font-weight: 700;
        margin: 0 0 0.3rem 0;
        letter-spacing: -0.02em;
    }
    .app-header p {
        font-size: 0.95rem;
        opacity: 0.8;
        margin: 0;
        font-weight: 300;
    }

    .kpi-card {
        background: white;
        border-radius: 10px;
        padding: 1.1rem 1.4rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.06);
        border-top: 3px solid #2E75B6;
    }
    .kpi-value { font-size: 1.8rem; font-weight: 700; color: #1F4E79; line-height: 1; }
    .kpi-label { font-size: 0.74rem; color: #9CA3AF; margin-top: 0.25rem; font-weight: 500; text-transform: uppercase; letter-spacing: 0.05em; }

    .section-label { font-size: 0.72rem; font-weight: 600; color: #6B7280; text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 0.5rem; }

    .offre-card {
        background: white;
        border-radius: 10px;
        padding: 1.3rem 1.6rem;
        margin-bottom: 0.8rem;
        border-left: 4px solid #2E75B6;
        box-shadow: 0 1px 3px rgba(0,0,0,0.05);
        transition: all 0.15s ease;
    }
    .offre-card:hover { box-shadow: 0 4px 14px rgba(31,78,121,0.10); transform: translateY(-1px); }
    .offre-rank { font-size: 0.7rem; font-weight: 600; color: #2E75B6; text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 0.25rem; }
    .offre-titre { font-size: 1.05rem; font-weight: 700; color: #111827; margin-bottom: 0.15rem; }
    .offre-entreprise { font-size: 0.88rem; color: #4B5563; font-weight: 500; margin-bottom: 0.5rem; }

    .tag { display: inline-block; padding: 0.16rem 0.6rem; border-radius: 20px; font-size: 0.72rem; font-weight: 500; margin-right: 0.3rem; }
    .tag-remote   { background: #ECFDF5; color: #065F46; }
    .tag-office   { background: #EFF6FF; color: #1D4ED8; }
    .tag-source   { background: #F5F3FF; color: #5B21B6; }
    .tag-lieu     { background: #FFF7ED; color: #92400E; }
    .tag-salaire  { background: #F0FDF4; color: #14532D; }

    .score-value { font-size: 1.45rem; font-weight: 700; color: #1F4E79; text-align: right; line-height: 1; }
    .score-label { font-size: 0.7rem; color: #9CA3AF; text-align: right; margin-bottom: 0.15rem; }
    .progress-track { background: #E5E7EB; border-radius: 99px; height: 5px; margin-top: 0.35rem; }
    .progress-fill  { background: linear-gradient(90deg, #2E75B6, #1F4E79); border-radius: 99px; height: 5px; }

    .btn-offre {
        display: inline-block; margin-top: 0.8rem;
        padding: 0.38rem 1rem; background: #1F4E79; color: white !important;
        border-radius: 6px; font-size: 0.8rem; font-weight: 600;
        text-decoration: none; letter-spacing: 0.02em; transition: background 0.15s;
    }
    .btn-offre:hover { background: #2E75B6; }

    .stButton > button {
        background: #1F4E79; color: white; font-weight: 600;
        border-radius: 8px; border: none; padding: 0.55rem 2rem;
        font-size: 0.92rem; width: 100%; transition: background 0.15s;
    }
    .stButton > button:hover { background: #2E75B6; }

    .result-info {
        background: #EFF6FF; border: 1px solid #BFDBFE; border-radius: 8px;
        padding: 0.7rem 1.1rem; color: #1D4ED8; font-size: 0.85rem;
        font-weight: 500; margin-bottom: 1rem;
    }

    .app-footer {
        text-align: center; color: #D1D5DB; font-size: 0.76rem;
        margin-top: 3rem; padding-top: 1.2rem; border-top: 1px solid #E5E7EB;
    }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────
# CHARGEMENT DONNEES
# ─────────────────────────────────────────

@st.cache_data(ttl=300)
def charger_donnees():
    try:
        df = charger_offres_db()
        df = preparer_texte(df)
        return df, None
    except Exception as e:
        return pd.DataFrame(), str(e)

@st.cache_resource
def get_modele(_df):
    return construire_modele_tfidf(_df, forcer=False)

df, erreur = charger_donnees()

if erreur:
    st.error(f"Connexion impossible : {erreur}")
    st.stop()
if df.empty:
    st.error("Aucune donnee disponible.")
    st.stop()


# ─────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────

st.markdown("""
<div class="app-header">
    <h1>Job Intelligent</h1>
    <p>Plateforme de recommandation d'offres d'emploi dans le domaine de la Data</p>
</div>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────
# KPI
# ─────────────────────────────────────────

nb_offres  = len(df)
nb_remote  = int(df["remote"].sum()) if "remote" in df.columns else 0
nb_sources = df["source"].nunique()
derniere   = str(df["date_scraping"].max())[:10]

for col, val, lbl in zip(
    st.columns(4),
    [nb_offres, nb_remote, nb_sources, derniere],
    ["Offres disponibles", "Offres en remote", "Sources actives", "Derniere mise a jour"]
):
    with col:
        st.markdown(f'<div class="kpi-card"><div class="kpi-value">{val}</div><div class="kpi-label">{lbl}</div></div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)


# ─────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────

with st.sidebar:
    st.markdown("## Job Intelligent")
    st.markdown("---")

    st.markdown("##### Mode de profil")
    # ✅ FIX: label non vide + label_visibility="collapsed" pour le masquer visuellement
    mode_profil = st.radio("Mode de profil", ["Titre de poste", "Texte du CV"], label_visibility="collapsed")

    st.markdown("##### Algorithme")
    # ✅ FIX: label non vide + label_visibility="collapsed" pour le masquer visuellement
    mode_algo = st.radio("Algorithme", ["TF-IDF  —  Rapide", "NLP  —  Precis"], label_visibility="collapsed")

    st.markdown("---")
    st.markdown("##### Filtres")
    top_n         = st.slider("Nombre de resultats", 3, 20, 10)
    filtre_remote = st.selectbox("Teletravail", ["Toutes", "Remote uniquement", "Presentiel uniquement"])
    sources       = ["Toutes"] + sorted(df["source"].dropna().unique().tolist())
    filtre_source = st.selectbox("Source", sources)
    filtre_lieu   = st.text_input("Ville", placeholder="Ex : Paris, Lyon...")

    st.markdown("---")
    st.markdown("##### Offres par source")
    for src, cnt in df["source"].value_counts().items():
        pct = int(cnt / len(df) * 100)
        st.markdown(f"""
        <div style="display:flex;justify-content:space-between;margin-bottom:4px;">
            <span style="font-size:0.8rem;">{src}</span>
            <span style="font-size:0.8rem;font-weight:600;">{cnt}</span>
        </div>
        <div style="background:rgba(255,255,255,0.2);border-radius:99px;height:3px;margin-bottom:8px;">
            <div style="background:white;border-radius:99px;height:3px;width:{pct}%;"></div>
        </div>
        """, unsafe_allow_html=True)


# ─────────────────────────────────────────
# ZONE SAISIE
# ─────────────────────────────────────────

st.markdown('<div class="section-label">Votre profil</div>', unsafe_allow_html=True)

if mode_profil == "Titre de poste":
    col_input, col_btn = st.columns([5, 1])
    with col_input:
        profil_input = st.text_input(
            "Votre recherche",
            placeholder="Ex : Data Engineer Python Spark, Data Scientist NLP, Data Analyst SQL...",
            label_visibility="visible"
        )
    with col_btn:
        st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
        rechercher = st.button("Rechercher")

    # Suggestions
    st.markdown("<div style='margin-top:0.3rem;font-size:0.75rem;color:#9CA3AF;'>Suggestions :</div>", unsafe_allow_html=True)
    suggestions = ["Data Scientist", "Data Engineer", "Data Analyst", "ML Engineer", "BI Developer"]
    cols_sug = st.columns(len(suggestions))
    for i, sug in enumerate(suggestions):
        if cols_sug[i].button(sug, key=f"sug_{i}"):
            profil_input = sug
else:
    profil_input = st.text_area(
        "Votre CV",
        height=200,
        placeholder="Collez ici le texte de votre CV...\n\nExemple :\nIngenieur Data, 3 ans d'experience en Python, SQL, Spark, Airflow.\nCompetences : Docker, Kubernetes, AWS, GCP, TensorFlow.\nRecherche un poste de Data Engineer senior.",
        label_visibility="visible"
    )
    _, col_btn, _ = st.columns([1.5, 2, 1.5])
    with col_btn:
        rechercher = st.button("Rechercher")


# ─────────────────────────────────────────
# RESULTATS
# ─────────────────────────────────────────

if rechercher:
    if not profil_input or len(profil_input.strip()) < 3:
        st.warning("Veuillez entrer un titre de poste ou coller votre CV.")
    else:
        st.markdown("---")

        remote_filter = None
        if filtre_remote == "Remote uniquement":
            remote_filter = True
        elif filtre_remote == "Presentiel uniquement":
            remote_filter = False

        source_filter = None if filtre_source == "Toutes" else filtre_source
        lieu_filter   = filtre_lieu.strip() or None
        algo          = "tfidf" if "TF-IDF" in mode_algo else "nlp"

        with st.spinner("Analyse en cours..."):
            if algo == "tfidf":
                vectorizer, matrice = get_modele(df)
                resultats = recommander_tfidf(
                    profil_input, df, vectorizer, matrice,
                    top_n, lieu_filter, remote_filter, source_filter
                )
            else:
                resultats = recommander_nlp(
                    profil_input, df, top_n,
                    lieu_filter, remote_filter, source_filter
                )

        if resultats.empty:
            st.info("Aucune offre trouvee avec ces criteres. Essayez d'elargir les filtres.")
        else:
            algo_label = "TF-IDF" if algo == "tfidf" else "NLP Semantique"
            st.markdown(f'<div class="result-info">{len(resultats)} offres trouvees &nbsp;·&nbsp; {algo_label}</div>', unsafe_allow_html=True)

            tab1, tab2 = st.tabs(["Cartes", "Tableau"])

            with tab1:
                score_col = "score_similarite" if "score_similarite" in resultats.columns else "score"
                for rank, (_, row) in enumerate(resultats.iterrows(), 1):
                    score     = float(row.get(score_col, 0))
                    score_pct = min(int(score * 100), 100)
                    remote    = row.get("remote", False)
                    lieu      = row.get("lieu", "") or "Non precise"
                    source    = row.get("source", "N/A")
                    lien      = row.get("lien", "#")
                    titre     = row.get("titre", "N/A")
                    entreprise= row.get("entreprise", "N/A")
                    date_s    = str(row.get("date_scraping", ""))[:10]

                    sal_min = row.get("salaire_min")
                    sal_max = row.get("salaire_max")
                    salaire_tag = ""
                    if pd.notna(sal_min) and pd.notna(sal_max) and sal_min and sal_max:
                        salaire_tag = f'<span class="tag tag-salaire">{int(sal_min):,} - {int(sal_max):,} EUR</span>'

                    remote_tag = '<span class="tag tag-remote">Remote</span>' if remote else '<span class="tag tag-office">Presentiel</span>'

                    col_info, col_score = st.columns([5, 1])
                    with col_info:
                        html_card = (
                            '<div class="offre-card">'
                            f'<div class="offre-rank">Offre #{rank:02d} &nbsp;&middot;&nbsp; {date_s}</div>'
                            f'<div class="offre-titre">{titre}</div>'
                            f'<div class="offre-entreprise">{entreprise}</div>'
                            '<div style="margin-top:0.4rem;">'
                            f'<span class="tag tag-lieu">{lieu}</span>'
                            f'{remote_tag}'
                            f'<span class="tag tag-source">{source}</span>'
                            f'{salaire_tag}'
                            '</div>'
                            '<div style="margin-top:0.8rem;">'
                            f'<a href="{lien}" target="_blank" style="display:inline-block;padding:0.35rem 1rem;background:#1F4E79;color:white;border-radius:6px;font-size:0.8rem;font-weight:600;text-decoration:none;">Voir l\'offre</a>'
                            '</div>'
                            '</div>'
                        )
                        st.markdown(html_card, unsafe_allow_html=True)
                    with col_score:
                        st.markdown(f"""
                        <div style="padding-top:1.4rem;">
                            <div class="score-label">Score</div>
                            <div class="score-value">{score:.3f}</div>
                            <div class="progress-track">
                                <div class="progress-fill" style="width:{score_pct}%;"></div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)

            with tab2:
                score_col = "score_similarite" if "score_similarite" in resultats.columns else "score"
                cols_show = [c for c in ["titre", "entreprise", "lieu", "remote", "source", "date_scraping", score_col] if c in resultats.columns]
                df_display = resultats[cols_show].copy()
                if score_col in df_display.columns:
                    df_display[score_col] = df_display[score_col].round(4)
                st.dataframe(
                    df_display.rename(columns={
                        "titre": "Titre", "entreprise": "Entreprise", "lieu": "Ville",
                        "remote": "Remote", "source": "Source",
                        "date_scraping": "Date", score_col: "Score"
                    }),
                    # ✅ FIX: use_container_width remplacé par width="stretch"
                    width="stretch", hide_index=True
                )
                csv = resultats.to_csv(index=False).encode("utf-8")
                st.download_button("Telecharger les resultats (CSV)", csv, "resultats.csv", "text/csv")


# ─────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────

st.markdown("""
<div class="app-footer">
    Job Intelligent &nbsp;|&nbsp; Scraping · PostgreSQL · TF-IDF · NLP Semantique · PowerBI &nbsp;|&nbsp; 2026
</div>
""", unsafe_allow_html=True)