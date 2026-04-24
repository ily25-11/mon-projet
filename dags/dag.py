"""
DAG Airflow — Pipeline scraping offres multi-domaines v2
=========================================================
Sources :
  - France Travail API  (officiel, gratuit, ~2 000-5 000 offres/jour)
  - Arbeitnow           (gratuit, international, ~200 offres/jour)
  - Adzuna API          (gratuit, tous secteurs, ~1 000-3 000 offres/jour)
  - The Muse API        (gratuit sans clé, ~1 000-3 000 offres/jour)
  - JSearch RapidAPI    (payant, optionnel)

Variables d'environnement à ajouter dans docker-compose.yml :
  FT_CLIENT_ID=<votre client_id France Travail>
  FT_CLIENT_SECRET=<votre client_secret France Travail>
  ADZUNA_APP_ID=<votre app_id Adzuna>
  ADZUNA_APP_KEY=<votre app_key Adzuna>
  RAPIDAPI_KEY=<votre clé JSearch — optionnel>
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, "/opt/airflow/scrapers")

from france_travail import scraper_france_travail_task
from cadremploi    import scraper_cadremploi
from jsearch       import scraper_jsearch_multi_postes
from adzuna        import scraper_adzuna_task
from themuse       import scraper_themuse_task
from fusion        import fusionner_offres_task, sauvegarder_en_db_task
from transform import transformer_offres_task


default_args = {
    "owner":       "job_intelligent",
    "retries":     2,
    "retry_delay": timedelta(minutes=5),
    "start_date":  datetime(2024, 1, 1),
}

# ─── TÂCHES SCRAPING ──────────────────────────────────────────────────────────

def task_france_travail():
    n = scraper_france_travail_task()
    print(f"France Travail : {n} offres")
    return n


def task_arbeitnow():
    """Arbeitnow / Cadremploi — source internationale sans clé API."""
    from cadremploi import scraper_cadremploi
    import pandas as pd

    domaines = [
    "data scientist", "data engineer", "data analyst",
    "machine learning engineer", "MLOps", "AI engineer",
    "software engineer", "devops", "cloud engineer",
    "fullstack developer", "backend developer", "cybersecurity",
    ]
    toutes = []
    for d in domaines:
        offres = scraper_cadremploi(d)
        toutes.extend(offres)

    if toutes:
        df = pd.DataFrame(toutes)
        df.drop_duplicates(subset=["titre", "entreprise"], keep="first", inplace=True)
        chemin = "/opt/airflow/data/offres_cadremploi.csv"
        df.to_csv(chemin, index=False, encoding="utf-8")
        print(f"Arbeitnow : {len(df)} offres sauvegardées")
        return len(df)
    return 0


def task_adzuna():
    n = scraper_adzuna_task()
    print(f"Adzuna : {n} offres")
    return n


def task_themuse():
    n = scraper_themuse_task()
    print(f"The Muse : {n} offres")
    return n


def task_jsearch():
    offres = scraper_jsearch_multi_postes(pages_max=5)
    return len(offres)


# ─── FUSION & DB ──────────────────────────────────────────────────────────────

def task_fusion():
    n = fusionner_offres_task()
    print(f"Fusion : {n} offres uniques")
    return n


def task_db():
    n = sauvegarder_en_db_task()
    print(f"DB : {n} offres insérées/mises à jour")
    return n
# ─── TRANSFORMATION ──────────────────────────────────────────────────────────────
def task_transform():
    n = transformer_offres_task()
    print(f"Transform : {n} offres nettoyées")
    return n


# ─── RAPPORT FINAL ────────────────────────────────────────────────────────────

def rapport_final(**context):
    ti = context["ti"]

    n_ft      = ti.xcom_pull(task_ids="scrape_france_travail") or 0
    n_arbeit  = ti.xcom_pull(task_ids="scrape_arbeitnow")      or 0
    n_adzuna  = ti.xcom_pull(task_ids="scrape_adzuna")         or 0
    n_themuse = ti.xcom_pull(task_ids="scrape_themuse")        or 0
    n_jsearch = ti.xcom_pull(task_ids="scrape_jsearch")        or 0
    n_fusion  = ti.xcom_pull(task_ids="fusionner_offres")      or 0
    n_db      = ti.xcom_pull(task_ids="sauvegarder_db")        or 0

    total_brut = n_ft + n_arbeit + n_adzuna + n_themuse + n_jsearch

    rapport = f"""
    ╔══════════════════════════════════════════════════╗
    ║   RAPPORT PIPELINE — {datetime.now().strftime('%d/%m/%Y %H:%M')}         ║
    ╠══════════════════════════════════════════════════╣
    ║  France Travail  : {str(n_ft).rjust(6)} offres               ║
    ║  Arbeitnow       : {str(n_arbeit).rjust(6)} offres               ║
    ║  Adzuna          : {str(n_adzuna).rjust(6)} offres               ║
    ║  The Muse        : {str(n_themuse).rjust(6)} offres               ║
    ║  JSearch         : {str(n_jsearch).rjust(6)} offres               ║
    ╠══════════════════════════════════════════════════╣
    ║  Total brut      : {str(total_brut).rjust(6)} offres               ║
    ║  Après fusion    : {str(n_fusion).rjust(6)} offres uniques        ║
    ║  Insérées en DB  : {str(n_db).rjust(6)}                       ║
    ╚══════════════════════════════════════════════════╝
    """
    print(rapport)
    return n_db


# ─── DÉFINITION DU DAG ────────────────────────────────────────────────────────

with DAG(
    dag_id="scraping_pipeline_v2",
    default_args=default_args,
    description="Scrape les offres tous domaines chaque jour à 7h",
    schedule_interval="0 7 * * *",
    catchup=False,
    tags=["scraping", "multi-domaines", "emploi"],
) as dag:

    t_ft = PythonOperator(
        task_id="scrape_france_travail",
        python_callable=task_france_travail,
    )
    t_arbeit = PythonOperator(
        task_id="scrape_arbeitnow",
        python_callable=task_arbeitnow,
    )
    t_adzuna = PythonOperator(
        task_id="scrape_adzuna",
        python_callable=task_adzuna,
    )
    t_themuse = PythonOperator(
        task_id="scrape_themuse",
        python_callable=task_themuse,
    )
    t_jsearch = PythonOperator(
        task_id="scrape_jsearch",
        python_callable=task_jsearch,
    )
    t_fusion = PythonOperator(
        task_id="fusionner_offres",
        python_callable=task_fusion,
    )
    t_transform = PythonOperator(
    task_id="transformer_offres",
    python_callable=task_transform,
)
    t_db = PythonOperator(
        task_id="sauvegarder_db",
        python_callable=task_db,
    )
    t_rapport = PythonOperator(
        task_id="rapport_final",
        python_callable=rapport_final,
        provide_context=True,
    )

    # 5 scrapers en parallèle → fusion → DB → rapport
    [t_ft, t_arbeit, t_adzuna, t_themuse, t_jsearch] >> t_fusion >> t_transform >> t_db >> t_rapport