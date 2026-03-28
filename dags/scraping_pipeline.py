from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os


sys.path.insert(0, '/opt/airflow/scrapers')

from indeed import scraper_indeed
from glassdoor import scraper_glassdoor
from cadremploi import scraper_cadremploi

default_args = {
    'owner': 'job_intelligent',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1)
}

# ─────────────────────────────────────────
# TÂCHES SCRAPING
# ─────────────────────────────────────────

def scraper_indeed_task():
    offres = scraper_indeed("data scientist")
    print(f"✅ Indeed : {len(offres)} offres récupérées")
    return len(offres)

def scraper_glassdoor_task():
    offres = scraper_glassdoor("data scientist")
    print(f"✅ Glassdoor : {len(offres)} offres récupérées")
    return len(offres)

def scraper_cadremploi_task():
    offres = scraper_cadremploi("data scientist")
    print(f"✅ Arbeitnow : {len(offres)} offres récupérées")
    return len(offres)

# ─────────────────────────────────────────
# TÂCHE FUSION CSV
# ─────────────────────────────────────────

def fusionner_offres():
    import pandas as pd

    fichiers = [
        "/opt/airflow/data/offres_indeed.csv",
        "/opt/airflow/data/offres_glassdoor.csv",
        "/opt/airflow/data/offres_cadremploi.csv"
    ]

    dfs = []
    for fichier in fichiers:
        if os.path.exists(fichier):
            df = pd.read_csv(fichier)
            dfs.append(df)
            print(f"📂 {fichier} : {len(df)} offres")
        else:
            print(f"⚠️ Fichier manquant : {fichier}")

    if dfs:
        df_final = pd.concat(dfs, ignore_index=True)
        df_final = df_final.drop_duplicates(subset=["titre", "entreprise"])
        chemin_final = "/opt/airflow/data/offres_all.csv"
        df_final.to_csv(chemin_final, index=False, encoding="utf-8")
        print(f"💾 Total fusionné : {len(df_final)} offres uniques")
        return len(df_final)
    else:
        raise Exception("❌ Aucun fichier CSV trouvé — scraping a échoué")

# ─────────────────────────────────────────
# TÂCHE SAVE TO DB
# ─────────────────────────────────────────

def sauvegarder_en_db():
    import pandas as pd
    import psycopg2

    DB_CONFIG = {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "database": os.getenv("POSTGRES_DB", "airflow"),
        "user": os.getenv("POSTGRES_USER", "airflow"),
        "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
        "port": int(os.getenv("POSTGRES_PORT", 5432))
    }

    fichier = "/opt/airflow/data/offres_all.csv"
    if not os.path.exists(fichier):
        raise FileNotFoundError("❌ offres_all.csv introuvable !")

    df = pd.read_csv(fichier)
    df = df.where(pd.notnull(df), None)
    print(f"📂 {len(df)} offres à insérer...")

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Créer la table si elle n'existe pas
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS offres_emploi (
            id SERIAL PRIMARY KEY,
            titre VARCHAR(500),
            entreprise VARCHAR(500),
            lieu VARCHAR(500),
            salaire_min FLOAT,
            salaire_max FLOAT,
            remote BOOLEAN,
            description TEXT,
            lien TEXT,
            tags VARCHAR(500),
            date_scraping DATE,
            source VARCHAR(100),
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(titre, entreprise, source)
        )
    """)
    conn.commit()

    inseres = 0
    ignores = 0
    erreurs = 0

    for _, row in df.iterrows():
        try:
            # Corriger salaire : "N/A" → None
            sal_min = row.get("salaire_min")
            sal_max = row.get("salaire_max")
            sal_min = None if str(sal_min) in ["N/A", "nan", "None", ""] else sal_min
            sal_max = None if str(sal_max) in ["N/A", "nan", "None", ""] else sal_max

            cursor.execute("""
                INSERT INTO offres_emploi 
                (titre, entreprise, lieu, salaire_min, salaire_max,
                 remote, description, lien, tags, date_scraping, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (titre, entreprise, source) DO NOTHING
            """, (
                row.get("titre"),
                row.get("entreprise"),
                row.get("lieu"),
                sal_min,
                sal_max,
                row.get("remote"),
                row.get("description"),
                row.get("lien"),
                row.get("tags"),
                row.get("date_scraping"),
                row.get("source")
            ))

            if cursor.rowcount > 0:
                inseres += 1
            else:
                ignores += 1

        except Exception as e:
            print(f"⚠️ Erreur ligne : {e}")
            erreurs += 1
            conn.rollback()
            continue

    conn.commit()
    cursor.close()
    conn.close()

    print(f"✅ {inseres} insérées | ⏭️ {ignores} doublons ignorés | ❌ {erreurs} erreurs")
    return inseres

# ─────────────────────────────────────────
# TÂCHE RAPPORT FINAL
# ─────────────────────────────────────────

def rapport_final(**context):
    ti = context['ti']

    n_indeed    = ti.xcom_pull(task_ids='scrape_indeed')    or 0
    n_glassdoor = ti.xcom_pull(task_ids='scrape_glassdoor') or 0
    n_arbeitnow = ti.xcom_pull(task_ids='scrape_arbeitnow') or 0
    n_fusion    = ti.xcom_pull(task_ids='fusionner_offres') or 0
    n_inseres   = ti.xcom_pull(task_ids='sauvegarder_db')   or 0

    rapport = f"""
    ╔══════════════════════════════════════╗
    ║       RAPPORT PIPELINE - {datetime.now().strftime('%d/%m/%Y')}   ║
    ╠══════════════════════════════════════╣
    ║  Indeed    : {str(n_indeed).rjust(5)} offres              ║
    ║  Glassdoor : {str(n_glassdoor).rjust(5)} offres              ║
    ║  Arbeitnow : {str(n_arbeitnow).rjust(5)} offres              ║
    ╠══════════════════════════════════════╣
    ║  Après fusion (uniques) : {str(n_fusion).rjust(5)}        ║
    ║  Insérées en DB         : {str(n_inseres).rjust(5)}        ║
    ╚══════════════════════════════════════╝
    """
    print(rapport)

# ─────────────────────────────────────────
# DÉFINITION DU DAG
# ─────────────────────────────────────────

with DAG(
    dag_id='scraping_pipeline',
    default_args=default_args,
    description='Scrape les offres data chaque jour à 8h',
    schedule_interval='0 8 * * *',
    catchup=False,
    tags=['scraping', 'data', 'emploi']
) as dag:

    task_indeed = PythonOperator(
        task_id='scrape_indeed',
        python_callable=scraper_indeed_task
    )

    task_glassdoor = PythonOperator(
        task_id='scrape_glassdoor',
        python_callable=scraper_glassdoor_task
    )

    task_cadremploi = PythonOperator(
        task_id='scrape_arbeitnow',
        python_callable=scraper_cadremploi_task
    )

    task_fusion = PythonOperator(
        task_id='fusionner_offres',
        python_callable=fusionner_offres
    )

    task_save_db = PythonOperator(
        task_id='sauvegarder_db',
        python_callable=sauvegarder_en_db
    )

    task_rapport = PythonOperator(
        task_id='rapport_final',
        python_callable=rapport_final,
        provide_context=True
    )

    # ─── DÉPENDANCES ───────────────────────
    # 3 scrapers en parallèle → fusion → DB → rapport
    [task_indeed, task_glassdoor, task_cadremploi] >> task_fusion >> task_save_db >> task_rapport