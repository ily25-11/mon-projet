from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
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

def fusionner_offres():
    import pandas as pd
    import os

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

    if dfs:
        df_final = pd.concat(dfs, ignore_index=True)

        # Supprimer les doublons par titre + entreprise
        df_final = df_final.drop_duplicates(subset=["titre", "entreprise"])

        chemin_final = "/opt/airflow/data/offres_all.csv"
        df_final.to_csv(chemin_final, index=False, encoding="utf-8")
        print(f"💾 Total fusionné : {len(df_final)} offres uniques")
        return len(df_final)
    else:
        print("❌ Aucun fichier trouvé")
        return 0

with DAG(
    dag_id='scraping_pipeline',
    default_args=default_args,
    description='Scrape les offres data chaque jour à 8h',
    schedule_interval='0 8 * * *',  # Tous les jours à 8h
    catchup=False,
    tags=['scraping', 'data', 'emploi']
) as dag:

    # Task 1 : Scraper Indeed
    task_indeed = PythonOperator(
        task_id='scrape_indeed',
        python_callable=scraper_indeed_task
    )

    # Task 2 : Scraper Glassdoor
    task_glassdoor = PythonOperator(
        task_id='scrape_glassdoor',
        python_callable=scraper_glassdoor_task
    )

    # Task 3 : Scraper Arbeitnow
    task_cadremploi = PythonOperator(
        task_id='scrape_arbeitnow',
        python_callable=scraper_cadremploi_task
    )

    # Task 4 : Fusionner tous les CSV
    task_fusion = PythonOperator(
        task_id='fusionner_offres',
        python_callable=fusionner_offres
    )

    # Indeed + Glassdoor + Arbeitnow en parallèle → puis