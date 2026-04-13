"""
save_to_db.py — conservé pour compatibilité
La logique DB est maintenant gérée par fusion.py
"""
import os
import pandas as pd
import psycopg2

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "postgres"),
    "database": os.getenv("POSTGRES_DB", "airflow"),
    "user":     os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
}

def creer_table():
    pass  # géré par fusion.py

def sauvegarder_en_db():
    pass  # géré par fusion.py