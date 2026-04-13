"""
transform.py — Couche Transformation ETL
==========================================
Étape T du pipeline ETL :
  1. Normalisation des titres de postes
  2. Extraction des compétences depuis la description
  3. Standardisation des villes / régions
  4. Enrichissement des salaires manquants (médiane par secteur)
  5. Détection du type de contrat
  6. Nettoyage général des données

À placer dans : /opt/airflow/scrapers/transform.py
Position dans le DAG : fusion >> transform >> sauvegarder_db
"""

import os
import re
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────────────────────────────────

OUTPUT_DIR = "/opt/airflow/data"
FICHIER_FUSIONNE = os.path.join(OUTPUT_DIR, "offres_all.csv")
FICHIER_TRANSFORME = os.path.join(OUTPUT_DIR, "offres_transformees.csv")

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "postgres"),
    "database": os.getenv("POSTGRES_DB",   "airflow"),
    "user":     os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
}

# ─── 1. NORMALISATION DES TITRES ──────────────────────────────────────────────

# Dictionnaire : pattern regex → titre normalisé
TITRE_MAPPING = {
    # Data / IA
    r"data\s*scien(tist|ce)":          "Data Scientist",
    r"data\s*engineer|ingénieur\s*data":"Data Engineer",
    r"data\s*anal(yst|yste)":          "Data Analyst",
    r"machine\s*learning|ml\s*engineer":"ML Engineer",
    r"deep\s*learning|ai\s*engineer":  "AI Engineer",
    r"data\s*architect":               "Data Architect",
    r"bi\s*(developer|analyst|dev)":   "BI Developer",
    r"business\s*intel":               "Business Intelligence",
    r"nlp|natural\s*language":         "NLP Engineer",
    r"computer\s*vision":              "Computer Vision Engineer",
    r"mlops|data\s*ops":               "MLOps Engineer",
    # Développement
    r"développeur|developer|dev\s*(full.?stack|backend|frontend)": "Développeur",
    r"full.?stack":                    "Développeur Full Stack",
    r"backend|back.end":               "Développeur Backend",
    r"frontend|front.end":             "Développeur Frontend",
    r"devops|site\s*reliability":      "DevOps Engineer",
    r"cloud\s*engineer|architecte\s*cloud": "Cloud Engineer",
    r"cybersécurité|security\s*engineer|soc\s*analyst": "Cybersécurité",
    # Finance
    r"comptabl|accountant":            "Comptable",
    r"contrôleur\s*de\s*gestion|controller": "Contrôleur de Gestion",
    r"analyste\s*financ|financial\s*analyst": "Analyste Financier",
    r"audit(eur|or)":                  "Auditeur",
    r"trésor(ier|y)":                  "Trésorier",
    r"risk\s*(analyst|manager)":       "Risk Manager",
    # RH
    r"ressources\s*humaines|human\s*resources|rh\s*généraliste": "Chargé RH",
    r"recruteur|talent\s*acquisition|recruiter": "Recruteur",
    r"drh|directeur\s*(rh|ressources)": "DRH",
    r"paie|payroll":                   "Gestionnaire Paie",
    # Marketing / Com
    r"chef\s*de\s*produit|product\s*manager": "Product Manager",
    r"marketing\s*manager|responsable\s*marketing": "Responsable Marketing",
    r"community\s*manager|social\s*media": "Community Manager",
    r"seo|sem|growth\s*hacker":        "Growth / SEO",
    r"chef\s*de\s*projet\s*digital|digital\s*project": "Chef de Projet Digital",
    # Commercial
    r"commercial|sales\s*(manager|rep|executive)": "Commercial",
    r"business\s*develop|développement\s*commercial": "Business Developer",
    r"account\s*manager|gestionnaire\s*de\s*compte": "Account Manager",
    # Santé
    r"infirm(ier|ière)":               "Infirmier",
    r"médecin|physician|doctor":       "Médecin",
    r"pharmacien":                     "Pharmacien",
    r"aide.soignant":                  "Aide-Soignant",
    r"kinésithér":                     "Kinésithérapeute",
    # Ingénierie
    r"ingénieur\s*(mécani|producti|qualité|process)": "Ingénieur Industriel",
    r"ingénieur\s*(électri|électroni)": "Ingénieur Électronique",
    r"chef\s*de\s*projet\s*(technique|infra|si)": "Chef de Projet Technique",
    r"architecte\s*(logiciel|solution|si)": "Architecte Logiciel",
    # Logistique
    r"logisti(cien|que\s*manager)":    "Logisticien",
    r"supply\s*chain":                 "Supply Chain Manager",
    r"chef\s*de\s*projet\s*logisti":   "Chef de Projet Logistique",
    r"responsable\s*(entrepôt|warehouse)": "Responsable Entrepôt",
    # Juridique
    r"juriste|legal\s*(counsel|officer)": "Juriste",
    r"avocat|attorney|lawyer":         "Avocat",
    r"paralégal|paralegal":            "Parajuriste",
    # BTP
    r"conducteur\s*de\s*travaux":      "Conducteur de Travaux",
    r"chef\s*de\s*chantier":           "Chef de Chantier",
    r"architecte(?!\s*(logiciel|cloud|solution|si))": "Architecte",
    r"géomètre":                       "Géomètre",
    # Management
    r"directeur\s*(général|opérations|technique)|ceo|cto|coo": "Directeur",
    r"manager|responsable\s*d[e']\s*(département|service|pôle)": "Manager",
}


def normaliser_titre(titre: str) -> str:
    """Retourne un titre de poste normalisé depuis le titre brut."""
    if not isinstance(titre, str) or not titre.strip():
        return "Autre"

    t = titre.lower()
    # Supprimer suffixes parasites : H/F, (h/f), Senior, Junior, Confirmé, CDI...
    t = re.sub(r"\b(h/f|f/h|m/f|senior|junior|confirmé|exp[eé]riment[eé]|"
               r"stage|alternance|apprentissage|cdi|cdd|freelance|remote|"
               r"télétravail|\d+\s*(ans?|years?))\b", "", t)
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()

    for pattern, label in TITRE_MAPPING.items():
        if re.search(pattern, t, re.IGNORECASE):
            return label

    # Capitaliser ce qu'on n'a pas reconnu (max 50 chars)
    return titre.strip()[:50].title()


# ─── 2. EXTRACTION DES COMPÉTENCES ────────────────────────────────────────────

COMPETENCES = {
    # Langages
    "Python", "R", "SQL", "Java", "Scala", "JavaScript", "TypeScript",
    "C++", "C#", "Go", "Rust", "Julia", "MATLAB", "SAS", "Bash",
    # Data / ML
    "Pandas", "NumPy", "Scikit-learn", "TensorFlow", "PyTorch", "Keras",
    "XGBoost", "LightGBM", "Spark", "PySpark", "Hadoop", "Kafka",
    "Airflow", "dbt", "MLflow", "Hugging Face",
    # BI / Viz
    "Power BI", "Tableau", "Looker", "Qlik", "Metabase", "Grafana",
    # Cloud / Infra
    "AWS", "Azure", "GCP", "Docker", "Kubernetes", "Terraform",
    "Databricks", "Snowflake", "BigQuery", "Redshift",
    # Bases de données
    "PostgreSQL", "MySQL", "MongoDB", "Elasticsearch", "Redis",
    "Cassandra", "Oracle", "SQL Server",
    # Outils
    "Git", "GitHub", "GitLab", "Jira", "Confluence", "Notion",
    "Excel", "SAP", "Salesforce", "HubSpot",
    # Soft skills
    "leadership", "communication", "autonomie", "rigueur", "gestion de projet",
    "management", "négociation", "esprit d'analyse", "travail en équipe",
}

COMP_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(c) for c in sorted(COMPETENCES, key=len, reverse=True)) + r")\b",
    re.IGNORECASE
)


def extraire_competences(description: str) -> str:
    """Extrait les compétences reconnues depuis la description, retourne string CSV."""
    if not isinstance(description, str):
        return ""
    matches = set(COMP_PATTERN.findall(description))
    # Normaliser la casse : garder la version du dictionnaire
    comp_dict = {c.lower(): c for c in COMPETENCES}
    result = sorted({comp_dict.get(m.lower(), m) for m in matches})
    return ", ".join(result[:15])  # max 15 compétences


# ─── 3. STANDARDISATION DES VILLES / RÉGIONS ──────────────────────────────────

REGION_MAPPING = {
    # Île-de-France
    r"paris|75\d{3}|île.de.france|idf|versailles|boulogne|neuilly|"
    r"saint.cloud|levallois|issy|puteaux|courbevoie|nanterre|la.défense": "Île-de-France",
    # Auvergne-Rhône-Alpes
    r"lyon|grenoble|clermont|saint.etienne|annecy|chambéry|valence|"
    r"rhône.alpes|auvergne": "Auvergne-Rhône-Alpes",
    # Occitanie
    r"toulouse|montpellier|nîmes|perpignan|occitanie": "Occitanie",
    # Nouvelle-Aquitaine
    r"bordeaux|pau|limoges|poitiers|nouvelle.aquitaine": "Nouvelle-Aquitaine",
    # PACA
    r"marseille|nice|toulon|aix.en.provence|paca|provence|côte.d.azur": "Provence-Alpes-Côte d'Azur",
    # Hauts-de-France
    r"lille|amiens|roubaix|tourcoing|hauts.de.france": "Hauts-de-France",
    # Grand Est
    r"strasbourg|metz|nancy|reims|mulhouse|grand.est|alsace|lorraine|champagne": "Grand Est",
    # Pays de la Loire
    r"nantes|angers|le.mans|saint.nazaire|pays.de.la.loire": "Pays de la Loire",
    # Bretagne
    r"rennes|brest|quimper|lorient|bretagne": "Bretagne",
    # Normandie
    r"rouen|caen|le.havre|normandie": "Normandie",
    # Bourgogne-Franche-Comté
    r"dijon|besançon|bourgogne|franche.comté": "Bourgogne-Franche-Comté",
    # Centre-Val de Loire
    r"tours|orléans|centre.val.de.loire": "Centre-Val de Loire",
    # International
    r"london|uk|united.kingdom|england":  "Royaume-Uni",
    r"berlin|munich|germany|deutschland": "Allemagne",
    r"new.york|san.francisco|usa|united.states|remote.*us": "États-Unis",
    r"barcelona|madrid|spain|españa":     "Espagne",
    r"amsterdam|netherlands|holland":     "Pays-Bas",
    r"remote|télétravail|full.remote":    "Remote",
}


def standardiser_region(lieu: str, pays: str = None) -> str:
    """Déduit la région française ou le pays depuis le champ lieu."""
    if not isinstance(lieu, str) or not lieu.strip():
        return "Non renseigné"

    l = lieu.lower()
    for pattern, region in REGION_MAPPING.items():
        if re.search(pattern, l, re.IGNORECASE):
            return region

    # Fallback : retourner le lieu nettoyé (50 chars max)
    return lieu.strip()[:50].title()


def standardiser_pays(lieu: str, pays_original: str = None) -> str:
    """Détermine le pays depuis le lieu ou le champ pays existant."""
    if isinstance(pays_original, str) and pays_original.strip():
        p = pays_original.strip().lower()
        if "france" in p or "fr" == p:
            return "France"
        return pays_original.strip().title()[:50]

    if not isinstance(lieu, str):
        return "France"  # défaut

    l = lieu.lower()
    if re.search(r"france|paris|lyon|marseille|bordeaux|toulouse|nantes|lille|"
                 r"strasbourg|rennes|nice|montpellier|grenoble|7[0-9]\d{3}|"
                 r"6[0-9]\d{3}|1[0-9]\d{3}|3[0-9]\d{3}|44\d{3}|13\d{3}", l):
        return "France"
    if re.search(r"uk|london|england|scotland|wales|britain", l):
        return "Royaume-Uni"
    if re.search(r"germany|berlin|munich|deutschland|hamburg", l):
        return "Allemagne"
    if re.search(r"usa|new york|california|san francisco|united states|chicago", l):
        return "États-Unis"
    if re.search(r"remote|télétravail", l):
        return "Remote"
    return "France"  # défaut raisonnable pour sources FR


# ─── 4. DÉTECTION TYPE DE CONTRAT ─────────────────────────────────────────────
def detecter_contrat(titre: str, description: str, tags: str) -> str:
    """Détecte le type de contrat depuis le titre, description ou tags."""
    texte = " ".join([
        str(titre or ""),
        str(description or "")[:500],
        str(tags or "")
    ]).lower()

    # Stage en premier (priorité haute)
    if re.search(r"\bstage\b|internship|stagiaire|\bintern\b", texte):
        return "Stage"

    # Alternance
    if re.search(r"alternance|apprentissage|apprenti|apprenticeship", texte):
        return "Alternance"

    # Freelance — codes sources inclus
    if re.search(r"\bfreelance\b|freelancer|indépendant|consultant\s*indépendant"
                 r"|\bexternal\b|\bfra\b|\blib\b", texte):
        return "Freelance"

    # CDD — codes sources inclus
    if re.search(r"\bcdd\b|contract\s*(fix|term)|temporary|temporaire"
                 r"|\bcontract\b|\bcontractor\b", texte):
        return "CDD"

    # CDI — codes sources inclus
    if re.search(r"\bcdi\b|permanent|indéterminée|perma"
                 r"|full.time|full_time", texte):
        return "CDI"

    # Temps partiel
    if re.search(r"part.time|temps\s*partiel|mi.temps", texte):
        return "Temps partiel"

    return "CDI"  # défaut le plus fréquent
# ─── 5. ENRICHISSEMENT SALAIRES ───────────────────────────────────────────────

def enrichir_salaires(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule salaire_annuel_estime si manquant.
    Logique :
      1. Si salaire_min et salaire_max présents → moyenne
      2. Si seulement l'un des deux → celui-là
      3. Si rien → médiane du (secteur + contrat_type)
      4. Si secteur inconnu → médiane globale
    Filtre les valeurs aberrantes : < 8 000 ou > 500 000 → NaN
    """
    def estimer(row):
        s_min = row.get("salaire_min")
        s_max = row.get("salaire_max")
        actuel = row.get("salaire_annuel_estime")

        # Déjà calculé et valide
        if pd.notna(actuel) and 8_000 < actuel < 500_000:
            return actuel

        # Calculer depuis min/max
        valeurs = []
        for v in [s_min, s_max]:
            try:
                v = float(v)
                if 8_000 < v < 500_000:
                    valeurs.append(v)
            except (TypeError, ValueError):
                pass

        if valeurs:
            return np.mean(valeurs)
        return np.nan

    df["salaire_annuel_estime"] = df.apply(estimer, axis=1)

    # Médiane par secteur + type contrat pour les NaN restants
    mediane_groupe = (
        df[df["salaire_annuel_estime"].notna()]
        .groupby(["secteur", "contrat_type"])["salaire_annuel_estime"]
        .median()
    )
    mediane_globale = df["salaire_annuel_estime"].median()

    def fallback_mediane(row):
        if pd.notna(row["salaire_annuel_estime"]):
            return row["salaire_annuel_estime"]
        key = (row["secteur"], row["contrat_type"])
        return mediane_groupe.get(key, mediane_globale)

    df["salaire_annuel_estime"] = df.apply(fallback_mediane, axis=1)
    return df


# ─── 6. FONCTION PRINCIPALE ───────────────────────────────────────────────────

def transformer_offres_task() -> int:
    """
    Charge offres_all.csv, applique toutes les transformations,
    sauvegarde offres_transformees.csv.
    Retourne le nombre d'offres transformées.
    """
    print("🔄 Démarrage transformation ETL...")

    if not os.path.exists(FICHIER_FUSIONNE):
        print(f"❌ Fichier introuvable : {FICHIER_FUSIONNE}")
        return 0

    df = pd.read_csv(FICHIER_FUSIONNE, low_memory=False)
    print(f"📂 {len(df)} offres chargées depuis {FICHIER_FUSIONNE}")

    # ── 1. Titre normalisé
    print("  ✏️  Normalisation des titres...")
    df["titre_normalise"] = df["titre"].apply(normaliser_titre)

    # ── 2. Compétences extraites
    print("  🔍 Extraction des compétences...")
    df["competences_extraites"] = df["description"].apply(extraire_competences)

    # ── 3. Région standardisée
    print("  🗺️  Standardisation des régions...")
    df["region"] = df.apply(
        lambda r: standardiser_region(r.get("lieu", ""), r.get("region", "")),
        axis=1
    )

    # ── 4. Pays standardisé
    print("  🌍 Standardisation des pays...")
    df["pays"] = df.apply(
        lambda r: standardiser_pays(r.get("lieu", ""), r.get("pays", "")),
        axis=1
    )

    # ── 5. Type de contrat
    print("  📄 Détection des types de contrat...")
    df["contrat_type"] = df.apply(
        lambda r: detecter_contrat(r.get("titre", ""), r.get("description", ""), r.get("tags", "")),
        axis=1
    )

    # ── 6. Salaires enrichis
    print("  💰 Enrichissement des salaires...")
    df = enrichir_salaires(df)

    # ── 7. Nettoyage général
    print("  🧹 Nettoyage général...")
    # Supprimer les titres vides
    df = df[df["titre"].notna() & (df["titre"].str.strip() != "")]
    # Tronquer les champs longs
    df["titre"]    = df["titre"].str[:500]
    df["lieu"]     = df["lieu"].fillna("").str[:300]
    df["tags"]     = df["tags"].fillna("").str[:500]
    # Date de scraping en format date
    df["date_scraping"] = pd.to_datetime(df["date_scraping"], errors="coerce").dt.date
    df["date_scraping"] = df["date_scraping"].fillna(datetime.now().date())
    # Remote en booléen propre
    df["remote"] = df["remote"].apply(
        lambda x: True if str(x).lower() in ["true", "1", "yes", "oui", "remote"] else False
    )

    # ── 8. Sauvegarde
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(FICHIER_TRANSFORME, index=False, encoding="utf-8")

    print(f"✅ Transformation terminée : {len(df)} offres → {FICHIER_TRANSFORME}")

    # Stats rapides
    print(f"\n📊 Stats post-transformation :")
    print(f"   Titres normalisés uniques : {df['titre_normalise'].nunique()}")
    print(f"   Offres avec compétences   : {(df['competences_extraites'] != '').sum()}")
    print(f"   Offres avec salaire       : {df['salaire_annuel_estime'].notna().sum()}")
    print(f"   Régions identifiées       : {df['region'].nunique()}")
    print(f"   Types de contrat          : {df['contrat_type'].value_counts().to_dict()}")

    return len(df)


# ─── MISE À JOUR fusion.py : lire offres_transformees.csv dans sauvegarder_en_db_task ──

def get_fichier_pour_db() -> str:
    """
    Retourne le fichier à utiliser pour l'insertion en DB.
    Préfère offres_transformees.csv si disponible, sinon offres_all.csv.
    """
    if os.path.exists(FICHIER_TRANSFORME):
        return FICHIER_TRANSFORME
    return FICHIER_FUSIONNE


# ─── POINT D'ENTRÉE STANDALONE ────────────────────────────────────────────────

if __name__ == "__main__":
    n = transformer_offres_task()
    print(f"\n🎯 Total transformé : {n} offres")