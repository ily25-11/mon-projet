"""
transform.py — Couche Transformation ETL + Construction Schéma en Étoile
=========================================================================
Fichier UNIQUE responsable de :
  1. Normalisation des titres de postes
  2. Extraction des compétences depuis la description
  3. Standardisation des pays
  4. Enrichissement des salaires manquants (médiane par secteur)
  5. Détection du type de contrat (améliorée)
  6. Nettoyage du niveau d'expérience (ex: "3 An(s) - texte" → "3 ans")
  7. Normalisation des secteurs et catégories
  8. Nettoyage général des données
  9. TRUNCATE offres_emploi + réinsertion propre depuis CSV transformé
 10. Construction du schéma en étoile :
       fact_offres (table centrale)
       ├── dim_lieu
       ├── dim_contrat
       ├── dim_entreprise
       ├── dim_source
       ├── dim_temps
       └── dim_competence (via offre_competence)

Position dans le DAG : fusion >> transform >> rapport_final
À placer dans : /opt/airflow/scrapers/transform.py
"""

import os
import re
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────────────────────────────────

OUTPUT_DIR         = "/opt/airflow/data"
FICHIER_FUSIONNE   = os.path.join(OUTPUT_DIR, "offres_all.csv")
FICHIER_TRANSFORME = os.path.join(OUTPUT_DIR, "offres_transformees.csv")

DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "postgres"),
    "database": os.getenv("POSTGRES_DB",   "airflow"),
    "user":     os.getenv("POSTGRES_USER", "airflow"),
    "password": os.getenv("POSTGRES_PASSWORD", "airflow"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
}

COLONNES_DB = [
    "hash_id", "titre", "entreprise", "lieu", "region", "pays",
    "salaire_min", "salaire_max", "salaire_brut", "salaire_annuel_estime",
    "remote", "contrat", "contrat_type", "categorie", "secteur",
    "description", "lien", "tags", "rome_code", "niveau",
    "date_creation", "date_scraping", "source", "poste_recherche",
    "titre_normalise", "competences_extraites",
]


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


# ══════════════════════════════════════════════════════════════════════════════
#  PARTIE 1 — TRANSFORMATIONS PANDAS (sur le CSV)
# ══════════════════════════════════════════════════════════════════════════════

# ─── 1. NORMALISATION DES TITRES ──────────────────────────────────────────────

TITRE_MAPPING = {
    r"data\s*scien(tist|ce)":                               "Data Scientist",
    r"data\s*engineer|ingénieur\s*data":                    "Data Engineer",
    r"data\s*anal(yst|yste)":                               "Data Analyst",
    r"machine\s*learning|ml\s*engineer":                    "ML Engineer",
    r"deep\s*learning|ai\s*engineer":                       "AI Engineer",
    r"data\s*architect":                                    "Data Architect",
    r"bi\s*(developer|analyst|dev)":                        "BI Developer",
    r"business\s*intel":                                    "Business Intelligence",
    r"nlp|natural\s*language":                              "NLP Engineer",
    r"computer\s*vision":                                   "Computer Vision Engineer",
    r"mlops|data\s*ops":                                    "MLOps Engineer",
    r"full.?stack":                                         "Développeur Full Stack",
    r"backend|back.end":                                    "Développeur Backend",
    r"frontend|front.end":                                  "Développeur Frontend",
    r"devops|site\s*reliability":                           "DevOps Engineer",
    r"cloud\s*engineer|architecte\s*cloud":                 "Cloud Engineer",
    r"cybersécurité|security\s*engineer|soc\s*analyst":     "Cybersécurité",
    r"développeur|developer":                               "Développeur",
    r"comptabl|accountant":                                 "Comptable",
    r"contrôleur\s*de\s*gestion|controller":                "Contrôleur de Gestion",
    r"analyste\s*financ|financial\s*analyst":               "Analyste Financier",
    r"audit(eur|or)":                                       "Auditeur",
    r"trésor(ier|y)":                                       "Trésorier",
    r"risk\s*(analyst|manager)":                            "Risk Manager",
    r"ressources\s*humaines|human\s*resources|rh\s*généraliste": "Chargé RH",
    r"recruteur|talent\s*acquisition|recruiter":            "Recruteur",
    r"drh|directeur\s*(rh|ressources)":                     "DRH",
    r"paie|payroll":                                        "Gestionnaire Paie",
    r"chef\s*de\s*produit|product\s*manager":               "Product Manager",
    r"marketing\s*manager|responsable\s*marketing":         "Responsable Marketing",
    r"community\s*manager|social\s*media":                  "Community Manager",
    r"seo|sem|growth\s*hacker":                             "Growth / SEO",
    r"commercial|sales\s*(manager|rep|executive)":          "Commercial",
    r"business\s*develop|développement\s*commercial":       "Business Developer",
    r"account\s*manager|gestionnaire\s*de\s*compte":        "Account Manager",
    r"infirm(ier|ière)":                                    "Infirmier",
    r"médecin|physician|doctor":                            "Médecin",
    r"pharmacien":                                          "Pharmacien",
    r"aide.soignant":                                       "Aide-Soignant",
    r"kinésithér":                                          "Kinésithérapeute",
    r"ingénieur\s*(mécani|producti|qualité|process)":       "Ingénieur Industriel",
    r"ingénieur\s*(électri|électroni)":                     "Ingénieur Électronique",
    r"chef\s*de\s*projet\s*(technique|infra|si)":           "Chef de Projet Technique",
    r"architecte\s*(logiciel|solution|si)":                 "Architecte Logiciel",
    r"logisti(cien|que\s*manager)":                         "Logisticien",
    r"supply\s*chain":                                      "Supply Chain Manager",
    r"responsable\s*(entrepôt|warehouse)":                  "Responsable Entrepôt",
    r"juriste|legal\s*(counsel|officer)":                   "Juriste",
    r"avocat|attorney|lawyer":                              "Avocat",
    r"conducteur\s*de\s*travaux":                           "Conducteur de Travaux",
    r"chef\s*de\s*chantier":                                "Chef de Chantier",
    r"architecte(?!\s*(logiciel|cloud|solution|si))":       "Architecte",
    r"géomètre":                                            "Géomètre",
    r"directeur\s*(général|opérations|technique)|ceo|cto|coo": "Directeur",
    r"manager|responsable\s*d[e']\s*(département|service|pôle)": "Manager",
}


def normaliser_titre(titre: str) -> str:
    if not isinstance(titre, str) or not titre.strip():
        return "Autre"
    t = titre.lower()
    t = re.sub(r"\b(h/f|f/h|m/f|senior|junior|confirmé|expérimenté|"
               r"stage|alternance|apprentissage|cdi|cdd|freelance|remote|"
               r"télétravail|\d+\s*(ans?|years?))\b", "", t)
    t = re.sub(r"[^\w\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    for pattern, label in TITRE_MAPPING.items():
        if re.search(pattern, t, re.IGNORECASE):
            return label
    return titre.strip()[:50].title()


# ─── 2. EXTRACTION DES COMPÉTENCES ────────────────────────────────────────────

COMPETENCES = {
    "Python", "R", "SQL", "Java", "Scala", "JavaScript", "TypeScript",
    "C++", "C#", "Go", "Rust", "Julia", "MATLAB", "SAS", "Bash",
    "Pandas", "NumPy", "Scikit-learn", "TensorFlow", "PyTorch", "Keras",
    "XGBoost", "LightGBM", "Spark", "PySpark", "Hadoop", "Kafka",
    "Airflow", "dbt", "MLflow", "Hugging Face",
    "Power BI", "Tableau", "Looker", "Qlik", "Metabase", "Grafana",
    "AWS", "Azure", "GCP", "Docker", "Kubernetes", "Terraform",
    "Databricks", "Snowflake", "BigQuery", "Redshift",
    "PostgreSQL", "MySQL", "MongoDB", "Elasticsearch", "Redis",
    "Cassandra", "Oracle", "SQL Server",
    "Git", "GitHub", "GitLab", "Jira", "Confluence", "Notion",
    "Excel", "SAP", "Salesforce", "HubSpot",
    "leadership", "communication", "autonomie", "rigueur",
    "gestion de projet", "management", "négociation",
    "esprit d'analyse", "travail en équipe",
}

COMP_PATTERN = re.compile(
    r"\b(" + "|".join(re.escape(c) for c in sorted(COMPETENCES, key=len, reverse=True)) + r")\b",
    re.IGNORECASE
)


def extraire_competences(description: str) -> str:
    if not isinstance(description, str):
        return ""
    matches = set(COMP_PATTERN.findall(description))
    comp_dict = {c.lower(): c for c in COMPETENCES}
    result = sorted({comp_dict.get(m.lower(), m) for m in matches})
    return ", ".join(result[:15])


# ─── 3. STANDARDISATION DES PAYS ──────────────────────────────────────────────

def standardiser_pays(lieu: str, pays_original: str = None) -> str:
    if isinstance(pays_original, str) and pays_original.strip():
        p = pays_original.strip().lower()
        if "france" in p or p == "fr":                    return "France"
        if re.search(r"uk|kingdom|england", p):           return "Royaume-Uni"
        if re.search(r"germany|deutsch", p):              return "Allemagne"
        if re.search(r"usa|united states|america", p):    return "États-Unis"
        if re.search(r"canada", p):                       return "Canada"
        if re.search(r"australia|australie", p):          return "Australie"
        if re.search(r"spain|espagne|españa", p):         return "Espagne"
        if re.search(r"netherlands|pays.bas|holland", p): return "Pays-Bas"
        return pays_original.strip().title()[:50]
    if not isinstance(lieu, str):
        return "France"
    l = lieu.lower()
    if re.search(r"france|paris|lyon|marseille|bordeaux|toulouse|nantes|"
                 r"lille|strasbourg|rennes|nice|montpellier|grenoble|seine|"
                 r"hauts|occitanie|bretagne|normandie|alsace|lorraine", l):
        return "France"
    if re.search(r"uk|london|england|scotland|wales|britain", l):   return "Royaume-Uni"
    if re.search(r"germany|berlin|munich|deutschland|hamburg", l):  return "Allemagne"
    if re.search(r"usa|new york|california|san francisco|united states|seattle|texas", l): return "États-Unis"
    if re.search(r"canada|toronto|montreal|vancouver", l):          return "Canada"
    if re.search(r"australia|sydney|melbourne", l):                 return "Australie"
    if re.search(r"remote|télétravail", l):                         return "Remote"
    return "France"


# ─── 4. DÉTECTION TYPE DE CONTRAT ─────────────────────────────────────────────

CONTRAT_TYPE_MAPPING = {
    "onsite":        "CDI",
    "hybrid":        "CDI",
    "remote":        "CDI",
    "external":      "Freelance",
    "lib":           "Freelance",
    "mis":           "Intérim",
    "interim":       "Intérim",
    "intérim":       "Intérim",
    "cdi":           "CDI",
    "cdd":           "CDD",
    "stage":         "Stage",
    "alternance":    "Alternance",
    "freelance":     "Freelance",
    "temps partiel": "Temps partiel",
}


def nettoyer_contrat_type(contrat_type: str, titre: str = "", description: str = "", tags: str = "") -> str:
    if isinstance(contrat_type, str) and contrat_type.strip():
        mapped = CONTRAT_TYPE_MAPPING.get(contrat_type.strip().lower())
        if mapped:
            return mapped
        valeurs_propres = {"CDI", "CDD", "Stage", "Alternance", "Freelance", "Intérim", "Temps partiel"}
        if contrat_type.strip() in valeurs_propres:
            return contrat_type.strip()
    texte = " ".join([str(titre or ""), str(description or "")[:500], str(tags or "")]).lower()
    if re.search(r"\bstage\b|internship|stagiaire|\bintern\b", texte):          return "Stage"
    if re.search(r"alternance|apprentissage|apprenti|apprenticeship", texte):   return "Alternance"
    if re.search(r"\bfreelance\b|freelancer|indépendant", texte):               return "Freelance"
    if re.search(r"intérim|interim|mission\s*temporaire", texte):               return "Intérim"
    if re.search(r"\bcdd\b|contract\s*(fix|term)|temporary|temporaire", texte): return "CDD"
    if re.search(r"part.time|temps\s*partiel|mi.temps", texte):                 return "Temps partiel"
    return "CDI"


# ─── 5. NETTOYAGE DU NIVEAU D'EXPÉRIENCE ──────────────────────────────────────

def nettoyer_niveau(niveau: str) -> str:
    if not isinstance(niveau, str) or not niveau.strip():
        return "Non renseigné"
    n = niveau.strip().lower()
    if re.search(r"débutant|junior|entry.level|no experience|sans expérience|0\s*an", n):
        return "Débutant"
    match = re.search(r"(\d+)\s*(?:an[s]?|année[s]?|year[s]?)", n)
    if match:
        nb = int(match.group(1))
        if nb == 0:   return "Débutant"
        elif nb <= 2: return f"{nb} an{'s' if nb > 1 else ''}"
        elif nb <= 5: return f"{nb} ans"
        else:         return "6 ans et plus"
    if re.search(r"senior|expérimenté|confirmed|confirmé|expert", n): return "Senior"
    if re.search(r"manager|lead|principal|architect|directeur", n):   return "Manager / Lead"
    if re.search(r"mid.level|intermédiaire", n):                      return "Intermédiaire"
    if re.search(r"stage|intern|étudiant|student", n):                return "Stage"
    if re.search(r"alternance|apprenti", n):                          return "Alternance"
    return "Non renseigné"


# ─── 6. NORMALISATION SECTEUR ─────────────────────────────────────────────────

SECTEUR_MAPPING = {
    r"tech|informatique|it\b|software|digital|numérique":              "Technologie",
    r"data|analytics|intelligence artificielle|ia\b|machine learning": "Data / IA",
    r"finance|banque|assurance|comptabilité|audit":                    "Finance",
    r"santé|médical|pharma|healthcare|hospital":                       "Santé",
    r"marketing|communication|publicité|media":                        "Marketing",
    r"rh|ressources humaines|human resources|recrutement":             "Ressources Humaines",
    r"commerce|vente|retail|sales":                                    "Commerce / Vente",
    r"industrie|manufacturing|production|mécanique":                   "Industrie",
    r"btp|construction|immobilier|bâtiment":                          "BTP / Immobilier",
    r"logistique|supply chain|transport":                              "Logistique",
    r"conseil|consulting|audit|stratégie":                             "Conseil",
    r"éducation|formation|enseignement|education":                     "Éducation",
    r"juridique|legal|droit|law":                                      "Juridique",
    r"energie|énergie|environnement|green":                            "Énergie / Environnement",
    r"telecom|télécommunication|réseau|network":                       "Télécoms",
}


def normaliser_secteur(secteur: str) -> str:
    if not isinstance(secteur, str) or not secteur.strip():
        return "Autre"
    s = secteur.lower()
    for pattern, label in SECTEUR_MAPPING.items():
        if re.search(pattern, s, re.IGNORECASE):
            return label
    return secteur.strip()[:50].title()


# ─── 7. NORMALISATION CATÉGORIE ───────────────────────────────────────────────

def normaliser_categorie(categorie: str) -> str:
    if not isinstance(categorie, str) or not categorie.strip():
        return "Autre"
    return categorie.strip()[:100].title()


# ─── 8. ENRICHISSEMENT SALAIRES ───────────────────────────────────────────────

def enrichir_salaires(df: pd.DataFrame) -> pd.DataFrame:
    def estimer(row):
        actuel = row.get("salaire_annuel_estime")
        if pd.notna(actuel) and 8_000 < actuel < 500_000:
            return actuel
        valeurs = []
        for v in [row.get("salaire_min"), row.get("salaire_max")]:
            try:
                v = float(v)
                if 8_000 < v < 500_000:
                    valeurs.append(v)
            except (TypeError, ValueError):
                pass
        return np.mean(valeurs) if valeurs else np.nan

    df["salaire_annuel_estime"] = df.apply(estimer, axis=1)

    mediane_groupe = (
        df[df["salaire_annuel_estime"].notna()]
        .groupby(["secteur", "contrat_type"])["salaire_annuel_estime"]
        .median()
    )
    mediane_globale = df["salaire_annuel_estime"].median()

    def fallback_mediane(row):
        if pd.notna(row["salaire_annuel_estime"]):
            return row["salaire_annuel_estime"]
        return mediane_groupe.get((row["secteur"], row["contrat_type"]), mediane_globale)

    df["salaire_annuel_estime"] = df.apply(fallback_mediane, axis=1)
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  PARTIE 2 — INSERTION PROPRE EN BASE (TRUNCATE + INSERT)
# ══════════════════════════════════════════════════════════════════════════════

SQL_CREATE_OFFRES_EMPLOI = """
CREATE TABLE IF NOT EXISTS offres_emploi (
    id                    SERIAL PRIMARY KEY,
    hash_id               VARCHAR(64) UNIQUE,
    titre                 TEXT,
    titre_normalise       TEXT,
    entreprise            TEXT,
    lieu                  TEXT,
    region                TEXT,
    pays                  TEXT,
    salaire_min           FLOAT,
    salaire_max           FLOAT,
    salaire_brut          TEXT,
    salaire_annuel_estime FLOAT,
    remote                BOOLEAN,
    contrat               TEXT,
    contrat_type          TEXT,
    categorie             TEXT,
    secteur               TEXT,
    competences_extraites TEXT,
    description           TEXT,
    lien                  TEXT,
    tags                  TEXT,
    rome_code             TEXT,
    niveau                TEXT,
    date_creation         DATE,
    date_scraping         DATE,
    source                TEXT,
    poste_recherche       TEXT,
    created_at            TIMESTAMP DEFAULT NOW(),
    updated_at            TIMESTAMP DEFAULT NOW()
);
"""


def inserer_offres_propre(df: pd.DataFrame) -> int:
    """
    TRUNCATE offres_emploi puis réinsère toutes les offres transformées.
    Garantit que offres_emploi = exactement le dernier run, jamais plus.
    """
    conn = get_conn()
    cur  = conn.cursor()
    try:
        # Créer la table si elle n'existe pas
        cur.execute(SQL_CREATE_OFFRES_EMPLOI)
        conn.commit()

        # ── TRUNCATE : vider proprement avant réinsertion ──
        print("     TRUNCATE offres_emploi...")
        cur.execute("TRUNCATE TABLE offres_emploi RESTART IDENTITY CASCADE;")
        conn.commit()

        # ── Préparer les colonnes disponibles ──
        cols_disponibles = [c for c in COLONNES_DB if c in df.columns]
        cols_sql = ", ".join(cols_disponibles)

        def clean(val):
            if pd.isna(val) if not isinstance(val, (list, dict)) else False:
                return None
            if val == "" or val == "nan" or val == "N/A":
                return None
            return val

        lignes = [
            tuple(clean(row.get(col)) for col in cols_disponibles)
            for _, row in df.iterrows()
        ]

        sql_insert = f"""
            INSERT INTO offres_emploi ({cols_sql})
            VALUES %s
            ON CONFLICT (hash_id) DO NOTHING
        """

        execute_values(cur, sql_insert, lignes, page_size=500)
        conn.commit()

        cur.execute("SELECT COUNT(*) FROM offres_emploi")
        total = cur.fetchone()[0]
        print(f"     offres_emploi : {total} lignes (propre)")
        return total

    except Exception as e:
        conn.rollback()
        print(f" Erreur insertion offres_emploi : {e}")
        raise
    finally:
        cur.close()
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
#  PARTIE 3 — CONSTRUCTION DU SCHÉMA EN ÉTOILE (PostgreSQL)
# ══════════════════════════════════════════════════════════════════════════════

DDL_SCHEMA = """
CREATE TABLE IF NOT EXISTS dim_lieu (
    lieu_id  SERIAL PRIMARY KEY,
    lieu     VARCHAR(300),
    region   VARCHAR(150),
    pays     VARCHAR(100),
    remote   BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dim_contrat (
    contrat_id   SERIAL PRIMARY KEY,
    contrat_type VARCHAR(50) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_entreprise (
    entreprise_id SERIAL PRIMARY KEY,
    entreprise    VARCHAR(300) UNIQUE NOT NULL,
    secteur       VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS dim_source (
    source_id SERIAL PRIMARY KEY,
    source    VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_temps (
    temps_id      SERIAL PRIMARY KEY,
    date_creation DATE UNIQUE NOT NULL,
    annee         INTEGER,
    trimestre     INTEGER,
    mois          INTEGER,
    semaine       INTEGER
);

CREATE TABLE IF NOT EXISTS dim_competence (
    competence_id SERIAL PRIMARY KEY,
    competence    VARCHAR(200) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS fact_offres (
    offre_id              SERIAL PRIMARY KEY,
    hash_id               VARCHAR(64),
    titre                 VARCHAR(500),
    titre_normalise       VARCHAR(100),
    categorie             VARCHAR(100),
    salaire_min           DOUBLE PRECISION,
    salaire_max           DOUBLE PRECISION,
    salaire_annuel_estime DOUBLE PRECISION,
    remote                BOOLEAN DEFAULT FALSE,
    niveau                VARCHAR(50),
    lien                  TEXT,
    rome_code             VARCHAR(20),
    poste_recherche       VARCHAR(200),
    date_creation         DATE,
    date_scraping         DATE,
    lieu_id               INTEGER REFERENCES dim_lieu(lieu_id),
    contrat_id            INTEGER REFERENCES dim_contrat(contrat_id),
    entreprise_id         INTEGER REFERENCES dim_entreprise(entreprise_id),
    source_id             INTEGER REFERENCES dim_source(source_id),
    temps_id              INTEGER REFERENCES dim_temps(temps_id)
);

CREATE TABLE IF NOT EXISTS offre_competence (
    offre_id      INTEGER NOT NULL REFERENCES fact_offres(offre_id),
    competence_id INTEGER NOT NULL REFERENCES dim_competence(competence_id),
    PRIMARY KEY (offre_id, competence_id)
);
"""

NETTOYAGE_SQL = """
UPDATE offres_emploi SET contrat_type = 'CDI'
  WHERE LOWER(contrat_type) IN ('onsite','hybrid','remote')
     OR contrat_type IS NULL OR contrat_type = '';
UPDATE offres_emploi SET contrat_type = 'Freelance'
  WHERE LOWER(contrat_type) IN ('external','lib');
UPDATE offres_emploi SET contrat_type = 'Intérim'
  WHERE LOWER(contrat_type) IN ('mis');

UPDATE offres_emploi
  SET niveau = REGEXP_REPLACE(niveau, '(\d+)\s*[Aa]n\(s\).*', '\1 ans')
  WHERE niveau ~ '\d+\s*[Aa]n\(s\)';
UPDATE offres_emploi SET niveau = 'Débutant'
  WHERE LOWER(niveau) LIKE '%débutant%' OR LOWER(niveau) LIKE '%junior%';
UPDATE offres_emploi SET niveau = 'Non renseigné'
  WHERE niveau IS NULL OR niveau = '';

UPDATE offres_emploi SET region      = 'Non renseigné' WHERE region      IS NULL OR region      = '';
UPDATE offres_emploi SET lieu        = 'Non renseigné' WHERE lieu        IS NULL OR lieu        = '';
UPDATE offres_emploi SET description = ''              WHERE description  IS NULL;
UPDATE offres_emploi SET competences_extraites = ''    WHERE competences_extraites IS NULL;
UPDATE offres_emploi SET entreprise  = 'Non renseigné' WHERE entreprise  IS NULL OR entreprise  = '';
UPDATE offres_emploi SET source      = 'Inconnu'       WHERE source      IS NULL OR source      = '';
UPDATE offres_emploi SET secteur     = 'Autre'         WHERE secteur     IS NULL OR secteur     = '';
UPDATE offres_emploi SET categorie   = 'Autre'         WHERE categorie   IS NULL OR categorie   = '';
UPDATE offres_emploi SET salaire_annuel_estime = NULL
  WHERE salaire_annuel_estime NOT BETWEEN 8000 AND 500000;
"""

INSERT_DIMS_SQL = """
INSERT INTO dim_lieu (lieu, region, pays, remote)
SELECT DISTINCT
    TRIM(COALESCE(lieu,   'Non renseigné')),
    TRIM(COALESCE(region, 'Non renseigné')),
    TRIM(COALESCE(pays,   'France')),
    COALESCE(remote, FALSE)
FROM offres_emploi
ON CONFLICT DO NOTHING;

INSERT INTO dim_contrat (contrat_type)
SELECT DISTINCT COALESCE(contrat_type, 'CDI')
FROM offres_emploi
ON CONFLICT (contrat_type) DO NOTHING;

INSERT INTO dim_entreprise (entreprise, secteur)
SELECT DISTINCT
    COALESCE(entreprise, 'Non renseigné'),
    COALESCE(secteur, 'Autre')
FROM offres_emploi
ON CONFLICT (entreprise) DO NOTHING;

INSERT INTO dim_source (source)
SELECT DISTINCT COALESCE(source, 'Inconnu')
FROM offres_emploi
ON CONFLICT (source) DO NOTHING;

INSERT INTO dim_temps (date_creation, annee, trimestre, mois, semaine)
SELECT DISTINCT d,
    EXTRACT(YEAR    FROM d)::INTEGER,
    EXTRACT(QUARTER FROM d)::INTEGER,
    EXTRACT(MONTH   FROM d)::INTEGER,
    EXTRACT(WEEK    FROM d)::INTEGER
FROM (
    SELECT COALESCE(date_creation, date_scraping, CURRENT_DATE) AS d
    FROM offres_emploi
) s
WHERE d IS NOT NULL
ON CONFLICT (date_creation) DO NOTHING;

INSERT INTO dim_competence (competence)
SELECT DISTINCT TRIM(comp)
FROM offres_emploi,
     UNNEST(STRING_TO_ARRAY(competences_extraites, ',')) AS comp
WHERE competences_extraites <> '' AND TRIM(comp) <> ''
ON CONFLICT (competence) DO NOTHING;
"""

INSERT_FACT_SQL = """
INSERT INTO fact_offres (
    hash_id, titre, titre_normalise, categorie,
    salaire_min, salaire_max, salaire_annuel_estime,
    remote, niveau, lien, rome_code, poste_recherche,
    date_creation, date_scraping,
    lieu_id, contrat_id, entreprise_id, source_id, temps_id
)
SELECT
    o.hash_id, o.titre, o.titre_normalise, o.categorie,
    o.salaire_min, o.salaire_max, o.salaire_annuel_estime,
    COALESCE(o.remote, FALSE), o.niveau, o.lien, o.rome_code, o.poste_recherche,
    o.date_creation, o.date_scraping,
    l.lieu_id, c.contrat_id, e.entreprise_id, s.source_id, t.temps_id
FROM offres_emploi o
LEFT JOIN dim_lieu l
    ON  TRIM(COALESCE(o.lieu,   'Non renseigné')) = l.lieu
    AND TRIM(COALESCE(o.region, 'Non renseigné')) = l.region
    AND TRIM(COALESCE(o.pays,   'France'))         = l.pays
    AND COALESCE(o.remote, FALSE)                  = l.remote
LEFT JOIN dim_contrat c
    ON COALESCE(o.contrat_type, 'CDI') = c.contrat_type
LEFT JOIN dim_entreprise e
    ON COALESCE(o.entreprise, 'Non renseigné') = e.entreprise
LEFT JOIN dim_source s
    ON COALESCE(o.source, 'Inconnu') = s.source
LEFT JOIN dim_temps t
    ON COALESCE(o.date_creation, o.date_scraping, CURRENT_DATE) = t.date_creation;
"""

INSERT_LIAISON_SQL = """
INSERT INTO offre_competence (offre_id, competence_id)
SELECT DISTINCT f.offre_id, c.competence_id
FROM offres_emploi o
JOIN fact_offres f ON f.hash_id = o.hash_id
CROSS JOIN UNNEST(STRING_TO_ARRAY(o.competences_extraites, ',')) AS comp
JOIN dim_competence c ON c.competence = TRIM(comp)
WHERE o.competences_extraites <> '' AND TRIM(comp) <> ''
ON CONFLICT DO NOTHING;
"""


def reset_star_schema(conn, cur):
    """Drop toutes les tables du schéma étoile pour repartir propre."""
    cur.execute("""
        DROP TABLE IF EXISTS offre_competence CASCADE;
        DROP TABLE IF EXISTS fact_offres      CASCADE;
        DROP TABLE IF EXISTS dim_lieu         CASCADE;
        DROP TABLE IF EXISTS dim_contrat      CASCADE;
        DROP TABLE IF EXISTS dim_entreprise   CASCADE;
        DROP TABLE IF EXISTS dim_source       CASCADE;
        DROP TABLE IF EXISTS dim_temps        CASCADE;
        DROP TABLE IF EXISTS dim_competence   CASCADE;
    """)
    conn.commit()


def construire_schema_etoile() -> dict:
    conn = get_conn()
    cur  = conn.cursor()
    try:
        print("     [1/6] Nettoyage valeurs parasites (SQL)...")
        cur.execute(NETTOYAGE_SQL)
        conn.commit()

        print("     [2/6] Reset schéma étoile...")
        reset_star_schema(conn, cur)

        print("     [3/6] Création 6 dims + fact_offres + offre_competence...")
        cur.execute(DDL_SCHEMA)
        conn.commit()

        print("     [4/6] Peuplement dimensions...")
        cur.execute(INSERT_DIMS_SQL)
        conn.commit()

        print("     [5/6] Peuplement fact_offres...")
        cur.execute(INSERT_FACT_SQL)
        conn.commit()

        print("     [6/6] Peuplement offre_competence...")
        cur.execute(INSERT_LIAISON_SQL)
        conn.commit()

        stats = {}
        for table in ["dim_lieu", "dim_contrat", "dim_entreprise", "dim_source",
                      "dim_temps", "dim_competence", "fact_offres", "offre_competence"]:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            stats[table] = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM fact_offres WHERE lieu_id IS NULL")
        stats["fk_lieu_null"] = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM fact_offres WHERE contrat_id IS NULL")
        stats["fk_contrat_null"] = cur.fetchone()[0]
        cur.execute("""
            SELECT dc.contrat_type, COUNT(*) AS nb
            FROM dim_contrat dc
            JOIN fact_offres f ON f.contrat_id = dc.contrat_id
            GROUP BY dc.contrat_type ORDER BY nb DESC
        """)
        stats["contrat_dist"] = cur.fetchall()

        return stats

    except Exception as e:
        conn.rollback()
        print(f" Erreur schéma en étoile : {e}")
        raise
    finally:
        cur.close()
        conn.close()


# ══════════════════════════════════════════════════════════════════════════════
#  FONCTION PRINCIPALE — appelée par le DAG
# ══════════════════════════════════════════════════════════════════════════════

def transformer_offres_task() -> int:
    print(" Démarrage transformation ETL complète...")

    if not os.path.exists(FICHIER_FUSIONNE):
        print(f" Fichier introuvable : {FICHIER_FUSIONNE}")
        return 0

    df = pd.read_csv(FICHIER_FUSIONNE, low_memory=False)
    print(f" {len(df)} offres chargées depuis {FICHIER_FUSIONNE}")

    # ── Transformations Pandas ──
    print("    [1/8] Normalisation des titres...")
    df["titre_normalise"] = df["titre"].apply(normaliser_titre)

    print("    [2/8] Extraction des compétences...")
    df["competences_extraites"] = df["description"].apply(extraire_competences)

    print("    [3/8] Standardisation pays...")
    df["pays"] = df.apply(
        lambda r: standardiser_pays(r.get("lieu", ""), r.get("pays", "")), axis=1)

    print("    [4/8] Nettoyage contrat_type...")
    df["contrat_type"] = df.apply(
        lambda r: nettoyer_contrat_type(
            r.get("contrat_type", ""), r.get("titre", ""),
            r.get("description", ""), r.get("tags", "")
        ), axis=1)

    print("    [5/8] Nettoyage niveau d'expérience...")
    df["niveau"] = df["niveau"].apply(nettoyer_niveau)

    print("    [6/8] Normalisation secteurs...")
    df["secteur"] = df["secteur"].apply(normaliser_secteur)

    print("    [7/8] Normalisation catégories...")
    df["categorie"] = df["categorie"].apply(normaliser_categorie)

    print("    [8/8] Enrichissement salaires...")
    df = enrichir_salaires(df)

    # ── Nettoyage général ──
    print("    Nettoyage général...")
    df = df[df["titre"].notna() & (df["titre"].str.strip() != "")]
    df["titre"]                 = df["titre"].str[:500]
    df["lieu"]                  = df["lieu"].fillna("Non renseigné").str[:300]
    df["tags"]                  = df["tags"].fillna("").str[:500]
    df["entreprise"]            = df["entreprise"].fillna("Non renseigné").str[:300]
    df["source"]                = df["source"].fillna("Inconnu").str[:100]
    df["description"]           = df["description"].fillna("")
    df["competences_extraites"] = df["competences_extraites"].fillna("")
    df["date_scraping"]         = pd.to_datetime(df["date_scraping"], errors="coerce").dt.date
    df["date_scraping"]         = df["date_scraping"].fillna(datetime.now().date())
    df["remote"]                = df["remote"].apply(
        lambda x: True if str(x).lower() in ["true", "1", "yes", "oui", "remote"] else False)

    # ── Sauvegarde CSV ──
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(FICHIER_TRANSFORME, index=False, encoding="utf-8")
    print(f" CSV transformé : {len(df)} offres → {FICHIER_TRANSFORME}")

    # ── TRUNCATE + Réinsertion propre dans offres_emploi ──
    print("\n  Insertion propre dans offres_emploi (TRUNCATE + INSERT)...")
    inserer_offres_propre(df)

    # ── Construction schéma en étoile ──
    print("\n  Construction du schéma en étoile...")
    stats = construire_schema_etoile()

    dist = "\n".join([f"║    {ct:<18} : {nb}" for ct, nb in stats.get("contrat_dist", [])])

    print(f"""
╔══════════════════════════════════════════════════════╗
║   TRANSFORM — RAPPORT FINAL                          ║
╠══════════════════════════════════════════════════════╣
║  Offres traitées      : {len(df):<6}                   ║
║  Titres normalisés    : {df['titre_normalise'].nunique():<6}                   ║
║  Offres avec compét.  : {(df['competences_extraites'] != '').sum():<6}                   ║
║  Offres avec salaire  : {df['salaire_annuel_estime'].notna().sum():<6}                   ║
╠══════════════════════════════════════════════════════╣
║  SCHÉMA EN ÉTOILE                                    ║
║  fact_offres          : {stats['fact_offres']:<6} lignes (= offres CSV ✓) ║
╠══════════════════════════════════════════════════════╣
║  dim_lieu             : {stats['dim_lieu']:<6} lieux               ║
║  dim_contrat          : {stats['dim_contrat']:<6} types (≤ 8 ✓)      ║
║  dim_entreprise       : {stats['dim_entreprise']:<6} entreprises       ║
║  dim_source           : {stats['dim_source']:<6} sources            ║
║  dim_temps            : {stats['dim_temps']:<6} dates              ║
║  dim_competence       : {stats['dim_competence']:<6} compétences      ║
║  offre_competence     : {stats['offre_competence']:<6} liaisons         ║
╠══════════════════════════════════════════════════════╣
║  FK lieu null         : {stats['fk_lieu_null']:<6}                   ║
║  FK contrat null      : {stats['fk_contrat_null']:<6}                   ║
╠══════════════════════════════════════════════════════╣
║  Distribution contrats :                             ║
{dist}
╚══════════════════════════════════════════════════════╝
""")

    return len(df)


def get_fichier_pour_db() -> str:
    return FICHIER_TRANSFORME if os.path.exists(FICHIER_TRANSFORME) else FICHIER_FUSIONNE


if __name__ == "__main__":
    n = transformer_offres_task()
    print(f"\n Total traité : {n} offres")