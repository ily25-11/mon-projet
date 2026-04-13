-- init_db.sql
-- Exécuté automatiquement au premier démarrage du conteneur PostgreSQL.
-- Crée la base de données job_intelligent et les tables nécessaires.

-- Créer la base si elle n'existe pas
SELECT 'CREATE DATABASE job_intelligent'
WHERE NOT EXISTS (
    SELECT FROM pg_database WHERE datname = 'job_intelligent'
)\gexec

-- Connexion à la base job_intelligent
\connect job_intelligent

-- ── Table principale des offres ───────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS offres_emploi (
    id                SERIAL PRIMARY KEY,
    titre             TEXT,
    entreprise        TEXT,
    localisation      TEXT,
    date_publication  DATE,
    description       TEXT,
    competences       TEXT,
    salaire           TEXT,
    type_contrat      TEXT,
    lien              TEXT UNIQUE NOT NULL,
    source            TEXT,
    inserted_at       TIMESTAMP DEFAULT NOW()
);

-- Index pour les performances PowerBI & recommandation
CREATE INDEX IF NOT EXISTS idx_offres_source       ON offres_emploi(source);
CREATE INDEX IF NOT EXISTS idx_offres_localisation  ON offres_emploi(localisation);
CREATE INDEX IF NOT EXISTS idx_offres_titre         ON offres_emploi(titre);
CREATE INDEX IF NOT EXISTS idx_offres_type_contrat  ON offres_emploi(type_contrat);
CREATE INDEX IF NOT EXISTS idx_offres_date          ON offres_emploi(date_publication);

-- ── Vue pour PowerBI ──────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_offres_dashboard AS
SELECT
    id,
    titre,
    entreprise,
    localisation,
    date_publication,
    EXTRACT(YEAR  FROM date_publication) AS annee,
    EXTRACT(MONTH FROM date_publication) AS mois,
    TO_CHAR(date_publication, 'YYYY-MM') AS annee_mois,
    type_contrat,
    salaire,
    source,
    -- Extraction des compétences les plus courantes
    CASE
        WHEN description ILIKE '%python%' OR competences ILIKE '%python%'       THEN true ELSE false
    END AS has_python,
    CASE
        WHEN description ILIKE '%sql%'    OR competences ILIKE '%sql%'          THEN true ELSE false
    END AS has_sql,
    CASE
        WHEN description ILIKE '%spark%'  OR competences ILIKE '%spark%'        THEN true ELSE false
    END AS has_spark,
    CASE
        WHEN description ILIKE '%machine learning%' OR competences ILIKE '%machine learning%'
             OR description ILIKE '%ml%'                                         THEN true ELSE false
    END AS has_ml,
    CASE
        WHEN description ILIKE '%aws%'    OR competences ILIKE '%aws%'
             OR description ILIKE '%azure%' OR competences ILIKE '%azure%'
             OR description ILIKE '%gcp%'   OR competences ILIKE '%gcp%'        THEN true ELSE false
    END AS has_cloud,
    CASE
        WHEN description ILIKE '%docker%' OR competences ILIKE '%docker%'
             OR description ILIKE '%kubernetes%'                                 THEN true ELSE false
    END AS has_devops,
    lien,
    inserted_at
FROM offres_emploi;

-- ── Vue statistiques par source ───────────────────────────────────────────────
CREATE OR REPLACE VIEW v_stats_sources AS
SELECT
    source,
    COUNT(*)                                        AS total_offres,
    COUNT(DISTINCT localisation)                    AS nb_villes,
    MIN(date_publication)                           AS date_min,
    MAX(date_publication)                           AS date_max,
    COUNT(*) FILTER (WHERE type_contrat ILIKE '%CDI%') AS nb_cdi,
    COUNT(*) FILTER (WHERE type_contrat ILIKE '%CDD%') AS nb_cdd
FROM offres_emploi
GROUP BY source;

-- ── Vue top compétences (pour PowerBI word cloud / bar chart) ─────────────────
CREATE OR REPLACE VIEW v_top_competences AS
SELECT unnest(string_to_array(competences, ',')) AS competence,
       COUNT(*) AS nb_offres
FROM offres_emploi
WHERE competences IS NOT NULL
GROUP BY competence
ORDER BY nb_offres DESC
LIMIT 50;

\echo '✅  Base job_intelligent initialisée avec succès.'