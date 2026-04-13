# 📊 Guide PowerBI – Job Intelligent Dashboard

## 1. Connexion PostgreSQL → PowerBI

### Prérequis
- PowerBI Desktop installé (gratuit)
- PostgreSQL ODBC Driver installé : [npgsql.org](https://github.com/npgsql/npgsql/releases)

### Étapes de connexion

1. Ouvrir **PowerBI Desktop**
2. `Accueil` → `Obtenir des données` → `Base de données PostgreSQL`
3. Renseigner :
   | Champ | Valeur |
   |-------|--------|
   | Serveur | `localhost:5432` |
   | Base de données | `job_intelligent` |
   | Mode de connectivité | `Import` |
4. Identifiants : `airflow` / `airflow`
5. Sélectionner les vues suivantes :
   - ✅ `v_offres_dashboard`
   - ✅ `v_stats_sources`
   - ✅ `v_top_competences`

---

## 2. Modèle de données (Relations)

```
v_offres_dashboard
    ├── source          → v_stats_sources.source
    └── date_publication → Table Calendrier (date)

Table Calendrier (créer avec DAX) :
Calendrier = CALENDAR(DATE(2023,1,1), DATE(2025,12,31))
```

### Table Calendrier DAX
```dax
Calendrier = 
ADDCOLUMNS(
    CALENDAR(DATE(2023,1,1), DATE(2025,12,31)),
    "Année",        YEAR([Date]),
    "Mois",         MONTH([Date]),
    "Nom Mois",     FORMAT([Date], "MMMM YYYY"),
    "Trimestre",    "T" & QUARTER([Date]),
    "Semaine",      WEEKNUM([Date])
)
```

---

## 3. Mesures DAX à créer

```dax
-- Nombre total d'offres
Total Offres = COUNTROWS(v_offres_dashboard)

-- Offres ce mois-ci
Offres Ce Mois = 
CALCULATE(
    [Total Offres],
    DATESMTD(v_offres_dashboard[date_publication])
)

-- % offres avec Python
% Python = 
DIVIDE(
    CALCULATE([Total Offres], v_offres_dashboard[has_python] = TRUE()),
    [Total Offres],
    0
) * 100

-- % offres avec Cloud
% Cloud = 
DIVIDE(
    CALCULATE([Total Offres], v_offres_dashboard[has_cloud] = TRUE()),
    [Total Offres],
    0
) * 100

-- Offres par source (texte formaté)
Résumé Sources = 
CONCATENATEX(
    v_stats_sources,
    v_stats_sources[source] & " : " & v_stats_sources[total_offres],
    UNICHAR(10),
    v_stats_sources[total_offres],
    DESC
)

-- Croissance mensuelle
Croissance MoM = 
VAR OffresMoisActuel = [Total Offres]
VAR OffresMoisPrec   = CALCULATE([Total Offres], PREVIOUSMONTH(Calendrier[Date]))
RETURN
IF(OffresMoisPrec = 0, BLANK(),
   DIVIDE(OffresMoisActuel - OffresMoisPrec, OffresMoisPrec) * 100
)
```

---

## 4. Structure du Dashboard (4 pages)

### 📄 Page 1 — Vue Générale

| Visuel | Données | Position |
|--------|---------|----------|
| Carte KPI | Total Offres | Haut gauche |
| Carte KPI | Offres ce mois | Haut centre |
| Carte KPI | Nb de sources | Haut droite |
| Histogramme | Offres par source (source / count) | Milieu gauche |
| Graphique courbes | Évolution temporelle (annee_mois / count) | Milieu droite |
| Carte géographique | Offres par ville (localisation) | Bas |

### 📄 Page 2 — Compétences & Technologies

| Visuel | Données | Config |
|--------|---------|--------|
| Graphique barres empilées | % Python, SQL, Spark, ML, Cloud, DevOps | Triées desc |
| Nuage de mots | v_top_competences (competence / nb_offres) | Taille = nb_offres |
| Matrice croisée | source × compétences | Valeurs = % |
| Jauge | % offres avec Python | Cible = 60% |

### 📄 Page 3 — Géographie & Contrats

| Visuel | Données | Config |
|--------|---------|--------|
| Treemap | Offres par localisation | Hiérarchie ville |
| Graphique anneau | Répartition CDI / CDD / Freelance | type_contrat |
| Tableau détail | Top 20 villes + nb offres + % CDI | Trié par offres |
| Graphique barres | Offres CDI vs CDD par source | Groupées |

### 📄 Page 4 — Tableau de Bord Opérationnel

| Visuel | Données | Config |
|--------|---------|--------|
| Tableau interactif | Toutes offres filtrables | Colonnes: titre, entreprise, ville, contrat, source, date |
| Filtre trancheur | Source | Multiple |
| Filtre trancheur | Type contrat | Multiple |
| Filtre trancheur | Localisation | Recherche |
| Filtre trancheur | Période | Plage de dates |
| Carte KPI | Croissance MoM % | Flèche directionnelle |

---

## 5. Filtres globaux (Slicers)

Ajouter en haut de chaque page :
- **Source** → `v_offres_dashboard[source]`
- **Type de contrat** → `v_offres_dashboard[type_contrat]`
- **Période** → `Calendrier[Date]` (plage de dates)
- **Localisation** → `v_offres_dashboard[localisation]`

Synchroniser les filtres entre pages :  
`Vue` → `Synchroniser les segments` → activer sur toutes les pages.

---

## 6. Thème & Design

### Palette de couleurs recommandée
```json
{
  "name": "Job Intelligent",
  "dataColors": [
    "#2E86AB",
    "#A23B72",
    "#F18F01",
    "#C73E1D",
    "#3B1F2B",
    "#44BBA4",
    "#E94F37"
  ],
  "background": "#F5F5F5",
  "foreground": "#1A1A2E",
  "tableAccent": "#2E86AB"
}
```

Importer via : `Affichage` → `Thèmes` → `Parcourir les thèmes` → charger `theme.json`

---

## 7. Actualisation automatique

### Option A – Actualisation planifiée (PowerBI Service)
1. Publier le rapport sur **PowerBI Service** (app.powerbi.com)
2. `Jeux de données` → `Paramètres` → `Actualisation planifiée`
3. Configurer : tous les jours à **07h00** (après le DAG Airflow à 06h00)
4. Configurer la **passerelle de données** pour atteindre votre PostgreSQL local

### Option B – Actualisation à la demande
Dans PowerBI Desktop : `Accueil` → `Actualiser`

---

## 8. Checklist finale

- [ ] Connexion PostgreSQL établie
- [ ] Vues `v_offres_dashboard`, `v_stats_sources`, `v_top_competences` chargées
- [ ] Table Calendrier créée en DAX
- [ ] Relations définies
- [ ] Mesures DAX créées
- [ ] 4 pages de rapport construites
- [ ] Filtres synchronisés entre pages
- [ ] Thème couleurs appliqué
- [ ] Publié sur PowerBI Service
- [ ] Actualisation planifiée configurée