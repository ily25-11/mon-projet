"""
Scraper France Travail API — Version élargie tous domaines
==========================================================
Inscription gratuite : https://francetravail.io
  → Mes applications → Créer une application
  → Cocher l'API "Offres d'emploi v2"
  → Récupérer CLIENT_ID et CLIENT_SECRET

Variables d'environnement :
  FT_CLIENT_ID=<votre client_id>
  FT_CLIENT_SECRET=<votre client_secret>

Différences avec la v1 :
  - POSTES couvre 50+ métiers dans tous les secteurs
  - Extraction du code ROME pour la catégorisation
  - Champ 'secteur' standardisé pour le dashboard
  - Hash_id pour déduplication croisée robuste
  - Extraction salaire améliorée
"""

import os
import re
import time
import hashlib
import requests
import pandas as pd
from datetime import datetime

# ─── CONFIG ───────────────────────────────────────────────────────────────────

FT_CLIENT_ID     = os.getenv("FT_CLIENT_ID", "")
FT_CLIENT_SECRET = os.getenv("FT_CLIENT_SECRET", "")

TOKEN_URL  = "https://entreprise.francetravail.fr/connexion/oauth2/access_token"
SEARCH_URL = "https://api.francetravail.io/partenaire/offresdemploi/v2/offres/search"
OUTPUT_PATH = "/opt/airflow/data/offres_france_travail.csv"

# ─── MÉTIERS COUVERTS — TOUS SECTEURS ─────────────────────────────────────────
# Structure : { "secteur_lisible": ["mot_cle1", "mot_cle2", ...] }
# France Travail cherche dans le titre + description simultanément.

POSTES_PAR_SECTEUR = {
    "Informatique / Tech": [
        "data scientist", "data engineer", "data analyst",
        "machine learning", "MLOps", "NLP engineer",
        "data architect", "business intelligence",
        "développeur python", "développeur java", "développeur react",
        "devops", "cloud architect", "SRE", "cybersécurité",
        "product manager tech", "scrum master",
    ],
    "Finance / Comptabilité": [
        "analyste financier", "contrôleur de gestion",
        "directeur financier", "comptable", "auditeur",
        "trésorier", "risk manager", "analyste crédit",
        "gestionnaire de portefeuille", "conseiller en gestion",
    ],
    "Marketing / Communication": [
        "chef de produit", "responsable marketing",
        "traffic manager", "SEO", "responsable communication",
        "brand manager", "content manager", "chargé de communication",
        "directeur marketing", "growth hacker",
    ],
    "Commercial / Ventes": [
        "commercial", "account manager", "business developer",
        "directeur commercial", "ingénieur commercial",
        "responsable des ventes", "chargé d'affaires",
        "key account manager", "technico-commercial",
    ],
    "Ressources Humaines": [
        "responsable RH", "recruteur", "HRBP",
        "chargé de formation", "responsable paie",
        "directeur des ressources humaines", "talent acquisition",
        "chargé de missions RH",
    ],
    "Ingénierie": [
        "ingénieur mécanique", "ingénieur électrique",
        "ingénieur industriel", "ingénieur R&D",
        "chef de projet ingénierie", "ingénieur production",
        "ingénieur qualité", "ingénieur travaux",
        "ingénieur automatisme", "technicien bureau d'études",
    ],
    "Santé / Médical": [
        "infirmier", "médecin généraliste", "pharmacien",
        "aide-soignant", "kinésithérapeute",
        "technicien de laboratoire", "manipulateur radio",
        "ergothérapeute", "orthophoniste",
        "responsable qualité santé",
    ],
    "Juridique": [
        "juriste", "avocat", "juriste d'entreprise",
        "compliance officer", "responsable juridique",
        "paralegal", "notaire", "juriste social",
    ],
    "Logistique / Supply Chain": [
        "supply chain manager", "logisticien",
        "responsable entrepôt", "chef de dépôt",
        "approvisionneur", "gestionnaire de stocks",
        "coordinateur logistique", "responsable transport",
        "cariste",
    ],
    "Management / Opérations": [
        "directeur général", "directeur opérations",
        "responsable de site", "chef de projet",
        "directeur de production", "responsable qualité",
        "lean manager", "responsable maintenance",
    ],
    "Education / Formation": [
        "formateur", "responsable formation",
        "ingénieur pédagogique", "consultant en formation",
        "directeur pédagogique", "chargé de formation continue",
    ],
    "Commerce / Distribution": [
        "responsable de rayon", "chef de magasin",
        "directeur de magasin", "merchandiser",
        "responsable achats", "acheteur",
        "category manager", "gestionnaire de point de vente",
    ],
    "Conseil": [
        "consultant stratégie", "consultant management",
        "consultant transformation digitale",
        "analyste consultant", "consultant SAP",
        "consultant ERP",
    ],
    "Immobilier / BTP": [
        "agent immobilier", "responsable immobilier",
        "conducteur de travaux", "chef de chantier",
        "architecte", "ingénieur bâtiment",
        "maître d'oeuvre", "chargé d'affaires BTP",
    ],
    "Hôtellerie / Restauration": [
        "chef de cuisine", "directeur d'hôtel",
        "responsable restauration", "maître d'hôtel",
        "responsable hébergement", "cuisinier",
    ],
}

# Aplatir pour compatibilité avec l'ancienne interface
POSTES = [p for postes in POSTES_PAR_SECTEUR.values() for p in postes]

# Mapping inverse poste → secteur
POSTE_TO_SECTEUR = {
    poste: secteur
    for secteur, postes in POSTES_PAR_SECTEUR.items()
    for poste in postes
}


# ─── AUTHENTIFICATION ─────────────────────────────────────────────────────────

def get_access_token() -> str:
    if not FT_CLIENT_ID or not FT_CLIENT_SECRET:
        raise EnvironmentError(
            "Variables FT_CLIENT_ID et FT_CLIENT_SECRET manquantes.\n"
            "Inscription gratuite : https://francetravail.io"
        )

    resp = requests.post(
        TOKEN_URL,
        params={"realm": "/partenaire"},
        data={
            "grant_type":    "client_credentials",
            "client_id":     FT_CLIENT_ID,
            "client_secret": FT_CLIENT_SECRET,
            "scope":         "api_offresdemploiv2 o2dsoffre",
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=15,
    )
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise ValueError(f"Token vide — réponse : {resp.text}")
    print("Token France Travail obtenu")
    return token


# ─── SCRAPING ─────────────────────────────────────────────────────────────────

def scraper_poste(poste: str, token: str, max_offres: int = 150) -> list[dict]:
    """
    Récupère les offres pour un poste donné (max 150 par l'API FT).
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept":        "application/json",
    }

    offres = []
    start  = 0
    step   = 100

    while start < max_offres and start <= 149:
        end = min(start + step - 1, 149)

        params = {
            "motsCles": poste,
            "range":    f"{start}-{end}",
            "sort":     "1",
        }

        try:
            resp = requests.get(
                SEARCH_URL, headers=headers, params=params, timeout=20
            )

            if resp.status_code == 204:
                break
            if resp.status_code == 401:
                raise PermissionError("Token France Travail expiré")

            resp.raise_for_status()
            data = resp.json()
            resultats = data.get("resultats", [])
            if not resultats:
                break

            for job in resultats:
                titre     = job.get("intitule", "N/A")
                entreprise = job.get("entreprise", {}).get("nom", "N/A")
                lien      = job.get("origineOffre", {}).get("urlOrigine", "N/A")

                # Hash unique
                hash_id = hashlib.sha1(
                    f"{titre}|{entreprise}|{lien}".encode()
                ).hexdigest()[:16]

                # Salaire
                salaire = job.get("salaire", {})
                sal_lib  = salaire.get("libelle", "")
                sal_min, sal_max = _parse_salaire(sal_lib)

                # Lieu + région
                lieu_info = job.get("lieuTravail", {})
                lieu      = lieu_info.get("libelle", "N/A")
                region    = _extract_region_ft(lieu_info)

                # Compétences
                competences = job.get("competences", [])
                tags = ", ".join(c.get("libelle", "") for c in competences[:10])

                # Code ROME (classification métier FT)
                rome = job.get("romeCode", "")

                offres.append({
                    "hash_id":         hash_id,
                    "titre":           titre,
                    "entreprise":      entreprise,
                    "lieu":            lieu,
                    "region":          region,
                    "salaire_min":     sal_min,
                    "salaire_max":     sal_max,
                    "salaire_brut":    sal_lib,
                    "remote":          _is_remote(job),
                    "contrat":         job.get("typeContratLibelle", "N/A"),
                    "contrat_type":    job.get("typeContrat", "N/A"),
                    "categorie":       poste,
                    "secteur":         POSTE_TO_SECTEUR.get(poste, "Autre"),
                    "description":     job.get("description", "N/A")[:500],
                    "lien":            lien,
                    "tags":            tags,
                    "rome_code":       rome,
                    "niveau":          job.get("experienceLibelle", "N/A"),
                    "date_creation":   job.get("dateCreation", "")[:10],
                    "date_scraping":   datetime.now().strftime("%Y-%m-%d"),
                    "source":          "france_travail",
                    "poste_recherche": poste,
                })

            # Vérifier Content-Range
            content_range = resp.headers.get("Content-Range", "")
            if content_range:
                total = int(content_range.split("/")[-1])
                if end >= total - 1:
                    break

            print(f"  [FT] '{poste}' tranche {start}-{end} : {len(resultats)} offres")
            start += step
            time.sleep(0.5)

        except PermissionError:
            raise
        except requests.HTTPError as e:
            print(f"  [FT] HTTP {e.response.status_code} pour '{poste}'")
            break
        except Exception as e:
            print(f"  [FT] Erreur '{poste}' : {e}")
            break

    return offres


# ─── UTILITAIRES ──────────────────────────────────────────────────────────────

def _parse_salaire(libelle: str) -> tuple:
    if not libelle:
        return None, None
    mensuel = "mensuel" in libelle.lower()
    nombres = re.findall(r"[\d]+(?:[,.][\d]+)?", libelle.replace(" ", ""))
    nombres = [float(n.replace(",", ".")) for n in nombres if n]
    if not nombres:
        return None, None
    if mensuel:
        nombres = [n * 12 for n in nombres]
    if len(nombres) == 1:
        return nombres[0], nombres[0]
    return min(nombres), max(nombres)


def _extract_region_ft(lieu_info: dict) -> str:
    """Extrait la région depuis l'objet lieuTravail France Travail."""
    libelle = lieu_info.get("libelle", "")
    # Format typique : "75 - Paris" ou "Île-de-France"
    if " - " in libelle:
        return libelle.split(" - ")[1].strip()
    return libelle or "N/A"


def _is_remote(job: dict) -> bool:
    lieu    = job.get("lieuTravail", {}).get("libelle", "").lower()
    desc    = job.get("description", "").lower()
    kws     = ["télétravail", "teletravail", "remote", "à distance"]
    return any(k in lieu or k in desc for k in kws)


# ─── FONCTION PRINCIPALE ──────────────────────────────────────────────────────

def scraper_france_travail(
    postes: list[str] | None = None,
    max_offres_par_poste: int = 150,
) -> list[dict]:
    """
    Point d'entrée principal — compatible DAG Airflow.
    Scrape tous les secteurs définis dans POSTES_PAR_SECTEUR.
    """
    if postes is None:
        postes = POSTES

    print(f"France Travail — {len(postes)} postes / {len(POSTES_PAR_SECTEUR)} secteurs")

    token  = get_access_token()
    toutes = []

    for poste in postes:
        print(f"Recherche : {poste}")
        offres = scraper_poste(poste, token, max_offres=max_offres_par_poste)
        toutes.extend(offres)
        print(f"  → {len(offres)} offres")
        time.sleep(1)

    if toutes:
        df = pd.DataFrame(toutes)
        avant = len(df)
        df.drop_duplicates(subset=["hash_id"], keep="first", inplace=True)
        print(f"\nDeduplication FT : {avant} → {len(df)} offres uniques")

        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8")
        print(f"Sauvegarde : {len(df)} offres → {OUTPUT_PATH}")
        return df.to_dict("records")

    print("Aucune offre France Travail récupérée.")
    return []


def scraper_france_travail_task():
    """Wrapper Airflow."""
    offres = scraper_france_travail()
    print(f"France Travail task : {len(offres)} offres")
    return len(offres)


# ─── TEST LOCAL ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    offres = scraper_france_travail(
        postes=["data scientist", "analyste financier", "infirmier", "commercial"],
        max_offres_par_poste=50,
    )
    print(f"\nTotal : {len(offres)} offres")
    for o in offres[:4]:
        sal = f"{int(o['salaire_min']):,}€" if o["salaire_min"] else "N/A"
        print(f"  [{o['secteur']}] {o['titre']} | {o['entreprise']} | {sal}")