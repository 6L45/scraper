#!/usr/bin/env python3
import requests
import re
import os
import sys
import time
from urllib.parse import urljoin
import pandas as pd

SLEEP=9
PARQUET=""
PDF_DIR="."

def download_scihub_article(doi, output_dir=PDF_DIR):
    """Télécharge un article depuis Sci-Hub avec le vrai lien PDF"""

    # Domaines à essayer
    domains = [
            "sci-hub.st",
            "sci-hub.fr",
            "sci-hub.ru",
            "sci-hub.ee", 
            "sci-hub.shop",
            "sci-hub.wf"
            ]

    headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'fr,fr-FR;q=0.8,en-US;q=0.5,en;q=0.3',
            }

    for domain in domains:
        try:
            print(f"\n🔍 Essai avec {domain}...")
            url = f"https://{domain}/{doi}"

            # Récupérer la page
            response = requests.get(url, headers=headers, timeout=15)

            if response.status_code != 200:
                print(f"  ❌ Code HTTP {response.status_code}")
                continue

            print(f"  ✅ Page chargée")

            # ANALYSE DU HTML POUR TROUVER LE VRAI PDF

            # 1. Chercher dans les éléments embed/object/iframe (le plus courant)
            embed_patterns = [
                    r'<embed[^>]+src=["\']([^"\']+\.pdf)["\']',
                    r'<object[^>]+data=["\']([^"\']+\.pdf)["\']',
                    r'<iframe[^>]+src=["\']([^"\']+\.pdf)["\']',
                    r'<embed[^>]+src=["\']([^"\']+)["\'][^>]*type=["\']application/pdf["\']',
                    ]

            pdf_url = None
            html_text = response.text

            for pattern in embed_patterns:
                match = re.search(pattern, html_text, re.IGNORECASE)
                if match:
                    pdf_url = match.group(1)
                    print(f"  📄 PDF trouvé via embed/object/iframe: {pdf_url}")
                    break

            # 2. Si pas trouvé, chercher les liens de téléchargement
            if not pdf_url:
                download_patterns = [
                        r'<a[^>]+href=["\'](/downloads/[^"\']+\.pdf)["\'][^>]*>',
                        r'<a[^>]+href=["\'](/storage/[^"\']+\.pdf)["\'][^>]*>',
                        r'<a[^>]+href=["\'](/papers/[^"\']+\.pdf)["\'][^>]*>',
                        r'<a[^>]+href=["\'](/pdf/[^"\']+\.pdf)["\'][^>]*>',
                        r'<a[^>]+href=["\'](https?://[^"\']+\.pdf)["\'][^>]*>',
                        ]

                for pattern in download_patterns:
                    match = re.search(pattern, html_text, re.IGNORECASE)
                    if match:
                        pdf_url = match.group(1)
                        print(f"  📄 PDF trouvé via lien: {pdf_url}")
                        break

            # 3. Chercher dans les scripts JavaScript
            if not pdf_url:
                script_patterns = [
                        r'["\'](/storage/[^"\']+\.pdf)["\']',
                        r'["\'](/downloads/[^"\']+\.pdf)["\']',
                        r'["\'](/papers/[^"\']+\.pdf)["\']',
                        r'["\'](https?://[^"\']+\.pdf)["\']',
                        ]

                for pattern in script_patterns:
                    matches = re.findall(pattern, html_text, re.IGNORECASE)
                    for match in matches:
                        if '{pdf}' not in match and '{doi}' not in match:
                            pdf_url = match
                            print(f"  📄 PDF trouvé via script: {pdf_url}")
                            break
                    if pdf_url:
                        break

            if not pdf_url:
                print(f"  ❌ Aucune URL PDF trouvée dans la page")
                continue

            # Construire l'URL complète si relative
            if pdf_url.startswith('//'):
                pdf_url = 'https:' + pdf_url
            elif pdf_url.startswith('/'):
                pdf_url = f'https://{domain}{pdf_url}'
            elif not pdf_url.startswith('http'):
                pdf_url = urljoin(f'https://{domain}', pdf_url)

            print(f"  🔗 URL PDF complète: {pdf_url}")

            # Extraire le titre pour le nom de fichier
            title = None

            # Chercher dans les meta tags
            title_match = re.search(r'<meta[^>]+name=["\']citation_title["\'][^>]+content=["\']([^"\']+)["\']', html_text)
            if title_match:
                title = title_match.group(1)
                print(f"  📝 Titre trouvé: {title[:80]}...")
            else:
                # Chercher dans le title tag
                title_match = re.search(r'<title>(.*?)</title>', html_text, re.IGNORECASE)
                if title_match:
                    title = title_match.group(1)
                    # Nettoyer
                    title = re.sub(r'^Sci-Hub\s*[|:]\s*', '', title)
                    title = re.sub(r'\s*-\s*DOI:\s*[^\s]+$', '', title)
                    print(f"  📝 Titre extrait: {title[:80]}...")

            # Générer le nom de fichier
            if title:
                # Nettoyer le titre
                title = re.sub(r'<[^>]+>', '', title)  # Enlever les tags HTML
                title = re.sub(r'[<>:"/\\|?*]', '_', title)  # Remplacer caractères invalides
                title = re.sub(r'\s+', ' ', title).strip()  # Normaliser espaces

                if len(title) > 120:
                    title = title[:120] + "..."

                filename = f"{title}.pdf"

            else:
                filename = f"{doi.replace('/', '_')}.pdf"

            filepath = os.path.join(output_dir, filename)

            # Créer le répertoire si nécessaire
            os.makedirs(output_dir, exist_ok=True)

            # Télécharger le PDF
            print(f"  ⬇️  Téléchargement vers: {filename}")

            pdf_response = requests.get(pdf_url, headers=headers, stream=True, timeout=60)

            if pdf_response.status_code != 200:
                print(f"  ❌ Erreur HTTP {pdf_response.status_code} lors du téléchargement")
                continue

            # Vérifier que c'est bien un PDF
            content_type = pdf_response.headers.get('content-type', '').lower()
            if 'pdf' not in content_type and 'application/pdf' not in content_type:
                print(f"  ⚠️  Content-Type suspect: {content_type}")
                # On continue quand même

            # Télécharger avec progression
            total_size = int(pdf_response.headers.get('content-length', 0))
            downloaded = 0

            with open(filepath, 'wb') as f:
                for chunk in pdf_response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if total_size > 0:
                            percent = (downloaded / total_size) * 100
                            sys.stdout.write(f"\r  📥 Progression: {percent:.1f}%")
                            sys.stdout.flush()

            print()  # Nouvelle ligne

            # Vérifier que le fichier est un PDF
            try:
                with open(filepath, 'rb') as f:
                    header = f.read(5)
                    if header[:4] != b'%PDF' and header != b'%PDF-':
                        # Essayer de chercher %PDF plus loin dans le fichier
                        f.seek(0)
                        content = f.read(500)
                        if b'%PDF' not in content:
                            os.remove(filepath)
                            print(f"  ❌ Le fichier téléchargé n'est pas un PDF valide")
                            continue
            except Exception as e:
                print(f"  ⚠️  Erreur de vérification PDF: {e}")

            file_size = os.path.getsize(filepath) / 1024 / 1024
            print(f"  ✅ Téléchargement réussi! ({file_size:.1f} MB)")

            return filepath

        except requests.exceptions.Timeout:
            print(f"  ⏱️  Timeout avec {domain}")
        except requests.exceptions.ConnectionError:
            print(f"  🔌 Erreur de connexion avec {domain}")
        except Exception as e:
            print(f"  ❌ Erreur avec {domain}: {str(e)}")

    return None


def main():
    df = pd.read_parquet(PARQUET)
    filtered_df = df[
                (df['Publication_Year'] < 2020) &
                (df['Is_Open_Access'] == True) &
                (df['PDF_on_S3'] == False)
            ]

    # Extraire tous les DOIs
    dois = filtered_df['DOI'].dropna()  # Supprime les NaN
    dois = dois[dois != '']             # Supprime les chaînes vides
    dois = dois.tolist()                # Convertit en liste

    for doi in dois:
        print("=" * 60)
        print(f"🔍 Recherche de l'article: {doi}")
        print("=" * 60)

        filepath = download_scihub_article(doi)


        if filepath:
            print(f"✅ Article téléchargé avec succès!")
            print(f"📁 Fichier: {filepath}")
            print(f"📂 Chemin complet: {os.path.abspath(filepath)}")
        else:
            print(f"\n❌ Échec du téléchargement {doi}")
        print("_" * 60)


        # Petite pause entre les essais
        time.sleep(SLEEP)

if __name__ == "__main__":
    main()
