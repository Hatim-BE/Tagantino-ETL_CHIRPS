import os
import sys
import argparse
import logging
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import gzip
import shutil
from datetime import timedelta
from dateutil.relativedelta import relativedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import setup_logging

# Configuration avancée du logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(levelname)s][%(asctime)s][%(name)s] %(message)s',
    handlers=[
        logging.FileHandler('chirps_download.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('CHIRPSLoader')

CHIRPS_BASE_URLS = {
    'daily': "https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_daily/tifs/p05/",
    'monthly': "https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_monthly/tifs/"
}


def decompress_gz_file(gz_path, output_path):
    """
    Décompresse un fichier gzip.
    
    Args:
        gz_path (str): Chemin du fichier compressé.
        output_path (str): Chemin où sauvegarder le fichier décompressé.
        
    Returns:
        bool: True si la décompression a réussi, False sinon.
    """
    try:
        with gzip.open(gz_path, 'rb') as f_in:
            with open(output_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
                
        logger.info(f"Fichier décompressé avec succès: {output_path}")
        return True
        
    except Exception as e:
        logger.error(f"Erreur lors de la décompression de {gz_path}: {e}")
        return False
    

def validate_date(date_str, data_type):
    """Valide et parse la date avec gestion d'erreur détaillée"""
    try:
        if data_type == 'daily':
            return datetime.strptime(date_str, '%Y-%m-%d')
        return datetime.strptime(date_str, '%Y-%m')
    except ValueError as e:
        logger.critical(f"Date invalide: {date_str} ({e})")
        sys.exit(1)

def generate_paths(date_obj, data_type):
    """Génère les URLs et chemins avec gestion de la structure"""
    if data_type == 'daily':
        return (
            f"{date_obj.year}/",
            f"chirps-v2.0.{date_obj.year}.{date_obj.month:02d}.{date_obj.day:02d}.tif.gz"
        )
    return (
        "",
        f"chirps-v2.0.{date_obj.year}.{date_obj.month:02d}.tif.gz"
    )

def download_with_retry(url, dest_path, max_retries=3):
    """Téléchargement avec réessai et journalisation progressive"""
    for attempt in range(max_retries):
        try:
            with requests.get(url, stream=True, timeout=30) as r:
                if r.status_code == 404:
                    return False  # Fichier non trouvé
                r.raise_for_status()
                with open(dest_path, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
                logger.info(f"Téléchargement réussi: {os.path.basename(dest_path)}")
                return True
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.info(f"Fichier non trouvé: {os.path.basename(dest_path)}")
                return False
            logger.warning(f"Tentative {attempt+1}/{max_retries} échouée: {e}")
        except Exception as e:
            logger.warning(f"Tentative {attempt+1}/{max_retries} échouée: {e}")
    return False

def main():
    parser = argparse.ArgumentParser(prog='CHIRPS Downloader')
    parser.add_argument('--start-date', required=True)
    parser.add_argument('--end-date', help='Optionnel pour les plages de dates')
    parser.add_argument('--data-type', choices=['daily', 'monthly'], default='daily')
    parser.add_argument('--output-dir')
    parser.add_argument('--decompress', action='store_true')
    parser.add_argument('--count', type=int, help='Nombre de jours/mois à télécharger après start-date')
    parser.add_argument('--max-fails', type=int, default=3, help='Nombre d\'échecs consécutifs avant d\'arrêter le téléchargement indéfini')
    args = parser.parse_args()

    if not args.output_dir:
        args.output_dir = f'data/{args.data_type}'

    date_start = validate_date(args.start_date, args.data_type)
    
    if args.end_date:
        date_end = validate_date(args.end_date, args.data_type)
        indefinite_mode = False
    elif args.count:
        # Calcule date_end en fonction du nombre de jours/mois spécifié
        if args.data_type == 'daily':
            date_end = date_start + timedelta(days=args.count - 1)
        else:
            date_end = date_start + relativedelta(months=args.count - 1)
        logger.info(f"Téléchargement de {args.count} {args.data_type} à partir de {args.start_date}")
        indefinite_mode = False
    else:
        logger.info(f"Mode téléchargement indéfini à partir de {args.start_date} jusqu'à ce que les fichiers ne soient plus disponibles")
        date_end = None  # Will be determined dynamically
        indefinite_mode = True

    current_date = date_start
    consecutive_fails = 0
    
    # Définir une date limite pour éviter une boucle infinie (aujourd'hui + 30 jours par sécurité)
    max_future_date = datetime.now() + timedelta(days=30)
    
    while True:
        # Vérifier si on a atteint la date de fin en mode défini
        if not indefinite_mode and current_date > date_end:
            break
            
        # Vérifier qu'on ne dépasse pas une date future excessive
        if current_date > max_future_date:
            logger.warning(f"Arrêt du téléchargement : date future excessive atteinte ({current_date.strftime('%Y-%m-%d')})")
            break
            
        dir_path, file_name = generate_paths(current_date, args.data_type)
        full_url = f"{CHIRPS_BASE_URLS[args.data_type]}{dir_path}{file_name}"
        dest_dir = os.path.join(args.output_dir, dir_path)
        os.makedirs(dest_dir, exist_ok=True)

        success = download_with_retry(full_url, os.path.join(dest_dir, file_name))
        
        if success:
            consecutive_fails = 0
            if args.decompress:
                # Décompression avec vérification d'intégrité
                try:
                    compressed_file = os.path.join(dest_dir, file_name)
                    # Le nom du fichier décompressé (sans le .gz)
                    decompressed_file = compressed_file[:-3]
                    
                    with gzip.open(compressed_file, 'rb') as f_in:
                        with open(decompressed_file, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    
                    # Vérifier que le fichier décompressé existe et a une taille non nulle
                    if os.path.exists(decompressed_file) and os.path.getsize(decompressed_file) > 0:
                        logger.info(f"Décompression réussie: {os.path.basename(decompressed_file)}")
                        
                        # Optionnel: supprimer le fichier compressé après décompression
                        os.remove(compressed_file)
                    else:
                        logger.error(f"Échec de décompression: fichier vide ou inexistant - {os.path.basename(decompressed_file)}")
                except Exception as e:
                    logger.error(f"Erreur lors de la décompression de {file_name}: {e}")
        else:
            consecutive_fails += 1
            logger.warning(f"Échec du téléchargement {consecutive_fails}/{args.max_fails}")
            
            # En mode indéfini, arrêter après plusieurs échecs consécutifs
            if indefinite_mode and consecutive_fails >= args.max_fails:
                logger.info(f"Arrêt du téléchargement après {args.max_fails} échecs consécutifs")
                break
        
        # Incrémentation de la date
        if args.data_type == 'daily':
            current_date += timedelta(days=1)
        else:
            current_date += relativedelta(months=1)

if __name__ == '__main__':
    main()