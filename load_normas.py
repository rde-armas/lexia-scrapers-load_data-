#!/usr/bin/env python3
"""
Script para cargar normas (leyes) al servidor Rails.

Funcionalidades:
- Scraping de normas por período de fechas
- Procesamiento de archivos JSON existentes
- Generación de embeddings para artículos
- Envío al API de Rails
- Eliminación de archivos procesados y guardado de JSONs
"""

import asyncio
import argparse
import json
import os
import requests
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List

from norms_json_ingestor import NormsJsonIngestor
from url_manager import remove_processed_urls_from_file, track_successful_url

# Configuración
API_URL = "http://api.lvh.me:3000/v1/norms"
BASE_DATA_PATH = Path(os.getenv("LEXIA_BRAIN_DATA_PATH", "./data"))
NORMS_JSON_DIR = BASE_DATA_PATH / "norms" / "json"
PROCESSED_JSON_DIR = BASE_DATA_PATH / "norms" / "processed_json"
NORMS_LINKS_FILE = BASE_DATA_PATH / "norms_links.txt"


SCRAPER_TASK_CONFIG = {
    "code": 8,
    "law": 5,
    "decree": 6
}

API_NORMTYPE_IDS_RAILS = {
    "law": 2,
    "decree": 3,
    "code": 1,
}

def format_norm_for_rails(norm_data: Dict[str, Any]) -> Dict[str, Any]:
    """Formatea los datos de norma para Rails."""
    articles_attributes = []
    if "processed_articles" in norm_data:
        for article in norm_data["processed_articles"]:
            articles_attributes.append({
                "number": article.get("number", 0),
                "title": article.get("title", ""),
                "titles": article.get("titles", ""),  # Campo adicional del controlador Rails
                "notes": article.get("notes", ""),
                "references": article.get("references", ""),
                "signers": article.get("signers", ""),
                "text": article.get("text", ""),
                "references_url": article.get("references_url", ""),
                "impo_url": article.get("impo_url", ""),
                "long_embeddings_attributes": article.get("long_embeddings_attributes", [])
            })
    
    formatted_data = {
        "norm_id": norm_data.get("norm_id"),
        "norm_type": norm_data.get("norm_type"),
        "number": norm_data.get("number"),
        "year": norm_data.get("year"),
        "title": norm_data.get("title", ""),
        "hearings": norm_data.get("hearings", ""),
        "references": norm_data.get("references", ""),
        "signers": norm_data.get("signers", ""),
        "references_url": norm_data.get("references_url", ""),
        "impo_url": norm_data.get("impo_url", ""),
        "newspaper_image_url": norm_data.get("newspaper_image_url", ""),
        "promulgated_at": norm_data.get("promulgated_at"),
        "published_at": norm_data.get("published_at"),
        "articles_attributes": articles_attributes
    }
    
    return formatted_data

def send_norm(norm_data: Dict[str, Any]) -> bool:
    """Envía una norma al API de Rails."""
    try:
        formatted_data = format_norm_for_rails(norm_data)
        payload = {"norm": formatted_data}
        headers = {"Content-Type": "application/json"}
        
        response = requests.post(API_URL, json=payload, headers=headers, timeout=30)
        
        if response.status_code in [200, 201]:
            return True
        else:
            print(f"✗ Error del servidor: {response.status_code} - {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"✗ Error de conexión: {e}")
        return False
    except Exception as e:
        print(f"✗ Error inesperado: {e}")
        return False

async def scrape_norms_for_period(
    start_date: datetime, 
    end_date: datetime, 
    norm_type: str,
    output_dir: Path
) -> List[str]:
    """
    Scraping de normas para un período específico.
    Retorna lista de URLs scrapeadas.
    """
    print(f"🔍 Iniciando scraping de {norm_type} para período: {start_date.strftime('%d/%m/%Y')} a {end_date.strftime('%d/%m/%Y')}")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # Importar scraper solo cuando sea necesario
        from scraper import impo_scraper
        
        scraper_id = SCRAPER_TASK_CONFIG.get(norm_type)
        if not scraper_id:
            print(f"⚠ Tipo de norma no soportado: {norm_type}")
            return []
        
        # Limpiar archivo de links existente
        if NORMS_LINKS_FILE.exists():
            NORMS_LINKS_FILE.unlink()
        
        # Ejecutar scraper
        await impo_scraper.scrape_norms(start_date, end_date, scraper_id)
        
        # Leer URLs scrapeadas
        urls = []
        if NORMS_LINKS_FILE.exists():
            with open(NORMS_LINKS_FILE, 'r', encoding='utf-8') as f:
                urls = [line.strip() for line in f if line.strip()]
        
        print(f"✓ Scraping completado. Encontradas {len(urls)} URLs")
        return urls
        
    except ImportError:
        print("⚠ Módulo 'scraper' no disponible. Solo se procesarán archivos existentes.")
        return []
    except Exception as e:
        print(f"✗ Error durante scraping: {e}")
        return []

def process_existing_json_files(json_dir: Path) -> List[Path]:
    """Procesa archivos JSON existentes en el directorio."""
    if not json_dir.exists():
        print(f"✗ Directorio no existe: {json_dir}")
        return []
    
    json_files = list(json_dir.glob("*.json"))
    if not json_files:
        print("No se encontraron archivos JSON")
        return []
    
    print(f"Encontrados {len(json_files)} archivos JSON existentes")
    return json_files

async def fetch_and_process_norm_from_url(url: str, ingestor: NormsJsonIngestor) -> Optional[Dict[str, Any]]:
    """Obtiene JSON de una URL y lo procesa."""
    try:
        # Convertir URL a formato JSON si es necesario
        json_url = url if "?json=true" in url else url.rstrip("/") + "?json=true"
        
        print(f"📥 Obteniendo JSON de: {json_url}")
        response = requests.get(json_url, timeout=30)
        response.raise_for_status()
        
        norm_json_content = response.json()
        
        temp_json_path = NORMS_JSON_DIR / f"temp_{datetime.now().timestamp()}.json"
        temp_json_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(temp_json_path, "w", encoding="utf-8") as f:
            json.dump(norm_json_content, f, indent=2, ensure_ascii=False)
        
        norm_data = ingestor.ingest_file(temp_json_path)
        
        if temp_json_path.exists():
            temp_json_path.unlink()
            
        return norm_data
        
    except Exception as e:
        print(f"✗ Error procesando URL {url}: {e}")
        return None

async def load_norms_for_period(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    norm_type: str = "international",
    process_existing: bool = True
):
    """
    Función principal para cargar normas al servidor Rails.
    """
    print(f"🚀 Iniciando carga de normas al servidor Rails")
    print(f"API: {API_URL}")
    print(f"Directorio JSON: {NORMS_JSON_DIR}")
    print(f"Tipo de norma: {norm_type}")
    
    # Crear directorios necesarios
    NORMS_JSON_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_JSON_DIR.mkdir(parents=True, exist_ok=True)
    
    ingestor = NormsJsonIngestor()
    
    urls_to_process = []
    json_files_to_process = []
    
    if start_date and end_date:
        scraped_urls = await scrape_norms_for_period(
            start_date, end_date, norm_type, NORMS_JSON_DIR
        )
        urls_to_process.extend(scraped_urls)
    
    # Procesar archivos JSON existentes si se solicita
    if process_existing:
        existing_files = process_existing_json_files(NORMS_JSON_DIR)
        json_files_to_process.extend(existing_files)
    
    if not urls_to_process and not json_files_to_process:
        print("No hay URLs ni archivos para procesar")
        return
    
    print(f"Total de URLs a procesar: {len(urls_to_process)}")
    print(f"Total de archivos JSON a procesar: {len(json_files_to_process)}")
    
    # Procesar URLs y archivos
    successful = 0
    failed = 0
    processed_urls = []  # Rastrear URLs procesadas exitosamente
    
    # Procesar URLs scrapeadas
    for url in urls_to_process:
        print(f"\nProcesando URL: {url}")
        
        try:
            # Obtener y procesar JSON de la URL
            norm_data = await fetch_and_process_norm_from_url(url, ingestor)
            
            if not norm_data:
                print(f"✗ No se pudo procesar URL: {url}")
                failed += 1
                continue
            
            try:
                url_hash = hash(url) % 1000000
                json_filename = f"norm_{url_hash}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                json_path = PROCESSED_JSON_DIR / json_filename
                
                # Asignar tipo de norma para Rails antes de guardar
                norm_data["norm_type"] = API_NORMTYPE_IDS_RAILS.get(norm_type)
                norm_data["source_url"] = url  # Agregar URL fuente para referencia
                
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(norm_data, f, ensure_ascii=False, indent=2, default=str)
                print(f"💾 JSON guardado: {json_filename}")
            except Exception as save_error:
                print(f"⚠ Error guardando JSON para {url}: {save_error}")
            
            # Validar campos mínimos (al menos uno de los identificadores principales)
            if not any([norm_data.get("number"), norm_data.get("title"), norm_data.get("impo_url")]):
                print(f"✗ Faltan campos identificadores mínimos en URL: {url} (JSON guardado para revisión)")
                failed += 1
                continue
            
            # Validar tipo de norma
            if not norm_data["norm_type"]:
                print(f"✗ Tipo de norma no válido: {norm_type} para URL: {url} (JSON guardado para revisión)")
                failed += 1
                continue
            
            # Enviar a API
            if send_norm(norm_data):
                successful += 1
                print(f"✓ Norma enviada exitosamente: {norm_data.get('number', 'Sin número')} - {norm_data.get('title', 'Sin título')[:50]}...")
            else:
                failed += 1
                print(f"✗ Error enviando norma: {norm_data.get('number', 'Sin número')} (JSON guardado para reintento)")
                
        except Exception as e:
            print(f"✗ Error procesando URL {url}: {e}")
            failed += 1
    
    # Procesar archivos JSON existentes
    for json_file in json_files_to_process:
        print(f"\nProcesando archivo: {json_file.name}")
        
        try:
            # Procesar JSON
            norm_data = ingestor.ingest_file(json_file)
            
            if not norm_data:
                print(f"✗ No se pudo procesar {json_file.name}")
                failed += 1
                continue
            
            try:
                norm_data["norm_type"] = API_NORMTYPE_IDS_RAILS.get(norm_type)
                norm_data["source_file"] = json_file.name
                
                # Guardar datos procesados como JSON
                json_filename = json_file.stem + "_processed.json"
                json_path = PROCESSED_JSON_DIR / json_filename
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(norm_data, f, ensure_ascii=False, indent=2, default=str)
                print(f"💾 JSON guardado: {json_filename}")
                
                # Eliminar archivo original
                json_file.unlink()
                print(f"🗑 Archivo original eliminado: {json_file.name}")
            except Exception as save_error:
                print(f"⚠ Error guardando JSON para {json_file.name}: {save_error}")
            
            # Validar campos mínimos (al menos uno de los identificadores principales)
            if not any([norm_data.get("number"), norm_data.get("title"), norm_data.get("impo_url")]):
                print(f"✗ Faltan campos identificadores mínimos en {json_file.name} (JSON guardado para revisión)")
                failed += 1
                continue
            
            # Validar tipo de norma
            if not norm_data["norm_type"]:
                print(f"✗ Tipo de norma no válido: {norm_type} para {json_file.name} (JSON guardado para revisión)")
                failed += 1
                continue
            
            # Enviar a API
            if send_norm(norm_data):
                successful += 1
                print(f"✓ Norma enviada exitosamente: {norm_data.get('number', 'Sin número')} - {norm_data.get('title', 'Sin título')[:50]}...")
            else:
                failed += 1
                print(f"✗ Error enviando norma: {norm_data.get('number', 'Sin número')} (JSON guardado para reintento)")
                
        except Exception as e:
            print(f"✗ Error procesando {json_file.name}: {e}")
            failed += 1
    
    # Resumen
    total = successful + failed
    print(f"\n📊 Resumen de procesamiento:")
    print(f"Total procesadas: {total}")
    print(f"Exitosas: {successful}")
    print(f"Fallidas: {failed}")
    
    if failed > 0:
        print(f"⚠ {failed} normas fallaron. Revisar logs para detalles.")
    else:
        print("✅ Todas las normas fueron procesadas exitosamente!")
    
    # Eliminar URLs procesadas exitosamente del archivo de links
    if processed_urls and start_date and end_date:  # Solo si se hizo scraping
        remove_processed_urls_from_file(processed_urls, NORMS_LINKS_FILE)

def main():
    parser = argparse.ArgumentParser(description="Cargar normas al servidor Rails")
    parser.add_argument("--start-date", help="Fecha de inicio (DD/MM/YYYY)")
    parser.add_argument("--end-date", help="Fecha de fin (DD/MM/YYYY)")
    parser.add_argument("--norm-type", default="law", 
                       choices=list(SCRAPER_TASK_CONFIG.keys()),
                       help="Tipo de norma a procesar")
    parser.add_argument("--all-types", action="store_true",
                       help="Procesar todos los tipos de normas (code, law, decree)")
    parser.add_argument("--no-existing", action="store_true", 
                       help="No procesar archivos JSON existentes")
    
    args = parser.parse_args()
    
    start_date = None
    end_date = None
    
    if args.start_date and args.end_date:
        try:
            start_date = datetime.strptime(args.start_date, "%d/%m/%Y")
            end_date = datetime.strptime(args.end_date, "%d/%m/%Y")
        except ValueError:
            print("✗ Formato de fecha inválido. Use DD/MM/YYYY")
            return
    elif args.start_date or args.end_date:
        print("✗ Debe proporcionar ambas fechas: --start-date y --end-date")
        return
    
    process_existing = not args.no_existing
    
    # Determinar qué tipos de normas procesar
    if args.all_types:
        norm_types_to_process = list(SCRAPER_TASK_CONFIG.keys())
        print(f"🔄 Procesando todos los tipos de normas: {', '.join(norm_types_to_process)}")
    else:
        norm_types_to_process = [args.norm_type]
    
    # Función async para procesar todos los tipos
    async def process_all_types():
        total_successful = 0
        total_failed = 0
        
        for norm_type in norm_types_to_process:
            print(f"\n{'='*60}")
            print(f"🚀 Procesando tipo de norma: {norm_type.upper()}")
            print(f"{'='*60}")
            
            try:
                await load_norms_for_period(
                    start_date=start_date,
                    end_date=end_date,
                    norm_type=norm_type,
                    process_existing=process_existing
                )
            except Exception as e:
                print(f"✗ Error procesando tipo {norm_type}: {e}")
        
        if args.all_types:
            print(f"\n{'='*60}")
            print(f"📊 RESUMEN FINAL - TODOS LOS TIPOS")
            print(f"{'='*60}")
            print(f"Tipos procesados: {', '.join(norm_types_to_process)}")
            print(f"✅ Procesamiento de todos los tipos completado")
    
    # Ejecutar el procesamiento
    asyncio.run(process_all_types())

if __name__ == "__main__":
    main()