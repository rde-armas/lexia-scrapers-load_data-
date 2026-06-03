#!/usr/bin/env python3
"""
Script simple para cargar sentencias al servidor Rails.
Puede procesar archivos existentes o scraping de períodos específicos.
"""

import asyncio
import os
import json
import asyncio
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any
import requests
import shutil

from sentence_html_ingestor import SentenceHTMLIngestor

# Configuración
#API_URL = "https://lexia.uy/v1/sentences"
API_URL = "http://localhost:3000/v1/sentences"
BASE_DATA_PATH = Path(os.getenv("LEXIA_BRAIN_DATA_PATH", "./data"))
SENTENCES_HTML_DIR = BASE_DATA_PATH / "sentences" / "html"
PROCESSED_JSON_DIR = BASE_DATA_PATH / "sentences" / "json"
PROCESSED_HTML_DIR = BASE_DATA_PATH / "sentences" / "processed_html"

# Configuración de chunking (compatible con sentence_processor.py)
MAX_TOKENS_CHUNKING = 512
OVERLAP = 0.20

def format_sentence_for_rails(sentence_data: Dict[str, Any]) -> Dict[str, Any]:
    """Formatea los datos para Rails."""
    return {
        "number": sentence_data.get("number"),
        "court_name": sentence_data.get("court"),
        "importance": sentence_data.get("importance"),
        "sentence_type": sentence_data.get("sentence_type"),
        "date": sentence_data.get("date"),
        "file_number": sentence_data.get("file_number"),
        "procedure": sentence_data.get("procedure"),
        "subjects": sentence_data.get("subjects", []),
        "summary": sentence_data.get("summary"),
        "text": sentence_data.get("text"),
        "raw_text": sentence_data.get("raw_text"),
        "judges": sentence_data.get("judges", []),
        "parties": sentence_data.get("parties", []),
        "compliance_data": sentence_data.get("compliance_data", {}),
        "legal_basis": sentence_data.get("legal_basis", []),
        "country_code": sentence_data.get("country_code"),
        "jurisdiction": sentence_data.get("jurisdiction"),
        "legal_effects": sentence_data.get("legal_effects"),
        "instance": sentence_data.get("instance"),
        "outcome": sentence_data.get("outcome"),
        "parent_id": sentence_data.get("parent_id"),
        "country": "UY",
        "court": sentence_data.get("court"),
        "descriptors": sentence_data.get("descriptors", []),
        "precomputed_vectors": sentence_data.get("precomputed_vectors", [])
    }

def send_sentence(sentence_data: Dict[str, Any]) -> bool:
    """Envía una sentencia a la API."""
    payload = {"sentence": format_sentence_for_rails(sentence_data)}
    headers = {"Content-Type": "application/json"}
    
    sentence_id = sentence_data.get("id", "unknown")
    print(f"Enviando sentencia {sentence_id}...")
    
    try:
        response = requests.post(API_URL, json=payload, headers=headers, timeout=30)
        
        if response.ok:
            print(f"✓ Sentencia {sentence_id} enviada exitosamente")
            return True
        else:
            print(f"✗ Error {response.status_code}: {response.text}")
            return False
            
    except Exception as e:
        print(f"✗ Error de conexión: {e}")
        return False

async def scrape_sentences_for_period(
    start_date: datetime, end_date: datetime, output_dir: Path
) -> List[Path]:
    """
    Scraping de sentencias para un período específico.
    Compatible con sentence_processor.py
    """
    print(f"🔍 Iniciando scraping para período: {start_date.strftime('%d/%m/%Y')} a {end_date.strftime('%d/%m/%Y')}")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # Importar scraper solo cuando sea necesario
        from scraper import sentences_scraper
        
        await sentences_scraper.scrape_sentences(
            session_start_date=start_date,
            session_end_date=end_date,
            output_dir=output_dir,
        )
        
        scraped_files = list(output_dir.glob("*.html"))
        print(f"✓ Scraping completado. Encontrados {len(scraped_files)} archivos")
        return scraped_files
        
    except ImportError:
        print("⚠ Módulo 'scraper' no disponible. Solo se procesarán archivos existentes.")
        return []
    except Exception as e:
        print(f"✗ Error durante scraping: {e}")
        return []

def process_existing_files(html_dir: Path) -> List[Path]:
    """Procesa archivos HTML existentes en el directorio."""
    if not html_dir.exists():
        print(f"✗ Directorio no existe: {html_dir}")
        return []
    
    html_files = list(html_dir.glob("*.html"))
    if not html_files:
        print("No se encontraron archivos HTML")
        return []
    
    print(f"Encontrados {len(html_files)} archivos HTML existentes")
    return html_files

async def load_sentences_for_period(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    process_existing: bool = True,
    limit: Optional[int] = None
):
    """
    Carga sentencias para un período específico o procesa archivos existentes.
    
    Args:
        start_date: Fecha de inicio para scraping (opcional)
        end_date: Fecha de fin para scraping (opcional)
        process_existing: Si procesar archivos existentes en el directorio
        limit: Límite de archivos a procesar
    """
    print("🚀 Iniciando carga de sentencias al servidor Rails")
    print(f"API: {API_URL}")
    print(f"Directorio: {SENTENCES_HTML_DIR}")
    
    # Crear directorios necesarios
    SENTENCES_HTML_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_JSON_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_HTML_DIR.mkdir(parents=True, exist_ok=True)
    
    # Crear ingestor CON embeddings
    ingestor = SentenceHTMLIngestor(
        chunking_params={
            "max_tokens": MAX_TOKENS_CHUNKING,
            "overlap": OVERLAP
        },
        force_chunking=True  # Activar embeddings
    )
    
    html_files = []
    
    # Scraping de período específico si se proporcionan fechas
    if start_date and end_date:
        scraped_files = await scrape_sentences_for_period(
            start_date, end_date, SENTENCES_HTML_DIR
        )
        html_files.extend(scraped_files)
    
    # Procesar archivos existentes si se solicita
    if process_existing:
        existing_files = process_existing_files(SENTENCES_HTML_DIR)
        html_files.extend(existing_files)
    
    if not html_files:
        print("No hay archivos para procesar")
        return
    
    # Eliminar duplicados manteniendo el orden
    html_files = list(dict.fromkeys(html_files))
    
    # Aplicar límite si se especifica
    if limit is not None:
        print(f"Aplicando límite de procesamiento: {limit} archivos de {len(html_files)} total")
        html_files = html_files[:limit]
        
    print(f"Total de archivos a procesar: {len(html_files)}")
    
    # Procesar archivos
    successful = 0
    failed = 0
    
    for html_file in html_files:
        print(f"\nProcesando: {html_file.name}")
        
        try:
            # Procesar HTML
            sentence_data = ingestor.ingest_file(html_file)
            
            if not sentence_data:
                print(f"✗ No se pudo procesar {html_file.name}")
                failed += 1
                continue
            
            # Validar campos requeridos
            if not sentence_data.get("number") or not sentence_data.get("court"):
                print(f"✗ Faltan campos requeridos en {html_file.name}")
                failed += 1
                continue
            
            # Enviar a API
            if send_sentence(sentence_data):
                successful += 1
                # Guardar JSON y eliminar HTML
                try:
                    # Guardar datos procesados como JSON
                    json_filename = html_file.stem + ".json"
                    json_path = PROCESSED_JSON_DIR / json_filename
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump(sentence_data, f, ensure_ascii=False, indent=2, default=str)
                    print(f"✓ JSON guardado: {json_filename}")
                    
                    # Mover archivo HTML a la carpeta de procesados
                    shutil.move(str(html_file), str(PROCESSED_HTML_DIR / html_file.name))
                    print(f"✓ HTML movido a procesados: {html_file.name}")
                except Exception as e:
                    print(f"⚠ Error al guardar JSON o eliminar HTML: {e}")
            else:
                failed += 1
                
        except Exception as e:
            print(f"✗ Error procesando {html_file.name}: {e}")
            failed += 1
    
    # Resumen
    print(f"\n{'='*50}")
    print(f"RESUMEN DE PROCESAMIENTO")
    print(f"{'='*50}")
    print(f"Exitosos: {successful}")
    print(f"Fallidos: {failed}")
    print(f"Total: {successful + failed}")
    if successful + failed > 0:
        success_rate = (successful / (successful + failed)) * 100
        print(f"Tasa de éxito: {success_rate:.1f}%")

def main():
    """Función principal con opciones de línea de comandos."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Cargar sentencias al servidor Rails")
    parser.add_argument(
        "--start-date", 
        type=str, 
        help="Fecha de inicio para scraping (formato: DD/MM/YYYY)"
    )
    parser.add_argument(
        "--end-date", 
        type=str, 
        help="Fecha de fin para scraping (formato: DD/MM/YYYY)"
    )
    parser.add_argument(
        "--no-existing", 
        action="store_true", 
        help="No procesar archivos existentes, solo hacer scraping"
    )
    parser.add_argument(
        "--limit", 
        type=int, 
        help="Límite de archivos a procesar"
    )
    
    args = parser.parse_args()
    
    start_date = None
    end_date = None
    
    # Parsear fechas si se proporcionan
    if args.start_date:
        try:
            start_date = datetime.strptime(args.start_date, "%d/%m/%Y")
        except ValueError:
            print("✗ Formato de fecha de inicio inválido. Use DD/MM/YYYY")
            return
    
    if args.end_date:
        try:
            end_date = datetime.strptime(args.end_date, "%d/%m/%Y")
        except ValueError:
            print("✗ Formato de fecha de fin inválido. Use DD/MM/YYYY")
            return
    
    # Validar que ambas fechas estén presentes si se usa scraping
    if (start_date and not end_date) or (end_date and not start_date):
        print("✗ Debe proporcionar tanto fecha de inicio como de fin para scraping")
        return
    
    if start_date and end_date and start_date > end_date:
        print("✗ La fecha de inicio debe ser anterior a la fecha de fin")
        return
    
    process_existing = not args.no_existing
    
    # Ejecutar procesamiento
    try:
        asyncio.run(load_sentences_for_period(
            start_date=start_date,
            end_date=end_date,
            process_existing=process_existing,
            limit=args.limit
        ))
    except KeyboardInterrupt:
        print("\n⚠ Interrumpido por el usuario")
    except Exception as e:
        print(f"\n❌ Error: {e}")

if __name__ == "__main__":
    main()