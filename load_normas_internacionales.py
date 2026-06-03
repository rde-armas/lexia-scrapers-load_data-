#!/usr/bin/env python3
import asyncio
import argparse
import json
import os
import sys
import requests
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List

from international_norms_parser import InternationalNormsParser

# Configuración
API_URL = "http://api.lvh.me:3000/v1/norms"
BASE_DATA_PATH = Path(os.getenv("LEXIA_BRAIN_DATA_PATH", "./data"))
NORMS_HTML_DIR = BASE_DATA_PATH / "norms" / "html" / "international"
PROCESSED_JSON_DIR = BASE_DATA_PATH / "norms" / "processed_json"
PROCESSED_HTML_DIR = BASE_DATA_PATH / "norms" / "processed_html" / "international"
NORMS_LINKS_FILE = BASE_DATA_PATH / "norms_links.txt"

def format_norm_for_rails(norm_data: Dict[str, Any]) -> Dict[str, Any]:
    """Formatea los datos de norma para Rails."""
    articles_attributes = []
    if "processed_articles" in norm_data:
        for article in norm_data["processed_articles"]:
            articles_attributes.append({
                "number": article.get("number", 0),
                "title": article.get("title", ""),
                "text": article.get("text", ""),
                "precomputed_vectors": [
                    {
                        "chunk": article.get("text", ""),
                        "dense_vector": article.get("embedding", [])
                    }
                ] if article.get("embedding") else []
            })
    
    rails_data = {
        "country": norm_data.get("country", "INT"),
        "norm_id": norm_data.get("norm_id", 0),
        "norm_type": 4,  # ID numérico para Rails (international)
        "number": norm_data.get("number", 0),
        "year": norm_data.get("year", 0),
        "title": norm_data.get("title", ""),
        "hearings": norm_data.get("hearings", ""),
        "references": norm_data.get("references", ""),
        "signers": norm_data.get("signers", ""),
        "references_url": norm_data.get("references_url", ""),
        "impo_url": norm_data.get("impo_url", ""),
        "newspaper_image_url": norm_data.get("newspaper_image_url", ""),
        "promulgated_at": norm_data.get("promulgated_at", ""),
        "published_at": norm_data.get("published_at", ""),
        "articles_attributes": articles_attributes
    }
    
    return rails_data

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
        
        # Para normas internacionales, el scraper_id es 10
        scraper_id = 10
        
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

def parse_norm_from_html(html_content: str) -> Optional[Dict[str, Any]]:
    """Parsea el contenido HTML de una norma para extraer sus datos."""
    parser = InternationalNormsParser()
    result = parser.parse_norm_from_html(html_content)
    
    if result:
        # Asegurar que norm_type sea "international" (string) para JSON
        result["norm_type"] = "international"
    
    return result

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

async def fetch_and_process_norm_from_url(url: str) -> tuple[Optional[Dict[str, Any]], Optional[Path]]:
    """
    Obtiene y procesa el HTML de una norma desde una URL.
    
    Returns:
        tuple: (norm_data, html_file_path)
    """
    try:
        print(f"📥 Descargando HTML de: {url}")
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # Decodificar usando la codificación correcta
        html_content = response.content.decode('iso-8859-1')
        
        # Guardar el HTML descargado
        NORMS_HTML_DIR.mkdir(parents=True, exist_ok=True)
        url_hash = hash(url) % 1000000
        html_filename = f"norm_{url_hash}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        html_file_path = NORMS_HTML_DIR / html_filename
        
        with open(html_file_path, "w", encoding="iso-8859-1") as f:
            f.write(html_content)
        print(f"💾 HTML guardado: {html_filename}")

        # Parsear el HTML para extraer la estructura de la norma
        parsed_data = parse_norm_from_html(html_content)

        if not parsed_data:
            print(f"✗ No se pudo parsear el HTML de la URL: {url}")
            return None, html_file_path
        
        # Agregar la URL fuente
        parsed_data["source_url"] = url
        
        print(f"✅ Datos parseados: título='{parsed_data.get('title', 'N/A')}', artículos={len(parsed_data.get('articles', []))}")
        return parsed_data, html_file_path

    except requests.exceptions.RequestException as e:
        print(f"✗ Error de conexión descargando URL {url}: {e}")
        return None, None
    except Exception as e:
        print(f"✗ Error procesando URL {url}: {e}")
        return None, None

async def load_norms_for_period(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    norm_type: str = "international",
    limit: Optional[int] = None
):
    """
    Función principal simplificada para cargar normas internacionales.
    """
    print(f"🚀 Iniciando carga de normas internacionales")
    print(f"API: {API_URL}")
    print(f"Período: {start_date} - {end_date}")
    
    # Crear directorios necesarios
    NORMS_HTML_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_JSON_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_HTML_DIR.mkdir(parents=True, exist_ok=True)
    
    # Set country in norm_data later
    country = getattr(load_norms_for_period, "country", "INT")
    
    # Obtener URLs del scraper
    if start_date and end_date:
        scraped_urls = await scrape_norms_for_period(
            start_date, end_date, norm_type, NORMS_HTML_DIR
        )
    else:
        # Leer URLs del archivo de links
        scraped_urls = []
        if NORMS_LINKS_FILE.exists():
            with open(NORMS_LINKS_FILE, 'r') as f:
                scraped_urls = [line.strip() for line in f if line.strip()]
    
    if not scraped_urls:
        print("No hay URLs para procesar")
        return
    
    # Aplicar límite si se especifica
    if limit:
        scraped_urls = scraped_urls[:limit]
    
    print(f"Total de URLs a procesar: {len(scraped_urls)}")
    
    # Procesar URLs
    successful = 0
    failed = 0
    
    # Procesar URLs
    for i, url in enumerate(scraped_urls, 1):
        print(f"\n[{i}/{len(scraped_urls)}] Procesando URL: {url}")
        html_file_path = None
        
        try:
            # Obtener y procesar HTML de la URL
            norm_data, html_file_path = await fetch_and_process_norm_from_url(url)
            
            if not norm_data:
                print(f"✗ No se pudo procesar URL: {url}")
                failed += 1
                continue
            
            # Guardar JSON siempre (para debugging)
            try:
                url_hash = hash(url) % 1000000
                json_filename = f"norm_{url_hash}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                json_path = PROCESSED_JSON_DIR / json_filename
                
                # Add country
                norm_data["country"] = getattr(load_norms_for_period, "country", "INT")
                
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(norm_data, f, ensure_ascii=False, indent=2, default=str)
                print(f"💾 JSON guardado: {json_filename}")
            except Exception as save_error:
                print(f"⚠ Error guardando JSON para {url}: {save_error}")
            
            # Validación mínima
            if not norm_data.get("title"):
                print(f"⚠ Sin título en URL: {url} (JSON guardado para revisión)")
                failed += 1
                continue
            
            # Enviar al API
            try:
                rails_data = format_norm_for_rails(norm_data)
                api_success = send_norm(rails_data)
                
                if api_success:
                    print(f"✅ Norma enviada exitosamente al API")
                    successful += 1
                    
                    # Mover archivo HTML a la carpeta de procesados
                    if html_file_path and html_file_path.exists():
                        shutil.move(str(html_file_path), str(PROCESSED_HTML_DIR / html_file_path.name))
                        print(f"✓ HTML movido a procesados: {html_file_path.name}")
                else:
                    print(f"✗ Error enviando norma (JSON guardado para reintento)")
                    failed += 1
                        
            except Exception as api_error:
                print(f"✗ Error en API para URL {url}: {api_error}")
                failed += 1
                    
        except Exception as e:
            print(f"✗ Error general procesando URL {url}: {e}")
            failed += 1
    
    # Resumen final
    print(f"\n📊 Resumen de procesamiento:")
    print(f"Total procesadas: {successful + failed}")
    print(f"Exitosas: {successful}")
    print(f"Fallidas: {failed}")
    
    if successful > 0:
        print("✅ Procesamiento completado!")
    if failed > 0:
        print(f"⚠ {failed} normas fallaron. Revisar logs y archivos HTML preservados.")
    


def main():
    parser = argparse.ArgumentParser(description="Cargar normas internacionales al servidor Rails")
    parser.add_argument("--start-date", help="Fecha de inicio (DD/MM/YYYY)")
    parser.add_argument("--end-date", help="Fecha de fin (DD/MM/YYYY)")
    parser.add_argument("--norm-type", default="international", help="Tipo de norma (siempre international)")
    parser.add_argument("--limit", type=int, help="Límite de URLs a procesar")
    parser.add_argument("--country", default="INT", help="País de la norma (ej: INT, UY)")
    
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
    
    # Store country in the function object
    load_norms_for_period.country = args.country
    
    # Ejecutar el procesamiento
    asyncio.run(load_norms_for_period(
        start_date=start_date,
        end_date=end_date,
        norm_type=args.norm_type,
        limit=args.limit
    ))

if __name__ == "__main__":
    main()