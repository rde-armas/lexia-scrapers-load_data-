"""
Utilidad para gestionar URLs procesadas y mantener limpio el archivo de links.
"""
from pathlib import Path
from typing import List

def remove_processed_urls_from_file(processed_urls: List[str], links_file: Path):
    """
    Elimina las URLs procesadas del archivo de links para evitar reprocesamiento.
    
    Args:
        processed_urls: Lista de URLs que fueron procesadas exitosamente
        links_file: Ruta al archivo de links (norms_links.txt)
    """
    if not links_file.exists() or not processed_urls:
        return
    
    try:
        # Leer todas las URLs del archivo
        with open(links_file, 'r', encoding='utf-8') as f:
            all_urls = [line.strip() for line in f if line.strip()]
        
        # Filtrar URLs procesadas
        remaining_urls = [url for url in all_urls if url not in processed_urls]
        
        # Reescribir el archivo con las URLs restantes
        with open(links_file, 'w', encoding='utf-8') as f:
            for url in remaining_urls:
                f.write(f"{url}\n")
        
        removed_count = len(all_urls) - len(remaining_urls)
        if removed_count > 0:
            print(f"🗑 Eliminadas {removed_count} URLs procesadas del archivo de links")
            print(f"📝 URLs restantes en archivo: {len(remaining_urls)}")
    
    except Exception as e:
        print(f"⚠ Error eliminando URLs del archivo de links: {e}")

def track_successful_url(url: str, processed_urls: List[str]):
    """
    Agrega una URL a la lista de URLs procesadas exitosamente.
    
    Args:
        url: URL que fue procesada exitosamente
        processed_urls: Lista donde agregar la URL
    """
    if url not in processed_urls:
        processed_urls.append(url)
