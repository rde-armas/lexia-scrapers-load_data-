#!/usr/bin/env python3
"""
Script simple para probar el parser de normas internacionales sin dependencias externas.
"""

import sys
from pathlib import Path

# Agregar el directorio actual al path para importar nuestros módulos
sys.path.insert(0, str(Path(__file__).parent))

def test_parser_with_file(html_file_path: str):
    """Prueba el parser con un archivo HTML específico."""
    try:
        # Importar solo lo necesario
        from international_norms_parser import InternationalNormsParser
        
        html_file = Path(html_file_path)
        if not html_file.exists():
            print(f"✗ Archivo no encontrado: {html_file}")
            return False
        
        print(f"📄 Probando parser con: {html_file.name}")
        print("=" * 50)
        
        # Leer el archivo con la codificación correcta
        html_content = html_file.read_text(encoding='iso-8859-1')
        
        # Crear el parser y procesar
        parser = InternationalNormsParser()
        parsed_data = parser.parse_norm_from_html(html_content)
        
        if parsed_data:
            print("✅ Parsing exitoso!")
            print(f"Título: {parsed_data.get('title', 'N/A')[:100]}...")
            
            # Mostrar metadatos
            metadata_info = []
            if parsed_data.get('number'):
                metadata_info.append(f"Número: {parsed_data['number']}")
            if parsed_data.get('type'):
                metadata_info.append(f"Tipo: {parsed_data['type']}")
            if parsed_data.get('year'):
                metadata_info.append(f"Año: {parsed_data['year']}")
            
            if metadata_info:
                print(f"Metadatos: {' | '.join(metadata_info)}")
            
            # Estadísticas
            stats = parser.get_parsing_stats(parsed_data)
            print(f"Artículos: {stats['articles']} | Anexos: {stats['annexes']} | Caracteres: {stats['total_chars']}")
            
            # Mostrar primer artículo
            if parsed_data.get('processed_articles'):
                first_article = parsed_data['processed_articles'][0]
                print(f"\n--- Primer Artículo ---")
                print(f"Número: {first_article.get('number')}")
                print(f"Título: {first_article.get('title')}")
                print(f"Texto: {first_article.get('text', '')[:150]}...")
                
            # Mostrar primer anexo si existe
            if parsed_data.get('annexes'):
                first_annex = parsed_data['annexes'][0]
                print(f"\n--- Primer Anexo ---")
                print(f"Título: {first_annex.get('title')}")
                print(f"Contenido: {first_annex.get('content', '')[:100]}...")
            
            return True
        else:
            print("✗ Falló el parsing")
            return False
            
    except ImportError as e:
        print(f"✗ Error de importación: {e}")
        print("Nota: Instalar dependencias con: pip install beautifulsoup4")
        return False
    except Exception as e:
        print(f"✗ Error inesperado: {e}")
        return False

def main():
    """Función principal."""
    print("🧪 Probando Parser de Normas Internacionales")
    print("=" * 60)
    
    # Probar con ambos archivos
    test_files = ['Ley.html', 'Decreto.html']
    results = []
    
    for test_file in test_files:
        success = test_parser_with_file(test_file)
        results.append((test_file, success))
        print("\n" + "=" * 60 + "\n")
    
    # Resumen
    print("📊 RESUMEN DE PRUEBAS")
    print("=" * 30)
    for filename, success in results:
        status = "✅ ÉXITO" if success else "❌ FALLO"
        print(f"{filename}: {status}")

if __name__ == "__main__":
    main()
