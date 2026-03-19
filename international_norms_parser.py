"""
Parser especializado para normas internacionales.

Este módulo contiene la lógica específica para extraer datos de las normas
internacionales desde HTML, incluyendo título, metadatos, artículos y anexos.
"""

import re
from typing import Dict, Any, Optional, List
from bs4 import BeautifulSoup


class InternationalNormsParser:
    """Parser para normas internacionales desde HTML."""
    
    def __init__(self):
        # Patrones de expresiones regulares para parsing (más flexibles)
        # Patrón para artículos: maneja "Artículo X", "Art. X", "Art X" con números o romanos
        self.article_pattern = r'(?i)(Artículo|Art\.?)\s+(\d+|[IVXLCDM]+)\s*[-–]?\s*([^\n]*?)\n(.*?)(?=\n\s*(?:Artículo|Art\.?|ANEXO|PARTE|$))'
        # Patrón para anexos
        self.annex_pattern = r'(?i)(ANEXO\s+[IVXLCDM]+|ANEXO)\s*\n\s*([^\n]*)\n(.*?)(?=\n\s*ANEXO|$)'
        # Patrón para partes
        self.part_pattern = r'(?i)(PARTE\s+[IVXLCDM]+)\s*\n\s*([^\n]*)\n'
    
    def parse_norm_from_html(self, html_content: str) -> Optional[Dict[str, Any]]:
        """
        Parsea el contenido HTML de una norma internacional para extraer sus datos.
        
        Args:
            html_content: Contenido HTML de la norma
            
        Returns:
            Diccionario con los datos extraídos o None si falla el parsing
        """
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extraer título principal
            title = self._extract_title(soup)
            
            # Extraer metadatos (información de aprobación)
            metadata = self._extract_metadata(soup)
            
            # Obtener todo el texto del contenido principal
            main_content = self._extract_main_content(soup)
            
            if not main_content:
                print("✗ No se pudo extraer el contenido principal del HTML.")
                return None
            
            # Extraer artículos
            articles = self._extract_articles(main_content)
            
            # Extraer anexos
            annexes = self._extract_annexes(main_content)
            
            # Construir el resultado
            norm_data = {
                "title": title,
                "number": metadata.get("number", ""),
                "year": metadata.get("year"),
                "approval_info": metadata.get("approval_info", ""),
                "impo_url": metadata.get("impo_url", ""),
                "processed_articles": articles,
                "annexes": annexes,
                "full_content": main_content[:1000] + "..." if len(main_content) > 1000 else main_content
            }
            
            return norm_data
            
        except Exception as e:
            print(f"✗ Error parseando HTML: {e}")
            return None
    
    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Extrae el título principal de la norma."""
        # Buscar el título en la estructura específica <div class="alert alert-info"><h1>...</h1></div>
        alert_div = soup.find('div', class_='alert-info')
        if alert_div:
            title_tag = alert_div.find('h1')
            if title_tag:
                return ' '.join(title_tag.get_text(separator=' ', strip=True).split())
        
        # Fallback al método original si no se encuentra la estructura esperada
        title_tag = soup.find('h1', style=re.compile(r'font-size:18px'))
        if title_tag:
            return ' '.join(title_tag.get_text(separator=' ', strip=True).split())

        return ""
    
    def _extract_metadata(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extrae metadatos como número de ley, decreto, año, etc."""
        metadata = {}

        # Iterar sobre todas las etiquetas <pre class="italica"> para encontrar la correcta
        approval_text = ''
        for tag in soup.find_all('pre', class_='italica'):
            if 'Aprobado/a por:' in tag.get_text():
                approval_text = tag.get_text(strip=True)
                break

        metadata["approval_info"] = approval_text

        # Extraer impo_url del enlace linkFicha dentro del <pre>
        link_ficha = soup.find('a', class_='linkFicha')
        if link_ficha and link_ficha.get('href'):
            href = link_ficha.get('href')
            if href.startswith('/'):
                metadata["impo_url"] = f"https://www.impo.com.uy{href}"
            else:
                metadata["impo_url"] = href

        # Regex mejoradas para capturar número y fecha, manejando 'N°', 'Nº', 'N ' o 'N.'
        # Ejemplo: Ley N° 20.367 de 23/09/2024
        # Ejemplo: Decreto N° 36/024 de 01/01/2024
        law_match = re.search(r'Ley N[°º]?\.?\s*([\d.,]+)', approval_text, re.IGNORECASE)
        decree_match = re.search(r'Decreto N[°º]?\.?\s*([\d./]+)', approval_text, re.IGNORECASE)
        date_match = re.search(r'de (\d{1,2}/\d{1,2}/\d{4})', approval_text)

        if law_match:
            metadata["number"] = law_match.group(1).replace('.', '')
            metadata["type"] = "ley"
        elif decree_match:
            metadata["number"] = decree_match.group(1)
            metadata["type"] = "decreto"

        if date_match:
            date_str = date_match.group(1)
            try:
                metadata["year"] = int(date_str.split('/')[-1])
            except (ValueError, IndexError):
                pass

        return metadata
    
    def _extract_main_content(self, soup: BeautifulSoup) -> str:
        """Extrae el contenido principal de la norma."""
        # Buscar todos los elementos pre que contienen el contenido
        pre_elements = soup.find_all('pre')
        
        # Filtrar el pre con clase italica (metadatos) pero mantener el último italica si contiene notas
        content_pres = []
        for pre in pre_elements:
            classes = pre.get('class', [])
            if 'italica' not in classes:
                content_pres.append(pre)
            elif 'Notas:' in pre.get_text() or 'Ampliar información' in pre.get_text():
                # Incluir notas finales
                content_pres.append(pre)
        
        if not content_pres:
            return ""
        
        # Combinar todo el texto de los elementos pre
        full_text = ""
        for pre in content_pres:
            text = pre.get_text()
            if text.strip():  # Solo agregar si tiene contenido
                full_text += text + "\n\n"
        
        return full_text.strip()
    
    def _extract_articles(self, content: str) -> List[Dict[str, Any]]:
        """Extrae los artículos de la norma."""
        articles = []
        
        # Usar regex para encontrar artículos
        matches = re.findall(self.article_pattern, content, re.DOTALL | re.MULTILINE)
        
        for match in matches:
            article_type = match[0]  # "Artículo" o "Art."
            article_number = match[1]
            article_title = match[2].strip() if match[2].strip() else f"Artículo {article_number}"
            article_text = match[3].strip()
            
            # Limpiar el texto del artículo
            article_text = self._clean_text(article_text)
            
            # Si el título está vacío, usar el número como título
            if not article_title or article_title == "-":
                article_title = f"Artículo {article_number}"
            
            articles.append({
                "number": article_number,
                "title": article_title,
                "text": article_text,
                "type": "article"
            })
        
        return articles
    
    def _extract_annexes(self, content: str) -> List[Dict[str, Any]]:
        """Extrae los anexos de la norma."""
        annexes = []
        
        # Usar regex para encontrar anexos
        matches = re.findall(self.annex_pattern, content, re.DOTALL | re.MULTILINE)
        
        for match in matches:
            annex_title = match[0].strip()  # "ANEXO I", "ANEXO II", etc.
            annex_subtitle = match[1].strip()  # Subtítulo del anexo
            annex_content = match[2].strip()
            
            # Limpiar el contenido del anexo
            annex_content = self._clean_text(annex_content)
            
            annexes.append({
                "title": annex_title,
                "subtitle": annex_subtitle,
                "content": annex_content,
                "type": "annex"
            })
        
        return annexes
    
    def _clean_text(self, text: str) -> str:
        """Limpia y normaliza el texto extraído."""
        if not text:
            return ""
        
        # Preservar saltos de línea importantes pero normalizar espacios
        lines = text.split('\n')
        cleaned_lines = []
        
        for line in lines:
            cleaned_line = re.sub(r'\s+', ' ', line.strip())
            if cleaned_line:  # Solo agregar líneas no vacías
                cleaned_lines.append(cleaned_line)
        
        # Unir las líneas con saltos de línea
        return '\n'.join(cleaned_lines)
    
    def get_parsing_stats(self, parsed_data: Dict[str, Any]) -> Dict[str, int]:
        """Retorna estadísticas del parsing realizado."""
        if not parsed_data:
            return {"articles": 0, "annexes": 0, "total_chars": 0}
        
        return {
            "articles": len(parsed_data.get("processed_articles", [])),
            "annexes": len(parsed_data.get("annexes", [])),
            "total_chars": len(parsed_data.get("full_content", ""))
        }
