# scripts/regenerar_urls_completas.py
"""
Regenera el archivo de URLs con TODAS las líneas sin eliminar duplicados
"""

from pathlib import Path
import re
from rich.console import Console

console = Console()

def regenerar_urls_completas():
    """Regenera el archivo con todas las URLs del archivo original"""
    
    archivo_original = Path("data_from_drive.txt")
    archivo_destino = Path("geoportal_links/geoportal_links_completas.txt")
    
    if not archivo_original.exists():
        console.print("[red]❌ No se encuentra data_from_drive.txt[/red]")
        return
    
    console.print("[cyan]📖 Leyendo archivo original...[/cyan]")
    
    urls_extraidas = 0
    lineas_procesadas = 0
    
    with open(archivo_original, 'r', encoding='utf-8', errors='ignore') as f_in:
        with open(archivo_destino, 'w', encoding='utf-8') as f_out:
            for linea in f_in:
                lineas_procesadas += 1
                linea = linea.strip()
                
                if not linea:
                    continue
                
                # Extraer URL de la línea (formato: URL|lat|lon)
                partes = linea.split('|')
                if len(partes) >= 1 and partes[0].startswith('https://geoportal.minetur.gob.es/VCTEL/detalleEstacion.do?emplazamiento='):
                    url = partes[0].strip()
                    f_out.write(url + '\n')
                    urls_extraidas += 1
                
                # Mostrar progreso cada 50,000 líneas
                if lineas_procesadas % 50000 == 0:
                    console.print(f"[yellow]📊 Procesadas {lineas_procesadas:,} líneas...[/yellow]")
    
    console.print(Panel.fit(
        f"[bold green]✅ ARCHIVO REGENERADO[/bold green]\n"
        f"[white]Líneas procesadas: {lineas_procesadas:,}\n"
        f"URLs extraídas: {urls_extraidas:,}\n"
        f"Archivo: {archivo_destino}[/white]",
        border_style="green"
    ))

if __name__ == "__main__":
    regenerar_urls_completas()
