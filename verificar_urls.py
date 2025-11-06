# scripts/verificar_urls.py
"""
Verifica y compara la cantidad de URLs
"""

from pathlib import Path

def contar_lineas_archivo(ruta_archivo):
    """Cuenta líneas no vacías en un archivo"""
    with open(ruta_archivo, 'r', encoding='utf-8') as f:
        return sum(1 for line in f if line.strip())

# Contar URLs en diferentes archivos
archivo_original = "data_from_drive.txt"
archivo_actual = "geoportal_links/geoportal_links_1.txt"
archivo_nuevo = "geoportal_links/geoportal_links_completas.txt"

print(f"📊 CONTEO DE URLs:")
print(f"data_from_drive.txt: {contar_lineas_archivo(archivo_original):,} líneas")
print(f"geoportal_links_1.txt: {contar_lineas_archivo(archivo_actual):,} URLs")
