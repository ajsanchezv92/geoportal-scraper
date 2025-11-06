#!/usr/bin/env python3
"""
Script para analizar resultados del scraping
"""
import json
from pathlib import Path
from collections import Counter
import statistics

def analizar_resultados():
    print("🔍 ANALIZADOR DE RESULTADOS - GEOPORTAL SCRAPER")
    print("=" * 50)
    
    resultados_dir = Path('data/resultados')
    archivos_resultados = list(resultados_dir.glob('*.json'))
    
    if not archivos_resultados:
        print("❌ No se encontraron archivos de resultados")
        return
    
    todas_estaciones = []
    for archivo in archivos_resultados:
        with open(archivo, 'r', encoding='utf-8') as f:
            data = json.load(f)
            todas_estaciones.extend(data.get('estaciones', []))
    
    print(f"📊 Total de estaciones analizadas: {len(todas_estaciones)}")
    
    # Análisis de tecnologías
    tecnologias = []
    operadores = []
    provincias = []
    
    for estacion in todas_estaciones:
        # Tecnologías
        tech_data = estacion.get('infraestructura_tecnologica', {}).get('resumen_tecnologico', {})
        tecnologias.extend(tech_data.get('tecnologias_activas', []))
        
        # Operadores
        ops_data = estacion.get('caracteristicas_estacion', {}).get('operadores_activos', [])
        for op in ops_data:
            operadores.append(op.get('nombre', 'Desconocido'))
        
        # Provincias
        geo_data = estacion.get('informacion_geografica', {}).get('direccion', {})
        provincias.append(geo_data.get('provincia', 'Desconocida'))
    
    # Mostrar análisis
    print("\n📡 DISTRIBUCIÓN DE TECNOLOGÍAS:")
    tech_counter = Counter(tecnologias)
    for tech, count in tech_counter.most_common():
        print(f"   {tech}: {count} estaciones")
    
    print("\n🏢 DISTRIBUCIÓN DE OPERADORES:")
    op_counter = Counter(operadores)
    for op, count in op_counter.most_common():
        print(f"   {op}: {count} apariciones")
    
    print("\n🗺️  DISTRIBUCIÓN POR PROVINCIAS:")
    prov_counter = Counter(provincias)
    for prov, count in prov_counter.most_common(10):  # Top 10
        print(f"   {prov}: {count} estaciones")

if __name__ == "__main__":
    analizar_resultados()
