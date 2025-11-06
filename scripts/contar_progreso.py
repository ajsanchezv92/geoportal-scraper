#!/usr/bin/env python3
"""
Script para contar y mostrar el progreso del scraping
"""
import json
from pathlib import Path
import sys

def contar_progreso():
    print("📊 CONTADOR DE PROGRESO - GEOPORTAL SCRAPER")
    print("=" * 50)
    
    # Contar archivos de resultados
    resultados_dir = Path('data/resultados')
    archivos_resultados = list(resultados_dir.glob('*.json'))
    
    total_estaciones = 0
    for archivo in archivos_resultados:
        with open(archivo, 'r', encoding='utf-8') as f:
            data = json.load(f)
            total_estaciones += len(data.get('estaciones', []))
    
    # Contar checkpoints
    checkpoints_dir = Path('data/checkpoints')
    archivos_checkpoints = list(checkpoints_dir.glob('*.json'))
    
    # Mostrar estadísticas
    print(f"📁 Archivos de resultados: {len(archivos_resultados)}")
    print(f"🏭 Estaciones procesadas: {total_estaciones}")
    print(f"💾 Checkpoints guardados: {len(archivos_checkpoints)}")
    print(f"📂 Backups disponibles: {len(list(Path('data/backups').glob('*.zip')))}")
    
    # Progreso estimado (si tenemos URLs totales)
    try:
        from src.url_manager import URLManager
        manager = URLManager()
        stats = manager.get_estadisticas_urls()
        
        print("\n🎯 PROGRESO DE URLs:")
        print(f"   Total URLs: {stats['total_urls']}")
        print(f"   Procesadas: {stats['procesadas']}")
        print(f"   Pendientes: {stats['pendientes']}")
        print(f"   Completado: {stats['porcentaje_completado']:.1f}%")
        
    except Exception as e:
        print(f"\n⚠️  No se pudo cargar información de URLs: {e}")

if __name__ == "__main__":
    contar_progreso()
