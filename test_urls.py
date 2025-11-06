#!/usr/bin/env python3
"""
Script de prueba para verificar la extracción de URLs
"""
import sys
from pathlib import Path

# Agregar src al path para imports
src_path = Path(__file__).parent.parent / 'src'
sys.path.append(str(src_path))

from url_manager import URLManager
import asyncio

async def test_url_extraction():
    print("🧪 TEST: Extracción de URLs desde Google Drive")
    print("=" * 50)
    
    manager = URLManager()
    drive_url = "https://drive.google.com/file/d/1jcKPQHXLo1hbwAd2ucg60qmn66P1s8P6/view?usp=drive_link"
    
    print(f"📥 Descargando de: {drive_url}")
    
    urls = await manager.cargar_urls_desde_drive(drive_url)
    
    if urls:
        print(f"✅ SUCCESS: {len(urls)} URLs encontradas")
        print("🔍 Primeras 10 URLs:")
        for i, url in enumerate(urls[:10]):
            print(f"   {i+1}. {url}")
        
        # Estadísticas
        stats = manager.get_estadisticas_urls()
        print(f"\n📊 ESTADÍSTICAS:")
        print(f"   Total URLs: {stats['total_urls']}")
        print(f"   Pendientes: {stats['pendientes']}")
        print(f"   Procesadas: {stats['procesadas']}")
    else:
        print("❌ FAIL: No se pudieron extraer URLs")

if __name__ == "__main__":
    asyncio.run(test_url_extraction())
