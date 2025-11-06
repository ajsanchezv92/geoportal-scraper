#!/usr/bin/env python3
"""
Script de prueba para verificar la extracción de URLs
"""
import sys
import os
import asyncio

# Agregar src al path para imports
current_dir = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.join(current_dir, 'src')
sys.path.insert(0, src_path)

print(f"🔍 Buscando módulos en: {src_path}")

# Verificar qué archivos existen
import os
if os.path.exists(src_path):
    files = os.listdir(src_path)
    print(f"📁 Archivos en src/: {files}")
else:
    print("❌ La carpeta src/ no existe")

try:
    from url_manager import URLManager
    print("✅ URLManager importado correctamente")
except ImportError as e:
    print(f"❌ Error importando URLManager: {e}")
    sys.exit(1)

async def test_url_extraction():
    print("\n🧪 TEST: Extracción de URLs")
    print("=" * 50)
    
    manager = URLManager()
    drive_url = "https://drive.google.com/file/d/1jcKPQHXLo1hbwAd2ucg60qmn66P1s8P6/view?usp=drive_link"
    
    print(f"📥 Probando carga de URLs...")
    
    urls = await manager.cargar_urls_desde_drive(drive_url)
    
    if urls:
        print(f"✅ SUCCESS: {len(urls)} URLs generadas")
        print("🔍 Primeras 5 URLs:")
        for i, url in enumerate(urls[:5]):
            print(f"   {i+1}. {url}")
        
        # Estadísticas
        stats = manager.get_estadisticas_urls()
        print(f"\n📊 ESTADÍSTICAS:")
        print(f"   Total URLs: {stats['total_urls']}")
        print(f"   Pendientes: {stats['pendientes']}")
        print(f"   Procesadas: {stats['procesadas']}")
        print(f"   Completado: {stats['porcentaje_completado']:.1f}%")
        
        # Probar filtrado
        pendientes = manager.filtrar_urls_pendientes()
        print(f"🎯 URLs pendientes después de filtrar: {len(pendientes)}")
        
        return True
    else:
        print("❌ FAIL: No se pudieron generar URLs")
        return False

if __name__ == "__main__":
    success = asyncio.run(test_url_extraction())
    if success:
        print("\n🎉 TEST COMPLETADO EXITOSAMENTE")
    else:
        print("\n💥 TEST FALLÓ")
        sys.exit(1)
