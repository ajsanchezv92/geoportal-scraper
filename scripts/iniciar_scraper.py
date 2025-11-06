#!/usr/bin/env python3
"""
SCRIPT PRINCIPAL DE INICIO - GeoScrape Sentinel
Sistema completo de scraping resiliente para Geoportal Minetur

Características:
✅ Inicia todos los componentes del sistema
✅ Detecta y reanuda desde checkpoints automáticamente  
✅ Manejo elegante de paradas (Ctrl+C)
✅ Supervivencia a reinicios y cierres
✅ Nunca pierde el progreso
"""

import asyncio
import signal
import sys
import time
from pathlib import Path

# Agregar src al path para imports
src_path = Path(__file__).parent.parent / 'src'
sys.path.append(str(src_path))

try:
    from scraper_principal import GeoportalScraper, ScraperConfig
    from guardado_automatico import SistemaGuardado
    from sesiones_automaticas import GestorSesiones
    from url_manager import URLManager
    from config_manager import ConfigManager
    print("✅ Todos los módulos importados correctamente")
except ImportError as e:
    print(f"❌ Error importando módulos: {e}")
    print("💡 Asegúrate de que todos los archivos estén en la estructura correcta")
    sys.exit(1)


class IniciadorSentinel:
    """Clase principal que orchesta todo el sistema de scraping"""
    
    def __init__(self):
        self.scraper = None
        self.guardado = None
        self.sesiones = None
        self.url_manager = None
        self.config_manager = ConfigManager()
        self.ejecucion_activa = True
        
    async def inicializar_sistema(self):
        """Inicializa todos los componentes del sistema"""
        print("\n" + "="*60)
        print("🛡️  INICIANDO GEOSCRAPE SENTINEL")
        print("="*60)
        
        # Cargar configuración
        config_data = self.config_manager.load_config()
        scraper_config_data = config_data.get('scraper', {})
        
        # Crear configuración del scraper
        config = ScraperConfig(
            max_workers=scraper_config_data.get('max_workers', 8),
            batch_size=scraper_config_data.get('batch_size', 25),
            timeout=scraper_config_data.get('timeout', 25),
            checkpoint_interval=scraper_config_data.get('checkpoint_interval', 3),
            max_retries=scraper_config_data.get('max_retries', 3),
            retry_delay=scraper_config_data.get('retry_delay', 2),
            request_delay=scraper_config_data.get('request_delay', 0.1),
            random_delay=scraper_config_data.get('random_delay', True),
            connection_pool_size=scraper_config_data.get('connection_pool_size', 12),
            progress_update_interval=scraper_config_data.get('progress_update_interval', 50),
            memory_check_interval=scraper_config_data.get('memory_check_interval', 25)
        )
        
        # Inicializar componentes
        self.scraper = GeoportalScraper(config)
        self.guardado = SistemaGuardado()
        self.sesiones = GestorSesiones()
        self.url_manager = URLManager()
        
        print("✅ Sistema inicializado correctamente")
        return True
    
    async def cargar_urls(self):
        """Carga las URLs desde Google Drive"""
        print("\n📥 CARGANDO URLs DESDE GOOGLE DRIVE...")
        
        # URL del archivo CSV en Google Drive
        drive_url = "https://drive.google.com/file/d/1jcKPQHXLo1hbwAd2ucg60qmn66P1s8P6/view?usp=drive_link"
        
        urls = await self.url_manager.cargar_urls_desde_drive(drive_url)
        
        if not urls:
            print("❌ No se pudieron cargar las URLs desde Google Drive")
            return False
        
        print(f"✅ {len(urls)} URLs cargadas correctamente")
        return urls
    
    async verificar_checkpoints(self):
        """Verifica y carga checkpoints existentes"""
        print("\n🔍 BUSCANDO CHECKPOINTS ANTERIORES...")
        
        checkpoint_files = list(Path('data/checkpoints').glob('*.json'))
        if checkpoint_files:
            # Encontrar el checkpoint más reciente
            latest_checkpoint = max(checkpoint_files, key=lambda x: x.stat().st_mtime)
            print(f"✅ Checkpoint encontrado: {latest_checkpoint.name}")
            print("🔄 El sistema reanudará desde el último estado guardado")
            return True
        else:
            print("ℹ️  No se encontraron checkpoints anteriores")
            print("🚀 Iniciando nueva ejecución desde cero")
            return False
    
    async def iniciar_servicios_secundarios(self):
        """Inicia los servicios de guardado y sesiones en segundo plano"""
        print("\n🔄 INICIANDO SERVICIOS EN SEGUNDO PLANO...")
        
        # Iniciar sistema de guardado automático
        await self.guardado.iniciar()
        print("✅ Sistema de guardado automático iniciado")
        
        # Iniciar gestor de sesiones automáticas
        await self.sesiones.iniciar()
        print("✅ Gestor de sesiones automáticas iniciado")
        
        print("💡 Servicios secundarios activos y monitoreando")
    
    async def ejecutar_scraping_principal(self, urls):
        """Ejecuta el scraping principal"""
        print("\n🎯 INICIANDO SCRAPING PRINCIPAL...")
        
        # Filtrar URLs pendientes
        urls_pendientes = self.url_manager.filtrar_urls_pendientes()
        
        if not urls_pendientes:
            print("✅ No hay URLs pendientes - scraping completado!")
            return True
        
        estadisticas = self.url_manager.get_estadisticas_urls()
        print(f"📊 ESTADÍSTICAS INICIALES:")
        print(f"   • URLs totales: {estadisticas['total_urls']}")
        print(f"   • URLs procesadas: {estadisticas['procesadas']}")
        print(f"   • URLs pendientes: {estadisticas['pendientes']}")
        print(f"   • Progreso: {estadisticas['porcentaje_completado']:.1f}%")
        
        # Calcular tiempo estimado (asumiendo ~25 URLs/minuto)
        tiempo_estimado_minutos = estadisticas['pendientes'] / 25
        horas = int(tiempo_estimado_minutos // 60)
        minutos = int(tiempo_estimado_minutos % 60)
        
        print(f"⏱️  TIEMPO ESTIMADO: {horas}h {minutos}m")
        print(f"🚀 INICIANDO CON {self.scraper.config.max_workers} WORKERS...")
        
        # Ejecutar scraping
        await self.scraper.ejecutar_scraping(urls_pendientes)
        
        return True
    
    def configurar_manejo_señales(self):
        """Configura el manejo elegante de señales (Ctrl+C)"""
        def manejar_señal(sig, frame):
            print(f"\n🛑 Señal {sig} recibida - Iniciando parada elegante...")
            self.ejecucion_activa = False
            asyncio.create_task(self.parada_elegante())
        
        # Registrar manejadores de señales
        signal.signal(signal.SIGINT, manejar_señal)   # Ctrl+C
        signal.signal(signal.SIGTERM, manejar_señal)  # Terminación
        print("✅ Manejadores de señales configurados (Ctrl+C para parada elegante)")
    
    async def parada_elegante(self):
        """Realiza una parada elegante de todo el sistema"""
        print("\n" + "="*50)
        print("🛑 INICIANDO PARADA ELEGANTE")
        print("="*50)
        
        # Detener componentes en orden
        if self.scraper:
            print("⏸️  Deteniendo scraper principal...")
            self.scraper.parada_elegante()
        
        if self.guardado:
            print("💾 Deteniendo sistema de guardado...")
            await self.guardado.detener()
        
        if self.sesiones:
            print("🔒 Deteniendo gestor de sesiones...")
            await self.sesiones.detener()
        
        print("✅ Parada elegante completada")
        print("📁 El progreso ha sido guardado y puede reanudarse posteriormente")
    
    async def ejecutar(self):
        """Método principal de ejecución"""
        try:
            # 1. Inicializar sistema
            if not await self.inicializar_sistema():
                return False
            
            # 2. Configurar manejo de señales
            self.configurar_manejo_señales()
            
            # 3. Verificar checkpoints existentes
            await self.verificar_checkpoints()
            
            # 4. Cargar URLs
            urls = await self.cargar_urls()
            if not urls:
                return False
            
            # 5. Iniciar servicios secundarios
            await self.iniciar_servicios_secundarios()
            
            # 6. Ejecutar scraping principal
            await self.ejecutar_scraping_principal(urls)
            
            # 7. Parada final elegante
            await self.parada_elegante()
            
            print("\n" + "="*50)
            print("🎉 SCRAPING COMPLETADO EXITOSAMENTE!")
            print("="*50)
            return True
            
        except Exception as e:
            print(f"\n❌ ERROR CRÍTICO: {e}")
            print("💡 Intentando parada de emergencia...")
            await self.parada_elegante()
            return False


async def main():
    """Función principal"""
    iniciador = IniciadorSentinel()
    exito = await iniciador.ejecutar()
    
    if exito:
        print("\n✅ GeoScrape Sentinel finalizado correctamente")
        sys.exit(0)
    else:
        print("\n❌ GeoScrape Sentinel encontró errores")
        sys.exit(1)


if __name__ == "__main__":
    # Verificar que existe la estructura de directorios
    directorios_necesarios = ['data/checkpoints', 'data/resultados', 'data/logs', 'data/backups', 'config']
    for directorio in directorios_necesarios:
        Path(directorio).mkdir(parents=True, exist_ok=True)
    
    # Ejecutar sistema
    asyncio.run(main())
