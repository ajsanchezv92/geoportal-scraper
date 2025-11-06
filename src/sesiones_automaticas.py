import asyncio
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
import aiohttp

class GestorSesiones:
    def __init__(self):
        self.sesion_activa = True
        self.inicio_sesion = datetime.now()
        self.duracion_sesion_horas = 2
        self.logger = self._configurar_logging()
        
    def _configurar_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - SESIONES - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('data/logs/sesiones.log'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)
    
    async def iniciar(self):
        """Inicia el sistema de sesiones automáticas"""
        self.logger.info("🚀 Iniciando gestor de sesiones automáticas")
        self.sesion_activa = True
        
        # Tarea de verificación de duración
        asyncio.create_task(self._verificar_duracion_sesion())
        
        # Tarea de monitoreo de recursos
        asyncio.create_task(self._monitorear_recursos())
    
    async def _verificar_duracion_sesion(self):
        """Verifica la duración de la sesión y programa reinicios"""
        while self.sesion_activa:
            tiempo_transcurrido = datetime.now() - self.inicio_sesion
            horas_transcurridas = tiempo_transcurrido.total_seconds() / 3600
            
            if horas_transcurridas >= self.duracion_sesion_horas:
                self.logger.info("🕒 Sesión completada, preparando reinicio...")
                await self._preparar_reinicio()
                break
            
            # Verificar cada 5 minutos
            await asyncio.sleep(300)
    
    async def _monitorear_recursos(self):
        """Monitorea el uso de recursos del sistema"""
        import psutil
        
        while self.sesion_activa:
            # Monitorear memoria
            memoria = psutil.virtual_memory()
            if memoria.percent > 85:
                self.logger.warning(f"⚠️  Uso de memoria alto: {memoria.percent}%")
            
            # Monitorear CPU
            cpu = psutil.cpu_percent(interval=1)
            if cpu > 80:
                self.logger.warning(f"⚠️  Uso de CPU alto: {cpu}%")
            
            await asyncio.sleep(60)  # Verificar cada minuto
    
    async def _preparar_reinicio(self):
        """Prepara el sistema para un reinicio elegante"""
        self.logger.info("🔄 Preparando reinicio de sesión...")
        
        # Guardar estado actual
        await self._guardar_estado_sesion()
        
        # Notificar otros componentes
        await self._notificar_reinicio()
    
    async def _guardar_estado_sesion(self):
        """Guarda el estado actual de la sesión"""
        estado = {
            'ultima_sesion': datetime.now().isoformat(),
            'duracion_configurada_horas': self.duracion_sesion_horas,
            'proxima_sesion': (datetime.now() + timedelta(minutes=5)).isoformat()
        }
        
        with open('data/checkpoints/estado_sesion.json', 'w') as f:
            import json
            json.dump(estado, f, indent=2)
        
        self.logger.info("💾 Estado de sesión guardado")
    
    async def _notificar_reinicio(self):
        """Notifica a otros componentes sobre el reinicio"""
        # Aquí se integraría con el scraper principal para parada elegante
        self.logger.info("📢 Notificando reinicio a componentes...")
    
    async def detener(self):
        """Detiene el gestor de sesiones"""
        self.sesion_activa = False
        self.logger.info("🛑 Gestor de sesiones detenido")
