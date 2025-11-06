import asyncio
import time
import logging
from pathlib import Path
import json
import shutil
from datetime import datetime, timedelta
import os

class SistemaGuardado:
    def __init__(self):
        self.activo = True
        self.intervalo_minutos = 10
        self.max_backups = 5
        self.logger = self._configurar_logging()
        self.ultimo_guardado = None
        
    def _configurar_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - GUARDADO - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('data/logs/guardado.log'),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(__name__)
    
    async def iniciar(self):
        """Inicia el sistema de guardado automático"""
        self.logger.info("💾 Iniciando sistema de guardado automático")
        self.activo = True
        
        # Crear directorio de backups si no existe
        Path('data/backups').mkdir(parents=True, exist_ok=True)
        
        # Iniciar loop de guardado
        asyncio.create_task(self._loop_guardado())
        
        # Iniciar limpieza automática
        asyncio.create_task(self._limpieza_automatica())
    
    async def _loop_guardado(self):
        """Loop principal de guardado automático"""
        while self.activo:
            try:
                await self._realizar_guardado()
                self.logger.info(f"✅ Guardado automático completado - Próximo en {self.intervalo_minutos} min")
                
                # Esperar hasta el próximo guardado
                await asyncio.sleep(self.intervalo_minutos * 60)
                
            except Exception as e:
                self.logger.error(f"❌ Error en guardado automático: {str(e)}")
                await asyncio.sleep(60)  # Reintentar en 1 minuto
    
    async def _realizar_guardado(self):
        """Realiza un guardado completo del sistema"""
        timestamp = int(time.time())
        
        # 1. Guardar checkpoint de datos
        await self._guardar_checkpoint_datos(timestamp)
        
        # 2. Crear backup de archivos críticos
        await self._crear_backup_completo(timestamp)
        
        # 3. Verificar integridad de datos
        await self._verificar_integridad()
        
        # 4. Limpiar backups antiguos
        await self._limpiar_backups_antiguos()
        
        self.ultimo_guardado = datetime.now()
    
    async def _guardar_checkpoint_datos(self, timestamp):
        """Guarda checkpoint de datos de scraping"""
        checkpoint_data = {
            'timestamp': timestamp,
            'fecha_iso': datetime.now().isoformat(),
            'tipo': 'checkpoint_automatico',
            'datos': self._obtener_estado_actual()
        }
        
        checkpoint_file = f'data/checkpoints/auto_checkpoint_{timestamp}.json'
        with open(checkpoint_file, 'w', encoding='utf-8') as f:
            json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)
        
        self.logger.info(f"📁 Checkpoint guardado: {checkpoint_file}")
    
    async def _crear_backup_completo(self, timestamp):
        """Crea un backup completo del sistema"""
        backup_dir = f'data/backups/backup_{timestamp}'
        Path(backup_dir).mkdir(parents=True, exist_ok=True)
        
        # Copiar archivos críticos
        archivos_criticos = [
            'data/resultados/centros_2_repeticiones.json',
            'data/checkpoints/',
            'config/config.json'
        ]
        
        for archivo in archivos_criticos:
            try:
                if os.path.isdir(archivo):
                    shutil.copytree(archivo, f'{backup_dir}/{Path(archivo).name}')
                else:
                    shutil.copy2(archivo, backup_dir)
            except Exception as e:
                self.logger.warning(f"No se pudo copiar {archivo}: {str(e)}")
        
        # Comprimir backup
        shutil.make_archive(backup_dir, 'zip', backup_dir)
        shutil.rmtree(backup_dir)  # Eliminar directorio sin comprimir
        
        self.logger.info(f"📦 Backup creado: {backup_dir}.zip")
    
    async def _verificar_integridad(self):
        """Verifica la integridad de los datos guardados"""
        try:
            # Verificar archivos de resultados
            resultados_files = list(Path('data/resultados').glob('*.json'))
            for file in resultados_files:
                with open(file, 'r', encoding='utf-8') as f:
                    json.load(f)  # Verificar que es JSON válido
            
            self.logger.info("✅ Integridad de datos verificada")
            
        except Exception as e:
            self.logger.error(f"❌ Error en verificación de integridad: {str(e)}")
    
    async def _limpiar_backups_antiguos(self):
        """Limpia backups antiguos manteniendo solo los más recientes"""
        backup_files = sorted(Path('data/backups').glob('backup_*.zip'))
        
        if len(backup_files) > self.max_backups:
            # Eliminar los más antiguos
            for old_backup in backup_files[:-self.max_backups]:
                old_backup.unlink()
                self.logger.info(f"🗑️  Backup antiguo eliminado: {old_backup.name}")
    
    async def _limpieza_automatica(self):
        """Limpieza automática de archivos temporales antiguos"""
        while self.activo:
            try:
                # Limpiar checkpoints muy antiguos (>7 días)
                cutoff_time = time.time() - (7 * 24 * 60 * 60)
                checkpoint_files = Path('data/checkpoints').glob('*.json')
                
                for checkpoint in checkpoint_files:
                    if checkpoint.stat().st_mtime < cutoff_time:
                        checkpoint.unlink()
                        self.logger.info(f"🧹 Checkpoint antiguo eliminado: {checkpoint.name}")
                
                await asyncio.sleep(3600)  # Verificar cada hora
                
            except Exception as e:
                self.logger.error(f"Error en limpieza automática: {str(e)}")
                await asyncio.sleep(300)
    
    def _obtener_estado_actual(self):
        """Obtiene el estado actual del sistema para el checkpoint"""
        return {
            'estado': 'activo',
            'ultima_verificacion': datetime.now().isoformat(),
            'archivos_resultados': len(list(Path('data/resultados').glob('*.json'))),
            'tamaño_datos_mb': self._calcular_tamaño_datos()
        }
    
    def _calcular_tamaño_datos(self):
        """Calcula el tamaño total de los datos"""
        total_size = 0
        for dir_path in ['data/resultados', 'data/checkpoints']:
            for file_path in Path(dir_path).rglob('*'):
                if file_path.is_file():
                    total_size += file_path.stat().st_size
        return round(total_size / (1024 * 1024), 2)
    
    async def detener(self):
        """Detiene el sistema de guardado"""
        self.activo = False
        # Realizar un último guardado antes de detenerse
        await self._realizar_guardado()
        self.logger.info("🛑 Sistema de guardado detenido")
