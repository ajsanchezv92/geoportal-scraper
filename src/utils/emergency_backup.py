import asyncio
import time
import zipfile
from pathlib import Path
import logging

class EmergencyBackup:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    async def create_emergency_backup(self):
        """Crea un backup de emergencia"""
        timestamp = int(time.time())
        backup_file = f"data/backups/emergency_{timestamp}.zip"
        
        try:
            with zipfile.ZipFile(backup_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
                # Incluir archivos críticos
                critical_files = [
                    'data/checkpoints/',
                    'data/resultados/',
                    'config/config.json'
                ]
                
                for file_pattern in critical_files:
                    for file_path in Path('.').glob(file_pattern):
                        if file_path.is_file():
                            zipf.write(file_path)
                        elif file_path.is_dir():
                            for sub_file in file_path.rglob('*'):
                                if sub_file.is_file():
                                    zipf.write(sub_file)
            
            self.logger.info(f"🆘 Backup de emergencia creado: {backup_file}")
            return backup_file
            
        except Exception as e:
            self.logger.error(f"❌ Error creando backup de emergencia: {str(e)}")
            return None
