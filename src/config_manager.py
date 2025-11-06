import json
from pathlib import Path
from typing import Dict, Any
import logging

class ConfigManager:
    def __init__(self, config_path: str = "config/config.json"):
        self.config_path = Path(config_path)
        self.logger = logging.getLogger(__name__)
        self._ensure_config_exists()
    
    def _ensure_config_exists(self):
        """Asegura que el archivo de configuración existe"""
        if not self.config_path.exists():
            self._create_default_config()
    
    def _create_default_config(self):
        """Crea configuración por defecto"""
        default_config = {
            "scraper": {
                "max_workers": 8,
                "batch_size": 25,
                "timeout": 25,
                "checkpoint_interval": 3,
                "max_retries": 3,
                "retry_delay": 2,
                "request_delay": 0.1,
                "random_delay": True,
                "connection_pool_size": 12,
                "progress_update_interval": 50,
                "memory_check_interval": 25
            },
            "guardado": {
                "intervalo_minutos": 10,
                "max_backups": 5,
                "limpieza_dias": 7,
                "tamaño_maximo_mb": 25
            },
            "sesiones": {
                "duracion_horas": 2,
                "verificacion_minutos": 5
            },
            "urls": {
                "geoportal_base": "https://geoportal.minetur.gob.es/VCTEL/detalleEstacion.do?emplazamiento=",
                "timeout_conexion": 30
            },
            "logging": {
                "level": "INFO",
                "max_file_size_mb": 10,
                "backup_count": 5
            }
        }
        
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(default_config, f, indent=2, ensure_ascii=False)
        
        self.logger.info("✅ Configuración por defecto creada")
    
    def load_config(self) -> Dict[str, Any]:
        """Carga la configuración desde el archivo"""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"Error cargando configuración: {str(e)}")
            return self._create_default_config()
    
    def update_config(self, new_config: Dict[str, Any]):
        """Actualiza la configuración"""
        current_config = self.load_config()
        current_config.update(new_config)
        
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(current_config, f, indent=2, ensure_ascii=False)
        
        self.logger.info("✅ Configuración actualizada")
    
    def get_scraper_config(self):
        """Obtiene configuración específica del scraper"""
        config = self.load_config()
        return config.get('scraper', {})
    
    def get_guardado_config(self):
        """Obtiene configuración específica de guardado"""
        config = self.load_config()
        return config.get('guardado', {})
