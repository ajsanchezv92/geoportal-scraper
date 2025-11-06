import aiohttp
import asyncio
import logging
from typing import List, Set
from pathlib import Path
import json
import re
import time

class URLManager:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.urls_procesadas: Set[str] = set()
        self.urls_pendientes: List[str] = []
        self._cargar_urls_procesadas()
    
    async def cargar_urls_desde_drive(self, drive_url: str) -> List[str]:
        """Carga URLs desde Google Drive - VERSIÓN SIMPLIFICADA PARA PRUEBAS"""
        self.logger.info(f"📥 Cargando URLs desde: {drive_url}")
        
        try:
            # SIMULACIÓN: Generar URLs de prueba basadas en el patrón que mostraste
            urls = self._generar_urls_prueba(100)
            self.urls_pendientes = urls
            self.logger.info(f"✅ {len(urls)} URLs de prueba generadas")
            
            # Mostrar ejemplos
            self.logger.info("🔍 Ejemplo de URLs generadas:")
            for i, url in enumerate(urls[:5]):
                self.logger.info(f"   {i+1}. {url}")
            
            return urls
            
        except Exception as e:
            self.logger.error(f"❌ Error: {str(e)}")
            return []
    
    def _generar_urls_prueba(self, cantidad: int) -> List[str]:
        """Genera URLs de prueba basadas en los ejemplos reales"""
        base_url = "https://geoportal.minetur.gob.es/VCTEL/detalleEstacion.do?emplazamiento="
        
        # Usar los IDs reales que me mostraste
        ids_reales = [
            "77689", "3100602", "0700022", "0700571", "0700158",
            "1200010", "1200011", "1200012", "0800150", "0800151"
        ]
        
        # Generar más IDs variados
        urls = []
        
        # Agregar IDs reales
        for emp_id in ids_reales:
            urls.append(base_url + emp_id)
        
        # Generar IDs adicionales con diferentes patrones
        for i in range(len(ids_reales), cantidad):
            # Diferentes patrones como en los ejemplos reales
            if i % 5 == 0:
                emp_id = f"77{i:03d}"  # Tipo 77689
            elif i % 5 == 1:
                emp_id = f"31{i:04d}"  # Tipo 3100602
            elif i % 5 == 2:
                emp_id = f"07{i:05d}"  # Tipo 0700022
            elif i % 5 == 3:
                emp_id = f"12{i:04d}"  # Tipo 1200010
            else:
                emp_id = f"08{i:04d}"  # Tipo 0800150
            
            urls.append(base_url + emp_id)
        
        return urls
    
    def _cargar_urls_procesadas(self):
        """Carga URLs ya procesadas desde checkpoints"""
        try:
            checkpoint_files = list(Path('data/checkpoints').glob('*.json'))
            urls_procesadas = set()
            
            for checkpoint_file in checkpoint_files:
                try:
                    with open(checkpoint_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                        if 'urls_procesadas_list' in data.get('stats', {}):
                            urls_procesadas.update(data['stats']['urls_procesadas_list'])
                except Exception as e:
                    self.logger.warning(f"⚠️  Error leyendo checkpoint: {e}")
            
            self.urls_procesadas = urls_procesadas
            self.logger.info(f"📊 {len(urls_procesadas)} URLs procesadas cargadas")
            
        except Exception as e:
            self.logger.warning(f"No se pudieron cargar URLs procesadas: {str(e)}")
            self.urls_procesadas = set()
    
    def filtrar_urls_pendientes(self) -> List[str]:
        """Filtra URLs pendientes de procesar"""
        pendientes = [url for url in self.urls_pendientes if url not in self.urls_procesadas]
        self.logger.info(f"🎯 {len(pendientes)} URLs pendientes de procesar")
        return pendientes
    
    def get_estadisticas_urls(self) -> dict:
        """Obtiene estadísticas de URLs"""
        total = len(self.urls_pendientes)
        procesadas = len(self.urls_procesadas)
        pendientes = total - procesadas
        porcentaje = (procesadas / total) * 100 if total > 0 else 0
        
        return {
            'total_urls': total,
            'procesadas': procesadas,
            'pendientes': pendientes,
            'porcentaje_completado': porcentaje
        }
