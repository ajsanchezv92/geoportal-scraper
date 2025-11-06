import aiohttp
import asyncio
import logging
from typing import List, Set
from pathlib import Path
import json
import csv

class URLManager:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.urls_procesadas: Set[str] = set()
        self.urls_pendientes: List[str] = []
        self._cargar_urls_procesadas()
    
    async def cargar_urls_desde_drive(self, drive_url: str) -> List[str]:
        """Carga URLs desde Google Drive"""
        self.logger.info(f"📥 Cargando URLs desde: {drive_url}")
        
        try:
            # Simulación - aquí iría la lógica real para descargar de Google Drive
            urls = await self._descargar_csv_drive(drive_url)
            self.urls_pendientes = urls
            self.logger.info(f"✅ {len(urls)} URLs cargadas desde Drive")
            return urls
            
        except Exception as e:
            self.logger.error(f"❌ Error cargando URLs: {str(e)}")
            return []
    
    async def _descargar_csv_drive(self, drive_url: str) -> List[str]:
        """Descarga y procesa CSV desde Google Drive"""
        async with aiohttp.ClientSession() as session:
            async with session.get(drive_url) as response:
                if response.status == 200:
                    content = await response.text()
                    return self._procesar_csv_content(content)
                else:
                    raise Exception(f"HTTP {response.status}")
    
    def _procesar_csv_content(self, content: str) -> List[str]:
        """Procesa contenido CSV para extraer URLs"""
        urls = []
        reader = csv.DictReader(content.splitlines())
        
        for row in reader:
            # Asumiendo que el CSV tiene columnas: emplazamiento, latitud, longitud
            emplazamiento = row.get('emplazamiento', '').strip()
            if emplazamiento:
                url = f"https://geoportal.minetur.gob.es/VCTEL/detalleEstacion.do?emplazamiento={emplazamiento}"
                urls.append(url)
        
        return urls
    
    def _cargar_urls_procesadas(self):
        """Carga URLs ya procesadas desde checkpoints"""
        try:
            checkpoint_files = Path('data/checkpoints').glob('*.json')
            for checkpoint_file in checkpoint_files:
                with open(checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if 'urls_procesadas' in data:
                        self.urls_procesadas.update(data['urls_procesadas'])
            
            self.logger.info(f"📊 {len(self.urls_procesadas)} URLs ya procesadas cargadas")
        except Exception as e:
            self.logger.warning(f"No se pudieron cargar URLs procesadas: {str(e)}")
    
    def filtrar_urls_pendientes(self) -> List[str]:
        """Filtra URLs pendientes de procesar"""
        pendientes = [url for url in self.urls_pendientes if url not in self.urls_procesadas]
        self.logger.info(f"🎯 {len(pendientes)} URLs pendientes de procesar")
        return pendientes
    
    def marcar_url_procesada(self, url: str):
        """Marca una URL como procesada"""
        self.urls_procesadas.add(url)
    
    def get_estadisticas_urls(self) -> dict:
        """Obtiene estadísticas de URLs"""
        return {
            'total_urls': len(self.urls_pendientes),
            'procesadas': len(self.urls_procesadas),
            'pendientes': len(self.urls_pendientes) - len(self.urls_procesadas),
            'porcentaje_completado': (len(self.urls_procesadas) / len(self.urls_pendientes)) * 100 if self.urls_pendientes else 0
        }
