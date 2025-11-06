import aiohttp
import asyncio
import logging
from typing import List, Set
from pathlib import Path
import json
import csv
import re

class URLManager:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.urls_procesadas: Set[str] = set()
        self.urls_pendientes: List[str] = []
        self._cargar_urls_procesadas()
    
    async def cargar_urls_desde_drive(self, drive_url: str) -> List[str]:
        """Carga URLs desde Google Drive real"""
        self.logger.info(f"📥 Cargando URLs desde: {drive_url}")
        
        try:
            # Descargar el contenido real de Google Drive
            contenido = await self._descargar_contenido_drive(drive_url)
            if not contenido:
                self.logger.error("❌ No se pudo descargar el contenido de Google Drive")
                return []
            
            # Extraer URLs del contenido
            urls = self._extraer_urls_del_contenido(contenido)
            
            if not urls:
                self.logger.error("❌ No se encontraron URLs en el documento")
                return []
                
            self.urls_pendientes = urls
            self.logger.info(f"✅ {len(urls)} URLs reales cargadas desde Google Drive")
            return urls
            
        except Exception as e:
            self.logger.error(f"❌ Error cargando URLs desde Google Drive: {str(e)}")
            return []
    
    async def _descargar_contenido_drive(self, drive_url: str) -> str:
        """Descarga el contenido del archivo de Google Drive"""
        try:
            # Convertir la URL de visualización a URL de descarga directa
            file_id = self._extraer_file_id(drive_url)
            if not file_id:
                self.logger.error("❌ No se pudo extraer el ID del archivo de Google Drive")
                return ""
            
            # URL de descarga directa
            download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(download_url) as response:
                    if response.status == 200:
                        contenido = await response.text()
                        self.logger.info(f"✅ Contenido descargado: {len(contenido)} caracteres")
                        return contenido
                    else:
                        self.logger.error(f"❌ Error HTTP {response.status} al descargar")
                        return ""
                        
        except Exception as e:
            self.logger.error(f"❌ Error descargando de Google Drive: {str(e)}")
            return ""
    
    def _extraer_file_id(self, drive_url: str) -> str:
        """Extrae el file ID de la URL de Google Drive"""
        # Patrones comunes de URLs de Google Drive
        patrones = [
            r'/file/d/([a-zA-Z0-9_-]+)',
            r'id=([a-zA-Z0-9_-]+)',
            r'([a-zA-Z0-9_-]{25,})'
        ]
        
        for patron in patrones:
            match = re.search(patron, drive_url)
            if match:
                return match.group(1)
        
        return ""
    
    def _extraer_urls_del_contenido(self, contenido: str) -> List[str]:
        """Extrae URLs del contenido descargado"""
        urls = []
        base_url = "https://geoportal.minetur.gob.es/VCTEL/detalleEstacion.do?emplazamiento="
        
        # Buscar patrones de URLs en el contenido
        # Patrón: emplazamiento=NUMERO (donde NUMERO puede tener de 1 a 10 dígitos)
        patrones = [
            r'emplazamiento=(\d{1,10})',  # emplazamiento=123456
            r'detalleEstacion\.do\?emplazamiento=(\d{1,10})',  # URL completa
            r'https://geoportal\.minetur\.gob\.es/VCTEL/detalleEstacion\.do\?emplazamiento=(\d{1,10})'  # URL completa con protocolo
        ]
        
        for patron in patrones:
            matches = re.findall(patron, contenido)
            for emp_id in matches:
                url_completa = base_url + emp_id
                if url_completa not in urls:
                    urls.append(url_completa)
        
        # También buscar líneas que contengan el formato que mostraste
        lineas = contenido.split('\n')
        for linea in lineas:
            # Buscar el patrón: emplazamiento=XXXXX| (con | después del número)
            match = re.search(r'emplazamiento=(\d{1,10})\|', linea)
            if match:
                emp_id = match.group(1)
                url_completa = base_url + emp_id
                if url_completa not in urls:
                    urls.append(url_completa)
            
            # Buscar URLs completas en la línea
            match = re.search(r'https://geoportal\.minetur\.gob\.es/VCTEL/detalleEstacion\.do\?emplazamiento=(\d{1,10})', linea)
            if match:
                url_completa = match.group(0)
                if url_completa not in urls:
                    urls.append(url_completa)
        
        self.logger.info(f"🔍 Encontradas {len(urls)} URLs únicas en el documento")
        return urls
    
    def _cargar_urls_procesadas(self):
        """Carga URLs ya procesadas desde checkpoints"""
        try:
            checkpoint_files = Path('data/checkpoints').glob('*.json')
            urls_procesadas = set()
            
            for checkpoint_file in checkpoint_files:
                with open(checkpoint_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if 'urls_procesadas_list' in data.get('stats', {}):
                        urls_procesadas.update(data['stats']['urls_procesadas_list'])
                    elif 'urls_procesadas' in data:
                        urls_procesadas.update(data['urls_procesadas'])
            
            self.urls_procesadas = urls_procesadas
            self.logger.info(f"📊 {len(urls_procesadas)} URLs ya procesadas cargadas")
            
        except Exception as e:
            self.logger.warning(f"No se pudieron cargar URLs procesadas: {str(e)}")
            self.urls_procesadas = set()
    
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
