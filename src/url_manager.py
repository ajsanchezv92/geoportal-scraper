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
        """Carga URLs desde Google Drive real - VERSIÓN OPTIMIZADA"""
        self.logger.info(f"📥 Cargando URLs desde: {drive_url}")
        
        try:
            # Descargar el contenido real de Google Drive
            start_time = time.time()
            contenido = await self._descargar_contenido_drive(drive_url)
            download_time = time.time() - start_time
            
            if not contenido:
                self.logger.error("❌ No se pudo descargar el contenido de Google Drive")
                return []
            
            self.logger.info(f"✅ Contenido descargado en {download_time:.2f}s: {len(contenido)} caracteres")
            
            # Extraer URLs del contenido
            start_extract = time.time()
            urls = self._extraer_urls_rapido(contenido)
            extract_time = time.time() - start_extract
            
            if not urls:
                self.logger.error("❌ No se encontraron URLs en el documento")
                return []
                
            self.urls_pendientes = urls
            self.logger.info(f"✅ {len(urls)} URLs extraídas en {extract_time:.2f}s")
            self.logger.info(f"📊 URLs únicas encontradas: {len(urls)}")
            
            # Mostrar algunas URLs de ejemplo
            if urls:
                self.logger.info(f"🔍 Ejemplo de URLs encontradas:")
                for i, url in enumerate(urls[:5]):
                    self.logger.info(f"   {i+1}. {url}")
                if len(urls) > 5:
                    self.logger.info(f"   ... y {len(urls) - 5} más")
            
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
            self.logger.info(f"🔗 URL de descarga: {download_url}")
            
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(download_url) as response:
                    if response.status == 200:
                        contenido = await response.text()
                        return contenido
                    else:
                        self.logger.error(f"❌ Error HTTP {response.status} al descargar")
                        return ""
                        
        except asyncio.TimeoutError:
            self.logger.error("❌ Timeout al descargar de Google Drive")
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
        
        self.logger.warning(f"⚠️  No se pudo extraer file_id de: {drive_url}")
        return ""
    
    def _extraer_urls_rapido(self, contenido: str) -> List[str]:
        """Extrae URLs RÁPIDAMENTE usando métodos optimizados"""
        self.logger.info("🔍 Iniciando extracción rápida de URLs...")
        
        urls = set()
        base_url = "https://geoportal.minetur.gob.es/VCTEL/detalleEstacion.do?emplazamiento="
        
        # MÉTODO 1: Buscar directamente los IDs de emplazamiento
        self.logger.info("🔍 Buscando IDs de emplazamiento...")
        
        # Patrón para encontrar emplazamiento=NUMERO
        patron_emplazamiento = r'emplazamiento=(\d{1,10})'
        matches_emplazamiento = re.findall(patron_emplazamiento, contenido)
        
        for emp_id in matches_emplazamiento:
            url_completa = base_url + emp_id
            urls.add(url_completa)
        
        self.logger.info(f"📊 Por emplazamiento=: {len(matches_emplazamiento)} encontrados, {len(urls)} únicos")
        
        # MÉTODO 2: Buscar URLs completas
        self.logger.info("🔍 Buscando URLs completas...")
        patron_url_completa = r'https://geoportal\.minetur\.gob\.es/VCTEL/detalleEstacion\.do\?emplazamiento=\d{1,10}'
        matches_urls = re.findall(patron_url_completa, contenido)
        
        for url in matches_urls:
            urls.add(url)
        
        self.logger.info(f"📊 URLs completas: {len(matches_urls)} encontradas, {len(urls)} únicas totales")
        
        # MÉTODO 3: Buscar en líneas con formato específico (como el que mostraste)
        self.logger.info("🔍 Buscando en formato específico...")
        lineas = contenido.split('\n')
        contador_lineas = 0
        
        for linea in lineas:
            # Buscar el patrón: emplazamiento=XXXXX| (con | después del número)
            match = re.search(r'emplazamiento=(\d{1,10})\|', linea)
            if match:
                emp_id = match.group(1)
                url_completa = base_url + emp_id
                urls.add(url_completa)
                contador_lineas += 1
            
            # Buscar URLs completas en la línea
            match_url = re.search(r'https://geoportal\.minetur\.gob\.es/VCTEL/detalleEstacion\.do\?emplazamiento=\d{1,10}', linea)
            if match_url:
                urls.add(match_url.group(0))
                contador_lineas += 1
        
        self.logger.info(f"📊 En líneas específicas: {contador_lineas} encontradas, {len(urls)} únicas totales")
        
        # Convertir a lista y ordenar
        urls_lista = sorted(list(urls))
        self.logger.info(f"🎯 EXTRACCIÓN COMPLETADA: {len(urls_lista)} URLs únicas encontradas")
        
        return urls_lista
    
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
                        elif 'urls_procesadas' in data:
                            urls_procesadas.update(data['urls_procesadas'])
                except Exception as e:
                    self.logger.warning(f"⚠️  Error leyendo checkpoint {checkpoint_file}: {e}")
            
            self.urls_procesadas = urls_procesadas
            self.logger.info(f"📊 {len(urls_procesadas)} URLs ya procesadas cargadas desde checkpoints")
            
        except Exception as e:
            self.logger.warning(f"No se pudieron cargar URLs procesadas: {str(e)}")
            self.urls_procesadas = set()
    
    def filtrar_urls_pendientes(self) -> List[str]:
        """Filtra URLs pendientes de procesar"""
        pendientes = [url for url in self.urls_pendientes if url not in self.urls_procesadas]
        self.logger.info(f"🎯 {len(pendientes)} URLs pendientes de procesar (de {len(self.urls_pendientes)} totales)")
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
