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
        """Carga TODAS las URLs REALES desde Google Drive"""
        self.logger.info(f"📥 Cargando URLs REALES desde: {drive_url}")
        
        try:
            # Descargar el contenido real de Google Drive
            start_time = time.time()
            contenido = await self._descargar_contenido_drive(drive_url)
            download_time = time.time() - start_time
            
            if not contenido:
                self.logger.error("❌ No se pudo descargar el contenido de Google Drive")
                return []
            
            self.logger.info(f"✅ Contenido descargado en {download_time:.2f}s: {len(contenido)} caracteres")
            
            # Extraer TODAS las URLs del contenido
            start_extract = time.time()
            urls = self._extraer_todas_las_urls_reales(contenido)
            extract_time = time.time() - start_extract
            
            if not urls:
                self.logger.error("❌ No se encontraron URLs en el documento")
                return []
                
            self.urls_pendientes = urls
            self.logger.info(f"✅ {len(urls)} URLs REALES extraídas en {extract_time:.2f}s")
            
            # Mostrar estadísticas
            self._mostrar_estadisticas_urls(urls)
            
            return urls
            
        except Exception as e:
            self.logger.error(f"❌ Error cargando URLs desde Google Drive: {str(e)}")
            return []
    
    async def _descargar_contenido_drive(self, drive_url: str) -> str:
        """Descarga el contenido REAL del archivo de Google Drive"""
        try:
            # Convertir la URL de visualización a URL de descarga directa
            file_id = self._extraer_file_id(drive_url)
            if not file_id:
                self.logger.error("❌ No se pudo extraer el ID del archivo de Google Drive")
                return ""
            
            # URL de descarga directa
            download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
            self.logger.info(f"🔗 Descargando de: {download_url}")
            
            timeout = aiohttp.ClientTimeout(total=60)  # Timeout más largo para archivo grande
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(download_url) as response:
                    if response.status == 200:
                        contenido = await response.text()
                        self.logger.info(f"📄 Archivo descargado: {len(contenido)} caracteres")
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
    
    def _extraer_todas_las_urls_reales(self, contenido: str) -> List[str]:
        """Extrae TODAS las URLs REALES del contenido descargado"""
        self.logger.info("🔍 Extrayendo TODAS las URLs reales...")
        
        urls = set()
        base_url = "https://geoportal.minetur.gob.es/VCTEL/detalleEstacion.do?emplazamiento="
        
        # MÉTODO 1: Buscar patrones de emplazamiento
        self.logger.info("🔍 Buscando patrones emplazamiento=...")
        patron_emplazamiento = r'emplazamiento=(\d{1,10})'
        matches_emplazamiento = re.findall(patron_emplazamiento, contenido)
        
        for emp_id in matches_emplazamiento:
            url_completa = base_url + emp_id
            urls.add(url_completa)
        
        self.logger.info(f"📊 Por emplazamiento=: {len(matches_emplazamiento)} encontrados")
        
        # MÉTODO 2: Buscar URLs completas
        self.logger.info("🔍 Buscando URLs completas...")
        patron_url_completa = r'https://geoportal\.minetur\.gob\.es/VCTEL/detalleEstacion\.do\?emplazamiento=\d{1,10}'
        matches_urls = re.findall(patron_url_completa, contenido)
        
        for url in matches_urls:
            urls.add(url)
        
        self.logger.info(f"📊 URLs completas: {len(matches_urls)} encontradas")
        
        # MÉTODO 3: Buscar en formato específico con pipes |
        self.logger.info("🔍 Buscando formato con pipes |...")
        lineas = contenido.split('\n')
        contador_lineas = 0
        
        for i, linea in enumerate(lineas):
            # Buscar el patrón: emplazamiento=XXXXX| (con | después del número)
            match = re.search(r'emplazamiento=(\d{1,10})\|', linea)
            if match:
                emp_id = match.group(1)
                url_completa = base_url + emp_id
                urls.add(url_completa)
                contador_lineas += 1
            
            # Mostrar progreso cada 50,000 líneas
            if i > 0 and i % 50000 == 0:
                self.logger.info(f"📊 Procesadas {i} líneas...")
        
        self.logger.info(f"📊 En formato pipe: {contador_lineas} encontradas")
        
        # Convertir a lista ordenada
        urls_lista = sorted(list(urls))
        
        self.logger.info(f"🎯 EXTRACCIÓN COMPLETADA: {len(urls_lista)} URLs únicas encontradas")
        
        return urls_lista
    
    def _mostrar_estadisticas_urls(self, urls: List[str]):
        """Muestra estadísticas detalladas de las URLs encontradas"""
        if not urls:
            return
        
        # Analizar patrones de IDs
        ids = []
        for url in urls:
            match = re.search(r'emplazamiento=(\d+)', url)
            if match:
                ids.append(match.group(1))
        
        # Estadísticas de longitud de IDs
        longitudes = {}
        for emp_id in ids:
            longitud = len(emp_id)
            longitudes[longitud] = longitudes.get(longitud, 0) + 1
        
        self.logger.info("📊 ESTADÍSTICAS DETALLADAS:")
        self.logger.info(f"   • Total URLs únicas: {len(urls)}")
        self.logger.info(f"   • Rango de IDs: {min(ids)} - {max(ids)}")
        self.logger.info(f"   • Distribución por longitud:")
        for longitud, count in sorted(longitudes.items()):
            self.logger.info(f"     - {longitud} dígitos: {count} URLs")
        
        # Mostrar ejemplos de diferentes patrones
        self.logger.info("🔍 Ejemplos de URLs encontradas:")
        ejemplos = []
        patrones_vistos = set()
        
        for url in urls:
            match = re.search(r'emplazamiento=(\d+)', url)
            if match:
                emp_id = match.group(1)
                patron = emp_id[:2]  # Primeros 2 dígitos como patrón
                
                if patron not in patrones_vistos and len(ejemplos) < 10:
                    ejemplos.append(url)
                    patrones_vistos.add(patron)
        
        for i, ejemplo in enumerate(ejemplos[:5]):
            self.logger.info(f"   {i+1}. {ejemplo}")
        
        if len(urls) > 5:
            self.logger.info(f"   ... y {len(urls) - 5} más")
    
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
            self.logger.info(f"📊 {len(urls_procesadas)} URLs procesadas cargadas desde checkpoints")
            
        except Exception as e:
            self.logger.warning(f"No se pudieron cargar URLs procesadas: {str(e)}")
            self.urls_procesadas = set()
    
    def filtrar_urls_pendientes(self) -> List[str]:
        """Filtra URLs pendientes de procesar"""
        pendientes = [url for url in self.urls_pendientes if url not in self.urls_procesadas]
        self.logger.info(f"🎯 {len(pendientes)} URLs pendientes de procesar (de {len(self.urls_pendientes)} totales)")
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
