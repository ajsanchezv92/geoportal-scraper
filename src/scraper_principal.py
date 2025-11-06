import asyncio
import aiohttp
import json
import time
import random
import signal
import sys
from pathlib import Path
from typing import Dict, List, Optional
from dataclasses import dataclass
import logging
import re
from datetime import datetime
from bs4 import BeautifulSoup
import urllib3

# Deshabilitar warnings de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

@dataclass
class ScraperConfig:
    max_workers: int = 8
    batch_size: int = 25
    timeout: int = 25
    checkpoint_interval: int = 3
    max_retries: int = 3
    retry_delay: int = 2
    request_delay: float = 0.1
    random_delay: bool = True
    connection_pool_size: int = 12
    progress_update_interval: int = 50
    memory_check_interval: int = 25


class GeoportalScraper:
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.activo = True
        self.session = None
        self.stats = {
            'urls_procesadas': 0,
            'urls_exitosas': 0,
            'urls_fallidas': 0,
            'inicio_tiempo': time.time(),
            'emplazamientos_validos': 0,
            'urls_procesadas_list': [],
            'con_coordenadas': 0,
            'sin_coordenadas': 0,
            'altitudes_obtenidas': 0
        }
        self.setup_logging()
        self.setup_directories()
        
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('data/logs/scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def setup_directories(self):
        directories = ['data/checkpoints', 'data/resultados', 'data/logs', 'data/backups']
        for dir_path in directories:
            Path(dir_path).mkdir(parents=True, exist_ok=True)
    
    async def _configure_session(self):
        connector = aiohttp.TCPConnector(
            limit=self.config.connection_pool_size,
            limit_per_host=self.config.connection_pool_size,
            verify_ssl=False
        )
        timeout = aiohttp.ClientTimeout(total=self.config.timeout)
        
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            }
        )
    
    async def procesar_url_con_delay(self, url: str) -> Optional[Dict]:
        if not self.activo:
            return None
            
        # Delay inteligente
        if self.stats['urls_procesadas'] % 5 == 0:
            delay = self.config.request_delay
            if self.config.random_delay:
                delay += random.uniform(-0.05, 0.05)
            await asyncio.sleep(delay)
        
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    html = await response.text()
                    datos = await self.extraer_datos_estacion_completo(html, url)
                    if datos and self.tiene_datos_validos(datos):
                        self.stats['urls_exitosas'] += 1
                        self.stats['emplazamientos_validos'] += 1
                        return datos
                    else:
                        self.stats['urls_fallidas'] += 1
                        return None
                else:
                    self.logger.warning(f"HTTP {response.status} para {url}")
                    self.stats['urls_fallidas'] += 1
                    return None
        except Exception as e:
            self.logger.error(f"Error procesando {url}: {str(e)}")
            self.stats['urls_fallidas'] += 1
            return None
    
    def tiene_datos_validos(self, datos):
        """Verifica que los datos extraídos sean realmente válidos"""
        return bool(datos.get('titular') or datos.get('caracteristicas_tecnicas'))
    
    async def extraer_datos_estacion_completo(self, html: str, url: str) -> Dict:
        """Extrae datos COMPLETOS usando el método probado de tu código"""
        soup = BeautifulSoup(html, 'html.parser')
        codigo = self.extraer_codigo_desde_url(url)
        
        if not self.es_pagina_valida(soup):
            return None
        
        datos = {
            "codigo_emplazamiento": codigo,
            "url": url,
            "titular": "",
            "direccion_completa": "",
            "municipio": "",
            "provincia": "",
            "caracteristicas_tecnicas": [],
            "niveles_medidos": [],
            "cumplimiento_normativa": ""
        }
        
        try:
            # Extraer LOCALIZACIÓN (método probado)
            h2_localizacion = soup.find('h2', string='LOCALIZACIÓN')
            if h2_localizacion:
                tabla_localizacion = h2_localizacion.find_next('table')
                if tabla_localizacion:
                    filas = tabla_localizacion.find_all('tr')
                    for fila in filas:
                        celdas = fila.find_all('td')
                        if len(celdas) >= 2:
                            texto_celda1 = celdas[0].get_text(strip=True)
                            texto_celda2 = celdas[1].get_text(strip=True)
                            
                            if ' - ' in texto_celda1:
                                partes = texto_celda1.split(' - ')
                                datos['titular'] = partes[0].strip()
                            
                            datos['direccion_completa'] = texto_celda2
                            
                            # Extraer municipio y provincia
                            if '. ' in texto_celda2:
                                partes_dir = texto_celda2.split('. ')
                                if len(partes_dir) >= 2:
                                    datos['municipio'] = partes_dir[1].split(',')[0].strip() if ',' in partes_dir[1] else partes_dir[1].strip()
                                    if ',' in texto_celda2:
                                        datos['provincia'] = texto_celda2.split(', ')[-1].strip()
            
            # Extraer CARACTERÍSTICAS TÉCNICAS
            h2_caracteristicas = soup.find('h2', string=re.compile('CARACTERISTICAS TÉCNICAS', re.IGNORECASE))
            if h2_caracteristicas:
                tabla = h2_caracteristicas.find_next('table')
                if tabla:
                    filas = tabla.find_all('tr')[1:]  # Saltar encabezado
                    for fila in filas:
                        celdas = fila.find_all('td')
                        if len(celdas) >= 3:
                            caracteristica = {
                                "operador": celdas[0].get_text(strip=True),
                                "referencia": celdas[1].get_text(strip=True),
                                "banda_asignada_mhz": celdas[2].get_text(strip=True)
                            }
                            datos['caracteristicas_tecnicas'].append(caracteristica)
            
            # Extraer NIVELES MEDIDOS
            h2_niveles = soup.find('h2', string=re.compile('NIVELES MEDIDOS', re.IGNORECASE))
            if h2_niveles:
                tabla = h2_niveles.find_next('table')
                if tabla:
                    filas = tabla.find_all('tr')[1:]  # Saltar encabezado
                    for i, fila in enumerate(filas):
                        celdas = fila.find_all('td')
                        if len(celdas) >= 3:
                            nivel = {
                                "punto_medida": i + 1,
                                "distancia_metros": celdas[0].get_text(strip=True),
                                "acimut_grados": celdas[1].get_text(strip=True),
                                "valor_medido_uw_cm2": celdas[2].get_text(strip=True)
                            }
                            datos['niveles_medidos'].append(nivel)
            
            # Extraer CUMPLIMIENTO
            texto_completo = soup.get_text()
            if 'cumplen la normativa legal vigente' in texto_completo.lower():
                datos['cumplimiento_normativa'] = 'CUMPLE'
            else:
                datos['cumplimiento_normativa'] = 'NO_ESPECIFICADO'
            
            # ✅ AÑADIR CAMPOS CALCULADOS MEJORADOS
            datos = self.calcular_campos_realistas(datos)
            
            # ✅ AÑADIR METADATOS DE SCRAPING
            datos['scraping_metadata'] = {
                'fecha_extraccion': datetime.now().isoformat(),
                'status_code': 200,
                'response_time_ms': 0,  # Podríamos medir esto
                'version_scraper': '3.0.0_sentinel'
            }
            
            self.logger.info(f"✅ {codigo} - {len(datos['caracteristicas_tecnicas'])} antenas - {datos['cumplimiento_normativa']}")
            
        except Exception as e:
            self.logger.error(f"Error extrayendo datos de {codigo}: {e}")
            return None
        
        return datos
    
    def es_pagina_valida(self, soup):
        """Determina si la página contiene datos válidos de estación"""
        titulo = soup.find('h1', string='ESTACIONES DE TELEFONÍA MÓVIL')
        return titulo is not None
    
    def extraer_codigo_desde_url(self, url):
        """Extrae el código de emplazamiento desde la URL"""
        try:
            match = re.search(r'emplazamiento=(\d+)', url)
            if match:
                return match.group(1)
        except:
            pass
        return "DESCONOCIDO"
    
    # ✅ MÉTODOS MEJORADOS DE TU CÓDIGO
    def determinar_tecnologia_mejorada(self, banda_mhz, referencia=""):
        """Clasificación más precisa de tecnologías"""
        try:
            numeros = re.findall(r'\d+\.?\d*', banda_mhz)
            if len(numeros) >= 2:
                freq_min = float(numeros[0])
                freq_max = float(numeros[1])
                freq_media = (freq_min + freq_max) / 2
                
                # Banda 700 MHz (4G/5G)
                if 694 <= freq_media <= 790:
                    return "4G/5G"
                # Banda 800 MHz (4G)
                elif 791 <= freq_media <= 862:
                    return "4G"
                # Banda 900 MHz (2G/3G)
                elif 880 <= freq_media <= 960:
                    return "2G/3G"
                # Banda 1800 MHz (4G)
                elif 1710 <= freq_media <= 1880:
                    return "4G"
                # Banda 2100 MHz (3G/4G)
                elif 1920 <= freq_media <= 2170:
                    return "3G/4G"
                # Banda 2600 MHz (4G)
                elif 2500 <= freq_media <= 2690:
                    return "4G"
                # Banda 3500 MHz (5G)
                elif 3400 <= freq_media <= 3800:
                    return "5G"
                
            # Por referencia si la frecuencia no es concluyente
            referencia_upper = referencia.upper()
            if "5G" in referencia_upper:
                return "5G"
            elif "4G" in referencia_upper or "LTE" in referencia_upper:
                return "4G"
            elif "3G" in referencia_upper or "UMTS" in referencia_upper:
                return "3G"
            elif "2G" in referencia_upper or "GSM" in referencia_upper:
                return "2G"
                
        except:
            pass
        return "Desconocida"
    
    def estimar_cobertura_relativa(self, frecuencia):
        """Estimación basada en frecuencia"""
        if frecuencia < 1000:
            return "LARGO_ALCANCE"
        elif frecuencia < 2500:
            return "MEDIO_ALCANCE" 
        else:
            return "CORTO_ALCANCE"
    
    def clasificar_riesgo_salud(self, valor_maximo):
        """Clasifica el riesgo para la salud basado en valores reales"""
        if valor_maximo <= 1:
            return "INSIGNIFICANTE"
        elif valor_maximo <= 10:
            return "MUY_BAJO" 
        elif valor_maximo <= 50:
            return "BAJO"
        elif valor_maximo <= 200:
            return "MODERADO"
        else:
            return "ELEVADO"
    
    def calcular_campos_realistas(self, datos):
        """Calcula TODOS los campos mejorados (de tu código probado)"""
        try:
            # 1. CÁLCULOS PARA CARACTERÍSTICAS TÉCNICAS (MEJORADO)
            for caracteristica in datos['caracteristicas_tecnicas']:
                banda = caracteristica['banda_asignada_mhz']
                referencia = caracteristica.get('referencia', '')
                
                # Extraer frecuencias
                numeros = re.findall(r'\d+\.?\d*', banda)
                if len(numeros) >= 2:
                    freq_min = float(numeros[0])
                    freq_max = float(numeros[1])
                    
                    # ✅ CAMPOS REALISTAS MEJORADOS
                    caracteristica['frecuencia_central_mhz'] = round((freq_min + freq_max) / 2, 2)
                    caracteristica['ancho_banda_mhz'] = round(freq_max - freq_min, 2)
                    caracteristica['tecnologia'] = self.determinar_tecnologia_mejorada(banda, referencia)
                    caracteristica['cobertura_relativa'] = self.estimar_cobertura_relativa(freq_min)
            
            # 2. ESTADÍSTICAS DE NIVELES MEDIDOS
            if datos['niveles_medidos']:
                valores_medidos = []
                distancias = []
                
                for nivel in datos['niveles_medidos']:
                    try:
                        # Manejar valores como "<0.01061"
                        valor_str = nivel['valor_medido_uw_cm2']
                        if valor_str.startswith('<'):
                            valor = float(valor_str[1:]) / 2  # Aproximación conservadora
                        else:
                            valor = float(valor_str)
                            
                        distancia = float(nivel['distancia_metros'])
                        valores_medidos.append(valor)
                        distancias.append(distancia)
                    except:
                        continue
                
                if valores_medidos:
                    max_valor = max(valores_medidos)
                    
                    # ✅ ESTADÍSTICAS MEJORADAS
                    datos['estadisticas_mediciones'] = {
                        'valor_maximo_uw_cm2': round(max_valor, 5),
                        'valor_minimo_uw_cm2': round(min(valores_medidos), 5),
                        'valor_medio_uw_cm2': round(sum(valores_medidos) / len(valores_medidos), 5),
                        'total_puntos_medida': len(datos['niveles_medidos']),
                        'distancia_promedio_metros': round(sum(distancias) / len(distancias), 1),
                        'rango_distancias_metros': f"{min(distancias)}-{max(distancias)}"
                    }
                    
                    # ✅ CUMPLIMIENTO NORMATIVO MEJORADO
                    datos['cumplimiento_normativas'] = {
                        'cumple_icnirp': max_valor <= 450,
                        'porcentaje_limite_icnirp': round((max_valor / 450) * 100, 3),
                        'margen_seguridad': round(100 - (max_valor / 450) * 100, 3),
                        'categoria_riesgo': self.clasificar_riesgo_salud(max_valor),
                        'limite_referencia_uw_cm2': 450
                    }
            
            # 3. ✅ ANÁLISIS DE COBERTURA
            datos['analisis_cobertura'] = self.analizar_cobertura(datos['caracteristicas_tecnicas'])
            
            # 4. ✅ CLASIFICACIÓN DE ESTACIÓN
            datos['clasificacion_estacion'] = self.clasificar_estacion(datos['caracteristicas_tecnicas'])
            
            # 5. ✅ METADATOS DEL ANÁLISIS
            datos['metadata_analisis'] = {
                'fecha_analisis': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'total_caracteristicas': len(datos['caracteristicas_tecnicas']),
                'total_niveles_medidos': len(datos['niveles_medidos']),
                'tecnologias_detectadas': list(set([ct.get('tecnologia', 'Desconocida') for ct in datos['caracteristicas_tecnicas']])),
                'version_analisis': '3.0_sentinel'
            }
            
        except Exception as e:
            self.logger.debug(f"Error calculando campos realistas: {e}")
        
        return datos
    
    def analizar_cobertura(self, caracteristicas):
        """Analiza la cobertura basada en las tecnologías disponibles"""
        tecnologias = [ct.get('tecnologia', 'Desconocida') for ct in caracteristicas]
        
        cobertura = {
            'tiene_2g': any('2G' in t for t in tecnologias),
            'tiene_3g': any('3G' in t for t in tecnologias),
            'tiene_4g': any('4G' in t for t in tecnologias),
            'tiene_5g': any('5G' in t for t in tecnologias),
            'tecnologias_activas': len(set(tecnologias)),
            'banda_mas_baja': min([ct.get('frecuencia_central_mhz', 0) for ct in caracteristicas]),
            'banda_mas_alta': max([ct.get('frecuencia_central_mhz', 0) for ct in caracteristicas])
        }
        
        # Calcular calidad de cobertura
        if cobertura['tiene_5g'] and cobertura['tiene_4g'] and cobertura['tiene_2g']:
            cobertura['calidad'] = "EXCELENTE"
        elif cobertura['tiene_4g'] and cobertura['tiene_2g']:
            cobertura['calidad'] = "BUENA"
        elif cobertura['tiene_2g']:
            cobertura['calidad'] = "BASICA"
        else:
            cobertura['calidad'] = "LIMITADA"
        
        return cobertura
    
    def clasificar_estacion(self, caracteristicas):
        """Clasifica el tipo de estación basado en sus características"""
        total_antenas = len(caracteristicas)
        operadores = list(set([ct['operador'] for ct in caracteristicas]))
        
        clasificacion = {
            'total_antenas': total_antenas,
            'operadores_activos': operadores,
            'total_operadores': len(operadores)
        }
        
        # Determinar tipo de estación
        if total_antenas >= 6:
            clasificacion['tipo'] = "ESTACION_COMPLETA"
        elif total_antenas >= 3:
            clasificacion['tipo'] = "ESTACION_MEDIA"
        else:
            clasificacion['tipo'] = "ESTACION_BASICA"
            
        # Determinar si es multioperador
        if len(operadores) > 1:
            clasificacion['multioperador'] = True
        else:
            clasificacion['multioperador'] = False
            
        return clasificacion
    
    async def procesar_batch(self, urls_batch: List[str]):
        """Procesa un batch de URLs concurrentemente"""
        if not self.activo:
            return []
        
        tasks = [self.procesar_url_con_delay(url) for url in urls_batch]
        resultados = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Filtrar resultados válidos
        resultados_validos = [r for r in resultados if r is not None and not isinstance(r, Exception)]
        
        # Actualizar estadísticas
        self.stats['urls_procesadas'] += len(urls_batch)
        self.stats['urls_procesadas_list'].extend(urls_batch)
        
        # Mostrar progreso
        if self.stats['urls_procesadas'] % self.config.progress_update_interval == 0:
            self.mostrar_progreso()
        
        return resultados_validos
    
    def mostrar_progreso(self):
        """Muestra el progreso actual"""
        tiempo_transcurrido = time.time() - self.stats['inicio_tiempo']
        urls_por_segundo = self.stats['urls_procesadas'] / tiempo_transcurrido if tiempo_transcurrido > 0 else 0
        
        print(f"\n📊 PROGRESO: {self.stats['urls_procesadas']} URLs procesadas")
        print(f"✅ Éxitos: {self.stats['urls_exitosas']} | ❌ Fallos: {self.stats['urls_fallidas']}")
        print(f"🏭 Emplazamientos válidos: {self.stats['emplazamientos_validos']}")
        print(f"⚡ Velocidad: {urls_por_segundo:.2f} URLs/segundo")
        print(f"⏱️  Tiempo: {tiempo_transcurrido/60:.1f} minutos")
    
    async def ejecutar_scraping(self, urls: List[str]):
        """Ejecuta el scraping principal"""
        await self._configure_session()
        
        try:
            lote_id = 1
            for i in range(0, len(urls), self.config.batch_size):
                if not self.activo:
                    break
                    
                batch = urls[i:i + self.config.batch_size]
                resultados = await self.procesar_batch(batch)
                
                # Guardar resultados del lote
                if resultados:
                    self.guardar_resultados_lote(resultados, lote_id)
                    lote_id += 1
                
                # Checkpoint cada X batches
                if (i // self.config.batch_size) % self.config.checkpoint_interval == 0:
                    self.guardar_checkpoint()
                    
        finally:
            await self.session.close()
    
    def guardar_resultados_lote(self, datos_lote: List[Dict], lote_id: int):
        """Guarda un lote de resultados"""
        try:
            archivo_salida = Path('data/resultados') / f"centros_lote_{lote_id:04d}.json"
            
            with open(archivo_salida, 'w', encoding='utf-8') as f:
                json.dump({
                    "metadata": {
                        "fecha_generacion": datetime.now().isoformat(),
                        "total_estaciones": len(datos_lote),
                        "lote_id": lote_id
                    },
                    "estaciones": datos_lote
                }, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"💾 Lote {lote_id} guardado: {len(datos_lote)} estaciones")
            
        except Exception as e:
            self.logger.error(f"Error guardando lote {lote_id}: {str(e)}")
    
    def guardar_checkpoint(self):
        """Guarda un checkpoint del estado actual"""
        try:
            checkpoint_data = {
                'stats': self.stats,
                'timestamp': time.time(),
                'urls_procesadas': self.stats['urls_procesadas'],
                'urls_procesadas_list': self.stats['urls_procesadas_list']
            }
            
            checkpoint_file = f"data/checkpoints/checkpoint_{int(time.time())}.json"
            with open(checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(checkpoint_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"💾 Checkpoint guardado: {checkpoint_file}")
            
        except Exception as e:
            self.logger.error(f"Error guardando checkpoint: {str(e)}")
    
    def parada_elegante(self):
        """Realiza una parada elegante del scraper"""
        self.logger.info("Iniciando parada elegante...")
        self.activo = False
        self.guardar_checkpoint()
