import json
import logging
from pathlib import Path
from typing import Dict, List
from datetime import datetime

class DataProcessor:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.resultados_dir = Path('data/resultados')
        self.resultados_dir.mkdir(parents=True, exist_ok=True)
    
    def procesar_datos_estacion(self, datos_brutos: Dict) -> Dict:
        """Procesa y enriquece los datos de la estación"""
        try:
            # Enriquecer con metadatos de procesamiento
            datos_procesados = {
                **datos_brutos,
                "metadata": {
                    **datos_brutos.get("metadata", {}),
                    "fecha_procesamiento": datetime.now().isoformat(),
                    "version_procesador": "2.0.0",
                    "calidad_datos": self._evaluar_calidad_datos(datos_brutos)
                }
            }
            
            # Calcular métricas adicionales
            datos_procesados = self._calcular_metricas_avanzadas(datos_procesados)
            
            # Validar estructura de datos
            datos_procesados = self._validar_estructura(datos_procesados)
            
            return datos_procesados
            
        except Exception as e:
            self.logger.error(f"Error procesando datos: {str(e)}")
            return datos_brutos
    
    def _evaluar_calidad_datos(self, datos: Dict) -> str:
        """Evalúa la calidad de los datos extraídos"""
        campos_requeridos = [
            'estacion_id', 'informacion_geografica', 'caracteristicas_estacion'
        ]
        
        campos_presentes = sum(1 for campo in campos_requeridos if campo in datos)
        porcentaje = (campos_presentes / len(campos_requeridos)) * 100
        
        if porcentaje >= 90:
            return "EXCELENTE"
        elif porcentaje >= 75:
            return "BUENA"
        elif porcentaje >= 60:
            return "ACEPTABLE"
        else:
            return "BAJA"
    
    def _calcular_metricas_avanzadas(self, datos: Dict) -> Dict:
        """Calcula métricas avanzadas de la estación"""
        # Métricas de diversidad tecnológica
        tecnologias = datos.get("infraestructura_tecnologica", {}).get("resumen_tecnologico", {}).get("tecnologias_activas", [])
        datos["metricas_avanzadas"] = {
            "diversidad_tecnologica": len(tecnologias),
            "indice_modernidad": self._calcular_indice_modernidad(tecnologias),
            "score_cobertura": self._calcular_score_cobertura(datos),
            "clasificacion_importancia": self._clasificar_importancia(datos)
        }
        
        return datos
    
    def _calcular_indice_modernidad(self, tecnologias: List[str]) -> float:
        """Calcula índice de modernidad tecnológica"""
        pesos = {'2G': 1, '3G': 2, '4G': 3, '5G': 4}
        return sum(pesos.get(tech, 0) for tech in tecnologias) / len(tecnologias) if tecnologias else 0
    
    def _calcular_score_cobertura(self, datos: Dict) -> float:
        """Calcula score de cobertura basado en múltiples factores"""
        score = 0.0
        
        # Factor de tecnologías
        tecnologias = datos.get("infraestructura_tecnologica", {}).get("resumen_tecnologico", {}).get("tecnologias_activas", [])
        score += len(tecnologias) * 0.2
        
        # Factor de operadores
        operadores = datos.get("caracteristicas_estacion", {}).get("operadores_activos", [])
        score += len(operadores) * 0.3
        
        # Factor de capacidad
        total_antenas = datos.get("caracteristicas_estacion", {}).get("clasificacion", {}).get("total_antenas", 0)
        score += min(total_antenas * 0.1, 0.5)
        
        return min(score, 1.0)
    
    def _clasificar_importancia(self, datos: Dict) -> str:
        """Clasifica la importancia de la estación"""
        score = self._calcular_score_cobertura(datos)
        
        if score >= 0.8:
            return "ALTA"
        elif score >= 0.6:
            return "MEDIA_ALTA"
        elif score >= 0.4:
            return "MEDIA"
        else:
            return "BAJA"
    
    def _validar_estructura(self, datos: Dict) -> Dict:
        """Valida y corrige la estructura de datos"""
        # Asegurar que todos los campos obligatorios existen
        campos_obligatorios = {
            'estacion_id': '',
            'url_oficial': '',
            'metadata': {},
            'informacion_geografica': {},
            'caracteristicas_estacion': {}
        }
        
        for campo, valor_default in campos_obligatorios.items():
            if campo not in datos:
                datos[campo] = valor_default
        
        return datos
    
    def guardar_resultados_lote(self, datos_lote: List[Dict], lote_id: int):
        """Guarda un lote de resultados procesados"""
        try:
            archivo_salida = self.resultados_dir / f"centros_lote_{lote_id:04d}.json"
            
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
