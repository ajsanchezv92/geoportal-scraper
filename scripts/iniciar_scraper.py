# scripts/iniciar_scraper.py
"""
Script de inicio con sesiones automáticas de 2 horas y reinicio automático
Usa el generator de progreso y sistema de batches del scraper_principal
"""

import asyncio
import signal
import sys
import time
import json
from datetime import datetime, timedelta
from pathlib import Path
import logging

# Rich imports
from rich.progress import Progress, BarColumn, TimeElapsedColumn, TimeRemainingColumn, TextColumn
from rich.console import Console
from rich.panel import Panel

console = Console()

# Añadir el directorio raíz al path para importaciones
sys.path.append(str(Path(__file__).parent.parent))

# Ahora importar después de ajustar el path
from scraper_principal import ejecutar_scraper

# Configuración de sesiones
SESION_DURACION_HORAS = 2  # Duración de cada sesión
TIEMPO_ENTRE_SESIONES_SEG = 30  # Tiempo entre sesiones
MAX_SESIONES = None  # None para infinito, o número máximo de sesiones

def setup_loggers():
    """Configura el sistema de logging"""
    Path("data/logs").mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("data/logs/scraper_sesiones.log", mode="a", encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ]
    )

    error_logger = logging.getLogger("error_logger")
    error_handler = logging.FileHandler("data/logs/error_sesiones.log", mode="a", encoding="utf-8")
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    error_logger.addHandler(error_handler)
    
    return error_logger

class GestorSesiones:
    """Gestiona múltiples sesiones de scraping automáticas"""
    
    def __init__(self):
        self.sesion_activa = True
        self.sesion_actual = 0
        self.sesiones_completadas = 0
        self.total_urls_procesadas = 0
        self.inicio_global = time.time()
        self.error_logger = setup_loggers()
        
    def ejecutar_sesion(self, numero_sesion: int, tiempo_limite: int = SESION_DURACION_HORAS * 3600):
        """Ejecuta una sesión individual con límite de tiempo"""
        console.print(Panel.fit(
            f"[bold green]🔄 INICIANDO SESIÓN {numero_sesion}[/bold green]\n"
            f"[yellow]Duración: {SESION_DURACION_HORAS}h | Hora de finalización: {(datetime.now() + timedelta(hours=SESION_DURACION_HORAS)).strftime('%H:%M:%S')}[/yellow]",
            border_style="green"
        ))
        
        inicio_sesion = time.time()
        sesion_activa = True
        urls_procesadas_sesion = 0
        
        def signal_handler(signum, frame):
            nonlocal sesion_activa
            console.print(f"\n[yellow]⚠️  Señal de interrupción recibida en sesión {numero_sesion}. Finalizando...[/yellow]")
            sesion_activa = False

        # Configurar manejo de señales
        signal.signal(signal.SIGINT, signal_handler)
        
        try:
            # Obtener el generator del scraper
            scraper_generator = ejecutar_scraper()
            
            # Configurar progress bar de Rich para la sesión
            with Progress(
                TextColumn("[progress.description]{task.description}"),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.1f}%"),
                TimeElapsedColumn(),
                TextColumn("•"),
                TimeRemainingColumn(),
                console=console
            ) as progress:
                
                task = progress.add_task(f"[cyan]Sesión {numero_sesion} - Scrapeando...", total=100)
                ultima_actualizacion = time.time()
                
                # Procesar cada actualización del generator con timeout
                for porcentaje, mensaje in scraper_generator:
                    if not sesion_activa:
                        console.print(f"\n[yellow]🛑 Sesión {numero_sesion} interrumpida por usuario[/yellow]")
                        break
                    
                    # Verificar tiempo límite de la sesión
                    tiempo_transcurrido = time.time() - inicio_sesion
                    if tiempo_transcurrido >= tiempo_limite:
                        console.print(f"\n[orange1]⏰ Límite de tiempo alcanzado en sesión {numero_sesion}[/orange1]")
                        break
                    
                    # Actualizar progreso
                    progress.update(task, completed=porcentaje, description=f"[cyan]Sesión {numero_sesion} - {mensaje}")
                    
                    # Mostrar información periódicamente
                    tiempo_restante = tiempo_limite - tiempo_transcurrido
                    if time.time() - ultima_actualizacion >= 30:  # Cada 30 segundos
                        horas_restantes = int(tiempo_restante // 3600)
                        minutos_restantes = int((tiempo_restante % 3600) // 60)
                        console.print(f"[blue]⏱️  Sesión {numero_sesion} - Tiempo restante: {horas_restantes}h {minutos_restantes}m - {mensaje}[/blue]")
                        ultima_actualizacion = time.time()
                    
                    # Contar URLs procesadas (estimado)
                    if "URLs" in mensaje:
                        try:
                            partes = mensaje.split("URLs ")[1].split("/")
                            if len(partes) >= 2:
                                urls_procesadas_sesion = int(partes[0])
                        except:
                            pass
                
                # Sesión completada
                duracion_sesion = time.time() - inicio_sesion
                horas = int(duracion_sesion // 3600)
                minutos = int((duracion_sesion % 3600) // 60)
                
                console.print(Panel.fit(
                    f"[bold green]✅ SESIÓN {numero_sesion} COMPLETADA[/bold green]\n"
                    f"[white]Duración: {horas}h {minutos}m | URLs procesadas: ~{urls_procesadas_sesion}[/white]",
                    border_style="green"
                ))
                
                self.total_urls_procesadas += urls_procesadas_sesion
                return True
                
        except Exception as e:
            self.error_logger.error(f"Error en sesión {numero_sesion}: {str(e)}")
            console.print(f"[red]❌ Error en sesión {numero_sesion}: {str(e)}[/red]")
            return False

    def ejecutar_ciclo_sesiones(self):
        """Ejecuta el ciclo completo de sesiones automáticas"""
        console.print(Panel.fit(
            "[bold cyan]🚀 INICIANDO CICLO DE SESIONES AUTOMÁTICAS[/bold cyan]\n"
            f"[yellow]Duración por sesión: {SESION_DURACION_HORAS}h | Pausa entre sesiones: {TIEMPO_ENTRE_SESIONES_SEG}s[/yellow]",
            border_style="cyan"
        ))
        
        sesion_numero = 1
        
        while self.sesion_activa:
            if MAX_SESIONES and sesion_numero > MAX_SESIONES:
                console.print(f"[green]✅ Máximo de {MAX_SESIONES} sesiones alcanzado. Finalizando...[/green]")
                break
            
            # Ejecutar sesión
            exito = self.ejecutar_sesion(sesion_numero)
            
            if exito:
                self.sesiones_completadas += 1
            
            # Preparar siguiente sesión (excepto si fue la última)
            if self.sesion_activa and (not MAX_SESIONES or sesion_numero < MAX_SESIONES):
                console.print(Panel.fit(
                    f"[bold yellow]⏳ PREPARANDO SIGUIENTE SESIÓN[/bold yellow]\n"
                    f"[white]Sesión actual: {sesion_numero} | Próxima sesión en: {TIEMPO_ENTRE_SESIONES_SEG} segundos[/white]",
                    border_style="yellow"
                ))
                
                # Contador regresivo para siguiente sesión
                for i in range(TIEMPO_ENTRE_SESIONES_SEG, 0, -1):
                    if not self.sesion_activa:
                        break
                    if i <= 10 or i % 30 == 0:  # Mostrar cada 30 segundos o últimos 10 segundos
                        console.print(f"[blue]🕒 Iniciando sesión {sesion_numero + 1} en {i} segundos...[/blue]")
                    time.sleep(1)
                
                if self.sesion_activa:
                    console.print(f"\n[green]🔄 INICIANDO SESIÓN {sesion_numero + 1}[/green]")
            
            sesion_numero += 1
        
        # Resumen final
        tiempo_total = time.time() - self.inicio_global
        horas_total = int(tiempo_total // 3600)
        minutos_total = int((tiempo_total % 3600) // 60)
        
        console.print(Panel.fit(
            "[bold magenta]📊 RESUMEN FINAL DEL CICLO[/bold magenta]\n"
            f"[white]Sesiones completadas: {self.sesiones_completadas}\n"
            f"Tiempo total: {horas_total}h {minutos_total}m\n"
            f"URLs totales procesadas: ~{self.total_urls_procesadas}[/white]",
            border_style="magenta"
        ))

    def parada_elegante(self):
        """Inicia parada elegante del ciclo"""
        console.print("\n[yellow]🛑 Solicitando parada elegante del ciclo de sesiones...[/yellow]")
        self.sesion_activa = False

def main():
    """Función principal que inicia el ciclo de sesiones"""
    gestor = GestorSesiones()
    
    def signal_handler(signum, frame):
        gestor.parada_elegante()

    # Configurar manejo de señales global
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        gestor.ejecutar_ciclo_sesiones()
    except Exception as e:
        console.print(f"[red]💥 Error crítico en el ciclo: {str(e)}[/red]")
        gestor.error_logger.error(f"Error crítico en el ciclo: {str(e)}")
    finally:
        console.print("[yellow]👋 Ciclo de sesiones finalizado[/yellow]")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n[red]⛔ Ciclo interrumpido por el usuario.[/red]")
    except Exception as e:
        console.print(f"\n[red]💥 Error inesperado: {str(e)}[/red]")
