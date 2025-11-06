import asyncio
import importlib.util
import sys
import os
import time
import traceback
import logging
from rich.console import Console
from rich.progress import (
    Progress,
    BarColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
    TextColumn,
    SpinnerColumn,
)
from rich.table import Table
from rich.panel import Panel
from rich.live import Live
from rich import box
from rich.logging import RichHandler

# ==========================
# CONFIGURACIÓN INICIAL
# ==========================
console = Console()
SRC_PATH = os.path.join(os.path.dirname(__file__), "..", "src")
sys.path.insert(0, SRC_PATH)

LOG_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "scraper.log")

# ==========================
# CONFIGURACIÓN DE LOGGING
# ==========================
logging.basicConfig(
    level="INFO",
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="[%H:%M:%S]",
    handlers=[
        RichHandler(console=console, markup=True, rich_tracebacks=True),
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
    ],
)
logger = logging.getLogger("geoportal_scraper")

# ==========================
# CARGAR SCRAPER PRINCIPAL
# ==========================
SCRAPER_PATH = os.path.join(SRC_PATH, "scraper_principal.py")
if not os.path.exists(SCRAPER_PATH):
    logger.error(f"No se encontró el archivo: {SCRAPER_PATH}")
    sys.exit(1)


def cargar_scraper():
    spec = importlib.util.spec_from_file_location("scraper_principal", SCRAPER_PATH)
    scraper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(scraper)
    return scraper


# ==========================
# FUNCIÓN PRINCIPAL ASÍNCRONA
# ==========================
async def ejecutar_scraper():
    console.clear()
    console.rule("[bold blue]🚀 INICIANDO GEOPORTAL SCRAPER[/bold blue]")

    scraper = cargar_scraper()

    if not hasattr(scraper, "main"):
        logger.error("El archivo scraper_principal.py no contiene una función main().")
        return

    # ==========================
    # CARGAR URLs
    # ==========================
    urls = []
    if hasattr(scraper, "inicializar_urls"):
        logger.info("📡 Cargando URLs desde geoportal_links/geoportal_links_1.txt...")
        try:
            urls = await scraper.inicializar_urls()
        except Exception as e:
            logger.exception(f"Error al cargar URLs: {e}")
            return
    else:
        logger.warning("⚠️ No se encontró la función inicializar_urls(). Se usará main() directamente.")

    total_urls = len(urls)
    if total_urls == 0:
        logger.error("No se encontraron URLs para procesar.")
        return

    # ==========================
    # CONFIGURAR RICH PROGRESS
    # ==========================
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(),
        TextColumn("[green]{task.completed}/{task.total}[/green]"),
        TextColumn("• [cyan]{task.percentage:>3.1f}%[/cyan]"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    )

    task = progress.add_task("Scrapeando estaciones...", total=total_urls)

    start_time = time.time()
    procesadas = 0
    errores = 0

    # ==========================
    # CALLBACK DE PROGRESO REAL
    # ==========================
    async def progreso_callback(evento: dict):
        nonlocal procesadas, errores
        if evento.get("tipo") == "procesada":
            procesadas += 1
            progress.update(task, completed=procesadas)
            logger.info(f"✅ URL procesada correctamente: {evento.get('url', 'desconocida')}")
        elif evento.get("tipo") == "error":
            errores += 1
            progress.console.print(
                f"[red]⚠️ Error en {evento.get('url', 'URL desconocida')}:[/red] {evento.get('detalle', 'Sin detalles')}"
            )
            logger.error(f"Error en {evento.get('url', 'desconocida')}: {evento.get('detalle', 'Sin detalles')}")
        elif evento.get("tipo") == "mensaje":
            mensaje = evento.get("mensaje", "")
            progress.console.print(f"[cyan]ℹ️ {mensaje}[/cyan]")
            logger.info(mensaje)

    # ==========================
    # EJECUCIÓN EN TIEMPO REAL
    # ==========================
    with Live(console=console, refresh_per_second=5):
        with progress:
            try:
                await scraper.main(callback=progreso_callback)

                duracion = time.time() - start_time

                # ==========================
                # TABLA FINAL DE RESULTADOS
                # ==========================
                resumen = Table(
                    title="📊 RESULTADOS DEL SCRAPING",
                    show_header=True,
                    header_style="bold magenta",
                    box=box.ROUNDED,
                )

                resumen.add_column("Métrica", style="bold cyan")
                resumen.add_column("Valor", style="bold yellow")

                resumen.add_row("URLs Totales", str(total_urls))
                resumen.add_row("URLs Procesadas", str(procesadas))
                resumen.add_row("Errores", str(errores))
                resumen.add_row("Duración", f"{duracion:.2f} s")
                resumen.add_row(
                    "Progreso", f"{(procesadas / total_urls) * 100:.1f}%" if total_urls > 0 else "0%"
                )

                console.print(
                    Panel(resumen, title="[bold green]✅ EJECUCIÓN FINALIZADA[/bold green]", expand=False)
                )

                logger.info("=== SCRAPING FINALIZADO ===")
                logger.info(f"Total URLs: {total_urls}")
                logger.info(f"Procesadas: {procesadas}")
                logger.info(f"Errores: {errores}")
                logger.info(f"Duración: {duracion:.2f}s")

            except Exception as e:
                logger.exception(f"Error durante la ejecución: {e}")
                traceback.print_exc()


# ==========================
# ENTRADA PRINCIPAL
# ==========================
if __name__ == "__main__":
    try:
        asyncio.run(ejecutar_scraper())
    except KeyboardInterrupt:
        logger.warning("⛔ Ejecución interrumpida por el usuario.")
        console.print("\n[bold yellow]⛔ Ejecución interrumpida por el usuario.[/bold yellow]")
