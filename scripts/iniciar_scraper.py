import asyncio
import importlib.util
import sys
import os
import time
import traceback
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

console = Console()

# ==========================
# CONFIGURACIÓN INICIAL
# ==========================
SRC_PATH = os.path.join(os.path.dirname(__file__), "..", "src")
sys.path.insert(0, SRC_PATH)

SCRAPER_PATH = os.path.join(SRC_PATH, "scraper_principal.py")
if not os.path.exists(SCRAPER_PATH):
    console.print(f"[bold red]❌ ERROR:[/bold red] No se encontró el archivo {SCRAPER_PATH}")
    sys.exit(1)


# ==========================
# FUNCIÓN PARA CARGAR MÓDULO
# ==========================
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
    console.rule("[bold blue]🚀 INICIANDO SPAIN MOBILE TOWERS SCRAPER[/bold blue]")

    scraper = cargar_scraper()

    if not hasattr(scraper, "main"):
        console.print("[red]❌ El archivo scraper_principal.py no contiene una función main().[/red]")
        return

    # Si el scraper tiene una función para inicializar URLs, la usamos
    urls = []
    if hasattr(scraper, "inicializar_urls"):
        console.print("[cyan]📡 Cargando URLs desde geoportal_links/geoportal_links_1.txt...[/cyan]")
        try:
            urls = await scraper.inicializar_urls()
        except Exception as e:
            console.print(f"[bold red]❌ Error al cargar URLs: {e}[/bold red]")
            traceback.print_exc()
            return
    else:
        console.print("[yellow]⚠️ No se encontró la función inicializar_urls(). Se procederá con main().[/yellow]")

    total_urls = len(urls)
    if total_urls == 0:
        console.print("[bold red]❌ No hay URLs para procesar.[/bold red]")
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
        elif evento.get("tipo") == "error":
            errores += 1
            progress.console.print(
                f"[red]⚠️ Error en {evento.get('url', 'URL desconocida')}:[/red] {evento.get('detalle', 'Sin detalles')}"
            )
        elif evento.get("tipo") == "mensaje":
            progress.console.print(f"[cyan]ℹ️ {evento.get('mensaje')}[/cyan]")

    # ==========================
    # EJECUCIÓN EN TIEMPO REAL
    # ==========================
    with Live(console=console, refresh_per_second=5):
        with progress:
            try:
                # Ejecuta el scraping con feedback en vivo
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

            except Exception as e:
                console.print(f"[bold red]❌ Error durante la ejecución: {str(e)}[/bold red]")
                traceback.print_exc()


# ==========================
# ENTRADA PRINCIPAL
# ==========================
if __name__ == "__main__":
    try:
        asyncio.run(ejecutar_scraper())
    except KeyboardInterrupt:
        console.print("\n[bold yellow]⛔ Ejecución interrumpida por el usuario.[/bold yellow]")
