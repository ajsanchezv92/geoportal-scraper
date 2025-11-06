# scripts/iniciar_scraper.py
"""
Script de inicio para ejecutar el GeoportalScraper con logs y progreso Rich.
Controla señales, muestra estadísticas y registra todo en data/logs/.
"""

import asyncio
import signal
import sys
from rich.progress import Progress, BarColumn, TimeElapsedColumn, TimeRemainingColumn
from rich.console import Console
from pathlib import Path
import logging
from src.scraper_principal import GeoportalScraper

console = Console()

def setup_loggers():
    Path("data/logs").mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("data/logs/scraper.log", mode="a", encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ]
    )

    error_logger = logging.getLogger("error_logger")
    error_handler = logging.FileHandler("data/logs/error.log", mode="a", encoding="utf-8")
    error_handler.setLevel(logging.ERROR)
    error_logger.addHandler(error_handler)
    return error_logger

async def main():
    error_logger = setup_loggers()
    scraper = GeoportalScraper()

    loop = asyncio.get_event_loop()

    # Manejo de señales para detener con Ctrl+C
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: setattr(scraper, "activo", False))

    console.print("[bold cyan]🚀 Iniciando GeoportalScraper...[/bold cyan]")
    await scraper._configure_session()

    urls_path = Path("geoportal_links/geoportal_links.txt")
    if not urls_path.exists():
        console.print(f"[red]❌ No se encontró {urls_path}[/red]")
        return

    with open(urls_path, "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    total_urls = len(urls)
    console.print(f"[bold yellow]📡 Total de URLs cargadas:[/bold yellow] {total_urls}")

    progreso = 0

    with Progress(
        "[progress.description]{task.description}",
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.1f}%",
        "•",
        TimeElapsedColumn(),
        "•",
        TimeRemainingColumn(),
    ) as progress:

        task = progress.add_task("[green]Scrapeando estaciones...", total=total_urls)

        resultados = []
        for i, url in enumerate(urls, start=1):
            if not scraper.activo:
                break

            try:
                datos = await scraper.procesar_url_con_delay(url)
                progreso += 1
                progress.update(task, advance=1)

                if datos:
                    resultados.append(datos)
                    if len(resultados) % 20 == 0:
                        console.print(f"[blue]💾 Guardados {len(resultados)} registros parciales...[/blue]")

            except Exception as e:
                error_logger.error(f"Error procesando {url}: {str(e)}")
                continue

        await scraper.session.close()

    console.print("\n[bold green]✅ Scraping finalizado[/bold green]")
    console.print(f"📊 URLs procesadas: {scraper.stats['urls_procesadas']}")
    console.print(f"✔️ Exitosas: {scraper.stats['urls_exitosas']} | ❌ Fallidas: {scraper.stats['urls_fallidas']}")
    console.print(f"🗂️ Resultados parciales: {len(resultados)} registros extraídos.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        console.print("\n[red]⛔ Ejecución interrumpida por el usuario.[/red]")
