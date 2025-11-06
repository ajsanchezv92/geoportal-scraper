# scripts/monitor_progreso.py
"""
Monitor en tiempo real de los datos scrapeados
"""

import json
import time
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.live import Live
from rich.layout import Layout
from datetime import datetime

console = Console()

def contar_estaciones_procesadas():
    """Cuenta estaciones procesadas en todos los archivos de resultados"""
    resultados_dir = Path("data/resultados")
    total_estaciones = 0
    archivos = []
    
    if resultados_dir.exists():
        for archivo in resultados_dir.glob("centros_lote_*.json"):
            try:
                with open(archivo, 'r', encoding='utf-8') as f:
                    datos = json.load(f)
                    estaciones_en_archivo = len(datos.get('estaciones', []))
                    total_estaciones += estaciones_en_archivo
                    archivos.append({
                        'nombre': archivo.name,
                        'estaciones': estaciones_en_archivo,
                        'fecha': datetime.fromtimestamp(archivo.stat().st_mtime)
                    })
            except Exception as e:
                continue
    
    # Ordenar por fecha de modificación (más reciente primero)
    archivos.sort(key=lambda x: x['fecha'], reverse=True)
    
    return total_estaciones, archivos

def crear_tabla_monitor():
    """Crea la tabla de monitorización en tiempo real"""
    total_estaciones, archivos = contar_estaciones_procesadas()
    
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("📊 ESTADO ACTUAL", style="cyan", width=40)
    table.add_column("VALOR", style="white")
    
    table.add_row("Total estaciones scrapeadas", f"[green]{total_estaciones:,}[/green]")
    table.add_row("Archivos de resultados", f"{len(archivos)}")
    table.add_row("Última actualización", datetime.now().strftime("%H:%M:%S"))
    
    if archivos:
        ultimo_archivo = archivos[0]
        table.add_row("Último archivo creado", ultimo_archivo['nombre'])
        table.add_row("Estaciones en último archivo", f"{ultimo_archivo['estaciones']}")
        table.add_row("Hora creación", ultimo_archivo['fecha'].strftime("%H:%M:%S"))
    
    # Tabla de últimos archivos
    if archivos:
        archivos_table = Table(show_header=True, header_style="bold blue")
        archivos_table.add_column("Últimos archivos", style="cyan")
        archivos_table.add_column("Estaciones", style="green")
        archivos_table.add_column("Hora", style="yellow")
        
        for archivo in archivos[:5]:  # Mostrar solo los 5 más recientes
            archivos_table.add_row(
                archivo['nombre'],
                str(archivo['estaciones']),
                archivo['fecha'].strftime("%H:%M:%S")
            )
    else:
        archivos_table = "[red]No hay archivos aún[/red]"
    
    layout = Layout()
    layout.split_column(
        Layout(Panel(table, title="🎯 PROGRESO DEL SCRAPING", border_style="green")),
        Layout(Panel(archivos_table, title="📁 ARCHIVOS RECIENTES", border_style="blue"))
    )
    
    return layout

def main():
    """Monitor en tiempo real"""
    console.print(Panel.fit(
        "[bold cyan]🔍 MONITOR DE PROGRESO EN TIEMPO REAL[/bold cyan]\n"
        "[yellow]Monitoreando data/resultados/ cada 5 segundos...[/yellow]",
        border_style="cyan"
    ))
    
    try:
        with Live(crear_tabla_monitor(), refresh_per_second=0.2) as live:
            while True:
                time.sleep(5)  # Actualizar cada 5 segundos
                live.update(crear_tabla_monitor())
    except KeyboardInterrupt:
        console.print("\n[yellow]👋 Monitor finalizado[/yellow]")

if __name__ == "__main__":
    main()
