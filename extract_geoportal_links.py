import os
import requests
from tqdm import tqdm
from colorama import Fore, Style, init

# Inicializa color en terminal
init(autoreset=True)

# ================================
# CONFIGURACIÓN
# ================================
GOOGLE_DRIVE_FILE_ID = "1jcKPQHXLo1hbwAd2ucg60qmn66P1s8P6"
OUTPUT_DIR = "geoportal_links"
# ================================


def log(msg, color=Fore.WHITE):
    print(f"{color}{msg}{Style.RESET_ALL}")


def download_file_from_google_drive(file_id, destination):
    """Descarga un archivo público desde Google Drive."""
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    
    # Usamos stream=True para manejar archivos grandes
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    # Obtenemos el tamaño total del archivo para la barra de progreso
    total_size = int(response.headers.get('content-length', 0))
    
    log(f"📥 Descargando archivo ({total_size / 1024 / 1024:.2f} MB)...", Fore.CYAN)
    
    with open(destination, "wb") as f:
        with tqdm(total=total_size, unit='B', unit_scale=True, desc="Descargando", colour="yellow") as pbar:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    pbar.update(len(chunk))
    
    log(f"✅ Archivo descargado correctamente: {destination}", Fore.GREEN)


def process_file_lines(filepath):
    """
    Lee el archivo línea por línea, extrae URL + coordenadas.
    Devuelve lista de líneas formateadas y conteo.
    """
    results = []
    with_coords = 0
    without_coords = 0
    ignored = 0
    processed_lines = 0

    # Primero contamos las líneas totales para la barra de progreso
    log("📊 Contando líneas del archivo...", Fore.CYAN)
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        total_lines = sum(1 for _ in f)
    
    log(f"📁 Archivo contiene {total_lines} líneas", Fore.CYAN)

    # Ahora procesamos las líneas
    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        for line in tqdm(f, total=total_lines, desc="Procesando líneas", colour="green"):
            processed_lines += 1
            parts = line.strip().split("|")

            if len(parts) >= 3 and parts[0].startswith("https://geoportal.minetur.gob.es/VCTEL/detalleEstacion.do?emplazamiento="):
                url = parts[0].strip()
                lat = parts[1].strip() if parts[1] else None
                lon = parts[2].strip() if parts[2] else None

                if lat and lon:
                    results.append(f"{url}|{lat}|{lon}")
                    with_coords += 1
                else:
                    results.append(url)
                    without_coords += 1
            else:
                ignored += 1

    return sorted(set(results)), with_coords, without_coords, ignored, processed_lines


def save_to_txt(results, output_dir):
    """Guarda todos los resultados en un único archivo."""
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "geoportal_links.txt")
    
    log(f"💾 Guardando {len(results):,} resultados en {output_file}...", Fore.YELLOW)
    
    with open(output_file, "w", encoding="utf-8") as f:
        for line in tqdm(results, desc="Guardando archivo", colour="blue"):
            f.write(line + "\n")
    
    file_size = os.path.getsize(output_file) / (1024 * 1024)
    log(f"💾 Archivo guardado: {output_file} ({file_size:.2f} MB)", Fore.YELLOW)
    
    return output_file


def main():
    log("🚀 Iniciando extracción de enlaces y coordenadas del Geoportal...", Fore.CYAN)
    input_file = "data_from_drive.txt"

    try:
        # 1️⃣ Descargar archivo desde Google Drive
        download_file_from_google_drive(GOOGLE_DRIVE_FILE_ID, input_file)

        # 2️⃣ Procesar el contenido
        results, with_coords, without_coords, ignored, processed_lines = process_file_lines(input_file)

        # 3️⃣ Guardar todos los resultados en un único archivo
        output_file = save_to_txt(results, OUTPUT_DIR)

        # 4️⃣ Mostrar resumen final
        total_unique = with_coords + without_coords
        log("\n📊 RESUMEN FINAL", Fore.MAGENTA)
        log(f"📄 Líneas procesadas: {processed_lines:,}", Fore.WHITE)
        log(f"🔗 Total de líneas únicas: {total_unique:,}", Fore.WHITE)
        log(f"📍 Con coordenadas: {with_coords:,}", Fore.GREEN)
        log(f"❌ Sin coordenadas: {without_coords:,}", Fore.RED)
        log(f"⚠️ Líneas ignoradas por formato inválido: {ignored:,}", Fore.YELLOW)
        log(f"🗂️ Archivo creado: {output_file}", Fore.CYAN)
        
        # Eficiencia del proceso
        efficiency = (total_unique / processed_lines) * 100 if processed_lines > 0 else 0
        log(f"📈 Eficiencia: {efficiency:.2f}% de líneas útiles", Fore.CYAN)
        
        log("✅ Proceso completado correctamente.", Fore.GREEN)

    except Exception as e:
        log(f"❌ Error durante el proceso: {str(e)}", Fore.RED)
        raise


if __name__ == "__main__":
    main()
