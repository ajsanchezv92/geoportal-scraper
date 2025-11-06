import re
import os
import requests
from tqdm import tqdm
from colorama import Fore, Style, init

# Inicializa colores para consola (Windows y Linux)
init(autoreset=True)

# ================================
# CONFIGURACIÓN
# ================================
GOOGLE_DRIVE_FILE_ID = "1jcKPQHXLo1hbwAd2ucg60qmn66P1s8P6"
OUTPUT_DIR = "geoportal_links"
MAX_FILE_SIZE_MB = 25
# ================================


def log(msg, color=Fore.WHITE):
    """Imprime mensajes con color y formato uniforme."""
    print(f"{color}{msg}{Style.RESET_ALL}")


def download_file_from_google_drive(file_id, destination):
    """Descarga un archivo público de Google Drive."""
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    response = requests.get(url)
    response.raise_for_status()
    with open(destination, "wb") as f:
        f.write(response.content)
    log(f"✅ Archivo descargado correctamente: {destination}", Fore.GREEN)


def extract_links_and_coords(text):
    """
    Extrae URLs del Geoportal y posibles coordenadas (lat, lon).
    Las coordenadas se detectan con expresiones regulares comunes.
    """
    pattern_url = r"https://geoportal\.minetur\.gob\.es/VCTEL/detalleEstacion\.do\?emplazamiento=[\w\d]+"
    urls = re.findall(pattern_url, text)

    results = []
    with_coords = 0
    without_coords = 0

    log(f"🔍 Extrayendo enlaces y coordenadas ({len(urls)} encontrados inicialmente)...", Fore.CYAN)

    for url in tqdm(urls, desc="Procesando enlaces", colour="green"):
        # Intentar buscar latitud/longitud en el texto próximo
        lat_match = re.search(r"lat(?:itud)?[:=]?\s*(-?\d+\.\d+)", url)
        lon_match = re.search(r"lon(?:gitud)?[:=]?\s*(-?\d+\.\d+)", url)
        if lat_match and lon_match:
            results.append(f"{url} | {lat_match.group(1)}, {lon_match.group(1)}")
            with_coords += 1
        else:
            results.append(url)
            without_coords += 1

    return sorted(set(results)), with_coords, without_coords


def save_to_txt_splitted(results, output_dir, max_size_mb):
    """Guarda los resultados en varios archivos si exceden el tamaño máximo."""
    os.makedirs(output_dir, exist_ok=True)
    file_index = 1
    current_file = os.path.join(output_dir, f"geoportal_links_{file_index}.txt")
    current_size = 0
    f = open(current_file, "w", encoding="utf-8")

    for line in tqdm(results, desc="Guardando archivos", colour="blue"):
        line_bytes = len(line.encode("utf-8")) + 1  # +1 por salto de línea
        if (current_size + line_bytes) / (1024 * 1024) > max_size_mb:
            f.close()
            log(f"💾 Guardado: {current_file} ({current_size/1024/1024:.2f} MB)", Fore.YELLOW)
            file_index += 1
            current_file = os.path.join(output_dir, f"geoportal_links_{file_index}.txt")
            f = open(current_file, "w", encoding="utf-8")
            current_size = 0
        f.write(line + "\n")
        current_size += line_bytes

    f.close()
    log(f"💾 Guardado final: {current_file} ({current_size/1024/1024:.2f} MB)", Fore.YELLOW)
    log("✅ Proceso completado correctamente.", Fore.GREEN)


def main():
    log("🚀 Iniciando extracción de enlaces del Geoportal...", Fore.CYAN)
    input_file = "data_from_drive.txt"

    # 1️⃣ Descargar el archivo
    download_file_from_google_drive(GOOGLE_DRIVE_FILE_ID, input_file)

    # 2️⃣ Leer contenido
    with open(input_file, "r", encoding="utf-8", errors="ignore") as f:
        text = f.read()

    # 3️⃣ Extraer URLs y coordenadas
    results, with_coords, without_coords = extract_links_and_coords(text)

    # 4️⃣ Guardar resultados
    save_to_txt_splitted(results, OUTPUT_DIR, MAX_FILE_SIZE_MB)

    # 5️⃣ Mostrar estadísticas finales
    total = with_coords + without_coords
    log("\n📊 RESUMEN FINAL", Fore.MAGENTA)
    log(f"🔗 Total de enlaces únicos: {total}", Fore.WHITE)
    log(f"📍 Con coordenadas: {with_coords}", Fore.GREEN)
    log(f"❌ Sin coordenadas: {without_coords}", Fore.RED)
    log(f"🗂️ Archivos creados en: {OUTPUT_DIR}/", Fore.YELLOW)


if __name__ == "__main__":
    main()
