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
MAX_FILE_SIZE_MB = 25
# ================================


def log(msg, color=Fore.WHITE):
    print(f"{color}{msg}{Style.RESET_ALL}")


def download_file_from_google_drive(file_id, destination):
    """Descarga un archivo público desde Google Drive."""
    url = f"https://drive.google.com/uc?export=download&id={file_id}"
    response = requests.get(url)
    response.raise_for_status()
    with open(destination, "wb") as f:
        f.write(response.content)
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

    with open(filepath, "r", encoding="utf-8", errors="ignore") as f:
        lines = f.readlines()

    log(f"🔍 Procesando {len(lines)} líneas...", Fore.CYAN)

    for line in tqdm(lines, desc="Extrayendo datos", colour="green"):
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

    return sorted(set(results)), with_coords, without_coords, ignored


def save_to_txt_splitted(results, output_dir, max_size_mb):
    """Guarda los resultados en varios archivos si exceden el tamaño máximo."""
    os.makedirs(output_dir, exist_ok=True)
    file_index = 1
    current_file = os.path.join(output_dir, f"geoportal_links_{file_index}.txt")
    current_size = 0
    f = open(current_file, "w", encoding="utf-8")

    for line in tqdm(results, desc="Guardando archivos", colour="blue"):
        line_bytes = len(line.encode("utf-8")) + 1
        # Si supera el tamaño máximo, crea un nuevo archivo
        if (current_size + line_bytes) / (1024 * 1024) > max_size_mb:
            f.close()
            log(f"💾 Guardado: {current_file} ({current_size / 1024 / 1024:.2f} MB)", Fore.YELLOW)
            file_index += 1
            current_file = os.path.join(output_dir, f"geoportal_links_{file_index}.txt")
            f = open(current_file, "w", encoding="utf-8")
            current_size = 0

        f.write(line + "\n")
        current_size += line_bytes

    f.close()
    log(f"💾 Guardado final: {current_file} ({current_size / 1024 / 1024:.2f} MB)", Fore.YELLOW)
    log("✅ Proceso completado correctamente.", Fore.GREEN)


def main():
    log("🚀 Iniciando extracción de enlaces y coordenadas del Geoportal...", Fore.CYAN)
    input_file = "data_from_drive.txt"

    # 1️⃣ Descargar archivo desde Google Drive
    download_file_from_google_drive(GOOGLE_DRIVE_FILE_ID, input_file)

    # 2️⃣ Procesar el contenido
    results, with_coords, without_coords, ignored = process_file_lines(input_file)

    # 3️⃣ Guardar resultados divididos por tamaño
    save_to_txt_splitted(results, OUTPUT_DIR, MAX_FILE_SIZE_MB)

    # 4️⃣ Mostrar resumen final
    total = with_coords + without_coords
    log("\n📊 RESUMEN FINAL", Fore.MAGENTA)
    log(f"🔗 Total de líneas únicas: {total}", Fore.WHITE)
    log(f"📍 Con coordenadas: {with_coords}", Fore.GREEN)
    log(f"❌ Sin coordenadas: {without_coords}", Fore.RED)
    log(f"⚠️ Líneas ignoradas por formato inválido: {ignored}", Fore.YELLOW)
    log(f"🗂️ Archivos creados en: {OUTPUT_DIR}/", Fore.CYAN)


if __name__ == "__main__":
    main()
