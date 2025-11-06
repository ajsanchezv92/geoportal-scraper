import os
import subprocess
from datetime import datetime
from colorama import Fore, Style, init

init(autoreset=True)

# ================================
# CONFIGURACIÓN
# ================================
REPO_PATH = "."       # "." = repositorio actual
BRANCH = "main"       # cámbialo si tu rama principal es "master", "dev", etc.
COMMIT_PREFIX = "🚀 AutoPush:"  # texto inicial del commit
# ================================


def log(msg, color=Fore.WHITE):
    print(f"{color}{msg}{Style.RESET_ALL}")


def run_command(command, cwd=REPO_PATH):
    """Ejecuta un comando y devuelve la salida."""
    result = subprocess.run(command, cwd=cwd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        log(f"⚠️ Error ejecutando: {command}\n{result.stderr}", Fore.RED)
    return result


def main():
    log("🚀 Iniciando sincronización automática con GitHub...", Fore.CYAN)

    # Verifica que hay un repositorio Git
    if not os.path.isdir(os.path.join(REPO_PATH, ".git")):
        log("❌ No se encontró un repositorio Git aquí.", Fore.RED)
        return

    # Sincroniza primero para evitar conflictos
    run_command(f"git pull origin {BRANCH}")

    # Agrega todos los cambios (nuevos, modificados, eliminados)
    run_command("git add -A")

    # Crea commit con fecha y hora
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    commit_message = f'{COMMIT_PREFIX} actualización automática ({timestamp})'
    run_command(f'git commit -m "{commit_message}"')

    # Empuja los cambios
    push_result = run_command(f"git push origin {BRANCH}")

    if push_result.returncode == 0:
        log("✅ Cambios subidos correctamente a GitHub.", Fore.GREEN)
    else:
        log("⚠️ Error al hacer push. Revisa tus credenciales o la rama remota.", Fore.RED)


if __name__ == "__main__":
    main()
