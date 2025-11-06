import json
import shutil
from pathlib import Path
from typing import Dict, Any

class FileManager:
    @staticmethod
    def ensure_directory(path: str):
        """Asegura que un directorio existe"""
        Path(path).mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def save_json(data: Dict[str, Any], filepath: str, ensure_ascii=False):
        """Guarda datos en formato JSON"""
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=ensure_ascii)
    
    @staticmethod
    def load_json(filepath: str) -> Dict[str, Any]:
        """Carga datos desde JSON"""
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    @staticmethod
    def get_file_size_mb(filepath: str) -> float:
        """Obtiene tamaño de archivo en MB"""
        return Path(filepath).stat().st_size / (1024 * 1024)
    
    @staticmethod
    def split_large_file(filepath: str, max_size_mb: float = 25):
        """Divide archivos grandes en partes"""
        file_size = FileManager.get_file_size_mb(filepath)
        
        if file_size > max_size_mb:
            # Lógica para dividir archivos grandes
            base_path = Path(filepath)
            data = FileManager.load_json(filepath)
            
            # Implementar lógica de división según estructura de datos
            # ...
            pass
