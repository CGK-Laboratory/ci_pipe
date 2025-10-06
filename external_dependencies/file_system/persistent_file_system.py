import os
import shutil
from typing import List

from external_dependencies.file_system.file_system_interface import FileSystemInterface

class PersistentFileSystem(FileSystemInterface):
    def write(self, path: str, content: str):
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content)

    def read(self, path: str) -> str:
        with open(path, 'r', encoding='utf-8') as f:
            return f.read()

    def exists(self, path: str) -> bool:
        return os.path.exists(path)

    def makedirs(self, path: str, exist_ok: bool = False):
        os.makedirs(path, exist_ok=exist_ok)

    def listdir(self, path: str) -> List[str]:
        return os.listdir(path)

    def open(self, path: str, mode: str = 'r', encoding: str = None):
        return open(path, mode, encoding=encoding)

    def copy2(self, src: str, dst: str):
        shutil.copy2(src, dst)

    def join(self, directory: str, filename: str):
        return os.path.join(directory, filename)

    def base_path(self, path):
        return os.path.basename(path)

    def split_text(self, path):
        return os.path.splitext(path)