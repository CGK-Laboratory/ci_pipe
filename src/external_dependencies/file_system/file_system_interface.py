from typing import List

class FileSystemInterface:
    def write(self, path: str, content: str):
        raise NotImplementedError

    def read(self, path: str) -> str:
        raise NotImplementedError
    
    def exists(self, path: str) -> bool:
        raise NotImplementedError

    def makedirs(self, path: str, exist_ok: bool = False):
        raise NotImplementedError

    def listdir(self, path: str) -> List[str]:
        raise NotImplementedError
    
    def subdirs(self, path: str) -> List[str]:
        raise NotImplementedError

    def open(self, path: str, mode: str = 'r', encoding: str = None):
        raise NotImplementedError

    def copy2(self, src: str, dst: str):
        raise NotImplementedError

    def join(self, directory, filename):
        raise NotImplementedError

    def base_path(self, path):
        raise NotImplementedError

    def split_text(self, path):
        raise NotImplementedError
    
    def remove(self, path: str):
        raise NotImplementedError