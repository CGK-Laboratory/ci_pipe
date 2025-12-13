from io import StringIO
from typing import List

from .file_system_interface import FileSystemInterface

class InMemoryFileSystem(FileSystemInterface):
    def __init__(self):
        self.files = {}
        self.directories = set()

    def write(self, path: str, content: str):
        from io import StringIO
        self.files[path] = StringIO(content)

    def read(self, path: str) -> str:
        file_obj = self.files.get(path, None)
        if file_obj is None:
            raise FileNotFoundError(f"No such file: {path}")
        file_obj.seek(0)
        return file_obj.read()

    def exists(self, path: str) -> bool:
        return path in self.files or path in self.directories

    def makedirs(self, path: str, exist_ok: bool = False):
        self.directories.add(path)

    def listdir(self, path: str) -> List[str]:
        if path not in self.directories:
            raise FileNotFoundError(f"No such directory: {path}")
        return [f for f in self.files if f.startswith(path)]
    
    def subdirs(self, path: str) -> List[str]:
        if path not in self.directories:
            raise FileNotFoundError(f"No such directory: {path}")
        subdirs = set()
        prefix = path if path.endswith("/") else path + "/"
        for file_path in self.files:
            if file_path.startswith(prefix):
                remainder = file_path[len(prefix):]
                subdir = remainder.split("/", 1)[0]
                subdirs.add(subdir)
        return list(subdirs)

    def open(self, path: str, mode: str = 'r', encoding: str = None):
        if 'w' in mode:
            file_content = ""
            self.files[path] = StringIO(file_content)
        elif 'r' in mode:
            content = self.files.get(path, None)
            if content is None:
                raise FileNotFoundError(f"No such file: {path}")
            return content
        return None

    def copy2(self, src: str, dst: str):
        if src in self.files:
            src_filename = self.base_path(src)
            dst_path = self.join(dst, src_filename)
            self.files[dst_path] = StringIO(self.files[src].getvalue())
            return dst_path
        else:
            raise FileNotFoundError(f"No such file: {src}")

    def join(self, directory, filename):
        return f"{directory}/{filename}"

    def base_path(self, path):
        return path.split("/")[-1]

    def split_text(self, path):
        if "." in path:
            idx = path.rfind(".")
            return (path[:idx], path[idx+1:])
        return (path, "")
    
    def remove(self, path):
        if path in self.files:
            del self.files[path]
        else:
            raise FileNotFoundError(f"No such file: {path}")