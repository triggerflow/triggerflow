import os
from pathlib import Path

CACHE_PATH = os.path.expanduser('~/.triggerflow')


class TriggerflowCache:
    def __init__(self, path, file_name, method):
        all_cache_path = Path(CACHE_PATH)
        if all_cache_path.is_file():
            all_cache_path.unlink()
        if not all_cache_path.exists():
            all_cache_path.mkdir()

        Path('/'.join([CACHE_PATH, path])).mkdir(parents=True, exist_ok=True)

        self.file_obj = open('/'.join([CACHE_PATH, path, file_name]), method)

    def __enter__(self):
        return self.file_obj

    def __exit__(self, type, value, traceback):
        self.file_obj.close()

    @staticmethod
    def list_dir(path):
        cache_path = Path('/'.join([CACHE_PATH, path]))
        files = [file.name for file in cache_path.iterdir() if file.is_file()]
        return files
