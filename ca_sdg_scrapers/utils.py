import requests
from typing import Union
from pathlib import Path


def download_file(url: str, path: Union[str, Path], chunk_size: int = 8192):
    """Download a file from a URL and save it to the specified location."""
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(path, 'wb') as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                f.write(chunk)
