from pathlib import Path
from typing import Optional

from . import paths


def get_commit(path: Optional[str] = None):
    try:
        path = path or paths.root_path()
        path_git = Path(path, '.git')
        head_name = Path(path_git, 'HEAD').read_text().strip()[len('ref: '):]
        head_ref = Path(path_git, head_name)
        return head_ref.read_text().strip()
    except Exception:
        return None
