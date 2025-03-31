import subprocess
from pathlib import Path

from fastapi import APIRouter, HTTPException

from config.globals import Globals

logger = Globals.logger

router = APIRouter()

def build_directory_tree(path: Path):
    if not path.is_dir():
        return None

    children = []
    for child in path.iterdir():
        if child.is_dir():
            subtree = build_directory_tree(child)
            if subtree:
                children.append(subtree)

    return {
        "name": path.name,
        "path": str(path.resolve()),
        "children": children
    }

@router.get("/envdir/get")
def get_user_env(username: str):
    result = subprocess.run(
        ["su", "-", username, "-c", "env"],
        capture_output=True, text=True
    )
    env_dict = {}
    for line in result.stdout.strip().split("\n"):
        if '=' in line:
            key, val = line.split('=', 1)
            env_dict[key] = val

    workspace_path = Path("/workspace")
    if not workspace_path.exists():
        raise HTTPException(status_code=404, detail="/workspace not found")
    directory_tree = build_directory_tree(workspace_path)

    return {
        "user_env": env_dict,
        "workspace_directory_tree": directory_tree
    }
