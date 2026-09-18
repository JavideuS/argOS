"""Push/pull robot render models (URDF + meshes) to/from a Hugging Face dataset repo.

browser_ui/static/models/ is gitignored -- these binaries never go into git.
This script is the only thing that populates that directory. Run `pull`
once before starting the server (e.g. in your deploy step); run `push`
after producing a new/updated model with compress_robot_model.py.

This repo only ever holds the compressed, browser-ready model -- the raw
.dae/.stl source has its own durable home already (the ROS package it came
from, e.g. ranger_mini3_ros2), so there's no need for argOS to archive it a
second time here.

Usage:
    # one-time, per model, after compress_robot_model.py
    python scripts/hf_model_sync.py push ranger_mini_v3 --repo <namespace>/argos-robot-models

    # before starting the server
    python scripts/hf_model_sync.py pull ranger_mini_v3 --repo <namespace>/argos-robot-models
    python scripts/hf_model_sync.py pull --all --repo <namespace>/argos-robot-models

Set $ARGOS_MODELS_REPO to avoid passing --repo every time.

New repos are created private by default (`--public` to opt out) -- check
the source model's license before publishing it, especially for
manufacturer-provided CAD/mesh assets (e.g. AgileX Ranger) you didn't
author yourself.

Requires: huggingface_hub (`pip install huggingface_hub`), and either
`huggingface-cli login` or $HF_TOKEN set with write access to the repo.
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

from huggingface_hub import HfApi, snapshot_download

DEFAULT_REPO = os.environ.get("ARGOS_MODELS_REPO", "")
MODELS_DIR = Path(__file__).resolve().parent.parent / "browser_ui" / "static" / "models"


def push(repo: str, slug: str, local_dir: Path, private: bool) -> None:
    if not local_dir.is_dir():
        raise SystemExit(f"No such directory: {local_dir}")
    api = HfApi()
    api.create_repo(repo, repo_type="dataset", private=private, exist_ok=True)
    api.upload_folder(
        repo_id=repo,
        repo_type="dataset",
        folder_path=str(local_dir),
        path_in_repo=slug,
        commit_message=f"Update {slug} render model",
    )
    print(f"Pushed {local_dir} -> hf.co/datasets/{repo}/{slug}")


def pull(repo: str, slug: str | None, local_dir: Path | None) -> None:
    if slug is None:
        snapshot_download(repo_id=repo, repo_type="dataset", local_dir=MODELS_DIR)
        print(f"Pulled all models -> {MODELS_DIR}")
        return
    target = local_dir or (MODELS_DIR / slug)
    snapshot_download(
        repo_id=repo,
        repo_type="dataset",
        allow_patterns=[f"{slug}/*"],
        local_dir=target.parent,
    )
    print(f"Pulled hf.co/datasets/{repo}/{slug} -> {target}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=["push", "pull"])
    ap.add_argument("slug", nargs="?", help="Model slug, e.g. ranger_mini_v3")
    ap.add_argument("--all", action="store_true", help="pull every model currently in the repo")
    ap.add_argument("--repo", default=DEFAULT_REPO, help="HF dataset repo id (or set $ARGOS_MODELS_REPO)")
    ap.add_argument("--dir", type=Path, help="Local model dir (default: browser_ui/static/models/<slug>)")
    ap.add_argument("--public", action="store_true", help="create the repo as public (default: private)")
    args = ap.parse_args()

    if not args.repo:
        raise SystemExit("Set --repo or $ARGOS_MODELS_REPO to a HF dataset repo id, e.g. <namespace>/argos-robot-models")

    if args.action == "pull" and args.all:
        pull(args.repo, None, None)
        return

    if not args.slug:
        raise SystemExit("slug is required unless --all is given")

    local_dir = args.dir or (MODELS_DIR / args.slug)
    if args.action == "push":
        push(args.repo, args.slug, local_dir, private=not args.public)
    else:
        pull(args.repo, args.slug, local_dir)


if __name__ == "__main__":
    main()
