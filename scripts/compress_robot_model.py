"""Compress a robot render model's meshes to glTF binary (.glb) for the browser.

Takes a model directory produced by the manual xacro-expansion step in
browser_ui/static/models/README.md (a flattened robot.urdf + a meshes/
directory copied from the ROS package), converts every mesh the URDF
actually references from STL/DAE to .glb via trimesh, and rewrites the
URDF's filename attributes to point at the new files.

Only meshes the URDF references are touched -- so this also drops stray
files for free (raw CAD sources like .stp/.STEP, accidental duplicate
copies from a careless `cp -r`) without needing to hunt for them by hand.

The output directory is ready to push with hf_model_sync.py, or to drop
straight into browser_ui/static/models/<slug>/ for local testing.

The conversion isn't glb-specific -- --format works in either direction, so
the same script "decompresses" a .glb model dir back to loose .obj/.gltf
files for use in other tools (Blender, MeshLab, etc). Note trimesh can read
Collada (.dae) but can't write it, so there's no exact round trip back to
.dae specifically; the original .dae is never touched or deleted by this
script in the first place, so keep it around if you need it later.

Usage:
    python scripts/compress_robot_model.py <model_dir> --out <output_dir>

    python scripts/compress_robot_model.py \
        ~/quantum_ws/Fraunhofer/rangers/ranger_mini3_ros2_raw \
        --out browser_ui/static/models/ranger_mini_v3

    # "decompress" a .glb model dir to loose .obj files for other tools
    python scripts/compress_robot_model.py browser_ui/static/models/ranger_mini_v3 \
        --out /tmp/ranger_mini_v3_obj --format obj

Requires: trimesh (`pip install trimesh`)
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

import trimesh

MESH_FILENAME_RE = re.compile(r'filename="([^"]+)"')


def resolve_source(model_dir: Path, filename: str) -> Path:
    """Map a URDF mesh filename to its source file on disk.

    Mirrors the browser's urdf-loader `packages` resolution (see
    loadUrdfForRobot in browser_ui/web/dashboard.html): a
    `package://<any-name>/rest/of/path` URI has its package name dropped
    entirely, and everything after it is treated as relative to the model
    directory. A plain relative filename (no package:// prefix) is used
    as-is.
    """
    if filename.startswith("package://"):
        rel = filename[len("package://"):].split("/", 1)[1]
    else:
        rel = filename
    return model_dir / rel


def relative_path(model_dir: Path, filename: str) -> str:
    return str(resolve_source(model_dir, filename).relative_to(model_dir))


def convert_mesh(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    scene = trimesh.load(src, force="scene")
    scene.export(dst.as_posix())


def compress(model_dir: Path, out_dir: Path, urdf_name: str, fmt: str) -> None:
    urdf_path = model_dir / urdf_name
    text = urdf_path.read_text()

    total_before = 0
    total_after = 0
    replacements: dict[str, str] = {}

    for filename in sorted(set(MESH_FILENAME_RE.findall(text))):
        src = resolve_source(model_dir, filename)
        if not src.exists():
            print(f"  ! skip (missing source): {filename}")
            continue

        rel = relative_path(model_dir, filename)
        rel_out = str(Path(rel).with_suffix(f".{fmt}"))
        dst = out_dir / rel_out
        convert_mesh(src, dst)

        before, after = src.stat().st_size, dst.stat().st_size
        total_before += before
        total_after += after
        print(f"  {rel} ({before / 1e6:.1f}MB) -> {rel_out} ({after / 1e6:.1f}MB)")

        if filename.startswith("package://"):
            pkg = filename[len("package://"):].split("/", 1)[0]
            replacements[filename] = f"package://{pkg}/{rel_out}"
        else:
            replacements[filename] = rel_out

    for old, new in replacements.items():
        text = text.replace(f'filename="{old}"', f'filename="{new}"')

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / urdf_name).write_text(text)

    # Carry provenance/license notices forward verbatim -- easy to forget to
    # copy by hand, and this is exactly the kind of thing that matters if the
    # output ever gets published (see hf_model_sync.py).
    for pattern in ("LICENSE*", "NOTICE*"):
        for src in model_dir.glob(pattern):
            if src.is_file():
                (out_dir / src.name).write_bytes(src.read_bytes())
                print(f"  (carried forward: {src.name})")

    print(f"\n{urdf_path} -> {out_dir / urdf_name}")
    if total_before:
        pct = 100 * (1 - total_after / total_before)
        print(f"Meshes: {total_before / 1e6:.1f}MB -> {total_after / 1e6:.1f}MB ({pct:.0f}% smaller)")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("model_dir", type=Path, help="Directory with robot.urdf + meshes/ (raw, xacro-expanded)")
    ap.add_argument("--out", type=Path, required=True, help="Output directory for the compressed model")
    ap.add_argument("--urdf", default="robot.urdf", help="URDF filename inside model_dir (default: robot.urdf)")
    ap.add_argument(
        "--format", default="glb", choices=["glb", "gltf", "obj", "stl"],
        help="Target mesh format (default: glb). Works in either direction -- e.g. "
             "--format obj on a directory whose meshes are already .glb 'decompresses' "
             "them to loose .obj files for use in other tools. trimesh can read Collada "
             "(.dae) but can't write it, so there's no exact round trip back to .dae.",
    )
    args = ap.parse_args()
    compress(args.model_dir, args.out, args.urdf, args.format)


if __name__ == "__main__":
    main()
