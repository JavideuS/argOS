# Robot render models

The dashboard loads each robot's real mesh via `urdf-loader` (Three.js) from
`/models/<model>/robot.urdf`, where `<model>` is the slug set per robot in
`config/ros2_fleet.example.yaml`'s `model:` field and pushed by
`bridges/ros2_bridge.py` in every pose payload. A robot with no `model` set
(or whose URDF hasn't loaded yet) falls back to a generic tinted placeholder
mesh, so this step is optional -- it's purely visual fidelity.

argOS itself has no ROS2/xacro dependency, so this directory's contents are
produced *once*, separately, in a ROS2-sourced shell (wherever `xacro` is
installed), not by anything in `browser_ui/`.

This directory is **gitignored** (except this file) and populated by
`scripts/hf_model_sync.py` from a Hugging Face dataset repo rather than
committed -- mesh files are large binaries and a growing fleet means many
of them, which doesn't belong in git history.

## Producing a model

1. Expand the xacro to a flat URDF in a ROS2-sourced shell -- `package://`
   mesh URIs are left as-is, `urdf-loader`'s `packages` option resolves them
   at load time regardless of the ROS package name (see `loadUrdfForRobot`
   in `web/dashboard.html`), so no path rewriting is needed here:

   ```bash
   source /opt/ros/<distro>/setup.bash   # wherever xacro is installed
   mkdir -p /tmp/<model>_raw
   xacro /path/to/robot.xacro.urdf > /tmp/<model>_raw/robot.urdf
   cp -r /path/to/meshes /tmp/<model>_raw/meshes
   ```
2. Compress it -- converts the meshes the URDF actually references to `.glb`
   (much smaller than raw STL/DAE, and this step also drops anything the
   URDF doesn't reference, like leftover CAD source files or accidental
   duplicate copies). Compression is one-way -- there's no exact path back
   from `.glb` to the original `.dae`/`.stl` -- but that's fine here: this
   directory only ever needs to hold the browser-ready copy, and the raw
   source already has a durable home in the ROS package it came from:

   ```bash
   python scripts/compress_robot_model.py /tmp/<model>_raw \
       --out browser_ui/static/models/<model>
   ```
3. Push it to the Hugging Face dataset repo so it doesn't need to be
   regenerated on every machine that runs the server:

   ```bash
   python scripts/hf_model_sync.py push <model> --repo <namespace>/argos-robot-models
   ```
4. Set `model: <model>` on that robot in the fleet YAML. Anyone else running
   the server just needs:

   ```bash
   python scripts/hf_model_sync.py pull <model> --repo <namespace>/argos-robot-models
   ```

   No server restart needed for new mesh files (served via FastAPI's
   StaticFiles); a new `model` value on an already-running robot picks up on
   its next pose push.

If you ever need the original `.dae`/`.stl` back (e.g. to reopen in
Blender/CAD), go back to the ROS package the model was expanded from --
that's its source of truth, not this directory.

## Example: ranger_mini_v3

```bash
source /opt/ros/jazzy/setup.bash
mkdir -p /tmp/ranger_mini_v3_raw
xacro ranger_mini3_ros2/robots/ranger_mini_v3.xacro.urdf \
    > /tmp/ranger_mini_v3_raw/robot.urdf
cp -r ranger_mini3_ros2/meshes \
    /tmp/ranger_mini_v3_raw/meshes

python scripts/compress_robot_model.py /tmp/ranger_mini_v3_raw \
    --out browser_ui/static/models/ranger_mini_v3
python scripts/hf_model_sync.py push ranger_mini_v3 --repo <namespace>/argos-robot-models
```

See `scripts/README.md` for the full `compress_robot_model.py` /
`hf_model_sync.py` reference, including the licensing note on
manufacturer-provided CAD/mesh assets before pushing a repo public.
