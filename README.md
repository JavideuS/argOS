# ArgOS

Natural language control for robot dogs. ArgOS is an intelligence and cloud layer
that sits on top of a navigation backend (currently DimOS) and lets you command a
quadruped robot in plain language from any browser, anywhere.

The robot sees its environment, remembers what it finds, understands what you ask,
and navigates to carry it out — while you watch a live 3D map from your phone.

---

## Capabilities

| | |
|---|---|
| **Sees** | YOLO11 segmentation on the workstation camera feed; objects published to LCM and ingested into the semantic map |
| **Remembers** | Spatial object store with confidence decay — persists in memory or S3 across sessions |
| **Understands** | LLM agent (AWS Bedrock / Claude) with MCP tool access to navigation and perception |
| **Navigates** | DimOS A\* planner with live costmap replanning; goals issued from the browser or via voice |
| **Works anywhere** | EC2-hosted FastAPI server with bridge scripts on the robot — full UI from a phone over 4G |

## What's here

```
browser_ui/   ArgOS cloud layer — FastAPI server, LLM agent, 3D lidar UI, semantic map
scripts/      Standalone tools: YOLO workstation, Jetson camera server, startup script
patches/dimos Five bug fixes to apply on top of a DimOS checkout before running ArgOS
```

See [`CONTRIBUTORS.md`](CONTRIBUTORS.md) for who built what.

---

## Getting started

### 1. Install DimOS

ArgOS runs on top of [DimOS](https://github.com/dimensionalOS/dimos).

```bash
# Skip large LFS files — not needed to run ArgOS
export GIT_LFS_SKIP_SMUDGE=1
git clone https://github.com/dimensionalOS/dimos.git
cd dimos
```

Install the base stack. The extras you need depend on your setup:

```bash
# Full install (GPU workstation with Go2 hardware)
uv pip install -e '.[base,unitree,sim,agents,perception,misc,cuda]'

# Minimal (simulation only, no hardware)
uv pip install -e '.[base,sim,agents]'
```

If you plan to use AWS Bedrock or S3 for the semantic map:

```bash
uv pip install langchain-aws boto3
```

### 2. Apply the DimOS patches

Five bug fixes are required for stable operation. Apply them on top of your DimOS
checkout before running:

```bash
cd /path/to/dimos
git apply /path/to/argOS/patches/dimos/*.patch
```

See [`patches/dimos/README.md`](patches/dimos/README.md) for what each patch does
and how to verify they applied cleanly.

### 3. Set up the ArgOS cloud layer

```bash
cd argOS/browser_ui
pip install -r requirements.txt
```

Environment variables (minimum to run locally without AWS):

```bash
export ANTHROPIC_API_KEY=sk-...      # or use AWS Bedrock — see browser_ui/README.md
export BRIDGE_PASSWORD=changeme      # shared secret between server and bridge scripts
```

### 4. Run

There are two modes depending on whether you are running against a real robot over
the internet or testing everything locally on one machine.

---

#### Mode A — Production (server on EC2, bridges on robot machine)

**On the EC2 server:**

```bash
cd argOS/browser_ui

export BRIDGE_PASSWORD="your-bridge-password"    # shared secret — must match robot side
export DASHBOARD_PASSWORD="your-ui-password"     # browser login
export MAX_VIEWERS=100                            # concurrent dashboard connections

AUTO_BRIDGES=false MCP_TRANSPORT=bridge python3 main.py --server-only
# Dashboard at http://<your-ec2-ip>:8080
```

`AUTO_BRIDGES=false` tells the server not to spawn bridge processes itself.
`MCP_TRANSPORT=bridge` routes LLM→MCP calls through the cloud queue instead of
trying to reach `localhost:9990` directly (which only exists on the robot).

**On the robot-side machine** (with DimOS running):

```bash
cd argOS/browser_ui

export BRIDGE_PASSWORD="your-bridge-password"    # must match server

python run_bridges.py --cloud-url http://<your-ec2-ip>:8080
```

`run_bridges.py` starts all bridges (nav, pointcloud, camera, MCP) as subprocesses.
Each bridge inherits `BRIDGE_PASSWORD` from the environment and uses it to
authenticate with the server.

---

#### Mode B — Local testing (everything on one machine)

Start DimOS first:

```bash
# With NVIDIA GPU (prime offload for laptops with discrete GPU)
__NV_PRIME_RENDER_OFFLOAD=1 __GLX_VENDOR_LIBRARY_NAME=nvidia MUJOCO_GL=glfw \
    dimos --simulation run unitree-go2-agentic

# Without GPU offload
dimos --simulation run unitree-go2-agentic

# Navigation only, no MCP agent tools
dimos --simulation run unitree-go2
```

Then start the ArgOS server without `--server-only` — it will launch both the
server and the bridge processes locally:

```bash
cd argOS/browser_ui
python3 main.py
# Dashboard at http://localhost:8080
```

> **Note:** argument and environment variable passing to `main.py` still needs
> cleanup — some options currently have to be set via env vars rather than flags.
> This is a known rough edge from the hackathon.

---

See [`browser_ui/README.md`](browser_ui/README.md) for all environment variables,
the EC2 deployment walkthrough, and the full API reference.

---

## Architecture

```
[Robot / DimOS machine]          [ArgOS server — local or EC2]     [Browser / Phone]
  nav_bridge.py  ──POST──────→   /ingest/pose                   ←── GET  /pose/live
  pc_bridge.py   ──POST──────→   /ingest/pointcloud             ←── GET  /pointcloud/live
  dimos_bridge.py ─poll/POST──→  /bridge/mcp/*    LLM agent ←──────── POST /query/stream
  camera_bridge.py ─POST─────→  /frames                        ←── GET  /frames/{id}/stream
  workstation_yolo.py ─POST──→  /ingest  (semantic objects)

[Workstation]
  workstation_yolo.py  → YOLO11 segmentation → DimOS LCM + cloud semantic map
```

The MCP bridge is what makes the LLM agent work remotely: the cloud server queues
tool calls, the robot-side bridge script polls, executes them against the local DimOS
MCP server (`localhost:9990`), and posts results back. No direct network path to the
robot is needed.

---

## Roadmap

Implemented and working:

- [x] Real-time 3D lidar pointcloud in the browser (WebGL, height-mapped)
- [x] Semantic object memory with confidence decay and S3 persistence
- [x] LLM agent with MCP tool access (navigate, query map, get pose)
- [x] YOLO11 segmentation → semantic map ingestion
- [x] Voice input via AWS Transcribe
- [x] Cloud-to-robot MCP bridge (works over the internet, no VPN needed)
- [x] Live camera stream with YOLO overlay

Worth exploring next:

- [ ] Costmap heatmap toggle alongside the lidar pointcloud
- [ ] Gaussian splatting visualization (most differentiated 3D view)
- [ ] Multi-robot support (infrastructure is there, needs UI work)
- [ ] On-board processing (remove Jetson USB dependency)
- [ ] Person re-identification across sessions
- [ ] Grounded SAM for tighter object segmentation
- [ ] ROS2 / easy_nav navigation backend (argrOS direction)

---

## Notes on the DimOS dependency

ArgOS is deliberately not a fork of DimOS. The five patches in `patches/dimos/`
are bug fixes that could reasonably be contributed upstream. The rest of ArgOS
(the cloud layer, bridges, semantic memory, UI) is independent code that only
communicates with DimOS via LCM topics and the MCP HTTP interface.

This means ArgOS could in principle support a different navigation backend
(ROS2 + Nav2, easy_nav, etc.) without touching `browser_ui/`. The MCP tool
interface is the abstraction boundary.

---

## Credits

See [`CONTRIBUTORS.md`](CONTRIBUTORS.md).
