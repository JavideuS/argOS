# Contributors

ArgOS was built during the 2026 RoboHack hackathon (May 2026) by the following team.
A fuller description of everyone's role is planned for the README.

---

## Javier Gil — [@javideus](https://github.com/javideus)
Lead on the cloud/intelligence layer.
- `browser_ui/` — FastAPI server, MCP agent, LLM streaming, 3D lidar UI
- EC2 deployment and cloud bridge architecture
- Overall system integration

## Minh Nhut Nguyen
YOLO perception pipeline and semantic detection integration.

## Harsh Talathi
Hackathon presentation and camera/Jetson work.

## Jiwon You
Voice interface and YOLO perception pipeline.

---

A huge thanks to all three — the robot wouldn't have seen, heard, or been presentable
without them. ArgOS is a team effort.

---

### What they built (for the record)

- `scripts/workstation_yolo.py` — YOLO11 segmentation running on the workstation,
  pulling frames from the Jetson USB camera stream, publishing detections to DimOS
  LCM and pushing semantic objects to the cloud server.
- `scripts/jetson_camera_server.py` — Jetson-side HTTP JPEG frame server (attempt
  to expose the onboard camera over USB; partially working during the hackathon).
- Semantic memory integration: `browser_ui/taxonomy.py`, `browser_ui/world_store.py`,
  `browser_ui/models.py`, `browser_ui/agent.py`

### DimOS stability patches
Five bug fixes applied on top of the upstream dimos codebase during the hackathon
(see `patches/dimos/` for the actual patch files):

- **`001-speak-skill-tts-guard`** — Skip TTS init when `OPENAI_API_KEY` is absent;
  without this the whole agentic blueprint failed to start.
- **`002-path-mask-no-crash`** — Don't raise when >5% of path points are occupied;
  return the mask so the local planner can trigger a clean replan instead of freezing.
- **`003-path-clearance-guard`** — Exception guard in `is_obstacle_ahead()` so a
  transient mask error doesn't kill the local planner thread.
- **`004-bbox-circles-fix`** — Initialize `circles` / `circles_length` on
  `ImageAnnotations`; missing fields caused LCM subscriber decode crashes.
- **`005-lcmservice-rate-limit`** — Rate-limit identical LCM error spam that fired
  thousands of times per second when a duplicate publisher was active.

---

To apply the dimos patches against a clean dimos checkout:

```bash
cd /path/to/dimos
git apply /path/to/argOS/patches/dimos/*.patch
```
