# Scripts

Standalone tools that complement the ArgOS stack. These run independently —
no `browser_ui/` server required unless noted.

Scripts in this folder were primarily built by **Minh Nhut Nguyen**, **Harsh Talathi**,
and **Jiwon You** during the RoboHack 2026 hackathon. See [`CONTRIBUTORS.md`](../CONTRIBUTORS.md).

---

## workstation_yolo.py

YOLO11 segmentation running on the workstation, pulling JPEG frames from the
Jetson/USB camera HTTP stream. Publishes detections to DimOS LCM and pushes
semantic objects to the ArgOS cloud server.

```bash
# Basic — display window, no cloud push
python scripts/workstation_yolo.py \
  --stream-url http://192.168.123.18:8888/frame \
  --model yolo11s-seg.pt

# Headless + push to cloud semantic map
python scripts/workstation_yolo.py \
  --stream-url http://192.168.123.18:8888/frame \
  --model yolo11s-seg.pt \
  --headless \
  --cloud-url http://<server-ip>:8080

# Also publish frames and detections into DimOS LCM
python scripts/workstation_yolo.py \
  --stream-url http://192.168.123.18:8888/frame \
  --feed-dimos \
  --cloud-url http://<server-ip>:8080 \
  --headless
```

Key options:

| Flag | Default | Description |
|---|---|---|
| `--stream-url` | `http://192.168.123.18:8888/frame` | Source JPEG stream |
| `--model` | `yolo11s-seg.pt` | YOLO model file |
| `--conf` | `0.3` | Detection confidence threshold |
| `--semantic-threshold` | `0.70` | Min confidence to add to semantic map |
| `--cloud-url` | `$ROBOHACK_CLOUD_URL` or `localhost:8080` | ArgOS server |
| `--feed-dimos` | off | Also publish to DimOS LCM topics |
| `--headless` | off | No display window |
| `--no-cloud` | off | Disable cloud push entirely |

Requires: `ultralytics`, `opencv-python`. DimOS not required unless `--feed-dimos`.

---

## jetson_camera_server.py

HTTP JPEG frame server intended to run on the Jetson Nano attached to the Go2
via USB. Exposes the onboard camera as `GET /frame` so `workstation_yolo.py`
and `camera_bridge.py` can pull from it.

```bash
# On the Jetson (or any Linux machine with a camera)
python scripts/jetson_camera_server.py --port 8888 --device 0
```

**Status:** partially worked during the hackathon. USB bandwidth and latency
between the Jetson and the workstation were the main constraints. Worth revisiting
with a direct network connection rather than USB tethering.

---

## run_robohack.sh

Startup script used during the hackathon to launch DimOS and the bridges in
one shot. Kept here as a reference — update the paths and environment variables
for your setup before using.

```bash
chmod +x scripts/run_robohack.sh
./scripts/run_robohack.sh
```

The main README has cleaner step-by-step instructions for running each component.
This script is useful as a base for writing your own launcher.
