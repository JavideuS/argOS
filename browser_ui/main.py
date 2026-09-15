"""
AWS Cloud Middleware — FastAPI Communication Layer

Endpoints:
  POST /ingest        — Robot pushes detections (Req 1)
  POST /query/stream  — User asks natural language question, SSE response (Req 2)
  GET  /map           — Merged world state from all robots
  GET  /              — Dashboard UI
  GET  /health        — Health check (no auth required)
"""

import asyncio
import base64
import os
import queue as _queue
import struct
import time
import json
import logging
import zlib
import uuid
import yaml
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse, Response
from fastapi.staticfiles import StaticFiles

from core.auth import (
    DASHBOARD_PASSWORD,
    create_session,
    verify_bridge,
    verify_dashboard_password,
    verify_session,
)
from core.models import (
    IngestRequest, IngestResponse,
    QueryRequest, MapResponse,
    ErrorResponse, WorldState,
    MissionExportRequest,
    MissionLaunchRequest,
)
from core.world_store import WorldStateStore

logger = logging.getLogger(__name__)

# ── Quiet the access log for high-frequency dashboard polling ───
# The browser polls /pose/live, /pointcloud/live, etc. up to 10x/sec per open
# tab (see the setInterval block in the dashboard JS below), which otherwise
# floods uvicorn's access log with nothing but 200s. Successful polls are
# dropped by default; failures (4xx/5xx) still print. Set POLL_ACCESS_LOG=true
# to see every request again while debugging the polling itself.
POLL_ACCESS_LOG = os.environ.get("POLL_ACCESS_LOG", "false").lower() == "true"
_NOISY_POLL_PATHS = (
    "/pose/live", "/pointcloud/live", "/path/live",
    "/costmap/live", "/goal/active", "/map", "/map/live",
    "/mission/launch/status", "/mission/launch/command",
)


class _QuietPollingFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if POLL_ACCESS_LOG:
            return True
        try:
            msg = record.getMessage()
        except Exception:
            return True
        status = msg.rsplit(" ", 1)[-1].strip()
        if not status.isdigit() or not status.startswith("2"):
            return True  # always show non-2xx (errors are never "noise")
        return not any(f'"GET {p} HTTP' in msg for p in _NOISY_POLL_PATHS)


logging.getLogger("uvicorn.access").addFilter(_QuietPollingFilter())

# ── Configuration ─────────────────────────────────────────────

S3_BUCKET = os.environ.get("S3_BUCKET", "robohack-map")
AWS_REGION = os.environ.get("AWS_REGION", "eu-west-1")
USE_MEMORY_STORE = os.environ.get("USE_MEMORY_STORE", "true").lower() == "true"


# ── App Lifespan ──────────────────────────────────────────────

# Spawn direct-LCM bridges when dimos is reachable locally. Set
# AUTO_BRIDGES=false to disable, e.g. when running the cloud UI on EC2.
AUTO_BRIDGES = os.environ.get("AUTO_BRIDGES", "true").lower() == "true"
CAMERA_BRIDGE_FPS = os.environ.get("CAMERA_BRIDGE_FPS", "10")
CAMERA_BRIDGE_SOURCE = os.environ.get("CAMERA_BRIDGE_SOURCE", "auto")
CAMERA_HTTP_URL = os.environ.get("CAMERA_HTTP_URL", "http://192.168.123.18:8888/frame")
CAMERA_PUBLISH_LCM = os.environ.get("CAMERA_PUBLISH_LCM", "true")
SELF_URL = os.environ.get("SELF_URL", "http://localhost:8080")
PC_ACCUM_VOXEL_CM = max(1, int(os.environ.get("PC_ACCUM_VOXEL_CM", "8")))
PC_ACCUM_MAX_POINTS = max(1000, int(os.environ.get("PC_ACCUM_MAX_POINTS", "120000")))
PC_OBS_RADIUS_CM = max(0, int(os.environ.get("PC_OBS_RADIUS_CM", "20")))
PC_HIT_SCORE = float(os.environ.get("PC_HIT_SCORE", "1.0"))
PC_MAX_SCORE = float(os.environ.get("PC_MAX_SCORE", "8.0"))
PC_MISS_DECAY = float(os.environ.get("PC_MISS_DECAY", "0.72"))
PC_TIME_DECAY_PER_SEC = float(os.environ.get("PC_TIME_DECAY_PER_SEC", "0.015"))
PC_RENDER_SCORE = float(os.environ.get("PC_RENDER_SCORE", "1.2"))
PC_DELETE_SCORE = float(os.environ.get("PC_DELETE_SCORE", "0.35"))
AWS_TRANSCRIBE_LANGUAGE = os.environ.get("AWS_TRANSCRIBE_LANGUAGE", "en-US")
AWS_TRANSCRIBE_TIMEOUT = float(os.environ.get("AWS_TRANSCRIBE_TIMEOUT", "45"))
SPEECH_S3_PREFIX = os.environ.get("SPEECH_S3_PREFIX", "speech")


def _load_aws_env_from_dimos() -> None:
    """Load AWS credentials from the sibling DimOS .env when not already set."""
    if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
        return
    candidates = [
        os.path.expanduser("~/robohack-epfl/dimos/.env"),
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dimos", ".env"),
    ]
    for path in candidates:
        if not os.path.exists(path):
            continue
        try:
            with open(path, encoding="utf-8") as fh:
                for raw in fh:
                    line = raw.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, val = line.split("=", 1)
                    key = key.strip()
                    val = val.strip().strip('"').strip("'")
                    if key.startswith("AWS_") or key in {"S3_BUCKET"}:
                        os.environ.setdefault(key, val)
            return
        except Exception as exc:
            logger.debug("Could not load AWS env from %s: %s", path, exc)


def _audio_format_from_content_type(content_type: str) -> tuple[str, str]:
    ct = (content_type or "").lower()
    if "webm" in ct:
        return "webm", "audio/webm"
    if "mp4" in ct or "m4a" in ct:
        return "mp4", "audio/mp4"
    if "mpeg" in ct or "mp3" in ct:
        return "mp3", "audio/mpeg"
    if "ogg" in ct:
        return "ogg", "audio/ogg"
    if "wav" in ct or "wave" in ct:
        return "wav", "audio/wav"
    return "webm", "audio/webm"


def _spawn_bridges() -> list:
    """Launch local bridges as subprocesses.

    Each bridge will reconnect on its own if dimos starts late. Failures here
    are logged but never fatal; the dashboard still serves without bridges.

    Bridge stdout/stderr goes to browser_ui/logs/<name>.log so failures are
    visible without cluttering the main server terminal.
    """
    import subprocess
    import sys
    here = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(here, "logs")
    os.makedirs(log_dir, exist_ok=True)
    procs: list = []
    py = sys.executable

    # All bridges talk to dimos via LCM directly — no Socket.IO / command-center.
    bd = os.path.join(here, "bridges")
    bridge_specs = [
        (
            "camera_bridge",
            [py, "-u", os.path.join(bd, "camera_bridge.py"),
             "--cloud-url", SELF_URL,
             "--fps", str(CAMERA_BRIDGE_FPS),
             "--source", CAMERA_BRIDGE_SOURCE,
             "--http-url", CAMERA_HTTP_URL,
             "--publish-lcm" if CAMERA_PUBLISH_LCM.lower() == "true" else "--no-publish-lcm"],
        ),
        (
            "pc_bridge",
            [py, "-u", os.path.join(bd, "pc_bridge.py"),
             "--cloud-url", SELF_URL,
             "--fps", os.environ.get("PC_BRIDGE_FPS", "5")],
        ),
        (
            "nav_bridge",
            [py, "-u", os.path.join(bd, "nav_bridge.py"),
             "--cloud-url", SELF_URL,
             "--pose-hz", os.environ.get("NAV_POSE_HZ", "15"),
             "--goal-hz", os.environ.get("NAV_GOAL_POLL_HZ", "5")],
        ),
        (
            "costmap_bridge",
            [py, "-u", os.path.join(bd, "costmap_bridge.py"),
             "--cloud-url", SELF_URL,
             "--ws-url", os.environ.get("DIMOS_WS_URL", "http://localhost:7779")],
        ),
    ]
    for name, cmd in bridge_specs:
        try:
            log_path = os.path.join(log_dir, f"{name}.log")
            log_fh = open(log_path, "a")
            p = subprocess.Popen(
                cmd,
                cwd=here,
                stdout=log_fh,
                stderr=log_fh,
            )
            procs.append((name, p, log_fh))
            logger.info(f"[lifespan] spawned {name} pid={p.pid} log={log_path}")
        except Exception as e:
            logger.warning(f"[lifespan] failed to spawn {name}: {e}")
    return procs


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize shared resources on startup."""
    app.state.world_store = WorldStateStore(
        bucket=S3_BUCKET,
        use_memory=USE_MEMORY_STORE,
        region=AWS_REGION,
    )
    logger.info(
        f"World store initialized: {'memory' if USE_MEMORY_STORE else 'S3'} "
        f"(bucket={S3_BUCKET})"
    )
    app.state.bridge_procs = _spawn_bridges() if AUTO_BRIDGES else []
    try:
        yield
    finally:
        # Best-effort cleanup of bridge subprocesses on shutdown.
        for entry in app.state.bridge_procs:
            name, p, *rest = entry
            fh = rest[0] if rest else None
            try:
                p.terminate()
                p.wait(timeout=2)
            except Exception:
                try:
                    p.kill()
                except Exception:
                    pass
            if fh:
                try:
                    fh.close()
                except Exception:
                    pass
            logger.info(f"[lifespan] stopped {name}")


app = FastAPI(
    title="AWS Cloud Middleware — DimOS",
    description="Communication and inference hub for Unitree Go2 robot",
    lifespan=lifespan,
)

# Robot render models (URDF + meshes) for the dashboard's URDFLoader --
# one directory per model slug, e.g. static/models/ranger_mini_v3/{robot.urdf,meshes/}.
# See config/ros2_fleet.example.yaml's `model:` field for how a robot picks one.
_MODELS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "models")
os.makedirs(_MODELS_DIR, exist_ok=True)
app.mount("/models", StaticFiles(directory=_MODELS_DIR), name="models")


# ── Health Check ──────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "store": "memory" if USE_MEMORY_STORE else "s3"}


# ── Auth ──────────────────────────────────────────────────────

@app.post("/auth/login")
async def login(request: Request):
    """Browser login — returns a session token on success.

    If DASHBOARD_PASSWORD is not set the server is open and returns
    {"token": "open", "auth": false} so the client skips the login screen.
    """
    if not DASHBOARD_PASSWORD:
        return {"token": "open", "auth": False}
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "Expected JSON body with 'password' key")
    verify_dashboard_password(body.get("password", ""))
    token = create_session()
    return {"token": token, "auth": True}


@app.post("/auth/ping")
async def session_ping(_tok: str = Depends(verify_session)):
    """Keepalive — browser sends this every 10 s to hold the session slot.
    If pings stop (tab closed), the session expires after SESSION_TTL seconds.
    """
    return {"ok": True}


# ── Speech-to-text (AWS Transcribe) ───────────────────────────

@app.post("/speech/transcribe")
async def transcribe_speech(request: Request, _tok: str = Depends(verify_session)):
    """Transcribe a short browser-recorded audio clip via Amazon Transcribe.

    The browser posts the audio blob directly. We upload it to S3 because
    Transcribe batch jobs require an S3 media URI, then poll the short job and
    return the text for the active Ask/Agent chat tab.
    """
    audio = await request.body()
    if not audio:
        raise HTTPException(400, "empty audio body")

    _load_aws_env_from_dimos()
    bucket = os.environ.get("S3_BUCKET", S3_BUCKET)
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION") or AWS_REGION
    lang = os.environ.get("AWS_TRANSCRIBE_LANGUAGE", AWS_TRANSCRIBE_LANGUAGE)
    timeout = float(os.environ.get("AWS_TRANSCRIBE_TIMEOUT", str(AWS_TRANSCRIBE_TIMEOUT)))
    fmt, content_type = _audio_format_from_content_type(
        request.headers.get("content-type", "")
    )

    try:
        import boto3
        import httpx
    except Exception as exc:
        raise HTTPException(500, f"missing AWS transcription dependency: {exc}")

    job = f"robohack-speech-{uuid.uuid4().hex}"
    key = f"{SPEECH_S3_PREFIX}/{job}.{fmt}"
    s3_uri = f"s3://{bucket}/{key}"

    s3 = boto3.client("s3", region_name=region)
    transcribe = boto3.client("transcribe", region_name=region)
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=audio, ContentType=content_type)
        transcribe.start_transcription_job(
            TranscriptionJobName=job,
            Media={"MediaFileUri": s3_uri},
            MediaFormat=fmt,
            LanguageCode=lang,
        )

        deadline = time.time() + timeout
        last_status = "QUEUED"
        while time.time() < deadline:
            info = transcribe.get_transcription_job(TranscriptionJobName=job)[
                "TranscriptionJob"
            ]
            last_status = info["TranscriptionJobStatus"]
            if last_status == "COMPLETED":
                transcript_uri = info["Transcript"]["TranscriptFileUri"]
                data = httpx.get(transcript_uri, timeout=10).json()
                text = (data.get("results", {}).get("transcripts") or [{}])[0].get(
                    "transcript", ""
                )
                return {"text": text, "job": job, "language": lang}
            if last_status == "FAILED":
                raise HTTPException(
                    502,
                    info.get("FailureReason", "Amazon Transcribe job failed"),
                )
            time.sleep(1.0)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(502, f"speech transcription failed: {exc}")
    finally:
        try:
            s3.delete_object(Bucket=bucket, Key=key)
        except Exception:
            pass

    raise HTTPException(504, f"speech transcription timed out ({last_status})")


# ── Robot Endpoints (Req 1: Ingestion) ────────────────────────

@app.post("/ingest", response_model=IngestResponse)
async def ingest(request: IngestRequest, _: None = Depends(verify_bridge)):
    store: WorldStateStore = app.state.world_store
    previous = store.load(request.robot_id)
    objects = _merge_detected_objects(previous.objects if previous else [], request.objects)
    state = WorldState(
        robot_id=request.robot_id,
        timestamp=time.time(),
        objects=objects,
    )
    try:
        store.save(state)
    except Exception as e:
        logger.error(f"Failed to save world state for {request.robot_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Storage error: {e}")

    return IngestResponse(
        status="saved",
        robot_id=request.robot_id,
        count=len(objects),
        timestamp=state.timestamp,
    )


def _merge_detected_objects(existing, incoming):
    """Merge semantic detections instead of replacing the whole map each push."""
    merged = [obj.model_copy(deep=True) for obj in existing]
    for obj in incoming:
        match = None
        for prev in merged:
            if prev.label.lower() != obj.label.lower():
                continue
            dx = prev.pose.x - obj.pose.x
            dy = prev.pose.y - obj.pose.y
            dz = prev.pose.z - obj.pose.z
            if (dx * dx + dy * dy + dz * dz) ** 0.5 <= 1.0:
                match = prev
                break
        if match is None:
            merged.append(obj)
            continue
        old_n = max(1, match.seen_count)
        new_n = old_n + max(1, obj.seen_count)
        match.pose.x = (match.pose.x * old_n + obj.pose.x) / new_n
        match.pose.y = (match.pose.y * old_n + obj.pose.y) / new_n
        match.pose.z = (match.pose.z * old_n + obj.pose.z) / new_n
        match.confidence = max(match.confidence, obj.confidence)
        match.seen_count = new_n
        match.last_seen = max(match.last_seen, obj.last_seen)
        match.source = obj.source or match.source
    return sorted(merged, key=lambda o: o.last_seen, reverse=True)[:200]


# ── User Endpoints (Req 2: Query) ────────────────────────────

def _stream_mcp_response(text: str, mode: str) -> StreamingResponse:
    """Shared SSE wrapper around the MCP-aware Bedrock agent."""
    from core.agent_mcp import run_mcp_agent_stream

    def generate():
        try:
            for token in run_mcp_agent_stream(text, mode=mode):
                yield f"data: {json.dumps(token)}\n\n"
            yield "data: [DONE]\n\n"
        except Exception as e:
            logger.exception("MCP agent stream error")
            yield f"data: {json.dumps(f'Error: {e}')}\n\n"
            yield "data: [DONE]\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.post("/query/stream")
async def query_stream(request: QueryRequest, _tok: str = Depends(verify_session)):
    """🧠 Ask mode — read-only Q&A over the dimos MCP.

    The agent has the full set of dimos read-only tools (observe, server_status,
    spatial-memory queries, …) but NO movement / action tools. If the user
    asks the robot to act, the agent explains and points them to the Agent tab.
    """
    use_mcp = os.environ.get("USE_MCP_AGENT", "true").lower() == "true"

    if use_mcp:
        try:
            return _stream_mcp_response(request.text, mode="ask")
        except ImportError as e:
            logger.warning(f"MCP agent unavailable: {e}")
    raise HTTPException(
        status_code=503,
        detail="Agent backend unavailable. Set USE_MCP_AGENT=true and ensure dimos MCP is running.",
    )


@app.post("/command/stream")
async def command_stream(request: QueryRequest, _tok: str = Depends(verify_session)):
    """🤖 Agent mode — full tool access. Can move the robot, explore, etc."""
    use_mcp = os.environ.get("USE_MCP_AGENT", "true").lower() == "true"
    if use_mcp:
        try:
            return _stream_mcp_response(request.text, mode="agent")
        except ImportError as e:
            logger.warning(f"MCP agent unavailable: {e}")
    raise HTTPException(
        status_code=503,
        detail="Agent backend unavailable. Set USE_MCP_AGENT=true and ensure dimos MCP is running.",
    )


# ── Camera Frames ─────────────────────────────────────────────

_latest_frames: dict[str, bytes] = {}


@app.post("/frames")
async def receive_frame(request: Request, _: None = Depends(verify_bridge)):
    robot_id = request.headers.get("X-Robot-Id", "go2_a")
    body = await request.body()
    if not body:
        raise HTTPException(400, "Empty frame")
    _latest_frames[robot_id] = body
    return {"status": "ok", "robot_id": robot_id, "size": len(body)}


@app.get("/frames/{robot_id}")
async def get_frame(robot_id: str, _tok: str = Depends(verify_session)):
    frame = _latest_frames.get(robot_id)
    if frame is None:
        raise HTTPException(404, "No frame available")
    return Response(content=frame, media_type="image/jpeg")


@app.get("/frames/{robot_id}/stream")
async def stream_frames(robot_id: str, _tok: str = Depends(verify_session)):
    import asyncio
    async def generate():
        while True:
            frame = _latest_frames.get(robot_id)
            if frame:
                yield b"--frame\r\nContent-Type: image/jpeg\r\n\r\n" + frame + b"\r\n"
            await asyncio.sleep(0.1)
    return StreamingResponse(generate(), media_type="multipart/x-mixed-replace; boundary=frame")


# ── LiDAR Point Cloud ─────────────────────────────────────────
# pc_bridge.py POSTs zlib-compressed int16 triplets (x_cm, y_cm, z_cm).
# We accumulate those raw lidar points into a coarse voxel map so the UI shows
# the explored area, not only the most recent scan.

_live_pc: dict[str, dict] = {}  # robot_id -> {b64, n, z_min, z_max, timestamp}
_pc_voxels: dict[str, dict[tuple[int, int, int], dict]] = {}

# _live_costmap is declared inside the costmap heatmap section below.


def _rebuild_accumulated_pointcloud(robot_id: str) -> None:
    voxels = _pc_voxels.get(robot_id, {})
    pts = [
        (v["x"], v["y"], v["z"])
        for v in voxels.values()
        if v.get("score", 0.0) >= PC_RENDER_SCORE
    ]
    if not pts:
        _live_pc[robot_id] = {
            "b64": "",
            "n": 0,
            "z_min": 0,
            "z_max": 0,
            "timestamp": time.time(),
            "voxel_cm": PC_ACCUM_VOXEL_CM,
            "obs_radius_cm": PC_OBS_RADIUS_CM,
            "render_score": PC_RENDER_SCORE,
            "accumulated": True,
        }
        return

    raw = bytearray(len(pts) * 6)
    z_min = 32767
    z_max = -32768
    for i, (x, y, z) in enumerate(pts):
        struct.pack_into("<hhh", raw, i * 6, x, y, z)
        z_min = min(z_min, z)
        z_max = max(z_max, z)

    _live_pc[robot_id] = {
        "b64": base64.b64encode(raw).decode(),
        "n": len(pts),
        "z_min": z_min,
        "z_max": z_max,
        "timestamp": time.time(),
        "voxel_cm": PC_ACCUM_VOXEL_CM,
        "obs_radius_cm": PC_OBS_RADIUS_CM,
        "render_score": PC_RENDER_SCORE,
        "accumulated": True,
    }


def _update_accumulated_voxels(
    robot_id: str,
    pts_bytes: bytes,
    n: int,
) -> None:
    """Merge a fresh lidar scan into a confidence-weighted accumulated map.

    Repeated hits increase a voxel's score and update its position by running
    average. Missing voxels in the currently observed neighborhood decay instead
    of being deleted immediately. This keeps stable structure visible while
    allowing one-off false positives to fade after the robot looks there again.
    """
    voxels = _pc_voxels.setdefault(robot_id, {})
    now = time.time()
    step = PC_ACCUM_VOXEL_CM
    obs_cells = max(0, round(PC_OBS_RADIUS_CM / step))
    current: dict[tuple[int, int, int], tuple[int, int, int]] = {}
    observed_xy: set[tuple[int, int]] = set()

    for i in range(n):
        x, y, z = struct.unpack_from("<hhh", pts_bytes, i * 6)
        kx = round(x / step)
        ky = round(y / step)
        kz = round(z / step)
        key = (kx, ky, kz)
        current[key] = (x, y, z)
        if obs_cells:
            for dx in range(-obs_cells, obs_cells + 1):
                for dy in range(-obs_cells, obs_cells + 1):
                    observed_xy.add((kx + dx, ky + dy))
        else:
            observed_xy.add((kx, ky))

    # Gentle global aging prevents old one-off artifacts from living forever.
    for key, val in list(voxels.items()):
        age = max(0.0, now - val.get("updated_at", now))
        if age > 0:
            val["score"] = max(0.0, val.get("score", 0.0) - age * PC_TIME_DECAY_PER_SEC)
            val["updated_at"] = now

    for key, (x, y, z) in current.items():
        val = voxels.get(key)
        if val is None:
            voxels[key] = {
                "x": x,
                "y": y,
                "z": z,
                "score": PC_HIT_SCORE,
                "hits": 1,
                "updated_at": now,
            }
            continue

        hits = val.get("hits", 1) + 1
        alpha = min(0.35, 1.0 / hits)
        val["x"] = round(val["x"] * (1.0 - alpha) + x * alpha)
        val["y"] = round(val["y"] * (1.0 - alpha) + y * alpha)
        val["z"] = round(val["z"] * (1.0 - alpha) + z * alpha)
        val["hits"] = hits
        val["score"] = min(PC_MAX_SCORE, val.get("score", 0.0) + PC_HIT_SCORE)
        val["updated_at"] = now

    # Stronger local decay: if the robot is actively observing this XY region
    # and a stored voxel is not seen again, lower its confidence but do not
    # erase it immediately. Consistent surfaces survive; false positives fade.
    if observed_xy:
        for key, val in list(voxels.items()):
            if key not in current and (key[0], key[1]) in observed_xy:
                val["score"] = val.get("score", 0.0) * PC_MISS_DECAY

    for key, val in list(voxels.items()):
        if val.get("score", 0.0) < PC_DELETE_SCORE:
            del voxels[key]

    # Bound memory and UI payload size. Dict order is insertion order; this
    # drops the weakest/oldest voxels first.
    overflow = len(voxels) - PC_ACCUM_MAX_POINTS
    if overflow > 0:
        drop = sorted(
            voxels,
            key=lambda k: (voxels[k].get("score", 0.0), voxels[k].get("updated_at", 0.0)),
        )[:overflow]
        for key in drop:
            del voxels[key]


@app.post("/ingest/pointcloud")
async def ingest_pointcloud(request: Request, _: None = Depends(verify_bridge)):
    robot_id = request.headers.get("X-Robot-Id", "go2_a")
    body = await request.body()
    if not body:
        raise HTTPException(400, "Empty payload")
    try:
        raw = zlib.decompress(body)
        n = struct.unpack_from("<i", raw)[0]
        pts_bytes = raw[4:]  # N × 6 bytes of int16 triplets
        if len(pts_bytes) < n * 6:
            raise ValueError(f"payload too short for {n} points")

        _update_accumulated_voxels(robot_id, pts_bytes, n)
        _rebuild_accumulated_pointcloud(robot_id)
    except Exception as e:
        raise HTTPException(400, f"Decode error: {e}")
    return {"status": "ok", "robot_id": robot_id, "n": _live_pc[robot_id]["n"]}


@app.get("/pointcloud/live")
async def get_live_pointcloud(_tok: str = Depends(verify_session)):
    return _live_pc


# ── Costmap heatmap ───────────────────────────────────────────
# costmap_bridge.py (DimOS Socket.IO) POSTs here.  Full and delta updates
# are accumulated server-side; the browser receives a complete flat grid
# each poll and only re-renders when the version counter changes.
#
# Per-robot state keys: buf (raw bytes), shape, v (version), data (b64),
# origin, resolution, timestamp.

_live_costmap: dict[str, dict] = {}


def _apply_costmap_update(robot_id: str, cm: dict) -> bool:
    """
    Decode DimOS OptimizedCostmapEncoder payload into a flat uint8 buffer.
    Only bumps the version counter when grid content, origin, or resolution
    actually changed — so the browser skips truly-unchanged polls.
    Returns True if the stored costmap was updated.
    """
    grid_data = cm.get("grid", {})
    update_type = grid_data.get("update_type", "")
    shape = grid_data.get("shape", [0, 0])
    existing = _live_costmap.get(robot_id, {})

    try:
        if update_type == "full" and grid_data.get("data"):
            buf = bytearray(zlib.decompress(base64.b64decode(grid_data["data"])))

        elif update_type == "delta" and "buf" in existing:
            chunks = grid_data.get("chunks", [])
            if not chunks:
                # Empty delta — nothing changed in the grid.
                # Still update origin/resolution if they shifted.
                origin = cm.get("origin", {})
                oc = origin.get("c", [0.0, 0.0, 0.0]) if isinstance(origin, dict) else [0.0, 0.0, 0.0]
                new_origin = {"x": oc[0], "y": oc[1]}
                new_res = cm.get("resolution", 0.05)
                old_origin = existing.get("origin", {})
                old_res = existing.get("resolution", 0.05)
                if (abs(new_origin.get("x", 0) - old_origin.get("x", 0)) > 1e-6 or
                    abs(new_origin.get("y", 0) - old_origin.get("y", 0)) > 1e-6 or
                    abs(new_res - old_res) > 1e-9):
                    # Origin or resolution shifted — bump version so browser
                    # repositions the plane, but reuse existing grid data.
                    existing["origin"] = new_origin
                    existing["resolution"] = new_res
                    existing["v"] = existing.get("v", 0) + 1
                    existing["timestamp"] = time.time()
                    return True
                return False
            buf   = bytearray(existing["buf"])
            shape = existing.get("shape", shape)
            h, w  = shape
            for chunk in chunks:
                cy, cx       = chunk["pos"]
                ch_h, ch_w   = chunk["size"]
                raw = zlib.decompress(base64.b64decode(chunk["data"]))
                for row in range(ch_h):
                    dst = (cy + row) * w + cx
                    buf[dst:dst + ch_w] = raw[row * ch_w:(row + 1) * ch_w]

        else:
            if update_type not in ("full", "delta"):
                logger.debug(f"[costmap] unknown update_type={update_type!r} for {robot_id}")
            elif update_type == "delta":
                logger.debug(f"[costmap] delta arrived before first full for {robot_id} — skipping")
            return False

    except Exception as e:
        logger.warning(f"[costmap] decode error for {robot_id}: {e}")
        return False

    buf_bytes = bytes(buf)
    origin = cm.get("origin", {})
    oc = origin.get("c", [0.0, 0.0, 0.0]) if isinstance(origin, dict) else [0.0, 0.0, 0.0]

    # Skip version bump if grid bytes are identical to what we already have.
    if existing.get("buf") == buf_bytes and existing.get("shape") == shape:
        new_origin = {"x": oc[0], "y": oc[1]}
        old_origin = existing.get("origin", {})
        new_res = cm.get("resolution", 0.05)
        old_res = existing.get("resolution", 0.05)
        if (abs(new_origin.get("x", 0) - old_origin.get("x", 0)) < 1e-6 and
            abs(new_origin.get("y", 0) - old_origin.get("y", 0)) < 1e-6 and
            abs(new_res - old_res) < 1e-9):
            return False
        # Origin/resolution changed, same grid — update metadata and bump.
        existing["origin"] = new_origin
        existing["resolution"] = new_res
        existing["v"] = existing.get("v", 0) + 1
        existing["timestamp"] = time.time()
        return True

    _live_costmap[robot_id] = {
        "buf":        buf_bytes,
        "shape":      shape,
        "v":          existing.get("v", 0) + 1,
        "data":       base64.b64encode(buf_bytes).decode(),
        "origin":     {"x": oc[0], "y": oc[1]},
        "resolution": cm.get("resolution", 0.05),
        "timestamp":  time.time(),
    }
    return True


@app.post("/ingest/costmap")
async def ingest_costmap(request: Request, _: None = Depends(verify_bridge)):
    robot_id = request.headers.get("X-Robot-Id", "go2_a")
    try:
        body = await request.json()
    except Exception as e:
        raise HTTPException(400, f"bad JSON: {e}")
    cm = body.get("costmap") or body
    updated = _apply_costmap_update(robot_id, cm)
    return {"status": "ok" if updated else "skipped", "robot_id": robot_id}


@app.get("/costmap/live")
async def get_live_costmap(_tok: str = Depends(verify_session)):
    # Exclude raw buf bytes — browser only needs b64-encoded data + metadata.
    return {
        rid: {k: v for k, v in cm.items() if k != "buf"}
        for rid, cm in _live_costmap.items()
    }


# ── Live robot pose ───────────────────────────────────────────

# Latest pose per robot. nav_bridge.py POSTs at ~15 Hz from /odom LCM.
_live_pose: dict[str, dict] = {}


@app.post("/ingest/pose")
async def ingest_pose(request: Request, _: None = Depends(verify_bridge)):
    robot_id = request.headers.get("X-Robot-Id", "go2_a")
    try:
        data = await request.json()
    except Exception as e:
        raise HTTPException(400, f"bad JSON: {e}")
    if not isinstance(data, dict):
        raise HTTPException(400, "expected pose dict")
    data["robot_id"] = robot_id
    _live_pose[robot_id] = data
    return {"status": "ok"}


@app.get("/pose/live")
async def get_live_pose(_tok: str = Depends(verify_session)):
    return _live_pose


# ── Planned path (from DimOS planner via costmap_bridge) ──────
# Points are [[x, y], ...] in robot map frame, updated whenever DimOS replans.

_live_path: dict[str, dict] = {}


@app.post("/ingest/path")
async def ingest_path(request: Request, _: None = Depends(verify_bridge)):
    robot_id = request.headers.get("X-Robot-Id", "go2_a")
    try:
        body = await request.json()
    except Exception as e:
        raise HTTPException(400, f"bad JSON: {e}")

    points = body.get("path", {}).get("points", [])
    goal_yaw = body.get("path", {}).get("goal_yaw")
    _live_path[robot_id] = {"points": points, "goal_yaw": goal_yaw, "timestamp": time.time()}

    # _active_goal (see /goal/active below) used to get overwritten here on
    # every path push -- fine for one robot, actively wrong once a fleet is
    # pushing paths concurrently: two robots racing to overwrite one global
    # "the goal" produced a stray marker at whichever robot posted last,
    # paired with a line drawn from a different (fixed, single) robot's
    # position -- looked exactly like a phantom, disconnected path. Each
    # robot's own current plan is already shown correctly, per-robot, via
    # /path/live -> updateFleetPaths' fleetGoalMarkers on the client; this
    # endpoint has no business touching the single click-to-navigate goal.
    return {"status": "ok", "robot_id": robot_id, "n": len(points)}


@app.get("/path/live")
async def get_live_path(_tok: str = Depends(verify_session)):
    return _live_path


# ── Static map (single shared source robot's nav2 /map) ───────
# Unlike /ingest/costmap, this is push-once, not a delta protocol -- a
# static map doesn't need one. `data` is a zlib+base64 blob of one bucket
# byte per cell (0=free, 1=occupied, 2=unknown), decompressed here and
# re-served as plain base64 so the browser only ever needs atob(), same
# pattern _apply_costmap_update already uses for its own grid payloads.

_live_map: dict | None = None


@app.post("/ingest/map")
async def ingest_map(request: Request, _: None = Depends(verify_bridge)):
    global _live_map
    try:
        body = await request.json()
        raw = zlib.decompress(base64.b64decode(body["data"]))
    except Exception as e:
        raise HTTPException(400, f"bad map payload: {e}")
    _live_map = {
        "width": body.get("width"),
        "height": body.get("height"),
        "resolution": body.get("resolution", 0.05),
        "origin": body.get("origin", {"x": 0.0, "y": 0.0}),
        "data": base64.b64encode(raw).decode(),
        "timestamp": time.time(),
    }
    return {"status": "ok", "width": body.get("width"), "height": body.get("height")}


@app.get("/map/live")
async def get_live_map(_tok: str = Depends(verify_session)):
    return _live_map or {}


# ── Mission export (visual multi-robot mission planner) ───────
# Turns the browser's in-progress mission draft (per-robot start/goal poses
# set by clicking the map, see dashboard.html's Plan Mission mode) into a
# real mission YAML -- field-for-field the shape fleet_coordinator.robot.
# Fleet.from_yaml already expects (see fleet-coordinator/config/
# mission.example.yaml), so this is authoring-time only: it hands back a
# file for you to point `coordinator_node`'s `mission_file` param at
# yourself. Actually *running* a mission (including seeding /initialpose
# from each robot's declared start) stays entirely fleet-coordinator's job,
# same as it already is today -- this doesn't add a second way to command
# a robot, just a visual way to write the file.

def _build_mission_yaml(robots: list, header: str) -> str:
    specs = [r.model_dump() for r in robots]
    return f"# {header}\n" + yaml.safe_dump(specs, sort_keys=False, default_flow_style=False)


@app.post("/mission/export")
async def export_mission(req: MissionExportRequest, _tok: str = Depends(verify_session)):
    if not req.robots:
        raise HTTPException(400, "mission draft has no robots")
    yaml_text = _build_mission_yaml(req.robots, "Exported from argOS's Plan Mission mode")
    return Response(
        content=yaml_text,
        media_type="application/x-yaml",
        headers={"Content-Disposition": "attachment; filename=mission.yaml"},
    )


# ── Mission launch (executes coordinator_node, doesn't just export) ────
# coordinator_node isn't wrapped as a server the way Spooky is -- Spooky's
# job (plan-and-return) is a natural request/response fit; coordinator_
# node's job is to own and drive a live mission for its full duration (TF,
# release-gating, dispatch, deadlock checks), a supervised one-shot process,
# not a stateless computation. So argOS doesn't launch anything itself
# (still no ROS2 dependency here) -- it just relays a launch/stop command
# to fleet_bridge.py, the ROS2-sourced process that actually owns the
# coordinator_node subprocess (mirrors run_bridges.py's own spawn/monitor/
# SIGTERM-then-SIGKILL pattern, plus process-group killing since `ros2 run`
# can spawn children a plain terminate() would orphan), and stores whatever
# status it reports back for the browser to poll.

_mission_run: dict = {"status": "idle", "pid": None, "returncode": None, "message": None}
_mission_command: dict | None = None  # consumed once by fleet_bridge.py's next poll


@app.post("/mission/launch")
async def launch_mission(req: MissionLaunchRequest, _tok: str = Depends(verify_session)):
    global _mission_command
    if _mission_run.get("status") in ("running", "starting"):
        raise HTTPException(409, "a mission is already running -- stop it first")
    if not req.robots:
        raise HTTPException(400, "mission draft has no robots")
    yaml_text = _build_mission_yaml(req.robots, "Launched from argOS's Plan Mission mode")
    _mission_command = {
        "action": "launch",
        "yaml": yaml_text,
        "params": req.params.model_dump(),
    }
    return {"status": "queued"}


@app.post("/mission/launch/stop")
async def stop_mission(_tok: str = Depends(verify_session)):
    global _mission_command
    _mission_command = {"action": "stop"}
    return {"status": "stop requested"}


@app.get("/mission/launch/command")
async def get_mission_command(_: None = Depends(verify_bridge)):
    global _mission_command
    cmd = _mission_command or {"action": None}
    _mission_command = None
    return cmd


@app.post("/mission/launch/status")
async def post_mission_status(request: Request, _: None = Depends(verify_bridge)):
    global _mission_run
    body = await request.json()
    _mission_run = {
        "status": body.get("status", "unknown"),
        "pid": body.get("pid"),
        "returncode": body.get("returncode"),
        "message": body.get("message"),
        "timestamp": time.time(),
    }
    return {"ok": True}


@app.get("/mission/launch/status")
async def get_mission_status(_tok: str = Depends(verify_session)):
    return _mission_run


# ── Navigation — goal queue ───────────────────────────────────

_goal_queue: list[dict] = []
_active_goal: dict | None = None


@app.post("/navigate")
async def navigate_to_point(x: float, y: float, z: float = 0.0, _tok: str = Depends(verify_session)):
    """Queue a navigation goal — nav_bridge forwards it to DimOS over LCM."""
    global _active_goal
    goal = {"x": x, "y": y, "z": z, "ts": time.time()}
    _goal_queue.append(goal)
    _active_goal = goal
    return {"status": "queued", "target": {"x": x, "y": y, "z": z}}


@app.get("/goal/active")
async def get_active_goal(_tok: str = Depends(verify_session)):
    """Current navigation goal — polled by all dashboard viewers."""
    return _active_goal or {}


@app.post("/goal/clear")
async def clear_active_goal(_tok: str = Depends(verify_session)):
    """Called by any viewer when the robot reaches the goal."""
    global _active_goal
    _active_goal = None
    return {"status": "cleared"}


@app.get("/goals/pending")
async def get_pending_goals(_: None = Depends(verify_bridge)):
    goals = list(_goal_queue)
    _goal_queue.clear()
    return {"goals": goals}


# ── MCP Bridge Proxy ──────────────────────────────────────────
# The robot runs a dimos_bridge script that polls GET /bridge/mcp/pending,
# executes each MCP JSON-RPC call against its local dimos server (localhost:9990),
# and POSTs the result back here.  Set DIMOS_MCP_BRIDGE=1 so agent_mcp.py
# routes calls through this queue instead of directly to localhost:9990.

@app.get("/bridge/mcp/pending")
async def get_pending_mcp_call(
    timeout: float = Query(25.0, ge=0.5, le=60.0),
    _: None = Depends(verify_bridge),
):
    """Long-poll: robot's bridge script waits here for the next MCP call to execute."""
    from core.agent_mcp import _bridge_call_queue
    loop = asyncio.get_running_loop()
    try:
        call = await loop.run_in_executor(
            None, lambda: _bridge_call_queue.get(timeout=timeout)
        )
        return call
    except _queue.Empty:
        return {}


@app.post("/bridge/mcp/result")
async def post_mcp_result(request: Request, _: None = Depends(verify_bridge)):
    """Robot's bridge script posts the MCP execution result here."""
    from core.agent_mcp import _bridge_result_data, _bridge_result_events, _bridge_lock
    data = await request.json()
    call_id = data.get("bridge_id")
    if call_id:
        with _bridge_lock:
            _bridge_result_data[call_id] = data
            ev = _bridge_result_events.get(call_id)
        if ev:
            ev.set()
    return {"ok": True}


# ── Map Endpoint ──────────────────────────────────────────────

@app.get("/map", response_model=MapResponse)
async def get_map(_tok: str = Depends(verify_session)):
    store: WorldStateStore = app.state.world_store
    states = store.load_all()
    merged = store.merge()
    return MapResponse(objects=merged, robot_count=len(states), timestamp=time.time())


@app.get("/debug/bridges")
async def debug_bridges(_: None = Depends(verify_bridge)):
    """Bridge health check — shows pid, alive status, and last 20 log lines."""
    import psutil
    here = os.path.dirname(os.path.abspath(__file__))
    log_dir = os.path.join(here, "logs")
    result = {}
    for entry in getattr(app.state, "bridge_procs", []):
        name, p, *_ = entry
        alive = p.poll() is None
        # Try to get CPU/mem from psutil if available
        try:
            proc = psutil.Process(p.pid)
            mem_mb = round(proc.memory_info().rss / 1e6, 1)
        except Exception:
            mem_mb = None
        # Last few lines of log
        log_path = os.path.join(log_dir, f"{name}.log")
        tail = []
        try:
            with open(log_path) as f:
                tail = f.readlines()[-20:]
        except Exception:
            pass
        result[name] = {
            "pid": p.pid,
            "alive": alive,
            "exit_code": p.returncode,
            "mem_mb": mem_mb,
            "log_tail": [l.rstrip() for l in tail],
        }
    return result


# ── Dashboard UI ──────────────────────────────────────────────
# Served from web/dashboard.html instead of an inline Python string so the
# frontend gets normal editor/devtools support. no-store: this file changes
# often during development and carries no ETag/Last-Modified, so without an
# explicit directive some browsers will serve a stale copy on reload.

_DASHBOARD_HTML_PATH = Path(__file__).parent / "web" / "dashboard.html"


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    html = _DASHBOARD_HTML_PATH.read_text(encoding="utf-8").replace(
        "_AUTH_PLACEHOLDER_",
        "true" if DASHBOARD_PASSWORD else "false",
    )
    return HTMLResponse(content=html, headers={"Cache-Control": "no-store"})




if __name__ == "__main__":
    import argparse
    import uvicorn

    parser = argparse.ArgumentParser(description="ArgOS cloud server")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8080, help="Bind port (default: 8080)")
    parser.add_argument(
        "--server-only", action="store_true",
        help="Disable auto-spawning of local LCM bridges (use on cloud/EC2)"
    )
    parser.add_argument(
        "--reload", action="store_true",
        help="Enable uvicorn auto-reload (development only)"
    )
    args = parser.parse_args()

    if args.server_only:
        os.environ["AUTO_BRIDGES"] = "false"

    uvicorn.run("main:app", host=args.host, port=args.port, reload=args.reload)
