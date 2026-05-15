# DimOS patches

Five bug fixes required for stable operation with ArgOS. Apply them on top of a
clean DimOS checkout before running anything.

## Apply

```bash
cd /path/to/dimos
git apply /path/to/argOS/patches/dimos/*.patch
```

Verify cleanly applied:

```bash
git diff --stat HEAD
# Should show 5 files changed
```

If a patch fails (e.g. DimOS updated the surrounding code), apply individually
and resolve by hand:

```bash
git apply patches/dimos/001-speak-skill-tts-guard.patch
# ... fix conflicts if any, then:
git apply patches/dimos/002-path-mask-no-crash.patch
# etc.
```

## What each patch does

| File | Problem fixed |
|---|---|
| `001-speak-skill-tts-guard.patch` | `SpeakSkill.start()` crashed on import if `OPENAI_API_KEY` was not set, taking down the whole agentic blueprint at startup |
| `002-path-mask-no-crash.patch` | `make_path_mask()` raised `ValueError` when more than 5% of path points were occupied, freezing the local planner instead of triggering a replan |
| `003-path-clearance-guard.patch` | `is_obstacle_ahead()` had no exception handling — a transient numpy mask error would kill the local planner thread permanently |
| `004-bbox-circles-fix.patch` | `Detection2DBBox.to_foxglove_annotations()` left `circles` and `circles_length` unset; the LCM subscriber read garbage for `circles_length` and crashed in a decode loop |
| `005-lcmservice-rate-limit.patch` | Identical decode errors in the LCM loop were logged thousands of times per second when a duplicate publisher was active (e.g. two processes both publishing on `/yolo11/annotations`) |

## DimOS version

These patches were written against DimOS at commit `b71d994e` (May 2026).
If DimOS has moved on significantly, read each `.patch` file — they are small
and the intent is clear enough to port by hand.
