#!/usr/bin/env python3
"""
v6-orchestrator coordinator.py (SSH-push architecture)

Runs on the coordinator server (Hermes/PVE-23).
Manages queue.json locally. SSH-pushes work orders to workers.
Workers never SSH back to coordinator — coordinator initiates all connections.

Main loop:
  1. scan_sources() — SSH into workers, rclone lsjson their remotes
  2. reconcile() — merge new items into queue.json
  3. dispatch() — for each idle worker, push next pending item (priority-sorted)
  4. collect() — read work_order_status.json from each worker
  5. reap_dead_workers() — workers with no heartbeat for N minutes
  6. retry_failed_items() — smart error classification (transient/resource/permanent)
  7. archive_old_items() — remove old done items
  8. sleep 60s

New in this version:
  - Atomic claim re-check to prevent race conditions
  - Smart retry with error classification (transient/resource/permanent)
  - Priority system (urgent > high > normal > low) with auto-detection
  - Enhanced health checks (GPU, disk space, rclone)
  - History tracking and analytics (history.json)
  - HTTP API server (configurable port)
"""

import fcntl
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler

import yaml

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
CONFIG_PATH = "/opt/v6-orchestrator/config.yaml"
QUEUE_PATH = "/opt/v6-orchestrator/queue.json"
QUEUE_LOCK_PATH = QUEUE_PATH + ".lock"
WORKERS_PATH = "/opt/v6-orchestrator/workers.json"
HISTORY_PATH = "/opt/v6-orchestrator/history.json"
LOG_PATH = "/opt/v6-orchestrator/logs/coordinator.log"
REPORT_PATH = "/opt/v6-orchestrator/logs/coordinator-report.json"


def setup_logging():
    os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
    logger = logging.getLogger("coordinator")
    logger.setLevel(logging.DEBUG)
    if logger.handlers:
        logger.handlers.clear()
    fh = logging.FileHandler(LOG_PATH)
    fh.setLevel(logging.DEBUG)
    sh = logging.StreamHandler(sys.stderr)
    sh.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh.setFormatter(fmt)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


logger = setup_logging()


def load_config(path=CONFIG_PATH):
    with open(path) as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# SSH helpers
# ---------------------------------------------------------------------------
def ssh_run(host, user, password, command, timeout=60):
    """Run command on remote host via sshpass + ssh."""
    port = "22"
    cmd = [
        "sshpass", "-p", password,
        "ssh", "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=15",
        "-p", port,
        f"{user}@{host}",
        command,
    ]
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout,
        )
        return result.returncode, result.stdout.strip(), result.stderr.strip()
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"
    except Exception as exc:
        return -1, "", str(exc)


def ssh_test_reachable(host, user, password, timeout=10):
    """Quick SSH reachability test."""
    rc, out, err = ssh_run(host, user, password, "echo pong", timeout=timeout)
    return rc == 0 and "pong" in out


# ---------------------------------------------------------------------------
# Queue locking
# ---------------------------------------------------------------------------
def lock_queue():
    """Acquire exclusive lock on queue.json. Returns lock fd."""
    lock_fd = open(QUEUE_LOCK_PATH, "w")
    fcntl.flock(lock_fd, fcntl.LOCK_EX)
    return lock_fd


def unlock_queue(lock_fd):
    fcntl.flock(lock_fd, fcntl.LOCK_UN)
    lock_fd.close()


def load_queue():
    with open(QUEUE_PATH) as f:
        return json.load(f)


def save_queue(data):
    tmp = QUEUE_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    os.replace(tmp, QUEUE_PATH)


# ---------------------------------------------------------------------------
# Title normalization + dedup
# ---------------------------------------------------------------------------
def normalize_title(title):
    """Normalize title for dedup: lowercase, strip noise, keep alphanum+dots."""
    t = title.lower().strip()
    t = re.sub(r'\.(2160p|1080p|720p|480p|4k|2k|hdr|sdr|dv|dolby.?vision)\.', '.', t)
    t = re.sub(r'\.(bluray|web.?dl|remux|webrip|brrip|hdtv|x264|x265|h264|h265|hevc|avc)\b', '', t)
    t = re.sub(r'\.[a-z0-9]{2,8}$', '', t)  # strip release group
    t = re.sub(r'[^a-z0-9.]', '', t)
    t = re.sub(r'\.+', '.', t).strip('.')
    return t


def title_id(title):
    """Deterministic ID for dedup."""
    return hashlib.sha256(normalize_title(title).encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Priority system
# ---------------------------------------------------------------------------
PRIORITY_ORDER = {"urgent": 0, "high": 1, "normal": 2, "low": 3}


def detect_priority(title, cfg=None):
    """Auto-detect priority from title and config rules."""
    title_lower = title.lower()

    # Auto-detect from title keywords
    if "4k" in title_lower or "imax" in title_lower:
        return "high"

    # Check config priority_rules if available
    if cfg:
        rules = cfg.get("scheduler", {}).get("priority_rules", {})
        # Support new list format: [{pattern: "IMAX", priority: "high"}, ...]
        if isinstance(rules, list):
            for rule in rules:
                if isinstance(rule, dict):
                    pat = rule.get("pattern", "")
                    pri = rule.get("priority", "normal")
                    if isinstance(pat, str) and pat.lower() in title_lower:
                        return pri
                elif isinstance(rule, str) and rule.lower() in title_lower:
                    return "high"
        # Legacy dict format: {"high": ["IMAX", "4K"], "normal": ["BluRay"]}
        elif isinstance(rules, dict):
            for rule_priority, keywords in rules.items():
                if isinstance(keywords, list):
                    for kw in keywords:
                        if kw.lower() in title_lower:
                            return rule_priority
                elif isinstance(keywords, str):
                    if keywords.lower() in title_lower:
                        return rule_priority

    return "normal"


# ---------------------------------------------------------------------------
# Scan sources
# ---------------------------------------------------------------------------
def scan_source_via_ssh(worker_name, worker_cfg, source, logger):
    """SSH into a worker and scan a source remote + folder."""
    host = worker_cfg.get("host")
    user = worker_cfg.get("ssh_user", "root")
    password = worker_cfg.get("ssh_pass", "")
    if not host or not password:
        return []

    items = []
    for folder in source.get("folders", []):
        remote = source.get("remote", "")
        src_type = source.get("type", "webdav")

        # Build rclone path
        if src_type == "local":
            # e.g. pve7-smb:/mnt/internal/FILMES - DUBLADOS - REMUX/
            rclone_path = f"{remote}/{folder}/"
        else:
            # e.g. seedbox:FILMES - DUBLADOS - REMUX/
            rclone_path = f"{remote}:{folder}/"

        cmd = (
            f"rclone lsjson {repr(rclone_path)} "
            f"--max-depth 5 --recursive --files-only "
            f"--no-modtime --no-mimetype 2>/dev/null"
        )
        rc, out, err = ssh_run(host, user, password, cmd, timeout=120)
        if rc != 0:
            logger.debug(f"  scan {worker_name}/{source['name']}/{folder}: rc={rc} {err[:100]}")
            continue

        # Parse rclone JSON output (array or JSON-lines)
        entries = []
        try:
            entries = json.loads(out)
            if isinstance(entries, dict):
                entries = [entries]
        except json.JSONDecodeError:
            # Fallback: JSON-lines format (older rclone versions)
            for line in out.strip().split("\n"):
                line = line.strip()
                if not line:
                    continue
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

        for entry in entries:
            size = entry.get("size", 0) or 0
            name = entry.get("name", "") or entry.get("Path", "")
            if not name:
                continue

            items.append({
                "name": name,
                "size": size,
                "source_name": source["name"],
                "source_remote": source["remote"],
                "source_type": src_type,
                "folder": folder,
                "worker_name": worker_name,
            })

    return items


def scan_all_sources(cfg, logger):
    """Scan all sources across all workers using source_remotes mapping."""
    sources = cfg.get("sources", {})
    workers = cfg.get("workers", {})
    min_size = cfg.get("encode_defaults", {}).get("min_size_gb", 20) * 1e9

    # Build source lookup by name
    source_by_name = {s["name"]: s for s in cfg.get("sources", [])}

    all_raw = []

    for worker_name, wcfg in workers.items():
        source_remotes = wcfg.get("source_remotes", {})
        if not source_remotes:
            logger.debug(f"  Worker {worker_name}: no source_remotes configured, skipping scan")
            continue

        host = wcfg.get("host")
        user = wcfg.get("ssh_user", "root")
        password = wcfg.get("ssh_pass", "")
        if not host or not password:
            continue

        for source_name, local_remote in source_remotes.items():
            source = source_by_name.get(source_name)
            if not source:
                logger.warning(f"  Worker {worker_name}: source '{source_name}' not found in config")
                continue

            src_type = source.get("type", "webdav")

            # Resolve canonical source for item metadata (used by other workers)
            canonical_name = source.get("canonical_source", source_name)
            canonical_source = source_by_name.get(canonical_name, source)
            canonical_remote = canonical_source.get("remote", source.get("remote", local_remote))
            canonical_type = canonical_source.get("type", src_type)

            for folder in source.get("folders", []):
                # Build rclone path for THIS worker's scan (uses its local remotes)
                # Auto-detect: if local_remote starts with '/', treat as local path
                if local_remote.startswith("/") or src_type == "local":
                    rclone_path = f"{local_remote}/{folder}/"
                else:
                    rclone_path = f"{local_remote}:{folder}/"

                cmd = (
                    f"rclone lsjson {repr(rclone_path)} "
                    f"--max-depth 2 --recursive --files-only "
                    f"--no-modtime --no-mimetype 2>/dev/null"
                )
                rc, out, err = ssh_run(host, user, password, cmd, timeout=120)
                if rc != 0:
                    logger.debug(f"  scan {worker_name}/{source_name}/{folder}: rc={rc}")
                    continue

                # Group files by their parent directory (movie folder)
                movie_dirs = defaultdict(lambda: {"size": 0, "files": []})

                # Parse rclone JSON output (array or JSON-lines)
                entries = []
                try:
                    entries = json.loads(out)
                    if isinstance(entries, dict):
                        entries = [entries]
                except json.JSONDecodeError:
                    # Fallback: JSON-lines format (older rclone versions)
                    for line in out.strip().split("\n"):
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entries.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue

                for entry in entries:
                    size = entry.get("Size", 0) or entry.get("size", 0) or 0
                    path = entry.get("Path", "") or entry.get("name", "") or entry.get("path", "")
                    if not path:
                        continue

                    # Get the parent directory (movie name)
                    # path format: "Movie.Name.2024.2160p/Movie.Name.2024.mkv"
                    parts = path.split("/")
                    if len(parts) >= 2:
                        movie_dir = parts[0]
                    else:
                        # File is directly in the folder (no subdirectory)
                        movie_dir = path.rsplit(".", 1)[0] if "." in path else path

                    movie_dirs[movie_dir]["size"] += size
                    movie_dirs[movie_dir]["files"].append(path)

                # Emit one item per movie directory
                for movie_name, info in movie_dirs.items():
                    all_raw.append({
                        "name": movie_name,
                        "size": info["size"],
                        "source_name": canonical_name,
                        "source_remote": canonical_remote,
                        "source_type": canonical_type,
                        "folder": folder,
                        "worker_name": worker_name,
                    })

    # Deduplicate and build queue items
    lock_fd = lock_queue()
    try:
        queue = load_queue()

        # Build a lookup of existing items by ID for size updates
        existing_items = {}
        for item in queue.get("items", []):
            existing_items[item["id"]] = item
        existing_ids = set(existing_items.keys())

        # Update items with size=0 but found with correct size
        size_updates = 0
        for entry in all_raw:
            tid = title_id(entry["name"])
            if tid in existing_ids:
                old = existing_items[tid]
                if old.get("size_bytes", 0) == 0 and entry["size"] > 0:
                    old["size_bytes"] = entry["size"]
                    old["size_gb"] = round(entry["size"] / 1e9, 1)
                    size_updates += 1

        new_items = []
        for entry in all_raw:
            tid = title_id(entry["name"])
            if tid in existing_ids:
                continue

            size_gb = entry["size"] / 1e9
            if entry["size"] < min_size and "remux" not in entry["name"].lower():
                continue

            profile = "2160p_hdr" if "2160p" in entry["name"] or "4k" in entry["name"].lower() else "1080p_sdr"

            # Auto-detect priority from title + config rules
            priority = detect_priority(entry["name"], cfg)

            item = {
                "id": tid,
                "title": entry["name"],
                "source_remote": entry["source_remote"],
                "source_type": entry["source_type"],
                "source_folder": entry["folder"],
                "source_worker": entry["worker_name"],
                "source_name": entry["source_name"],
                "size_bytes": entry["size"],
                "size_gb": round(size_gb, 1),
                "profile": profile,
                "priority": priority,
                "status": "pending",
                "assigned_to": None,
                "assigned_at": None,
                "started_at": None,
                "completed_at": None,
                "output_path": None,
                "error": None,
                "retry_count": 0,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            new_items.append(item)
            existing_ids.add(tid)

        if new_items:
            queue.setdefault("items", []).extend(new_items)
            queue["last_scan"] = datetime.now(timezone.utc).isoformat()
            save_queue(queue)
            logger.info(f"Scan found {len(new_items)} new items (total in queue: {len(queue['items'])})")
            for it in new_items[:5]:
                logger.info(f"  + {it['title'][:80]} ({it['size_gb']}GB, {it['profile']}, priority={it['priority']})")
            if len(new_items) > 5:
                logger.info(f"  ... and {len(new_items) - 5} more")
        elif size_updates:
            save_queue(queue)
            logger.info(f"Scan complete: no new items, updated sizes for {size_updates} items")
        else:
            logger.info("Scan complete: no new items found")

    finally:
        unlock_queue(lock_fd)

    return new_items


# ---------------------------------------------------------------------------
# Dispatch work to workers
# ---------------------------------------------------------------------------
def dispatch_work(cfg, logger):
    """Push pending items to idle workers via SSH with priority ordering and atomic claim."""
    workers_cfg = cfg.get("workers", {})
    lock_fd = lock_queue()
    try:
        queue = load_queue()
        workers_status = load_workers_status()

        for worker_id, wcfg in workers_cfg.items():
            # Check if worker is alive (recent heartbeat)
            ws = workers_status.get(worker_id, {})
            last_seen = ws.get("last_seen", 0)
            if isinstance(last_seen, str):
                try:
                    last_seen = datetime.fromisoformat(last_seen).timestamp()
                except (ValueError, TypeError):
                    last_seen = 0

            timeout = cfg.get("coordinator", {}).get("heartbeat_timeout_seconds", 300)
            if last_seen > 0 and time.time() - last_seen > timeout:
                logger.debug(f"  Worker {worker_id} dead (last_seen {time.time()-last_seen:.0f}s ago), skipping")
                continue

            # Check if worker is already processing something
            if ws.get("status", "").startswith("processing:"):
                logger.debug(f"  Worker {worker_id} already busy, skipping")
                continue

            # Also check if work_order.json exists on the worker (has pending work)
            rc, wo_out, _ = ssh_run(
                wcfg["host"], wcfg.get("ssh_user", "root"),
                wcfg.get("ssh_pass", ""),
                "test -f /opt/v6-orchestrator/work_order.json && echo EXISTS || echo NONE",
                timeout=10,
            )
            if "EXISTS" in wo_out:
                logger.debug(f"  Worker {worker_id} already has work_order.json, skipping")
                continue

            # Re-load queue inside loop for atomic claim — ensure we have freshest state
            # and verify items haven't been claimed by another dispatch cycle
            queue = load_queue()

            # Collect all candidate items with their current status
            claimed_items = []   # already claimed for this worker
            pending_items = []   # still available

            for item in queue.get("items", []):
                if item["status"] == "claimed" and item.get("assigned_to") == worker_id:
                    claimed_items.append(item)
                elif item["status"] == "pending":
                    pending_items.append(item)

            # Select work: prefer claimed item first, then pick highest-priority pending
            work_item = None
            if claimed_items:
                work_item = claimed_items[0]
            elif pending_items:
                # Sort pending items by priority (urgent > high > normal > low)
                pending_items.sort(key=lambda x: PRIORITY_ORDER.get(x.get("priority", "normal"), 2))
                work_item = pending_items[0]

            if not work_item:
                continue

            # ATOMIC CLAIM RE-CHECK: Verify the item is still in the expected state
            # before claiming. This prevents race conditions when multiple dispatch
            # cycles run concurrently (or when the same worker was already picked up).
            if work_item.get("assigned_to") == worker_id and work_item["status"] == "claimed":
                # Already claimed for this worker, proceed to push
                pass
            elif work_item["status"] == "pending":
                # Still pending, claim it
                pass
            else:
                # Another dispatch cycle already claimed it — skip
                logger.debug(f"  Item {work_item['title'][:60]} already claimed elsewhere, skipping for {worker_id}")
                continue

            # Claim the item
            work_item["status"] = "dispatched"
            work_item["assigned_to"] = worker_id
            work_item["assigned_at"] = datetime.now(timezone.utc).isoformat()
            save_queue(queue)

            # Resolve source_remote for the destination worker.
            # If the item was scanned via a local/SMB source on a different worker,
            # translate the local path to the destination worker's own remote.
            if work_item.get("source_name") and work_item.get("source_worker") != worker_id:
                source_by_name = {s["name"]: s for s in cfg.get("sources", [])}
                source_name = work_item["source_name"]
                src = source_by_name.get(source_name, {})
                canonical_name = src.get("canonical_source", source_name)
                dest_remotes = wcfg.get("source_remotes", {})
                if canonical_name in dest_remotes:
                    old_remote = work_item.get("source_remote", "")
                    new_remote = dest_remotes[canonical_name]
                    if old_remote != new_remote:
                        work_item["source_remote"] = new_remote
                        # Infer type from the remote (paths starting with / are local)
                        work_item["source_type"] = "local" if new_remote.startswith("/") else "webdav"
                        logger.debug(
                            f"  Resolved source_remote for {worker_id}: "
                            f"{source_name} ({old_remote}) -> {canonical_name} ({new_remote})"
                        )

            # Push work order to worker via SSH
            host = wcfg.get("host")
            user = wcfg.get("ssh_user", "root")
            password = wcfg.get("ssh_pass", "")

            # Create work_order.json on the worker
            work_order_json = json.dumps(work_item, ensure_ascii=False)
            escaped = work_order_json.replace("'", "'\\''")
            cmd = f"mkdir -p /opt/v6-orchestrator && echo '{escaped}' > /opt/v6-orchestrator/work_order.json"
            rc, out, err = ssh_run(host, user, password, cmd, timeout=15)
            if rc != 0:
                logger.warning(f"  Failed to push work to {worker_id}: {err[:100]}")
                work_item["status"] = "pending"
                work_item["assigned_to"] = None
                work_item["assigned_at"] = None
                save_queue(queue)
                continue

            # Trigger worker to process
            worker_cmd = (
                f"cd /opt/v6-orchestrator && "
                f"python3 worker.py "
                f"--worker-id {worker_id} "
                f"--mode process-work-order "
                f">> /opt/v6-orchestrator/logs/worker_{worker_id}.log 2>&1 &"
            )
            rc, out, err = ssh_run(host, user, password, worker_cmd, timeout=10)
            logger.info(
                f"  Dispatched to {worker_id}: {work_item['title'][:80]} "
                f"(priority={work_item.get('priority', 'normal')})"
            )

    finally:
        unlock_queue(lock_fd)


# ---------------------------------------------------------------------------
# Collect results from workers
# ---------------------------------------------------------------------------
def collect_results(cfg, logger):
    """Read work_order_status.json from each worker and update queue."""
    workers_cfg = cfg.get("workers", {})
    lock_fd = lock_queue()
    try:
        queue = load_queue()

        completed_items = []  # Track items that transition to "done" for history

        for worker_id, wcfg in workers_cfg.items():
            host = wcfg.get("host")
            user = wcfg.get("ssh_user", "root")
            password = wcfg.get("ssh_pass", "")

            rc, out, err = ssh_run(
                host, user, password,
                "cat /opt/v6-orchestrator/work_order_status.json 2>/dev/null",
                timeout=10,
            )
            if rc != 0 or not out:
                continue

            try:
                status = json.loads(out)
            except json.JSONDecodeError:
                continue

            item_id = status.get("item_id")
            new_status = status.get("status")  # done, failed

            # Find and update the item in queue
            for item in queue.get("items", []):
                if item["id"] == item_id:
                    if new_status == "done":
                        # Always accept done — first successful completion wins
                        item["status"] = "done"
                        item["completed_at"] = datetime.now(timezone.utc).isoformat()
                        item["output_path"] = status.get("output_path", "")
                        item["error"] = None
                        logger.info(f"  ✅ {worker_id}: {item['title'][:80]}")
                        completed_items.append({
                            "item_id": item["id"],
                            "title": item.get("title", ""),
                            "worker": worker_id,
                            "profile": item.get("profile", ""),
                            "size_gb": item.get("size_gb", 0),
                            "started_at": item.get("started_at"),
                            "assigned_at": item.get("assigned_at"),
                            "completed_at": item["completed_at"],
                            "error_type": None,
                        })
                    elif new_status == "failed":
                        # NEVER overwrite done or failed_permanent with failed
                        if item["status"] == "done":
                            logger.info(f"  ⏭️ {worker_id}: {item['title'][:80]} — failed but already done by another worker, ignoring")
                        elif item["status"] == "failed_permanent":
                            logger.debug(f"  ⏭️ {worker_id}: {item['title'][:80]} — failed but already marked permanent, ignoring")
                        else:
                            item["status"] = "failed"
                            item["error"] = status.get("error", "")
                            item["retry_count"] = item.get("retry_count", 0) + 1
                            logger.warning(f"  ❌ {worker_id}: {item['title'][:80]} — {status.get('error', '')[:100]}")
                    break

            # Clean up the status file on the worker
            ssh_run(host, user, password, "rm -f /opt/v6-orchestrator/work_order_status.json /opt/v6-orchestrator/work_order.json", timeout=5)

        save_queue(queue)

        # Append completed items to history
        for ci in completed_items:
            append_to_history(ci, logger)

    finally:
        unlock_queue(lock_fd)


# ---------------------------------------------------------------------------
# Workers status
# ---------------------------------------------------------------------------
def load_workers_status():
    if os.path.exists(WORKERS_PATH):
        try:
            with open(WORKERS_PATH) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def save_workers_status(data):
    tmp = WORKERS_PATH + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, WORKERS_PATH)


def update_worker_heartbeat(cfg, logger):
    """SSH to each worker to test reachability and run enhanced health checks."""
    workers_cfg = cfg.get("workers", {})
    workers_status = load_workers_status()

    for worker_id, wcfg in workers_cfg.items():
        host = wcfg.get("host")
        user = wcfg.get("ssh_user", "root")
        password = wcfg.get("ssh_pass", "")

        reachable = ssh_test_reachable(host, user, password, timeout=15)

        if reachable:
            # --- GPU check ---
            gpu_status = "unknown"
            rc, gpu_out, _ = ssh_run(
                host, user, password,
                "nvidia-smi --query-gpu=name,memory.total --format=csv,noheader 2>/dev/null || echo 'no-nvidia'",
                timeout=10,
            )
            if rc == 0 and gpu_out and gpu_out.strip() != "no-nvidia":
                gpus = []
                for line in gpu_out.strip().split("\n"):
                    line = line.strip()
                    if line:
                        gpus.append(line)
                gpu_status = "; ".join(gpus) if gpus else "nvidia-detected"
            else:
                # Fall back to /dev/dri check
                rc, dri_out, _ = ssh_run(
                    host, user, password,
                    "ls /dev/dri/render* 2>/dev/null | head -1 || echo 'no-dri'",
                    timeout=10,
                )
                dri = dri_out.strip()
                if dri and dri != "no-dri":
                    gpu_status = f"vpu-at-{dri}"
                else:
                    gpu_status = "no-gpu"

            # --- Disk space checks ---
            temp_dir = wcfg.get("temp_dir", "/tmp")
            conv_dir = wcfg.get("conv_dir", "/opt/v6-orchestrator/output")

            rc, disk_out, _ = ssh_run(
                host, user, password,
                f"df -BG {repr(temp_dir)} {repr(conv_dir)} 2>/dev/null | awk 'NR>1{{print $4, $6}}'",
                timeout=10,
            )
            disk_temp_free_gb = 0
            disk_conv_free_gb = 0
            if rc == 0 and disk_out:
                for line in disk_out.strip().split("\n"):
                    parts = line.strip().split()
                    if len(parts) >= 2:
                        try:
                            free_gb = int(parts[0].rstrip("G"))
                            mount = parts[1]
                            if mount == temp_dir:
                                disk_temp_free_gb = free_gb
                            elif mount == conv_dir:
                                disk_conv_free_gb = free_gb
                        except (ValueError, IndexError):
                            pass

            # --- Rclone check ---
            rclone_ok = False
            rc, rclone_out, _ = ssh_run(
                host, user, password,
                "rclone listremotes 2>/dev/null",
                timeout=10,
            )
            if rc == 0 and rclone_out.strip():
                rclone_ok = True

            workers_status[worker_id] = {
                "worker_id": worker_id,
                "host": host,
                "last_seen": time.time(),
                "last_seen_iso": datetime.now(timezone.utc).isoformat(),
                "status": "idle",
                "gpu_status": gpu_status,
                "disk_temp_free_gb": disk_temp_free_gb,
                "disk_conv_free_gb": disk_conv_free_gb,
                "rclone_ok": rclone_ok,
                "encoder": wcfg.get("encoder", "unknown"),
                "reachable": True,
            }
        else:
            if worker_id in workers_status:
                workers_status[worker_id]["reachable"] = False
                workers_status[worker_id]["status"] = "unreachable"

    save_workers_status(workers_status)
    alive = sum(1 for w in workers_status.values() if w.get("reachable"))
    logger.info(f"Heartbeat: {alive}/{len(workers_cfg)} workers reachable")


# ---------------------------------------------------------------------------
# Reclaim stale items
# ---------------------------------------------------------------------------
def reclaim_stale_items(cfg, logger):
    """Reclaim items from dead workers back to pending."""
    timeout = cfg.get("coordinator", {}).get("heartbeat_timeout_seconds", 300)
    workers_status = load_workers_status()
    now = time.time()

    dead_workers = set()
    for wid, ws in workers_status.items():
        last = ws.get("last_seen", 0)
        if isinstance(last, str):
            try:
                last = datetime.fromisoformat(last).timestamp()
            except (ValueError, TypeError):
                last = 0
        if last > 0 and now - last > timeout:
            dead_workers.add(wid)

    if not dead_workers:
        return

    lock_fd = lock_queue()
    try:
        queue = load_queue()
        reclaimed = 0
        for item in queue.get("items", []):
            if item.get("assigned_to") in dead_workers and item["status"] in ("dispatched", "claimed", "processing"):
                item["status"] = "pending"
                item["assigned_to"] = None
                item["assigned_at"] = None
                reclaimed += 1
                logger.warning(f"  Reclaimed {item['title'][:80]} from dead worker {item.get('assigned_to')}")

        if reclaimed:
            save_queue(queue)
            logger.info(f"Reclaimed {reclaimed} items from {len(dead_workers)} dead workers")
    finally:
        unlock_queue(lock_fd)


# ---------------------------------------------------------------------------
# Smart retry with error classification
# ---------------------------------------------------------------------------
# Error classification keywords
TRANSIENT_KEYWORDS = ["timeout", "connection", "refused", "timed out"]
RESOURCE_KEYWORDS = ["disk full", "no space", "cannot allocate", "disk"]
PERMANENT_KEYWORDS = ["unsupported", "corrupt", "invalid", "no video", "codec not found"]


def classify_error(error_msg):
    """Classify an error message as transient, resource, or permanent."""
    if not error_msg:
        return "unknown"

    error_lower = error_msg.lower()

    # Check permanent first (more specific)
    for kw in PERMANENT_KEYWORDS:
        if kw in error_lower:
            return "permanent"

    # Check resource
    for kw in RESOURCE_KEYWORDS:
        if kw in error_lower:
            return "resource"

    # Check transient
    for kw in TRANSIENT_KEYWORDS:
        if kw in error_lower:
            return "transient"

    return "unknown"


def retry_failed_items(cfg, logger):
    """Smart retry with error classification.

    - Transient errors: retry immediately
    - Resource errors: retry after 5 min delay (check disk first)
    - Permanent errors: skip retry, mark failed_permanent
    """
    max_retries = cfg.get("coordinator", {}).get("max_retries", 3)
    resource_retry_delay = cfg.get("coordinator", {}).get("resource_retry_delay_seconds", 300)  # 5 min default

    lock_fd = lock_queue()
    try:
        queue = load_queue()

        # Build a set of IDs already completed by any worker
        done_ids = set()
        for item in queue.get("items", []):
            if item["status"] == "done":
                done_ids.add(item["id"])

        retried = 0
        perm_skipped = 0
        resource_delayed = 0
        now = time.time()

        for item in queue.get("items", []):
            if item["status"] != "failed":
                continue

            # Skip if another worker already completed this item
            if item["id"] in done_ids:
                logger.info(f"  ⏭️ Skipping retry for {item['title'][:80]} — already done by another worker")
                continue

            error_msg = item.get("error", "")
            error_type = classify_error(error_msg)
            retry_count = item.get("retry_count", 0)

            if error_type == "permanent":
                # Permanent errors: never retry, mark as failed_permanent
                item["status"] = "failed_permanent"
                item["error"] = f"[permanent] {error_msg}"
                item["error_type"] = "permanent"
                perm_skipped += 1
                logger.error(f"  🚫 Permanent failure (no retry): {item['title'][:80]} — {error_msg[:100]}")

            elif error_type == "resource":
                # Resource errors: check if we have room before retrying
                # Check if enough time has passed since last retry
                last_retry = item.get("last_retry_at")
                if last_retry:
                    try:
                        last_ts = datetime.fromisoformat(last_retry).timestamp()
                        if now - last_ts < resource_retry_delay:
                            logger.debug(f"  ⏳ Resource retry delayed for {item['title'][:80]} (cooldown active)")
                            continue
                    except (ValueError, TypeError):
                        pass

                if retry_count < max_retries:
                    item["status"] = "pending"
                    item["assigned_to"] = None
                    item["assigned_at"] = None
                    item["retry_count"] = retry_count + 1
                    item["last_retry_at"] = datetime.now(timezone.utc).isoformat()
                    item["error"] = f"[resource retry {retry_count + 1}/{max_retries}] {error_msg}"
                    item["error_type"] = "resource"
                    retried += 1
                    resource_delayed += 1
                    logger.warning(
                        f"  🔄 Resource retry: {item['title'][:80]} "
                        f"(delayed {resource_retry_delay}s, attempt {retry_count + 1}/{max_retries})"
                    )
                else:
                    item["status"] = "failed_permanent"
                    item["error"] = f"[permanent: resource exhausted] {error_msg}"
                    item["error_type"] = "permanent"
                    perm_skipped += 1
                    logger.error(f"  🚫 Resource retry exhausted: {item['title'][:80]}")

            elif error_type == "transient":
                # Transient errors: retry immediately
                if retry_count < max_retries:
                    item["status"] = "pending"
                    item["assigned_to"] = None
                    item["assigned_at"] = None
                    item["retry_count"] = retry_count + 1
                    item["last_retry_at"] = datetime.now(timezone.utc).isoformat()
                    item["error"] = f"[transient retry {retry_count + 1}/{max_retries}] {error_msg}"
                    item["error_type"] = "transient"
                    retried += 1
                    logger.info(f"  ⚡ Transient retry: {item['title'][:80]} (attempt {retry_count + 1}/{max_retries})")
                else:
                    item["status"] = "failed_permanent"
                    item["error"] = f"[permanent: transient retries exhausted] {error_msg}"
                    item["error_type"] = "permanent"
                    perm_skipped += 1
                    logger.error(f"  🚫 Transient retries exhausted: {item['title'][:80]}")

            else:
                # Unknown error: fall back to original behavior
                if retry_count < max_retries:
                    item["status"] = "pending"
                    item["assigned_to"] = None
                    item["assigned_at"] = None
                    item["retry_count"] = retry_count + 1
                    item["last_retry_at"] = datetime.now(timezone.utc).isoformat()
                    item["error"] = f"[retry {retry_count + 1}/{max_retries}] {error_msg}"
                    item["error_type"] = "unknown"
                    retried += 1
                    logger.info(f"  🔄 Unknown error retry: {item['title'][:80]} (attempt {retry_count + 1}/{max_retries})")
                else:
                    item["status"] = "failed_permanent"
                    item["error"] = f"[permanent: retries exhausted] {error_msg}"
                    item["error_type"] = "permanent"
                    perm_skipped += 1
                    logger.error(f"  🚫 Unknown error retries exhausted: {item['title'][:80]}")

        if retried or perm_skipped:
            save_queue(queue)
            logger.info(f"Retry: {retried} retried, {perm_skipped} marked permanent, {resource_delayed} resource-delayed")

    finally:
        unlock_queue(lock_fd)


# ---------------------------------------------------------------------------
# Archive old items
# ---------------------------------------------------------------------------
def archive_old_items(cfg, logger):
    days = cfg.get("coordinator", {}).get("archive_after_days", 7)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    lock_fd = lock_queue()
    try:
        queue = load_queue()
        archived = 0
        remaining = []
        for item in queue.get("items", []):
            completed = item.get("completed_at")
            if item["status"] in ("done", "failed_permanent") and completed:
                try:
                    ct = datetime.fromisoformat(completed)
                    if ct < cutoff:
                        archived += 1
                        continue
                except (ValueError, TypeError):
                    pass
            remaining.append(item)

        if archived:
            queue["items"] = remaining
            save_queue(queue)
            logger.info(f"Archived {archived} old items from queue")
    finally:
        unlock_queue(lock_fd)


# ---------------------------------------------------------------------------
# History & Analytics
# ---------------------------------------------------------------------------
def _load_history():
    """Load history.json, return list of entries."""
    if os.path.exists(HISTORY_PATH):
        try:
            with open(HISTORY_PATH) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            return []
    return []


def _save_history(entries):
    """Save history entries to history.json."""
    tmp = HISTORY_PATH + ".tmp"
    os.makedirs(os.path.dirname(HISTORY_PATH), exist_ok=True)
    with open(tmp, "w") as f:
        json.dump(entries, f, indent=2, ensure_ascii=False)
    os.replace(tmp, HISTORY_PATH)


def append_to_history(item_data, logger=None):
    """Append a completed item to history.json.

    item_data should have: item_id, title, worker, profile, size_gb,
    started_at, assigned_at, completed_at, error_type
    """
    # Calculate duration
    duration_seconds = None
    completed_at = item_data.get("completed_at")
    started_at = item_data.get("started_at")
    assigned_at = item_data.get("assigned_at")

    if completed_at:
        try:
            ct = datetime.fromisoformat(completed_at)
            st = None
            if started_at:
                try:
                    st = datetime.fromisoformat(started_at)
                except (ValueError, TypeError):
                    pass
            if st is None and assigned_at:
                try:
                    st = datetime.fromisoformat(assigned_at)
                except (ValueError, TypeError):
                    pass
            if st:
                duration_seconds = round((ct - st).total_seconds(), 1)
        except (ValueError, TypeError):
            pass

    entry = {
        "item_id": item_data.get("item_id"),
        "title": item_data.get("title", ""),
        "worker": item_data.get("worker", ""),
        "profile": item_data.get("profile", ""),
        "size_gb": item_data.get("size_gb", 0),
        "duration_seconds": duration_seconds,
        "started_at": started_at,
        "completed_at": completed_at,
        "error_type": item_data.get("error_type"),
    }

    # Thread-safe history append
    history = _load_history()
    history.append(entry)
    _save_history(history)

    if logger:
        dur_str = f"{duration_seconds}s" if duration_seconds else "N/A"
        logger.info(f"  📊 History: {item_data.get('title', '')[:60]} → done on {entry['worker']} ({dur_str})")


def get_history_stats():
    """Return analytics stats from history.json."""
    history = _load_history()
    if not history:
        return {
            "total_converted": 0,
            "total_gb": 0,
            "success_rate": 0,
            "avg_encode_time_per_worker": {},
            "most_active_worker": None,
        }

    total_converted = 0
    total_gb = 0
    total_failed = 0
    worker_times = defaultdict(list)  # worker -> list of duration_seconds
    worker_counts = defaultdict(int)

    for entry in history:
        if entry.get("error_type") is None:
            total_converted += 1
            total_gb += entry.get("size_gb", 0) or 0
        else:
            total_failed += 1

        worker = entry.get("worker", "unknown")
        worker_counts[worker] += 1

        dur = entry.get("duration_seconds")
        if dur is not None and dur > 0:
            worker_times[worker].append(dur)

    total_items = total_converted + total_failed
    success_rate = round(total_converted / total_items, 3) if total_items > 0 else 0

    avg_encode_time = {}
    for worker, times in worker_times.items():
        avg_encode_time[worker] = round(sum(times) / len(times), 1)

    most_active = None
    if worker_counts:
        most_active = max(worker_counts.items(), key=lambda x: x[1])[0]

    return {
        "total_converted": total_converted,
        "total_gb": round(total_gb, 1),
        "success_rate": success_rate,
        "avg_encode_time_per_worker": avg_encode_time,
        "most_active_worker": most_active,
    }


def archive_history(days=30):
    """Remove history entries older than N days. Returns number of entries removed."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    history = _load_history()

    original_count = len(history)
    filtered = []

    for entry in history:
        completed = entry.get("completed_at")
        if completed:
            try:
                ct = datetime.fromisoformat(completed)
                if ct >= cutoff:
                    filtered.append(entry)
                    continue
            except (ValueError, TypeError):
                pass
        # Keep entries without completed_at (safety net)
        filtered.append(entry)

    removed = original_count - len(filtered)
    if removed > 0:
        _save_history(filtered)

    return removed


# ---------------------------------------------------------------------------
# HTTP API Server
# ---------------------------------------------------------------------------
class _CoordinatorHTTPHandler(BaseHTTPRequestHandler):
    """Simple HTTP request handler for coordinator API."""

    # Class-level reference set by start_http_server
    _cfg = None
    _logger = None
    _start_time: float = 0.0

    def log_message(self, format, *args):
        """Override to suppress default stderr logging — use our logger instead."""
        if self._logger:
            self._logger.debug(f"HTTP API: {format % args}")

    def _send_json(self, data, status=200):
        body = json.dumps(data, indent=2, ensure_ascii=False, default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?")[0]  # strip query params

        if path == "/health":
            uptime = time.time() - (self._start_time or time.time())
            self._send_json({
                "status": "ok",
                "service": "v6-orchestrator-coordinator",
                "uptime_seconds": round(uptime, 1),
                "uptime_human": str(timedelta(seconds=int(uptime))),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })

        elif path == "/status":
            try:
                lock_fd = lock_queue()
                try:
                    queue = load_queue()
                    summary = queue_summary(queue)
                finally:
                    unlock_queue(lock_fd)

                workers = load_workers_status()
                worker_summary = {}
                for wid, winfo in workers.items():
                    worker_summary[wid] = {
                        "reachable": winfo.get("reachable", False),
                        "status": winfo.get("status", "unknown"),
                        "gpu_status": winfo.get("gpu_status", "unknown"),
                        "last_seen": winfo.get("last_seen_iso", "never"),
                    }

                self._send_json({
                    "queue_summary": summary,
                    "workers": worker_summary,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                })
            except Exception as e:
                self._send_json({"error": str(e)}, 500)

        elif path == "/queue":
            try:
                lock_fd = lock_queue()
                try:
                    queue = load_queue()
                    items = queue.get("items", [])
                    # Return last 50 items
                    last_50 = items[-50:] if len(items) > 50 else items
                finally:
                    unlock_queue(lock_fd)

                self._send_json({
                    "total_items": len(items),
                    "returned": len(last_50),
                    "items": last_50,
                })
            except Exception as e:
                self._send_json({"error": str(e)}, 500)

        elif path == "/history":
            try:
                stats = get_history_stats()
                self._send_json(stats)
            except Exception as e:
                self._send_json({"error": str(e)}, 500)

        else:
            self._send_json({"error": "not found", "endpoints": ["/health", "/status", "/queue", "/history"]}, 404)


def start_http_server(cfg, logger):
    """Start HTTP API server in a daemon thread. Returns the server instance or None."""
    port = cfg.get("coordinator", {}).get("http_api_port", 8765)
    if not port or port == 0:
        logger.info("HTTP API disabled (port=0)")
        return None

    _CoordinatorHTTPHandler._cfg = cfg
    _CoordinatorHTTPHandler._logger = logger
    _CoordinatorHTTPHandler._start_time = time.time()

    try:
        server = HTTPServer(("0.0.0.0", port), _CoordinatorHTTPHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        logger.info(f"HTTP API server started on port {port}")
        return server
    except OSError as e:
        logger.warning(f"Failed to start HTTP API server on port {port}: {e}")
        return None


# ---------------------------------------------------------------------------
# Queue summary
# ---------------------------------------------------------------------------
def queue_summary(queue):
    counts = {}
    for item in queue.get("items", []):
        s = item.get("status", "unknown")
        counts[s] = counts.get(s, 0) + 1
    return counts


# ---------------------------------------------------------------------------
# Report generation (for log-watcher / Telegram delivery)
# ---------------------------------------------------------------------------


def collect_worker_progress(cfg, logger):
    """SSH into each worker and read work_progress.json for real-time status."""
    workers_cfg = cfg.get("workers", {})
    progress_by_worker = {}

    for wid, wcfg in workers_cfg.items():
        host = wcfg.get("host")
        user = wcfg.get("ssh_user", "root")
        password = wcfg.get("ssh_pass", "")
        workdir = wcfg.get("workdir", "/opt/v6-orchestrator")

        if not host or not password:
            continue

        cmd = f"cat {workdir}/work_progress.json 2>/dev/null || echo NONE"
        rc, out, _ = ssh_run(host, user, password, cmd, timeout=10)

        if rc == 0 and out.strip() and out.strip() != "NONE":
            try:
                progress = json.loads(out.strip())
                progress_by_worker[wid] = progress
            except json.JSONDecodeError:
                progress_by_worker[wid] = {"phase": "unknown", "detail": "parse error"}
        else:
            progress_by_worker[wid] = {"phase": "idle", "title": "", "detail": ""}

    return progress_by_worker


def generate_report(cfg, logger):
    """Generate a JSON report of orchestrator state for log-watcher to consume."""
    workers = cfg.get("workers", {})

    # Collect real-time progress from workers
    worker_progress = collect_worker_progress(cfg, logger)

    lock_fd = lock_queue()
    try:
        q = load_queue()
        items = q.get("items", [])

        # Count by status
        status_counts = {}
        for item in items:
            s = item.get("status", "unknown")
            status_counts[s] = status_counts.get(s, 0) + 1

        # Current processing items
        processing = []
        for item in items:
            if item.get("status") in ("dispatched", "claimed", "processing"):
                processing.append({
                    "title": item.get("title", "unknown")[:80],
                    "worker": item.get("assigned_to", "unknown"),
                    "status": item.get("status"),
                    "retry_count": item.get("retry_count", 0),
                    "priority": item.get("priority", "normal"),
                })

        # Recent errors (last 20 log lines with ERROR)
        recent_errors = []
        try:
            with open(LOG_PATH) as f:
                lines = f.readlines()
                for line in lines[-50:]:
                    if "[ERROR]" in line or "[WARNING]" in line:
                        recent_errors.append(line.strip().split("] ", 1)[-1] if "] " in line else line.strip())
        except Exception:
            pass

        # Worker heartbeats
        worker_status = {}
        try:
            with open(WORKERS_PATH) as f:
                whb = json.load(f)
                for wid, winfo in whb.items():
                    progress = worker_progress.get(wid, {})
                    worker_status[wid] = {
                        "last_seen": winfo.get("last_seen_iso", winfo.get("last_heartbeat", "never")),
                        "reachable": winfo.get("reachable", False),
                        "status": winfo.get("status", "unknown"),
                        "gpu_status": winfo.get("gpu_status", "unknown"),
                        "disk_temp_free_gb": winfo.get("disk_temp_free_gb", 0),
                        "disk_conv_free_gb": winfo.get("disk_conv_free_gb", 0),
                        "rclone_ok": winfo.get("rclone_ok", False),
                        "current_phase": progress.get("phase", "idle"),
                        "current_title": progress.get("title", ""),
                        "current_detail": progress.get("detail", ""),
                    }
        except Exception:
            pass

        report = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "queue_summary": status_counts,
            "processing": processing,
            "worker_status": worker_status,
            "worker_progress": worker_progress,
            "recent_errors": recent_errors[-10:],
        }

        os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
        with open(REPORT_PATH, "w") as f:
            json.dump(report, f, indent=2)

        logger.debug(f"Report written to {REPORT_PATH}")
    finally:
        unlock_queue(lock_fd)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
def main():
    cfg = load_config()
    scan_interval = cfg.get("coordinator", {}).get("scan_interval_seconds", 1800)
    report_interval = cfg.get("coordinator", {}).get("report_interval_seconds", 300)
    history_archive_interval = cfg.get("coordinator", {}).get("history_archive_interval_seconds", 86400)  # daily
    last_scan = 0
    last_report = 0
    last_history_archive = 0

    logger.info("v6-orchestrator coordinator starting")
    logger.info(f"Config: {CONFIG_PATH}")
    logger.info(f"Queue: {QUEUE_PATH}")
    logger.info(f"History: {HISTORY_PATH}")
    logger.info(f"Scan interval: {scan_interval}s")
    logger.info(f"Report interval: {report_interval}s")

    # Start HTTP API server in daemon thread
    http_server = start_http_server(cfg, logger)

    while True:
        try:
            now = time.time()

            # 1. Heartbeat — test worker reachability + health checks
            update_worker_heartbeat(cfg, logger)

            # 2. Collect results from workers (appends to history on "done")
            collect_results(cfg, logger)

            # 3. Reclaim items from dead workers
            reclaim_stale_items(cfg, logger)

            # 4. Retry failed items (smart error classification)
            retry_failed_items(cfg, logger)

            # 5. Archive old items
            archive_old_items(cfg, logger)

            # 6. Archive old history entries
            if now - last_history_archive >= history_archive_interval:
                removed = archive_history()
                if removed:
                    logger.info(f"Archived {removed} old history entries")
                last_history_archive = now

            # 7. Scan sources (periodic)
            if now - last_scan >= scan_interval:
                logger.info("Starting source scan...")
                scan_all_sources(cfg, logger)
                last_scan = now

            # 8. Dispatch work to idle workers (priority-sorted)
            dispatch_work(cfg, logger)

            # 9. Summary
            lock_fd = lock_queue()
            try:
                q = load_queue()
                summary = queue_summary(q)
                logger.info(f"Queue: {summary}")
            finally:
                unlock_queue(lock_fd)

            # 10. Generate report (periodic)
            if now - last_report >= report_interval:
                generate_report(cfg, logger)
                last_report = now

            time.sleep(60)

        except KeyboardInterrupt:
            logger.info("Coordinator shutting down")
            if http_server:
                http_server.shutdown()
            break
        except Exception as exc:
            logger.exception(f"Error in main loop: {exc}")
            time.sleep(30)


if __name__ == "__main__":
    main()
