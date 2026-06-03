#!/usr/bin/env python3
"""
v6-orchestrator coordinator.py (SSH-push architecture)

Runs on the coordinator server (Hermes/PVE-23).
Manages queue.json locally. SSH-pushes work orders to workers.
Workers never SSH back to coordinator — coordinator initiates all connections.

Main loop:
  1. scan_sources() — SSH into workers, rclone lsjson their remotes
  2. reconcile() — merge new items into queue.json
  3. dispatch() — for each idle worker, push next pending item
  4. collect() — read work_order_status.json from each worker
  5. reap_dead_workers() — workers with no heartbeat for N minutes
  6. retry_failed_items() — bump retry_count, reset to pending
  7. archive_old_items() — remove old done items
  8. sleep 60s
"""

import fcntl
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import yaml

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
CONFIG_PATH = "/opt/v6-orchestrator/config.yaml"
QUEUE_PATH = "/opt/v6-orchestrator/queue.json"
QUEUE_LOCK_PATH = QUEUE_PATH + ".lock"
WORKERS_PATH = "/opt/v6-orchestrator/workers.json"
LOG_PATH = "/opt/v6-orchestrator/logs/coordinator.log"


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

        # Parse JSON lines
        for line in out.strip().split("\n"):
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue

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

                for line in out.strip().split("\n"):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                    except json.JSONDecodeError:
                        continue

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
                logger.info(f"  + {it['title'][:80]} ({it['size_gb']}GB, {it['profile']})")
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
    """Push pending items to idle workers via SSH."""
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

            # Find next pending item assigned to this worker (claimed but not started)
            # or any pending item
            work_item = None
            for item in queue.get("items", []):
                if item["status"] == "claimed" and item.get("assigned_to") == worker_id:
                    work_item = item
                    break
                if work_item is None and item["status"] == "pending":
                    work_item = item

            if not work_item:
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
            logger.info(f"  Dispatched to {worker_id}: {work_item['title'][:80]}")

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
    """SSH to each worker to test reachability and update workers.json."""
    workers_cfg = cfg.get("workers", {})
    workers_status = load_workers_status()

    for worker_id, wcfg in workers_cfg.items():
        host = wcfg.get("host")
        user = wcfg.get("ssh_user", "root")
        password = wcfg.get("ssh_pass", "")

        reachable = ssh_test_reachable(host, user, password, timeout=15)

        if reachable:
            # Get GPU info from worker
            rc, gpu_out, _ = ssh_run(host, user, password, "nvidia-smi --query-gpu=name --format=csv,noheader 2>/dev/null || echo 'no-nvidia'", timeout=10)
            gpu_name = gpu_out.strip().split("\n")[0].strip() if gpu_out else "unknown"

            rc, dri_out, _ = ssh_run(host, user, password, "ls /dev/dri/render* 2>/dev/null | head -1 || echo 'no-dri'", timeout=10)
            dri = dri_out.strip()

            workers_status[worker_id] = {
                "worker_id": worker_id,
                "host": host,
                "last_seen": time.time(),
                "last_seen_iso": datetime.now(timezone.utc).isoformat(),
                "status": "idle",
                "gpu_name": gpu_name if gpu_name != "no-nvidia" else (dri if dri != "no-dri" else "unknown"),
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
                logger.warning(f"  Reclaimed {item['title'][:80]} from dead worker {item['assigned_to']}")

        if reclaimed:
            save_queue(queue)
            logger.info(f"Reclaimed {reclaimed} items from {len(dead_workers)} dead workers")
    finally:
        unlock_queue(lock_fd)


# ---------------------------------------------------------------------------
# Retry failed items
# ---------------------------------------------------------------------------
def retry_failed_items(cfg, logger):
    max_retries = cfg.get("coordinator", {}).get("max_retries", 3)

    lock_fd = lock_queue()
    try:
        queue = load_queue()

        # Build a set of IDs already completed by any worker
        done_ids = set()
        for item in queue.get("items", []):
            if item["status"] == "done":
                done_ids.add(item["id"])

        retried = 0
        permuted = 0
        for item in queue.get("items", []):
            if item["status"] == "failed" and item.get("retry_count", 0) < max_retries:
                # Skip if another worker already completed this item
                if item["id"] in done_ids:
                    logger.info(f"  ⏭️ Skipping retry for {item['title'][:80]} — already done by another worker")
                    continue
                item["status"] = "pending"
                item["assigned_to"] = None
                item["assigned_at"] = None
                item["error"] = f"[retry {item['retry_count'] + 1}/{max_retries}] {item.get('error', '')}"
                retried += 1
                logger.info(f"  Retrying: {item['title'][:80]}")
            elif item["status"] == "failed" and item.get("retry_count", 0) >= max_retries:
                # Skip if another worker already completed this item
                if item["id"] in done_ids:
                    logger.info(f"  ⏭️ Skipping permanent for {item['title'][:80]} — already done by another worker")
                    continue
                item["status"] = "failed_permanent"
                permuted += 1
                logger.error(f"  Permanent failure: {item['title'][:80]} (retried {item['retry_count']} times)")

        if retried:
            save_queue(queue)
            logger.info(f"Retried {retried} failed items")
        elif permuted:
            # Save even if no retries — persist permanent status so we don't re-log
            save_queue(queue)
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
            logger.info(f"Archived {archived} old items")
    finally:
        unlock_queue(lock_fd)


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
REPORT_PATH = "/opt/v6-orchestrator/logs/coordinator-report.json"


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
    last_scan = 0
    last_report = 0

    logger.info("v6-orchestrator coordinator starting")
    logger.info(f"Config: {CONFIG_PATH}")
    logger.info(f"Queue: {QUEUE_PATH}")
    logger.info(f"Scan interval: {scan_interval}s")
    logger.info(f"Report interval: {report_interval}s")

    while True:
        try:
            now = time.time()

            # 1. Heartbeat — test worker reachability
            update_worker_heartbeat(cfg, logger)

            # 2. Collect results from workers
            collect_results(cfg, logger)

            # 3. Reclaim items from dead workers
            reclaim_stale_items(cfg, logger)

            # 4. Retry failed items
            retry_failed_items(cfg, logger)

            # 5. Archive old items
            archive_old_items(cfg, logger)

            # 6. Scan sources (periodic)
            if now - last_scan >= scan_interval:
                logger.info("Starting source scan...")
                scan_all_sources(cfg, logger)
                last_scan = now

            # 7. Dispatch work to idle workers
            dispatch_work(cfg, logger)

            # 8. Summary
            lock_fd = lock_queue()
            try:
                q = load_queue()
                summary = queue_summary(q)
                logger.info(f"Queue: {summary}")
            finally:
                unlock_queue(lock_fd)

            # 9. Generate report (periodic)
            if now - last_report >= report_interval:
                generate_report(cfg, logger)
                last_report = now

            time.sleep(60)

        except KeyboardInterrupt:
            logger.info("Coordinator shutting down")
            break
        except Exception as exc:
            logger.exception(f"Error in main loop: {exc}")
            time.sleep(30)


if __name__ == "__main__":
    main()
