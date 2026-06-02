#!/usr/bin/env python3
"""
v6-orchestrator worker.py (SSH-push architecture)

Runs locally on each encoder server (SolidVPS, Gorilla, PVE14, PVE24).
Two modes:

1. --mode process-work-order (default):
   Read /opt/v6-orchestrator/work_order.json, process the item,
   write result to /opt/v6-orchestrator/work_order_status.json.
   The coordinator pushes the work_order via SSH and triggers this script.

2. --mode standalone:
   Traditional loop: claim → download → encode → upload → report.
   For use when running a worker independently (no coordinator).

Usage:
    python3 worker.py --worker-id solidvps --mode process-work-order
    python3 worker.py --worker-id solidvps --mode standalone
"""

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import yaml

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
CONFIG_PATH = "/opt/v6-orchestrator/config.yaml"
WORK_ORDER_PATH = "/opt/v6-orchestrator/work_order.json"
WORK_STATUS_PATH = "/opt/v6-orchestrator/work_order_status.json"
WORK_PROGRESS_PATH = "/opt/v6-orchestrator/work_progress.json"
LOG_DIR = "/opt/v6-orchestrator/logs"


def load_config(path=CONFIG_PATH):
    with open(path) as f:
        return yaml.safe_load(f)


def setup_logging(worker_id):
    os.makedirs(LOG_DIR, exist_ok=True)
    log_path = os.path.join(LOG_DIR, f"worker_{worker_id}.log")
    logger = logging.getLogger(f"worker_{worker_id}")
    logger.setLevel(logging.DEBUG)
    if logger.handlers:
        logger.handlers.clear()
    fh = logging.FileHandler(log_path)
    fh.setLevel(logging.DEBUG)
    sh = logging.StreamHandler(sys.stderr)
    sh.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh.setFormatter(fmt)
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


# ---------------------------------------------------------------------------
# Run helpers
# ---------------------------------------------------------------------------
def run(cmd, timeout=600, cwd=None, env=None):
    """Run a shell command, return (returncode, stdout, stderr)."""
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True,
            timeout=timeout, cwd=cwd, env=env,
        )
        return result.returncode, result.stdout.strip(), result.stderr.strip()
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"
    except Exception as exc:
        return -1, "", str(exc)


# ---------------------------------------------------------------------------
# GPU detection
# ---------------------------------------------------------------------------
def detect_gpu_info():
    info = {"encoder": "unknown", "gpu_name": "unknown"}

    rc, out, _ = run("nvidia-smi --query-gpu=name --format=csv,noheader", timeout=10)
    if rc == 0 and out.strip():
        info["encoder"] = "nvenc"
        info["gpu_name"] = out.strip().split("\n")[0].strip()
        return info

    rc, out, _ = run("ls /dev/dri/render* 2>/dev/null", timeout=5)
    if rc == 0 and out.strip():
        info["encoder"] = "vaapi"
        info["gpu_name"] = out.strip().split("\n")[0].strip()
        return info

    return info


# ---------------------------------------------------------------------------
# File detection
# ---------------------------------------------------------------------------
def find_video_files(directory):
    """Find video files in directory, return sorted by size (largest first)."""
    video_files = []
    for root, dirs, files in os.walk(directory):
        for f in files:
            if f.lower().endswith((".mkv", ".mp4", ".avi", ".ts", ".m2ts", ".iso")):
                fp = os.path.join(root, f)
                try:
                    size = os.path.getsize(fp)
                    video_files.append((fp, size))
                except OSError:
                    continue
    video_files.sort(key=lambda x: x[1], reverse=True)
    return video_files


def detect_profile(video_path):
    """Use ffprobe to detect resolution and HDR."""
    cmd = (
        f"ffprobe -v error -select_streams v:0 "
        f"-show_entries stream=width,height,codec_name,color_transfer "
        f"-of json {repr(video_path)}"
    )
    rc, out, _ = run(cmd, timeout=30)
    if rc != 0:
        return "1080p_sdr", "unknown"

    try:
        data = json.loads(out)
        stream = data.get("streams", [{}])[0]
        width = stream.get("width", 1920)
        height = stream.get("height", 1080)
        codec = stream.get("codec_name", "h264")
        color_transfer = stream.get("color_transfer", "")

        is_hdr = "smpte2084" in color_transfer or "arib-std-b67" in color_transfer
        is_4k = width >= 3000 or height >= 1800

        if is_4k or is_hdr:
            return "2160p_hdr", "hevc"
        else:
            return "1080p_sdr", codec
    except (json.JSONDecodeError, IndexError, KeyError):
        return "1080p_sdr", "unknown"


# ---------------------------------------------------------------------------
# Encoding
# ---------------------------------------------------------------------------
def build_ffmpeg_cmd(input_path, output_path, profile_cfg, encoder_type, logger):
    """Build ffmpeg command for 2-pass or single-pass encode."""
    codec = profile_cfg.get("codec", "hevc_nvenc")
    target_gb = profile_cfg.get("target_gb", 7)
    fmt = profile_cfg.get("format", "nv12")

    # Get duration for bitrate calc
    cmd_dur = f"ffprobe -v error -show_entries format=duration -of csv=p=0 {repr(input_path)}"
    rc, dur_out, _ = run(cmd_dur, timeout=30)
    try:
        duration = float(dur_out.strip())
    except (ValueError, TypeError):
        duration = 7200  # fallback 2h

    # Calculate target bitrate
    target_bps = (target_gb * 8e9) / duration
    max_bps = target_bps * 1.5
    buf_bps = target_bps * 2

    # Mode: cq/qp (single pass, quality-targeted)
    mode = profile_cfg.get("mode", "cq")
    quality_val = profile_cfg.get("cq", profile_cfg.get("qp", 22))

    if encoder_type == "nvenc":
        # NVENC: use -cq (constant quality)
        quality_flag = f"-cq {quality_val}"
        hw_args = ""
    else:
        # VAAPI: use -qp (NOT -cq, which is silently ignored)
        quality_flag = f"-qp {quality_val}"
        hw_args = f"-vf 'format={fmt},hwupload' -vaapi_device /dev/dri/renderD128"

    # Audio: copy all tracks
    audio_args = "-map 0:v -map 0:a? -map 0:s? -c:a copy -c:s copy"

    full_cmd = (
        f"ffmpeg -y -i {repr(input_path)} "
        f"-c:v {codec} {quality_flag} "
        f"-b:v {int(target_bps)} -maxrate {int(max_bps)} -bufsize {int(buf_bps)} "
        f"{hw_args} "
        f"{audio_args} "
        f"-movflags +faststart "
        f"{repr(output_path)}"
    )

    return full_cmd


def encode_item(input_path, output_path, profile_name, worker_cfg, logger):
    """Encode a video file."""
    profiles = worker_cfg.get("profiles", {})
    profile_cfg = profiles.get(profile_name, profiles.get("1080p_sdr", {}))
    encoder_type = worker_cfg.get("encoder", "unknown")

    logger.info(f"  Encoding: {profile_name} ({encoder_type}, {profile_cfg.get('mode')}={profile_cfg.get('cq', profile_cfg.get('qp'))})")

    ffmpeg_cmd = build_ffmpeg_cmd(input_path, output_path, profile_cfg, encoder_type, logger)

    # Create output directory
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    rc, out, err = run(ffmpeg_cmd, timeout=86400)  # 24h timeout
    if rc != 0:
        # Log last 50 lines of stderr for debugging
        err_lines = err.split("\n")[-50:]
        logger.error(f"  ffmpeg failed (rc={rc}): {' | '.join(err_lines[-5:])}")
        return False

    # Verify output exists
    if not os.path.exists(output_path):
        logger.error(f"  Output file not found: {output_path}")
        return False

    output_size = os.path.getsize(output_path)
    logger.info(f"  Encode OK: {output_size / 1e9:.1f} GB")
    return True


# ---------------------------------------------------------------------------
# Rclone helpers
# ---------------------------------------------------------------------------
def rclone_download(remote, remote_path, local_dir, logger):
    """Download from rclone remote to local directory."""
    os.makedirs(local_dir, exist_ok=True)
    cmd = f"rclone copy '{remote}{remote_path}/' '{local_dir}/' -P --transfers 2 --checkers 4 --timeout 1h --bwlimit '05:00,off 14:00,60M'"
    rc, out, err = run(cmd, timeout=86400)
    if rc != 0:
        logger.error(f"  Download failed: {err[:300]}")
        return False
    logger.info(f"  Download OK: {local_dir}")
    return True


def rclone_upload(local_dir, remote, remote_path, logger):
    """Upload from local directory to rclone remote."""
    # Auto-detect: if remote starts with '/', treat as local path
    if remote.startswith("/"):
        dest = f"{remote}/{remote_path}/"
    else:
        dest = f"{remote}:{remote_path}/"
    cmd = f"rclone copy '{local_dir}/' '{dest}' -P --transfers 2 --tpslimit 4 --timeout 2h --retries 3 --bwlimit '05:00,off 14:00,60M'"
    rc, out, err = run(cmd, timeout=86400)
    if rc != 0:
        logger.error(f"  Upload failed: {err[:300]}")
        return False
    logger.info(f"  Upload OK: {dest}")
    return True


def cleanup_dirs(dirs, logger):
    """Remove directories."""
    for d in dirs:
        if os.path.exists(d):
            run(f"rm -rf {repr(d)}", timeout=60)
            logger.debug(f"  Cleaned: {d}")


# ---------------------------------------------------------------------------
# Process a work order
# ---------------------------------------------------------------------------
def process_work_order(work_order, worker_cfg, logger):
    """Process a single item from work_order.json."""
    item_id = work_order.get("id", "unknown")
    title = work_order.get("title", "unknown")
    source_name = work_order.get("source_name", "")
    source_folder = work_order.get("source_folder", "")
    source_type = work_order.get("source_type", "webdav")
    worker_id = work_order.get("assigned_to", "unknown")

    # Resolve source_remote: the coordinator sends the generic name (e.g. "seedbox-pve21")
    # but the worker needs its local rclone remote name from source_remotes config
    source_remotes = worker_cfg.get("source_remotes", {})
    local_remote = source_remotes.get(source_name, "")
    if not local_remote:
        # Fallback: use what the coordinator sent
        local_remote = work_order.get("source_remote", "")
        logger.warning(f"  No source_remotes mapping for '{source_name}', using '{local_remote}'")

    temp_dir = worker_cfg.get("temp_dir", "/opt/v6-converter/temp")
    conv_dir = worker_cfg.get("conv_dir", "/opt/v6-converter/conversions")
    upload_dest_remote = worker_cfg.get("upload_dest_remote", "")
    upload_dest_base = worker_cfg.get("upload_dest_base", "")

    item_temp = os.path.join(temp_dir, title)
    item_conv = os.path.join(conv_dir, title)
    cleanup_list = [item_temp, item_conv]

    try:
        # 1. Build rclone path using the resolved local remote
        # Auto-detect: if local_remote starts with '/', treat as local path
        if local_remote.startswith("/") or source_type == "local":
            rclone_full_path = f"{local_remote}/{source_folder}/{title}"
        else:
            rclone_full_path = f"{local_remote}:{source_folder}/{title}"

        logger.info(f"Processing: {title}")
        logger.info(f"  Source: {rclone_full_path}")

        # Check if output already exists (skip download + encode, go straight to upload)
        wo_profile = work_order.get("profile", "")
        if wo_profile:
            output_filename_check = f"{title}_{wo_profile}.mkv"
            output_path_check = os.path.join(item_conv, output_filename_check)
            if os.path.exists(output_path_check) and os.path.getsize(output_path_check) > 0:
                logger.info(f"  ⏭️ Output already exists ({os.path.getsize(output_path_check)/1e9:.1f} GB), skipping download and encode")
                dest_folder = f"{upload_dest_base}/{source_folder}/{title}_{wo_profile}"
                write_progress(worker_id, title, "uploading", f"to {upload_dest_remote}:{dest_folder}")
                logger.info(f"  Uploading to {upload_dest_remote}:{dest_folder}")
                if not rclone_upload(item_conv, upload_dest_remote, dest_folder, logger):
                    write_progress(worker_id, title, "failed", "upload failed")
                    return False, "upload_failed"
                cleanup_dirs(cleanup_list, logger)
                write_progress(worker_id, title, "done", output_path_check)
                logger.info(f"  ✅ Complete: {title}")
                return True, output_path_check

        # 2. Download (skip if temp already has video files)
        existing_videos = find_video_files(item_temp) if os.path.exists(item_temp) else []
        if existing_videos:
            logger.info(f"  ⏭️ Temp already has video file ({existing_videos[0][1]/1e9:.1f} GB), skipping download")
        else:
            write_progress(worker_id, title, "downloading", f"from {local_remote}")
            logger.info("  Downloading...")
            os.makedirs(item_temp, exist_ok=True)
            cmd = f"rclone copy {repr(rclone_full_path)} {repr(item_temp + '/')} -P --transfers 2 --checkers 4 --timeout 1h"
            rc, out, err = run(cmd, timeout=86400)
            if rc != 0:
                logger.error(f"  Download failed: {err[:300]}")
                write_progress(worker_id, title, "failed", f"download: {err[:200]}")
                return False, "download_failed"
            logger.info(f"  Download OK: {item_temp}")

        # Find the video file
        video_files = find_video_files(item_temp)
        if not video_files:
            write_progress(worker_id, title, "failed", "no video file found")
            return False, "no_video_file_found"

        input_path = video_files[0][0]
        logger.info(f"  Video: {os.path.basename(input_path)} ({video_files[0][1]/1e9:.1f} GB)")

        # 3. Detect profile
        profile_name, codec = detect_profile(input_path)
        logger.info(f"  Profile: {profile_name}, codec: {codec}")

        # Use the profile from work_order if set
        if work_order.get("profile"):
            profile_name = work_order["profile"]

        # 4. Encode
        os.makedirs(item_conv, exist_ok=True)
        output_filename = f"{title}_{profile_name}.mkv"
        output_path = os.path.join(item_conv, output_filename)

        write_progress(worker_id, title, "encoding", f"{profile_name} via {worker_cfg.get('encoder', 'unknown')}")
        logger.info("  Encoding...")
        if not encode_item(input_path, output_path, profile_name, worker_cfg, logger):
            write_progress(worker_id, title, "failed", "encode failed")
            return False, "encode_failed"

        # 5. Upload
        dest_folder = f"{upload_dest_base}/{source_folder}/{title}_{profile_name}"
        write_progress(worker_id, title, "uploading", f"to {upload_dest_remote}:{dest_folder}")
        logger.info(f"  Uploading to {upload_dest_remote}:{dest_folder}")
        if not rclone_upload(item_conv, upload_dest_remote, dest_folder, logger):
            write_progress(worker_id, title, "failed", "upload failed")
            return False, "upload_failed"

        # 6. Cleanup
        cleanup_dirs([item_temp, item_conv], logger)

        write_progress(worker_id, title, "done", output_path)
        logger.info(f"  ✅ Complete: {title}")
        return True, output_path

    except Exception as exc:
        logger.exception(f"  Error processing {title}: {exc}")
        write_progress(worker_id, title, "failed", str(exc)[:200])
        cleanup_dirs([item_temp, item_conv], logger)
        return False, str(exc)


def write_status(item_id, status, error=None, output_path=None):
    """Write work_order_status.json for the coordinator to read."""
    status_data = {
        "item_id": item_id,
        "status": status,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    if error:
        status_data["error"] = str(error)[:500]
    if output_path:
        status_data["output_path"] = output_path

    os.makedirs(os.path.dirname(WORK_STATUS_PATH), exist_ok=True)
    with open(WORK_STATUS_PATH, "w") as f:
        json.dump(status_data, f, indent=2, ensure_ascii=False)


def write_progress(worker_id, title, phase, detail="", progress_pct=None):
    """Write work_progress.json for real-time monitoring."""
    progress_data = {
        "worker_id": worker_id,
        "title": title,
        "phase": phase,  # "downloading", "encoding", "uploading", "done", "failed"
        "detail": detail,
        "progress_pct": progress_pct,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    os.makedirs(os.path.dirname(WORK_PROGRESS_PATH), exist_ok=True)
    with open(WORK_PROGRESS_PATH, "w") as f:
        json.dump(progress_data, f, indent=2, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="v6-orchestrator worker")
    parser.add_argument("--worker-id", default=None,
                        help="Worker ID (default: hostname)")
    parser.add_argument("--config", default=CONFIG_PATH,
                        help=f"Path to config.yaml")
    parser.add_argument("--mode", default="process-work-order",
                        choices=["process-work-order", "standalone"],
                        help="Worker mode")
    return parser.parse_args()


def main():
    args = parse_args()
    cfg = load_config()

    # Determine worker ID
    worker_id = args.worker_id or socket.gethostname().lower().split(".")[0]
    name_map = {"solidvps": "solidvps", "gorilla": "gorilla", "pve14": "pve14", "pve24": "pve24"}
    for key, val in name_map.items():
        if key in worker_id:
            worker_id = val
            break

    logger = setup_logging(worker_id)
    logger.info(f"v6-orchestrator worker '{worker_id}' starting (mode={args.mode})")

    # Load worker config
    workers_cfg = cfg.get("workers", {})
    worker_cfg = workers_cfg.get(worker_id, {})
    if not worker_cfg:
        logger.warning(f"Worker '{worker_id}' not found in config, using defaults")
        worker_cfg = {
            "encoder": "unknown",
            "temp_dir": "/opt/v6-converter/temp",
            "conv_dir": "/opt/v6-converter/conversions",
            "upload_dest_remote": "",
            "upload_dest_base": "",
            "profiles": {},
        }

    # GPU detection
    gpu_info = detect_gpu_info()
    if worker_cfg.get("encoder") and worker_cfg["encoder"] != "unknown":
        gpu_info["encoder"] = worker_cfg["encoder"]
    logger.info(f"GPU: {gpu_info['gpu_name']}, encoder: {gpu_info['encoder']}")

    if args.mode == "process-work-order":
        # Read work_order.json
        if not os.path.exists(WORK_ORDER_PATH):
            logger.error(f"No work order found at {WORK_ORDER_PATH}")
            write_status("unknown", "failed", "no_work_order")
            sys.exit(1)

        with open(WORK_ORDER_PATH) as f:
            work_order = json.load(f)

        success, result = process_work_order(work_order, worker_cfg, logger)

        if success:
            write_status(work_order.get("id"), "done", output_path=result)
        else:
            write_status(work_order.get("id"), "failed", error=result)


        sys.exit(0 if success else 1)

    elif args.mode == "standalone":
        logger.info("Standalone mode not yet implemented")
        sys.exit(1)


if __name__ == "__main__":
    import socket  # imported here to avoid unused import in some modes
    main()
