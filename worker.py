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
import socket
import subprocess
import sys
import threading
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


def is_bdrom_folder(directory):
    """Check if directory is a BDROM structure (has BDMV/ subdirectory)."""
    bdmv_path = os.path.join(directory, "BDMV")
    return os.path.isdir(bdmv_path)


def rip_bdrom(bdrom_dir, output_dir, logger):
    """Extract main title from BDROM using MakeMKV.
    Returns (mkv_path, error_message).
    """
    ripper_path = "/opt/v6-orchestrator/bdrom_ripper.py"
    if not os.path.exists(ripper_path):
        return None, f"bdrom_ripper.py not found at {ripper_path}"
    
    os.makedirs(output_dir, exist_ok=True)
    
    cmd = f"python3 {repr(ripper_path)} {repr(bdrom_dir)} {repr(output_dir)}"
    logger.info(f"  Running MakeMKV rip: {cmd}")
    
    rc, out, err = run(cmd, timeout=7200)  # 2h timeout for BD rip
    
    if rc != 0 or not out.strip():
        error_msg = err[:300] if err else "no output from ripper"
        logger.error(f"  MakeMKV rip failed: {error_msg}")
        return None, f"makemkv rip failed: {error_msg}"
    
    mkv_path = out.strip()
    if not os.path.exists(mkv_path):
        return None, f"ripper reported success but file not found: {mkv_path}"
    
    mkv_size = os.path.getsize(mkv_path)
    logger.info(f"  MakeMKV rip OK: {mkv_path} ({mkv_size/1e9:.1f} GB)")
    return mkv_path, None


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
# Disk space check
# ---------------------------------------------------------------------------
def check_disk_space(path, required_bytes):
    """Check if there is enough disk space at the given path.
    
    Returns (ok, free_gb, required_gb).
    """
    try:
        stat = os.statvfs(path)
        free_bytes = stat.f_bavail * stat.f_frsize
    except OSError:
        return False, 0.0, 0.0
    
    free_gb = free_bytes / 1e9
    required_gb = required_bytes / 1e9
    return free_bytes >= required_bytes, free_gb, required_gb


# ---------------------------------------------------------------------------
# Error classification
# ---------------------------------------------------------------------------
def classify_error(error_message):
    """Classify an error message into a category.
    
    Returns one of: "transient", "resource", "permanent", "unknown"
    """
    msg = error_message.lower()
    
    transient_patterns = [
        "timeout", "connection refused", "network unreachable",
        "broken pipe", "timed out", "connection reset",
    ]
    resource_patterns = [
        "disk full", "no space left", "no space", "cannot allocate memory",
        "out of memory", "enough space", "insufficient space",
    ]
    permanent_patterns = [
        "unsupported codec", "corrupt file", "invalid data",
        "file not found", "no such file", "invalid argument",
        "unsupported pixel format", "decoder not found",
        "stream not found", "unknown encoder",
    ]
    
    for pattern in transient_patterns:
        if pattern in msg:
            return "transient"
    for pattern in resource_patterns:
        if pattern in msg:
            return "resource"
    for pattern in permanent_patterns:
        if pattern in msg:
            return "permanent"
    
    return "unknown"


# ---------------------------------------------------------------------------
# Output validation
# ---------------------------------------------------------------------------
def validate_output(output_path, input_path, profile_name, logger):
    """Validate the encoded output file using ffprobe.
    
    Checks:
      - File exists and size > 100MB
      - Has a video stream
      - Video codec matches expected (hevc for 2160p, h264 for 1080p)
      - Duration within 10% of source duration
      - All audio streams present (count matches source)
    
    Returns (ok, error_message).
    """
    # 1. File exists and size > 100MB
    if not os.path.exists(output_path):
        return False, "output file does not exist"
    
    output_size = os.path.getsize(output_path)
    if output_size < 100 * 1024 * 1024:
        return False, f"output file too small: {output_size / 1e9:.2f} GB (need > 100 MB)"
    
    # 2. Probe output file
    cmd_out = (
        f"ffprobe -v error -select_streams v:0 "
        f"-show_entries stream=codec_name,width,height "
        f"-show_entries format=duration "
        f"-show_entries stream=index -select_streams a "
        f"-of json {repr(output_path)}"
    )
    rc, probe_out, probe_err = run(cmd_out, timeout=30)
    if rc != 0:
        return False, f"ffprobe on output failed: {probe_err[:200]}"
    
    try:
        data = json.loads(probe_out)
    except json.JSONDecodeError:
        return False, f"ffprobe output not valid JSON: {probe_out[:200]}"
    
    # 3. Has video stream
    video_streams = data.get("streams", [])
    if not video_streams:
        return False, "output file has no video stream"
    
    # 4. Video codec matches expected
    vcodec = video_streams[0].get("codec_name", "").lower()
    expected_codec = "hevc" if "2160p" in profile_name else "h264"
    if expected_codec not in vcodec:
        return False, f"output video codec '{vcodec}' does not match expected '{expected_codec}'"
    
    # 5. Duration within 10% of source
    out_duration = data.get("format", {}).get("duration")
    if out_duration is None:
        return False, "output file has no duration info"
    
    out_duration = float(out_duration)
    
    cmd_src = (
        f"ffprobe -v error -show_entries format=duration "
        f"-of csv=p=0 {repr(input_path)}"
    )
    rc_src, src_dur_out, _ = run(cmd_src, timeout=30)
    try:
        src_duration = float(src_dur_out.strip())
    except (ValueError, TypeError):
        src_duration = None
    
    if src_duration is not None and src_duration > 0:
        ratio = out_duration / src_duration
        if ratio < 0.9 or ratio > 1.1:
            return False, (
                f"output duration {out_duration:.1f}s differs from source "
                f"{src_duration:.1f}s by {abs(1 - ratio) * 100:.1f}% (> 10%)"
            )
    
    # 6. Audio stream count matches source
    out_audio_count = len([s for s in data.get("streams", []) if s.get("codec_type") == "audio"])
    # Re-probe input for audio count (use a separate focused probe)
    cmd_audio_src = (
        f"ffprobe -v error -select_streams a -show_entries stream=index "
        f"-of json {repr(input_path)}"
    )
    rc2, audio_probe_out, _ = run(cmd_audio_src, timeout=30)
    src_audio_count = 0
    if rc2 == 0:
        try:
            audio_data = json.loads(audio_probe_out)
            src_audio_count = len(audio_data.get("streams", []))
        except (json.JSONDecodeError, KeyError):
            src_audio_count = 0
    
    if src_audio_count > 0 and out_audio_count < src_audio_count:
        return False, (
            f"output has {out_audio_count} audio stream(s) but source has "
            f"{src_audio_count}"
        )
    
    logger.info(f"  Validation OK: {vcodec}, {out_duration:.0f}s, {out_audio_count} audio, {output_size/1e9:.1f} GB")
    return True, None


# ---------------------------------------------------------------------------
# Real-time ffmpeg progress
# ---------------------------------------------------------------------------
# Regex for ffmpeg progress lines: "time=01:23:45.67 fps=30 speed=1.2x"
_FFMPEG_TIME_RE = re.compile(
    r"time=(\d+):(\d+):(\d+(?:\.\d+)?)\s+.*?fps=(\d+(?:\.\d+)?)\s+.*?speed=(\d+(?:\.\d+)?)x"
)


def _parse_time_hms(h, m, s):
    """Convert hours, minutes, seconds strings to total seconds."""
    return float(h) * 3600 + float(m) * 60 + float(s)


def run_ffmpeg_with_progress(ffmpeg_cmd, duration, progress_callback, logger):
    """Run ffmpeg via Popen and parse stderr for progress.
    
    Args:
        ffmpeg_cmd: ffmpeg command string.
        duration: total duration of source in seconds (for progress %).
        progress_callback: function(current_time, progress_pct, fps, speed) called ~every 10s.
        logger: logger instance.
    
    Returns: (returncode, stdout, stderr)
    """
    last_callback_time = time.time()
    
    try:
        proc = subprocess.Popen(
            ffmpeg_cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    except Exception as exc:
        return -1, "", str(exc)
    
    # Read stderr in a background thread so we can poll progress
    stderr_lines = []
    stderr_done = threading.Event()
    
    def _read_stderr():
        """Read ffmpeg stderr line by line."""
        if proc.stderr is None:
            stderr_done.set()
            return
        try:
            for line in proc.stderr:
                stderr_lines.append(line)
        except Exception:
            pass
        stderr_done.set()
    
    stderr_thread = threading.Thread(target=_read_stderr, daemon=True)
    stderr_thread.start()
    
    # Poll the process and parse progress
    while proc.poll() is None:
        time.sleep(1)  # poll every second
        
        now = time.time()
        if now - last_callback_time < 10:
            continue
        
        # Parse the most recent progress line from stderr_lines
        for line in reversed(stderr_lines):
            m = _FFMPEG_TIME_RE.search(line)
            if m:
                h, mi, s, fps, speed = m.groups()
                current_time = _parse_time_hms(h, mi, s)
                fps_val = float(fps)
                speed_val = float(speed)
                
                if duration > 0:
                    progress_pct = min((current_time / duration) * 100, 100.0)
                else:
                    progress_pct = 0.0
                
                progress_callback(current_time, progress_pct, fps_val, speed_val)
                break
        
        last_callback_time = now
    
    # Wait for stderr thread to finish (with timeout)
    stderr_done.wait(timeout=5)
    
    rc = proc.returncode or 0
    stdout = ""
    stderr = "".join(stderr_lines)
    return rc, stdout, stderr


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

    return full_cmd, duration


def encode_item(input_path, output_path, profile_name, worker_cfg, logger, progress_callback=None):
    """Encode a video file with real-time progress reporting.
    
    Args:
        progress_callback: optional function(current_time, progress_pct, fps, speed)
                          called approximately every 10 seconds.
    """
    profiles = worker_cfg.get("profiles", {})
    profile_cfg = profiles.get(profile_name, profiles.get("1080p_sdr", {}))
    encoder_type = worker_cfg.get("encoder", "unknown")

    logger.info(f"  Encoding: {profile_name} ({encoder_type}, {profile_cfg.get('mode')}={profile_cfg.get('cq', profile_cfg.get('qp'))})")

    ffmpeg_cmd, duration = build_ffmpeg_cmd(input_path, output_path, profile_cfg, encoder_type, logger)

    # Create output directory
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Use Popen with progress parsing if callback provided, otherwise fallback to run()
    if progress_callback:
        rc, out, err = run_ffmpeg_with_progress(ffmpeg_cmd, duration, progress_callback, logger)
    else:
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

    # Track all temp dirs for cleanup (BDROM creates an extra one)
    cleanup_list = [item_temp, item_conv]

    try:
        # 0. Disk space check before download
        source_size_bytes = work_order.get("source_size_bytes", 0)
        if source_size_bytes > 0:
            required_bytes = int(source_size_bytes * 2.5)  # source temp + output temp
            disk_ok, free_gb, required_gb = check_disk_space(temp_dir, required_bytes)
            if not disk_ok:
                msg = f"disk_insufficient_space: need {required_gb:.1f} GB free, have {free_gb:.1f} GB"
                logger.error(f"  {msg}")
                write_progress(worker_id, title, "failed", msg)
                return False, "disk_insufficient_space", "resource"

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
                    return False, "upload_failed", "unknown"
                cleanup_dirs(cleanup_list, logger)
                write_progress(worker_id, title, "done", output_path_check)
                logger.info(f"  ✅ Complete: {title}")
                return True, output_path_check, None

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
                return False, "download_failed", classify_error(err)
            logger.info(f"  Download OK: {item_temp}")

        # 3. Detect if this is a BDROM folder and handle accordingly
        is_bdrom = is_bdrom_folder(item_temp)
        
        if is_bdrom:
            # BDROM: extract main title with MakeMKV first
            write_progress(worker_id, title, "ripping", "extracting main title from BDROM")
            logger.info("  BDROM detected, extracting main title with MakeMKV...")
            
            rip_output_dir = os.path.join(temp_dir, f"{title}_ripped")
            cleanup_list.append(rip_output_dir)  # Add to cleanup list
            
            mkv_path, rip_error = rip_bdrom(item_temp, rip_output_dir, logger)
            
            if rip_error:
                write_progress(worker_id, title, "failed", f"bdrom rip: {rip_error[:200]}")
                return False, f"bdrom_rip_failed: {rip_error}", "permanent"
            
            input_path = mkv_path
            logger.info(f"  Ripped MKV: {os.path.basename(input_path)} ({os.path.getsize(input_path)/1e9:.1f} GB)")
        else:
            # Regular Remux: find video file normally
            video_files = find_video_files(item_temp)
            if not video_files:
                write_progress(worker_id, title, "failed", "no video file found")
                return False, "no_video_file_found", "permanent"

            input_path = video_files[0][0]
            logger.info(f"  Video: {os.path.basename(input_path)} ({video_files[0][1]/1e9:.1f} GB)")

        # 4. Detect profile
        profile_name, codec = detect_profile(input_path)
        logger.info(f"  Profile: {profile_name}, codec: {codec}")

        # Use the profile from work_order if set
        if work_order.get("profile"):
            profile_name = work_order["profile"]

        # 5. Encode with real-time progress
        os.makedirs(item_conv, exist_ok=True)
        output_filename = f"{title}_{profile_name}.mkv"
        output_path = os.path.join(item_conv, output_filename)

        write_progress(worker_id, title, "encoding", f"{profile_name} via {worker_cfg.get('encoder', 'unknown')}")
        logger.info("  Encoding...")

        # Progress callback: update work_progress.json every ~10s
        _last_progress_update = [time.time()]  # mutable list for closure
        _encode_meta = {"duration": 0.0}  # shared state for closure

        def _progress_cb(current_time, progress_pct, fps, speed):
            now = time.time()
            if now - _last_progress_update[0] < 10:
                return
            _last_progress_update[0] = now

            eta_seconds = None
            dur = _encode_meta["duration"]
            if speed > 0 and dur:
                eta_seconds = (dur - current_time) / speed

            detail = f"{progress_pct:.1f}% @ {fps:.0f}fps, {speed:.1f}x"
            if eta_seconds is not None and eta_seconds > 0:
                detail += f", ETA {eta_seconds/60:.0f}m"

            write_progress(
                worker_id, title, "encoding", detail,
                progress_pct=round(progress_pct, 1),
                fps=fps, speed=speed, eta_seconds=eta_seconds,
            )

        # Get duration from input
        cmd_dur = f"ffprobe -v error -show_entries format=duration -of csv=p=0 {repr(input_path)}"
        rc_dur, dur_out, _ = run(cmd_dur, timeout=30)
        try:
            _encode_meta["duration"] = float(dur_out.strip())
        except (ValueError, TypeError):
            _encode_meta["duration"] = 0

        if not encode_item(input_path, output_path, profile_name, worker_cfg, logger, progress_callback=_progress_cb):
            write_progress(worker_id, title, "failed", "encode failed")
            return False, "encode_failed", "permanent"

        # 5b. Validate output before upload
        logger.info("  Validating output...")
        valid, validation_error = validate_output(output_path, input_path, profile_name, logger)
        if not valid:
            logger.error(f"  Output validation failed: {validation_error}")
            write_progress(worker_id, title, "failed", f"validation failed: {validation_error}")
            return False, f"validation_failed: {validation_error}", "permanent"

        # 6. Upload
        dest_folder = f"{upload_dest_base}/{source_folder}/{title}_{profile_name}"
        write_progress(worker_id, title, "uploading", f"to {upload_dest_remote}:{dest_folder}")
        logger.info(f"  Uploading to {upload_dest_remote}:{dest_folder}")
        if not rclone_upload(item_conv, upload_dest_remote, dest_folder, logger):
            write_progress(worker_id, title, "failed", "upload failed")
            return False, "upload_failed", "transient"

        # 7. Cleanup
        cleanup_dirs(cleanup_list, logger)

        write_progress(worker_id, title, "done", output_path)
        logger.info(f"  ✅ Complete: {title}")
        return True, output_path, None

    except Exception as exc:
        logger.exception(f"  Error processing {title}: {exc}")
        write_progress(worker_id, title, "failed", str(exc)[:200])
        cleanup_dirs(cleanup_list, logger)
        return False, str(exc), classify_error(str(exc))


def write_status(item_id, status, error=None, output_path=None, error_category=None):
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
    if error_category:
        status_data["error_category"] = error_category

    os.makedirs(os.path.dirname(WORK_STATUS_PATH), exist_ok=True)
    with open(WORK_STATUS_PATH, "w") as f:
        json.dump(status_data, f, indent=2, ensure_ascii=False)


def write_progress(worker_id, title, phase, detail="", progress_pct=None, fps=None, speed=None, eta_seconds=None):
    """Write work_progress.json for real-time monitoring."""
    progress_data = {
        "worker_id": worker_id,
        "title": title,
        "phase": phase,  # "downloading", "ripping", "encoding", "uploading", "done", "failed"
        "detail": detail,
        "progress_pct": progress_pct,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    if fps is not None:
        progress_data["fps"] = fps
    if speed is not None:
        progress_data["speed"] = speed
    if eta_seconds is not None:
        progress_data["eta_seconds"] = eta_seconds

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

        success, result, error_category = process_work_order(work_order, worker_cfg, logger)

        if success:
            write_status(work_order.get("id"), "done", output_path=result)
        else:
            write_status(work_order.get("id"), "failed", error=result, error_category=error_category)


        sys.exit(0 if success else 1)

    elif args.mode == "standalone":
        logger.info("Standalone mode not yet implemented")
        sys.exit(1)


if __name__ == "__main__":
    main()
