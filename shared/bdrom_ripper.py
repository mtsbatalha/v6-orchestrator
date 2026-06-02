#!/usr/bin/env python3
"""
BDROM Ripper - Extract main title from Blu-ray BDMV folder using MakeMKV

Usage:
    python3 bdrom_ripper.py <bdrom_folder> <output_dir> [timeout_seconds]

Returns:
    - Path to extracted .mkv file on success
    - Exit code 1 on failure
"""
import os
import re
import subprocess
import sys
import time

MAKEMKV_CMD = "makemkvcon"
INFO_TIMEOUT = 60  # seconds for info command
RIP_TIMEOUT = 7200  # 2 hours for full rip


def run(cmd, timeout=600, cwd=None):
    """Run a shell command, return (returncode, stdout, stderr)."""
    try:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True,
            timeout=timeout, cwd=cwd,
        )
        return result.returncode, result.stdout.strip(), result.stderr.strip()
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"
    except Exception as exc:
        return -1, "", str(exc)


def is_bdrom_folder(folder):
    """Check if folder is a BDROM structure (has BDMV/ subdirectory)."""
    bdmv_path = os.path.join(folder, "BDMV")
    return os.path.isdir(bdmv_path)


def get_makemkv_info(bdrom_folder):
    """Get MakeMKV info about the BDROM, return list of titles with durations."""
    cmd = f'{MAKEMKV_CMD} -r info file:{repr(bdrom_folder)}'
    rc, out, err = run(cmd, timeout=INFO_TIMEOUT)
    
    if rc != 0:
        return None, f"makemkvcon info failed: rc={rc}, err={err[:200]}"
    
    # Parse robot-mode output
    titles = []
    current_title = {}
    
    for line in out.split("\n"):
        if line.startswith("TCOUNT:"):
            # Total count of titles
            continue
        elif line.startswith("CINFO:"):
            # Disc info (skip)
            continue
        elif line.startswith("TINFO:"):
            # Title info
            parts = line.split(":", 3)
            if len(parts) >= 4:
                title_idx = int(parts[1])
                field = int(parts[2])
                value = parts[3]
                
                if field == 1:  # Title name
                    current_title["name"] = value
                elif field == 9:  # Duration in seconds
                    try:
                        # Format: "hh:mm:ss"
                        h, m, s = value.split(":")
                        current_title["duration"] = int(h) * 3600 + int(m) * 60 + int(s)
                    except ValueError:
                        current_title["duration"] = 0
                elif field == 11:  # Size in sectors (approximate size)
                    try:
                        current_title["sectors"] = int(value)
                    except ValueError:
                        current_title["sectors"] = 0
                elif field == 27:  # Title ID
                    current_title["id"] = value
        
        elif line.startswith("MSG:"):
            # Messages (check for errors)
            msg_parts = line.split(",", 3)
            if len(msg_parts) >= 4:
                msg_code = int(msg_parts[1])
                msg_flags = int(msg_parts[2])
                # Error messages have specific codes
                if msg_code in (5000, 5001, 5002, 5003, 5004, 5005):
                    return None, f"MakeMKV error: {msg_parts[3]}"
    
    # Collect titles
    if current_title and "duration" in current_title:
        titles.append(current_title)
    
    # Re-parse to collect all titles
    titles = []
    current = {}
    for line in out.split("\n"):
        if line.startswith("TINFO:"):
            parts = line.split(":", 3)
            if len(parts) >= 4:
                title_idx = int(parts[1])
                field = int(parts[2])
                value = parts[3]
                
                if title_idx != current.get("idx", -1):
                    if current and "duration" in current:
                        titles.append(current)
                    current = {"idx": title_idx}
                
                if field == 1:
                    current["name"] = value
                elif field == 9:
                    try:
                        h, m, s = value.split(":")
                        current["duration"] = int(h) * 3600 + int(m) * 60 + int(s)
                    except ValueError:
                        current["duration"] = 0
                elif field == 11:
                    try:
                        current["sectors"] = int(value)
                    except ValueError:
                        current["sectors"] = 0
                elif field == 27:
                    current["id"] = value
    
    if current and "duration" in current:
        titles.append(current)
    
    return titles, None


def select_main_title(titles):
    """Select the main title (longest duration, or largest size)."""
    if not titles:
        return None, "no titles found"
    
    # Filter out very short titles (< 10 minutes = extras, menus, etc.)
    meaningful = [t for t in titles if t.get("duration", 0) >= 600]
    
    if not meaningful:
        # If all titles are short, pick the longest anyway
        meaningful = titles
    
    # Sort by duration (descending), then by size (descending)
    meaningful.sort(key=lambda t: (t.get("duration", 0), t.get("sectors", 0)), reverse=True)
    
    main = meaningful[0]
    return main, None


def rip_title(bdrom_folder, title_idx, output_dir, logger=None):
    """Extract a title to MKV using MakeMKV."""
    os.makedirs(output_dir, exist_ok=True)
    
    cmd = f'{MAKEMKV_CMD} -r mkv file:{repr(bdrom_folder)} {title_idx} {repr(output_dir)}'
    
    if logger:
        logger.info(f"  Starting MakeMKV rip: title {title_idx}")
    
    rc, out, err = run(cmd, timeout=RIP_TIMEOUT)
    
    if rc != 0:
        error_msg = err[:500] if err else out[:500]
        return None, f"makemkvcon mkv failed (rc={rc}): {error_msg}"
    
    # Find the output MKV file
    mkv_files = []
    for root, dirs, files in os.walk(output_dir):
        for f in files:
            if f.lower().endswith(".mkv"):
                fp = os.path.join(root, f)
                size = os.path.getsize(fp)
                mkv_files.append((fp, size))
    
    if not mkv_files:
        return None, "no MKV file found after rip"
    
    # Return the largest MKV file (main title)
    mkv_files.sort(key=lambda x: x[1], reverse=True)
    return mkv_files[0][0], None


def rip_bdrom(bdrom_folder, output_dir, logger=None):
    """Main function: detect BDROM, select main title, rip to MKV."""
    if not is_bdrom_folder(bdrom_folder):
        return None, f"not a BDROM folder (no BDMV/): {bdrom_folder}"
    
    if logger:
        logger.info(f"  BDROM detected: {bdrom_folder}")
    
    # Get title info
    titles, err = get_makemkv_info(bdrom_folder)
    if err:
        return None, f"failed to get title info: {err}"
    
    if not titles:
        return None, "no titles found in BDROM"
    
    if logger:
        logger.info(f"  Found {len(titles)} titles")
        for i, t in enumerate(titles[:5]):  # Show first 5
            dur = t.get("duration", 0)
            size_gb = (t.get("sectors", 0) * 2048) / (1024**3)
            logger.info(f"    Title {t.get('idx', i)}: {dur//60}min, {size_gb:.1f} GB")
    
    # Select main title
    main_title, err = select_main_title(titles)
    if err:
        return None, f"failed to select main title: {err}"
    
    title_idx = main_title.get("idx", 0)
    duration = main_title.get("duration", 0)
    
    if logger:
        logger.info(f"  Selected main title: {title_idx} ({duration//60}min)")
    
    # Rip the title
    mkv_path, err = rip_title(bdrom_folder, title_idx, output_dir, logger)
    if err:
        return None, f"rip failed: {err}"
    
    mkv_size = os.path.getsize(mkv_path) if mkv_path else 0
    
    if logger:
        logger.info(f"  Rip complete: {mkv_path} ({mkv_size/1e9:.1f} GB)")
    
    return mkv_path, None


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <bdrom_folder> <output_dir> [timeout_seconds]")
        sys.exit(1)
    
    bdrom = sys.argv[1]
    out_dir = sys.argv[2]
    
    mkv_path, err = rip_bdrom(bdrom, out_dir)
    if err:
        print(f"ERROR: {err}", file=sys.stderr)
        sys.exit(1)
    
    print(mkv_path)
