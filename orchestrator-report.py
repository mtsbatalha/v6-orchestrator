#!/usr/bin/env python3
"""
Orchestrator Report v3 — formatted like the classic beautiful report.
Per-worker sections with emoji, status, GPU/CPU/RAM, disk, last 5 converted.
Optimized: single SSH call per worker for all system info.
"""
import json
import subprocess
import sys
from datetime import datetime, timezone

REPORT = "/opt/v6-orchestrator/logs/coordinator-report.json"
QUEUE = "/opt/v6-orchestrator/queue.json"
COORD_LOG = "/opt/v6-orchestrator/logs/coordinator.log"

WORKER_CONFIG = {
    "gorilla": {
        "icon": "🟢",
        "label": "GORILLA",
        "encoder": "VAAPI H.264 10Mbps",
        "host": "104.250.135.122",
        "gpu_type": "vaapi",
    },
    "solidvps": {
        "icon": "🔵",
        "label": "SOLIDVPS",
        "encoder": "NVENC HEVC P1000",
        "host": "38.70.138.179",
        "gpu_type": "nvenc",
    },
    "computebox": {
        "icon": "🟡",
        "label": "COMPUTEBOX",
        "encoder": "H265+AV1 RTX 4000 Ada",
        "host": None,
        "gpu_type": "nvenc",
    },
    "pve14": {
        "icon": "🟠",
        "label": "PVE-14",
        "encoder": "VAAPI H.264 VBR 10M",
        "host": "152.228.132.75",
        "gpu_type": "vaapi",
    },
    "pve24": {
        "icon": "🟤",
        "label": "PVE-24",
        "encoder": "VAAPI H.264 VBR 10M",
        "host": "51.210.32.216",
        "gpu_type": "vaapi",
    },
}

SSH_OPTS = ["ssh", "-o", "ConnectTimeout=5", "-o", "StrictHostKeyChecking=no"]

# Commands per GPU type — written as shell scripts to avoid escaping issues
NVENC_SYS_CMD = r"""
gpu=$(nvidia-smi --query-gpu=utilization.gpu,memory.used,memory.total,temperature.gpu --format=csv,noheader,nounits 2>/dev/null | head -1)
mem=$(free -m | awk '/Mem:/{print $3"/"$2}')
cpu=$(top -bn1 | grep '%Cpu' | sed 's/.*: *//' | cut -d' ' -f1 | tr -d ',')
disk_root=$(df -h / | tail -1)
disk_vz=$(df -h /var/lib/vz 2>/dev/null | tail -1)
echo "GPU=$gpu"
echo "MEM=$mem"
echo "CPU=$cpu"
echo "DISK_ROOT=$disk_root"
echo "DISK_VZ=$disk_vz"
"""

VAAPI_SYS_CMD = r"""
gpu=$(intel_gpu_frequency 2>/dev/null | grep 'cur:' | awk '{print $2, $3}' || echo '?')
mem=$(free -m | awk '/Mem:/{print $3"/"$2}')
cpu=$(top -bn1 | grep '%Cpu' | sed 's/.*: *//' | cut -d' ' -f1 | tr -d ',')
disk_root=$(df -h / | tail -1)
disk_vz=$(df -h /var/lib/vz 2>/dev/null | tail -1)
echo "GPU=$gpu"
echo "MEM=$mem"
echo "CPU=$cpu"
echo "DISK_ROOT=$disk_root"
echo "DISK_VZ=$disk_vz"
"""


def ssh_run(host, cmd, timeout=10):
    if not host:
        return ""
    try:
        r = subprocess.run(
            SSH_OPTS + ["root@" + host, cmd],
            capture_output=True, text=True, timeout=timeout
        )
        return r.stdout.strip()
    except Exception:
        return ""


def get_system_info(name):
    """Single SSH call per worker to get all system info."""
    host = WORKER_CONFIG[name].get("host")
    if not host:
        return ("OFFLINE", "CPU:? RAM:?", "N/A")

    gpu_type = WORKER_CONFIG[name]["gpu_type"]
    cmd = NVENC_SYS_CMD if gpu_type == "nvenc" else VAAPI_SYS_CMD

    out = ssh_run(host, cmd, timeout=12)

    gpu_line = "OFFLINE"
    cpu_val = "?"
    mem_val = "?"
    disk_lines = []

    for line in out.split("\n"):
        line = line.strip()
        if line.startswith("GPU="):
            val = line[4:].strip()
            if gpu_type == "nvenc" and val:
                parts = [p.strip() for p in val.split(",")]
                if len(parts) >= 4:
                    gpu_line = f"GPU:{parts[0]}% VRAM:{parts[1]}/{parts[2]}MB {parts[3]}°C"
                else:
                    gpu_line = f"GPU:{val}"
            elif gpu_type == "vaapi":
                gpu_line = f"VAAPI {val}" if val and val != "?" else "VAAPI ?"
        elif line.startswith("MEM="):
            mem_val = line[4:].strip()
        elif line.startswith("CPU="):
            cpu_val = line[4:].strip()
        elif line.startswith("DISK_ROOT="):
            disk_lines.append(line[10:].strip())
        elif line.startswith("DISK_VZ="):
            vz = line[8:].strip()
            if vz and vz not in disk_lines:
                disk_lines.append(vz)

    cpu_ram = f"CPU:{cpu_val}% RAM:{mem_val}"
    disk = "\n   ".join(disk_lines) if disk_lines else "N/A"

    return (gpu_line, cpu_ram, disk)


def main():
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    # Load queue
    queue_items = []
    try:
        with open(QUEUE) as f:
            q = json.load(f)
        queue_items = q.get("items", [])
    except Exception:
        pass

    # Load report
    report_data = None
    try:
        with open(REPORT) as f:
            report_data = json.load(f)
    except Exception:
        pass

    ws = {}
    wp = {}
    if report_data:
        ws = report_data.get("worker_status", {})
        wp = report_data.get("worker_progress", {})

    # Build per-worker last-5-done lists
    worker_done = {}
    for item in queue_items:
        if item.get("status") == "done":
            worker = item.get("assigned_to", "")
            if worker:
                worker_done.setdefault(worker, []).append(item.get("title", ""))

    # Collect alerts
    alerts = []

    # Check for failed items
    for item in queue_items:
        s = item.get("status", "")
        if s in ("failed", "failed_permanent"):
            alerts.append(f"{item.get('assigned_to', '?')}: {item.get('title', '?')[:60]} ({s})")

    # Print each worker section
    for worker_key in ["gorilla", "solidvps", "computebox", "pve14", "pve24"]:
        cfg = WORKER_CONFIG[worker_key]
        w = ws.get(worker_key, {})
        p = wp.get(worker_key, {})

        reachable = w.get("reachable", False)
        phase = p.get("phase", w.get("current_phase", "idle"))
        current_title = p.get("title", w.get("current_title", ""))
        detail = p.get("detail", w.get("current_detail", ""))
        progress = p.get("progress_pct", "")

        # Skip computebox if inactive
        if not cfg.get("host") and not current_title:
            continue

        gpu_info, cpu_ram, disk_info = get_system_info(worker_key)

        # Determine status line
        if current_title and phase not in ("idle", "waiting"):
            title_short = current_title[:90]
            if progress:
                status_line = f"   🏃 Encode: {title_short}"
                status_line += f"\n   ⏱️  {progress}"
            else:
                status_line = f"   🏃 {phase}: {title_short}"
            if detail:
                status_line += f" ({detail})"
        else:
            status_line = "   ⏸️ Idle — aguardando próximo"

        print(f"{cfg['icon']} {cfg['label']} | {cfg['encoder']}")
        print(status_line)
        print(f"   📊 {gpu_info} | {cpu_ram}")
        if disk_info:
            for dl in disk_info.split("\n"):
                print(f"   💾 {dl.strip()}")
        else:
            print(f"   💾 N/A")

        # Last 5 converted
        done_list = worker_done.get(worker_key, [])[-5:]
        if done_list:
            print(f"   ✅ Últimos 5 convertidos:")
            for t in done_list:
                print(f"     • {t[:80]}")
        print()

    # Alerts
    if alerts:
        print("⚠️  ALERTAS:")
        for a in alerts:
            print(f"   • {a}")
        print()

    print("--- End Report ---")


if __name__ == "__main__":
    main()
