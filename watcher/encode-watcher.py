#!/usr/bin/env python3
"""
encode-watcher.py — v6-orchestrator sidecar.

Fase 1: observa e reporta
Fase 2: propõe ações, executa após aprovação (arquivo de sinal)
Fase 3: autonomia limitada (baixo risco = executa direto)
Fase 4: autonomia total (exceto hard blocks)

ZERO alteração no coordinator.py.
"""

import json
import os
import subprocess
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

ORCH_DIR = "/opt/v6-orchestrator"
SSH_KEY = os.path.expanduser("~/.ssh/id_ed25519")
APPROVAL_FILE = os.path.expanduser("~/.hermes/encode-watcher-approve.json")
STATE_FILE = os.path.expanduser("~/.hermes/encode-watcher-state.json")
PATTERN_REGISTRY_PATH = os.path.expanduser("~/.hermes/encode-watcher-patterns.json")
METRICS_PATH = os.path.expanduser("~/.hermes/encode-watcher-metrics.json")
WATCHER_MODE = "phase4"  # phase1, phase2, phase3, phase4

# Thresholds
HEARTBEAT_TIMEOUT_SEC = 300
DISK_WARN_PCT = 85
DISK_CRIT_PCT = 92
RETRY_WARN_COUNT = 2
TMPFS_WARN_PCT = 70
# Hard blocks — NUNCA executa, mesmo em fase4
# These are truly dangerous operations with no safe automation path
HARD_BLOCKS = [
    "deletar conversões concluídas",
    "parar webdav",
    "parar smb",
    "alterar credenciais",
]

WORKERS = {
    "solidvps": "38.70.138.179",
    "gorilla": "104.250.135.122",
    "pve14": "152.228.132.75",
    "pve24": "51.210.32.216",
}


# ─── Pattern Registry ──────────────────────────────────────────────────────
# Known problem patterns that DON'T need LLM diagnosis.
# LLM discovers new patterns → they get saved here → future uses are instant.
#
# Each pattern: {
#   "key": "unique identifier (type:error_fingerprint)",
#   "type": "issue type (coordinator_dead, tmpfs_high, etc.)",
#   "match": {"field": "value"} or {"error_contains": "substring"},
#   "action": {"command": "...", "description": "...", "risk": "low|medium|high"},
#   "learned_at": ISO timestamp,
#   "times_matched": int,
#   "last_seen": ISO timestamp,
# }

DEFAULT_PATTERNS = {
    "coordinator_dead": {
        "key": "coordinator_dead",
        "type": "coordinator_dead",
        "match": {"type": "coordinator_dead"},
        "action": {
            "description": "Restart coordinator",
            "command": "cd /opt/v6-orchestrator && python3 -c \"import subprocess; subprocess.Popen(['python3','coordinator.py'], cwd='/opt/v6-orchestrator', stdout=open('/dev/null','w'), stderr=open('/dev/null','w'), start_new_session=True)\"",
            "risk": "low",
        },
        "times_matched": 0,
    },
    "coordinator_duplicate": {
        "key": "coordinator_duplicate",
        "type": "coordinator_duplicate",
        "match": {"type": "coordinator_duplicate"},
        "action": {
            "description": "Kill all coordinators and restart one",
            "command": "pkill -9 -f 'python3 coordinator.py'; sleep 2; cd /opt/v6-orchestrator && python3 -c \"import subprocess; subprocess.Popen(['python3','coordinator.py'], cwd='/opt/v6-orchestrator', stdout=open('/dev/null','w'), stderr=open('/dev/null','w'), start_new_session=True)\"",
            "risk": "medium",
        },
        "times_matched": 0,
    },
    "tmpfs_high": {
        "key": "tmpfs_high",
        "type": "tmpfs_high",
        "match": {"type": "tmpfs_high"},
        "action": {
            "description": "Limpar buffers VAAPI órfãos em /dev/shm",
            "command": "ssh -o StrictHostKeyChecking=no -i ~/.ssh/id_ed25519 root@{host} 'rm -rf /dev/shm/vaapi-* /dev/shm/*dri*'",
            "risk": "low",
        },
        "times_matched": 0,
    },
    "disk_critical": {
        "key": "disk_critical",
        "type": "disk_critical",
        "match": {"type": "disk_critical"},
        "action": {
            "description": "Limpar arquivos temporários e falhas no worker",
            "command": "ssh -o StrictHostKeyChecking=no -i ~/.ssh/id_ed25519 root@{host} 'find /opt/v6-converter/falhas/ -type f -mtime +7 -delete 2>/dev/null; find /opt/v6-converter/temp/ -name \"*.tmp\" -delete 2>/dev/null; du -sh /opt/v6-converter/falhas /opt/v6-converter/temp /opt/v6-converter/conversions 2>/dev/null'",
            "risk": "low",
        },
        "times_matched": 0,
    },
    "recoverable_download_fail": {
        "key": "recoverable_download_fail",
        "type": "recoverable_download_fail",
        "match": {"type": "recoverable_download_fail"},
        "action": {
            "description": "Reset item para pending (source_remote mismatch corrigido)",
            "command": "python3 -c 'import json;f=\"/opt/v6-orchestrator/queue.json\";q=json.load(open(f));q[\"items\"]=[{{**i,\"status\":\"pending\",\"retry_count\":0,\"error\":None,\"assigned_to\":None}} if i.get(\"id\")==\"{item_id}\" else i for i in q[\"items\"]];json.dump(q,open(f,\"w\"),indent=2)'",
            "risk": "low",
            "backup_before": True,
        },
        "times_matched": 0,
    },
    "download_retry_loop": {
        "key": "download_retry_loop",
        "type": "download_retry_loop",
        "match": {"type": "download_retry_loop"},
        "action": {
            "description": "Reset item com download_failed para pending (re-dispatch com source_remote correto)",
            "command": "python3 -c 'import json;f=\"/opt/v6-orchestrator/queue.json\";q=json.load(open(f));q[\"items\"]=[{{**i,\"status\":\"pending\",\"retry_count\":0,\"error\":None,\"assigned_to\":None}} if i.get(\"id\")==\"{item_id}\" else i for i in q[\"items\"]];json.dump(q,open(f,\"w\"),indent=2)'",
            "risk": "low",
            "backup_before": True,
        },
        "times_matched": 0,
    },
}


def load_patterns():
    """Load pattern registry, seeded with defaults."""
    if os.path.exists(PATTERN_REGISTRY_PATH):
        try:
            with open(PATTERN_REGISTRY_PATH) as f:
                patterns = json.load(f)
            # Merge with defaults (add any new defaults not already in registry)
            for key, default in DEFAULT_PATTERNS.items():
                if key not in patterns:
                    patterns[key] = default
            return patterns
        except (json.JSONDecodeError, OSError):
            pass
    # Return a fresh copy of defaults
    return {k: dict(v) for k, v in DEFAULT_PATTERNS.items()}


def save_patterns(patterns):
    """Persist pattern registry."""
    try:
        os.makedirs(os.path.dirname(PATTERN_REGISTRY_PATH), exist_ok=True)
        with open(PATTERN_REGISTRY_PATH, "w") as f:
            json.dump(patterns, f, indent=2)
    except Exception:
        pass


def match_known_pattern(issue, patterns):
    """Check if an issue matches a known pattern. Returns (matched, action) or (False, None)."""
    issue_type = issue.get("type", "")

    for key, pattern in patterns.items():
        match = pattern.get("match", {})

        # Simple type match
        if match.get("type") == issue_type:
            # Update stats
            pattern["times_matched"] = pattern.get("times_matched", 0) + 1
            pattern["last_seen"] = datetime.now(timezone.utc).isoformat()
            save_patterns(patterns)

            # Build action from template, substituting placeholders
            template = pattern.get("action", {})
            action = dict(template)
            # Substitute {host}, {item_id} etc from issue
            for k, v in action.items():
                if isinstance(v, str):
                    action[k] = v.format(
                        host=issue.get("host", ""),
                        item_id=issue.get("item_id", ""),
                        title=issue.get("title", ""),
                        worker=issue.get("worker", ""),
                    )
            action["type"] = issue_type
            action["description"] = action.get("description", issue.get("description", ""))
            action["risk"] = action.get("risk", issue.get("risk", "medium"))
            return True, action

    return False, None


def load_metrics():
    """Load usage metrics."""
    if os.path.exists(METRICS_PATH):
        try:
            with open(METRICS_PATH) as f:
                return json.load(f)
        except Exception:
            pass
    return {
        "total_runs": 0,
        "llm_calls": 0,
        "patterns_matched": 0,
        "actions_auto": 0,
        "actions_approval": 0,
        "token_cost_total": 0,
        "first_run": datetime.now(timezone.utc).isoformat(),
    }


def save_metrics(metrics):
    try:
        os.makedirs(os.path.dirname(METRICS_PATH), exist_ok=True)
        with open(METRICS_PATH, "w") as f:
            json.dump(metrics, f, indent=2)
    except Exception:
        pass


def bump_metric(metrics, key, amount=1):
    metrics[key] = metrics.get(key, 0) + amount
    return metrics


# ─── Helpers ───────────────────────────────────────────────────────────────

def load_json_safe(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def ssh_cmd(host, cmd, timeout=15):
    full_cmd = (
        f"ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 "
        f"-i {SSH_KEY} root@{host} {cmd!r}"
    )
    try:
        r = subprocess.run(full_cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout.strip(), r.stderr.strip()
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"


def ssh_cmd_with_pass(host, cmd, password, timeout=15):
    full_cmd = (
        f"sshpass -p {password!r} ssh -o StrictHostKeyChecking=no "
        f"-o ConnectTimeout=5 root@{host} {cmd!r}"
    )
    try:
        r = subprocess.run(full_cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return r.returncode, r.stdout.strip(), r.stderr.strip()
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"


def load_worker_passwords():
    passwords = {}
    config_path = os.path.join(ORCH_DIR, "config.yaml")
    try:
        import yaml
        with open(config_path) as f:
            cfg = yaml.safe_load(f)
        for name, wcfg in cfg.get("workers", {}).items():
            if "ssh_pass" in wcfg:
                passwords[name] = wcfg["ssh_pass"]
    except Exception:
        pass
    return passwords


def worker_ssh(host, name, cmd, passwords, timeout=15):
    """Try key auth, fallback to password."""
    rc, out, err = ssh_cmd(host, cmd, timeout=timeout)
    if rc != 0:
        pwd = passwords.get(name)
        if pwd:
            rc, out, err = ssh_cmd_with_pass(host, cmd, pwd, timeout=timeout)
    return rc, out, err


# Hard blocks — NUNCA executa, mesmo em fase4
# These are truly dangerous operations with no safe automation path
HARD_BLOCKS = [
    "deletar conversões concluídas",
    "parar webdav",
    "parar smb",
    "alterar credenciais",
]


def is_hard_blocked(action_desc):
    desc_lower = action_desc.lower()
    for block in HARD_BLOCKS:
        if block in desc_lower:
            return True
    return False


def can_auto_execute(action, phase):
    """Determine if an action can execute without approval."""
    if is_hard_blocked(action.get("description", "")):
        return False, "HARD BLOCK: " + action["description"]

    risk = action.get("risk", "medium")
    action_type = action.get("type", "")

    # Conditional relaxations (phase4 only):

    # 3. Kill ffmpeg — only if proven stalled
    if action_type == "kill_stalled_ffmpeg":
        if phase == "phase4":
            return True, "Fase4: ffmpeg comprovadamente travado"
        return False, "Phase<4: precisa aprovação para matar ffmpeg"

    # 4. Coordinator — only if duplicate
    if action_type == "coordinator_duplicate":
        if phase == "phase4":
            return True, "Fase4: coordinator duplicata, safe kill+restart"
        return False, "Phase<4: precisa aprovação"

    # 1/7. Config/rclone — only known patches
    if action.get("safe_patch", False):
        if phase == "phase4":
            return True, f"Fase4: patch conhecido ({action.get('safe_patch', '')})"
        return False, "Phase<4: precisa aprovação para config change"

    # 8. Queue.json — allowed if backup flag is set
    if action_type in ("queue_json_update", "recoverable_download_fail", "download_retry_loop") and action.get("backup_before", False):
        if phase == "phase4":
            return True, "Fase4: queue.json com backup automático"
        return False, "Phase<4: precisa aprovação"

    # Default path
    if phase == "phase4":
        if risk in ("low", "medium"):
            return True, f"Fase4: {risk} risk, executa direto"
        return False, "Fase4: high risk, pede aprovação"

    if phase == "phase3":
        if risk == "low":
            return True, "Fase3: baixo risco, executa direto"
        return False, "Fase3: risco médio/alto, pede aprovação"

    # phase1: never executes, phase2: always requires approval
    return False, f"{phase}: só observação/aprovação"


def check_approval():
    """Check if user has approved pending actions."""
    if not os.path.exists(APPROVAL_FILE):
        return None
    try:
        with open(APPROVAL_FILE) as f:
            approval = json.load(f)
        # Check if approval is recent (within 10 min)
        age = time.time() - approval.get("approved_at", 0)
        if age > 600:
            os.remove(APPROVAL_FILE)
            return None
        return approval
    except (json.JSONDecodeError, OSError):
        os.remove(APPROVAL_FILE)
        return None


def clear_approval():
    if os.path.exists(APPROVAL_FILE):
        os.remove(APPROVAL_FILE)


def execute_action(action):
    """Execute an approved action. Returns (success, output)."""
    cmd = action.get("command", "")
    description = action.get("description", "")

    if not cmd:
        return False, "No command to execute"

    if is_hard_blocked(description):
        return False, f"HARD BLOCKED: {description}"

    # Backup before queue.json modifications
    if action.get("backup_before", False):
        try:
            import shutil
            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            src = "/opt/v6-orchestrator/queue.json"
            dst = f"/opt/v6-orchestrator/queue.json.bak.auto.{ts}"
            if os.path.exists(src):
                shutil.copy2(src, dst)
        except Exception:
            pass  # Non-fatal, proceed anyway

    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=action.get("timeout", 30))
        output = r.stdout.strip()
        if r.stderr.strip():
            output += "\n" + r.stderr.strip()
        return r.returncode == 0, output
    except subprocess.TimeoutExpired:
        return False, "Command timed out"
    except Exception as e:
        return False, str(e)


# ─── Detection Functions ───────────────────────────────────────────────────

def detect_coordinator_dead():
    """Check if coordinator.py is running (ignore bash wrappers)."""
    # Only count actual python3 processes running coordinator.py
    r = subprocess.run(
        "ps aux | grep 'python3.*coordinator\\.py' | grep -v 'bash\\|grep\\|awk' | awk '{print $2}'",
        shell=True, capture_output=True, text=True
    )
    pids = [p.strip() for p in r.stdout.strip().split() if p.strip()]
    if not pids:
        return {
            "type": "coordinator_dead",
            "severity": "critical",
            "description": "coordinator.py NÃO está rodando",
            "suggested_action": "Restart coordinator",
            "command": f"cd {ORCH_DIR} && python3 -c \"import subprocess,os; subprocess.Popen(['python3','coordinator.py'], cwd='{ORCH_DIR}', stdout=open('/dev/null','w'), stderr=open('/dev/null','w'), start_new_session=True)\"",
            "risk": "low",
            "phase_required": "phase2",
        }
    if len(pids) > 1:
        return {
            "type": "coordinator_duplicate",
            "severity": "high",
            "description": f"{len(pids)} processos coordinator.py rodando (esperado: 1)",
            "suggested_action": "Kill all and restart one coordinator",
            "command": f"pkill -9 -f 'python3 coordinator.py'; sleep 2; cd {ORCH_DIR} && python3 -c \"import subprocess; subprocess.Popen(['python3','coordinator.py'], cwd='{ORCH_DIR}', stdout=open('/dev/null','w'), stderr=open('/dev/null','w'), start_new_session=True)\"",
            "risk": "medium",
            "phase_required": "phase2",
        }
    return None


def detect_stale_heartbeats(workers_data):
    """Check for stale worker heartbeats."""
    if not workers_data:
        return None
    now = time.time()
    stale = []
    for wid, winfo in workers_data.items():
        last_seen = winfo.get("last_seen", 0)
        age = now - last_seen
        if age > HEARTBEAT_TIMEOUT_SEC:
            stale.append((wid, age / 60, winfo.get("last_seen_iso", "?")))
    if stale and len(stale) == len(workers_data):
        # ALL stale — coordinator is probably dead (already handled above)
        return None
    if stale:
        details = "; ".join(f"{w} ({a:.0f}min)" for w, a, _ in stale)
        return {
            "type": "stale_heartbeats",
            "severity": "warning",
            "description": f"Workers com heartbeat parado: {details}",
            "suggested_action": "Investigar workers individualmente via SSH",
            "command": None,  # No auto-action, just observe
            "risk": "none",
            "phase_required": "phase1",
        }
    return None


def detect_permanent_failures(queue_data):
    """Check for permanently failed items — with recovery detection."""
    if not queue_data:
        return None
    perm_items = [i for i in queue_data.get("items", []) if i["status"] == "failed_permanent"]
    if not perm_items:
        return None
    items_desc = "; ".join(
        f"{i.get('title', '?')[:50]} ({i.get('error', '?')[:40]})"
        for i in perm_items
    )
    return {
        "type": "permanent_failures",
        "severity": "warning",
        "description": f"{len(perm_items)} item(s) permanent failed: {items_desc}",
        "suggested_action": "Analisar causa e decidir se retry ou descarta",
        "command": None,
        "risk": "none",
        "phase_required": "phase1",
    }


def detect_recoverable_failures(queue_data, passwords):
    """Check if failed_permanent items with 'download_failed' might have
    source_remote mismatch — verify if file exists on remote and suggest reset."""
    if not queue_data:
        return []

    issues = []
    config_path = os.path.join(ORCH_DIR, "config.yaml")
    try:
        import yaml
        with open(config_path) as f:
            cfg = yaml.safe_load(f)
    except Exception:
        return []

    source_by_name = {s["name"]: s for s in cfg.get("sources", [])}

    for item in queue_data.get("items", []):
        if item["status"] != "failed_permanent":
            continue
        error = item.get("error", "")
        if "download_failed" not in error and "directory not found" not in error:
            continue

        title = item.get("title", "?")[:60]
        source_name = item.get("source_name", "")
        source_folder = item.get("source_folder", "")
        assigned = item.get("assigned_to", "")

        # Look up the correct remote for this worker
        worker_cfg = cfg.get("workers", {}).get(assigned, {})
        worker_remotes = worker_cfg.get("source_remotes", {})
        src = source_by_name.get(source_name, {})
        canonical = src.get("canonical_source", source_name)
        correct_remote = worker_remotes.get(canonical) or worker_remotes.get(source_name)

        if not correct_remote:
            continue  # Can't resolve, skip

        # Check if file exists on the correct remote
        remote_path = f"{source_folder}/{item['title']}/"
        if correct_remote.startswith("/"):
            # Local path
            check_cmd = f"ls -d {correct_remote!r}/{item['title']!r}"
            host = WORKERS.get(assigned)
            if not host:
                continue
            rc, out, err = ssh_cmd(host, check_cmd, timeout=10)
        else:
            # rclone remote — check on the worker
            host = WORKERS.get(assigned)
            if not host:
                continue
            rc, out, err = worker_ssh(
                host, assigned,
                f"rclone lsd '{correct_remote}:{remote_path}' 2>&1 | head -1",
                passwords, timeout=15
            )

        # If file exists, this is a recoverable source_remote mismatch
        if rc == 0 and out and "No such file" not in out and "not found" not in out.lower():
            # Build a clean reset script
            reset_script = (
                f"python3 -c \""
                f"import json,json;"
                f"q=json.load(open('/opt/v6-orchestrator/queue.json'));"
                f"q['items']=[{{**i,'status':'pending','retry_count':0,'error':None,'assigned_to':None}} "
                f"if i.get('id')=='{item.get('id','')}' else i for i in q['items']];"
                f"json.dump(q,open('/opt/v6-orchestrator/queue.json','w'),indent=2)\""
            )
            issues.append({
                "type": "recoverable_download_fail",
                "severity": "info",
                "description": f"{title}: arquivo existe no remote correto ({correct_remote}) — source_remote mismatch",
                "suggested_action": f"Reset item para pending (remote correto: {correct_remote})",
                "command": reset_script,
                "item_id": item.get("id", ""),
                "item_title": item.get("title", ""),
                "risk": "low",
                "phase_required": "phase3",
                "backup_before": True,
            })

    return issues


def detect_report_stale():
    """Check if coordinator-report.json is stale."""
    report_path = os.path.join(ORCH_DIR, "logs", "coordinator-report.json")
    if not os.path.exists(report_path):
        return {
            "type": "report_missing",
            "severity": "warning",
            "description": "coordinator-report.json não existe",
            "suggested_action": "Coordinator pode estar parado",
            "command": None,
            "risk": "none",
            "phase_required": "phase1",
        }
    mtime = os.path.getmtime(report_path)
    age_min = (time.time() - mtime) / 60
    if age_min > 10:
        return {
            "type": "report_stale",
            "severity": "warning",
            "description": f"coordinator-report.json desatualizado ({age_min:.0f}min)",
            "suggested_action": "Coordinator pode estar parado",
            "command": None,
            "risk": "none",
            "phase_required": "phase1",
        }
    return None


def detect_worker_issues(passwords):
    """SSH into workers and check for issues."""
    issues = []

    for name, host in WORKERS.items():
        # Disk check
        rc, out, err = worker_ssh(host, name, "df -h 2>/dev/null | grep '%'", passwords, timeout=10)
        if rc == 0:
            for line in out.splitlines():
                parts = line.split()
                if len(parts) >= 5:
                    try:
                        pct = int(parts[4].replace("%", ""))
                        mount = parts[5] if len(parts) > 5 else "?"
                        if pct >= DISK_CRIT_PCT:
                            issues.append({
                                "type": "disk_critical",
                                "severity": "critical",
                                "description": f"{name} {mount}: {pct}% cheio",
                                "worker": name,
                                "host": host,
                                "suggested_action": f"Limpar arquivos temporários em {name}",
                                "command": f"ssh -o StrictHostKeyChecking=no -i {SSH_KEY} root@{host} 'du -sh /opt/v6-converter/falhas/* 2>/dev/null | sort -rh | head -5; du -sh /tmp/* 2>/dev/null | sort -rh | head -5'",
                                "risk": "low",
                                "phase_required": "phase2",
                            })
                        elif pct >= DISK_WARN_PCT:
                            issues.append({
                                "type": "disk_warning",
                                "severity": "warning",
                                "description": f"{name} {mount}: {pct}% cheio",
                                "worker": name,
                                "suggested_action": "Monitorar",
                                "command": None,
                                "risk": "none",
                                "phase_required": "phase1",
                            })
                    except ValueError:
                        pass

        # tmpfs check
        rc, out, err = worker_ssh(host, name, "df /dev/shm 2>/dev/null | tail -1", passwords, timeout=5)
        if rc == 0 and out:
            parts = out.split()
            if len(parts) >= 5:
                try:
                    pct = int(parts[4].replace("%", ""))
                    if pct >= TMPFS_WARN_PCT:
                        issues.append({
                            "type": "tmpfs_high",
                            "severity": "warning",
                            "description": f"{name}: tmpfs /dev/shm em {pct}%",
                            "worker": name,
                            "host": host,
                            "suggested_action": f"Limpar buffers VAAPI órfãos em {name}",
                            "command": f"ssh -o StrictHostKeyChecking=no -i {SSH_KEY} root@{host} 'rm -rf /dev/shm/vaapi-* /dev/shm/*dri*'",
                            "risk": "low",
                            "phase_required": "phase3",  # Auto-execute in phase3+
                        })
                except ValueError:
                    pass

    return issues


def detect_worker_failed_status():
    """Check coordinator-report.json for workers stuck in failed phase."""
    report_path = os.path.join(ORCH_DIR, "logs", "coordinator-report.json")
    report = load_json_safe(report_path)
    if not report:
        return []

    issues = []
    for wid, winfo in report.get("worker_status", {}).items():
        phase = winfo.get("current_phase", "")
        if phase == "failed":
            detail = winfo.get("current_detail", "")
            title = winfo.get("current_title", "?")
            issues.append({
                "type": "worker_failed",
                "severity": "warning",
                "description": f"{wid}: failed em {title[:60]} — {detail[:100]}",
                "worker": wid,
                "host": WORKERS.get(wid, ""),
                "suggested_action": "Verificar source_remote e retry",
                "command": None,
                "risk": "none",
                "phase_required": "phase1",
            })
    return issues


def detect_download_retry_loop(queue_data):
    """Detect items stuck in retry loop with download_failed."""
    if not queue_data:
        return []

    issues = []
    for item in queue_data.get("items", []):
        error = item.get("error", "") or ""
        if "download_failed" in error and item["status"] in ("dispatched", "failed", "failed_permanent"):
            retries = item.get("retry_count", 0)
            title = item.get("title", "?")[:70]
            assigned = item.get("assigned_to", "?")
            if item["status"] == "failed_permanent":
                issues.append({
                    "type": "download_retry_loop",
                    "severity": "warning",
                    "description": f"{title}: download_failed permanent ({retries}/3) em {assigned}",
                    "worker": assigned,
                    "host": WORKERS.get(assigned, ""),
                    "item_id": item.get("id", ""),
                    "suggested_action": "Reset para pending (re-dispatch com source_remote corrigido)",
                    "command": None,
                    "risk": "none",
                    "phase_required": "phase1",
                })
            elif retries >= 1:
                title = item.get("title", "?")[:70]
                assigned = item.get("assigned_to", "?")
                issues.append({
                    "type": "download_retry_loop",
                    "severity": "warning",
                    "description": f"{title}: download_failed retry {retries}/3 em {assigned}",
                    "worker": assigned,
                    "host": WORKERS.get(assigned, ""),
                    "item_id": item.get("id", ""),
                    "suggested_action": "Verificar source_remote/accessibilidade do source",
                    "command": None,
                    "risk": "none",
                    "phase_required": "phase1",
                })
    return issues


def detect_queue_anomalies(queue_data):
    """Detect queue-level anomalies."""
    if not queue_data:
        return []

    issues = []
    items = queue_data.get("items", [])

    # Failed items with retry count
    for item in items:
        if item["status"] == "failed":
            retries = item.get("retry_count", 0)
            error = item.get("error", "")
            title = item.get("title", "?")[:70]

            if retries >= RETRY_WARN_COUNT:
                issues.append({
                    "type": "high_retry",
                    "severity": "warning",
                    "description": f"{title}: failed com {retries} retries — {error[:80]}",
                    "suggested_action": "Investigar erro antes de mais retries",
                    "command": None,
                    "risk": "none",
                    "phase_required": "phase1",
                })

    # More dispatched than workers
    dispatched = [i for i in items if i["status"] in ("dispatched", "processing")]
    if len(dispatched) > len(WORKERS):
        issues.append({
            "type": "over_dispatch",
            "severity": "high",
            "description": f"{len(dispatched)} items dispatched mas só {len(WORKERS)} workers",
            "suggested_action": "Verificar dispatch duplicado",
            "command": None,
            "risk": "none",
            "phase_required": "phase1",
        })

    return issues


# ─── LLM Diagnosis ─────────────────────────────────────────────────────────

DASHSCOPE_API_KEY = ""
DASHSCOPE_BASE_URL = "https://coding-intl.dashscope.aliyuncs.com/v1"
DASHSCOPE_MODEL = "qwen3.6-plus"

def _load_dashscope_key():
    global DASHSCOPE_API_KEY
    if DASHSCOPE_API_KEY:
        return DASHSCOPE_API_KEY
    try:
        with open(os.path.expanduser("~/.hermes/.env")) as f:
            for line in f:
                if line.startswith("DASHSCOPE_API_KEY="):
                    DASHSCOPE_API_KEY = line.split("=", 1)[1].strip()
                    return DASHSCOPE_API_KEY
    except Exception:
        pass
    return ""


def _collect_context(issue, passwords):
    """Gather relevant context for LLM diagnosis based on issue type."""
    lines = []

    # Always add recent coordinator log lines
    log_path = os.path.join(ORCH_DIR, "logs", "coordinator.log")
    if os.path.exists(log_path):
        try:
            with open(log_path) as f:
                all_lines = f.readlines()
            recent = all_lines[-50:]
            # Filter for relevant errors
            rel = [l for l in recent if any(kw in l.lower() for kw in ["error", "fail", "warn", "exception", "traceback", "exit", "kill"])]
            if rel:
                lines.append("=== Recent coordinator log errors ===")
                lines.extend(rel[-20:])
        except Exception:
            pass

    # Worker-specific context
    worker = issue.get("worker")
    if worker and worker in WORKERS:
        host = WORKERS[worker]
        # Check running processes on the worker
        rc, out, err = worker_ssh(host, worker, "ps aux | grep -E 'ffmpeg|worker' | grep -v grep", passwords, timeout=10)
        if rc == 0 and out:
            lines.append(f"=== {worker} processes ===")
            lines.append(out[:1000])

        # Check disk
        rc, out, err = worker_ssh(host, worker, "df -h / /opt /tmp /dev/shm 2>/dev/null | grep '%'", passwords, timeout=10)
        if rc == 0 and out:
            lines.append(f"=== {worker} disk ===")
            lines.append(out[:500])

    # Queue context for retry/failure issues
    if issue.get("type") in ("high_retry", "permanent_failures"):
        queue_path = os.path.join(ORCH_DIR, "queue.json")
        qd = load_json_safe(queue_path)
        if qd:
            status_counts = {}
            for item in qd.get("items", []):
                s = item.get("status", "?")
                status_counts[s] = status_counts.get(s, 0) + 1
            lines.append(f"=== Queue summary ===")
            lines.append(f"Status: {status_counts}")
            # Show failed items
            failed = [i for i in qd.get("items", []) if i.get("status") in ("failed", "failed_permanent")]
            if failed:
                lines.append(f"Failed items: {len(failed)}")
                for f in failed[:5]:
                    lines.append(f"  - {f.get('title', '?')[:60]}: {f.get('error', '?')[:100]}")

    return "\n".join(lines)


def llm_diagnose(issue, context_text=""):
    """Call qwen3.6-plus to diagnose an anomaly. Returns (diagnosis, llm_info, confidence)."""
    import urllib.request
    import urllib.error

    api_key = _load_dashscope_key()
    if not api_key:
        return "API key não encontrada", "", 0.0

    system_prompt = (
        "Você é um especialista em infraestrutura de encoding de vídeo e Linux. "
        "Analise problemas no v6-orchestrator (sistema distribuído de encode com ffmpeg, "
        "rclone, SSH, VAAPI/NVENC). Responda SEMPRE em português. "
        "Seja direto: 1) causa raiz, 2) ação recomendada, 3) comando se aplicável. "
        "Se não tiver certeza, diga. NÃO invente diagnósticos."
    )

    user_prompt = f"Tipo: {issue.get('type', '?')}\nDescrição: {issue.get('description', '?')}\n"
    if context_text:
        user_prompt += f"\nContexto:\n{context_text[:3000]}\n"
    user_prompt += "\nQual a causa raiz provável? O que fazer?"

    payload = json.dumps({
        "model": DASHSCOPE_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "max_tokens": 512,
        "temperature": 0.3,
    }).encode("utf-8")

    url = f"{DASHSCOPE_BASE_URL}/chat/completions"
    req = urllib.request.Request(url, data=payload, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", f"Bearer {api_key}")

    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            body = json.loads(resp.read().decode("utf-8"))
        if "error" in body:
            return f"LLM API error: {body['error'].get('message', str(body['error']))}", "", 0.0
        diagnosis = body.get("choices", [{}])[0].get("message", {}).get("content", "")
        usage = body.get("usage", {})
        tokens_in = usage.get("prompt_tokens", 0)
        tokens_out = usage.get("completion_tokens", 0)
        cost = (tokens_in * 0.0000005) + (tokens_out * 0.000002)
        return diagnosis.strip(), f"~${cost:.4f} ({tokens_in}in+{tokens_out}out tokens)", 0.8
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8", errors="replace")[:300]
        return f"LLM HTTP {e.code}: {err_body}", "", 0.0
    except Exception as e:
        return f"LLM error: {e}", "", 0.0


# ─── Main ──────────────────────────────────────────────────────────────────

def run():
    """Main entry point."""
    lines = []
    actions_pending = []
    actions_auto = []
    llm_results = []  # Track LLM calls for metrics

    # Load state
    passwords = load_worker_passwords()
    workers_data = load_json_safe(os.path.join(ORCH_DIR, "workers.json"))
    queue_data = load_json_safe(os.path.join(ORCH_DIR, "queue.json"))
    patterns = load_patterns()
    metrics = load_metrics()
    metrics = bump_metric(metrics, "total_runs")

    # Run all detectors
    detectors = [
        detect_coordinator_dead,
        lambda: detect_stale_heartbeats(workers_data),
        lambda: detect_permanent_failures(queue_data),
        lambda: detect_worker_failed_status(),  # NEW: workers stuck in failed phase
        lambda: detect_download_retry_loop(queue_data),  # NEW: download retry loops
        detect_report_stale,
        lambda: detect_worker_issues(passwords),  # returns list
        lambda: detect_queue_anomalies(queue_data),  # returns list
        lambda: detect_recoverable_failures(queue_data, passwords),  # returns list
    ]

    all_issues = []
    for detector in detectors:
        result = detector()
        if result:
            if isinstance(result, list):
                all_issues.extend(result)
            else:
                all_issues.append(result)

    matched_issue_keys = set()  # Track issues consumed by pattern matching

    # Classify issues into actions
    for issue in all_issues:
        # ─── PATTERN MATCHING FIRST (zero LLM cost) ───
        # Even if risk="none", check for registered patterns first
        matched, pattern_action = match_known_pattern(issue, patterns)
        if matched:
            metrics = bump_metric(metrics, "patterns_matched")
            action = pattern_action
            action["diagnosis"] = "Pattern reconhecido — auto-fix"
            action["llm_info"] = ""
            can_auto, reason = can_auto_execute(action, WATCHER_MODE)
            if can_auto:
                actions_auto.append(action)
            elif WATCHER_MODE != "phase1":
                actions_pending.append(action)
            # Track as consumed — exclude from observations
            issue_key = f"{issue.get('type', '')}:{issue.get('item_id', '')}:{issue.get('worker', '')}"
            matched_issue_keys.add(issue_key)
            # Mark as handled to avoid duplicates
            state = load_json_safe(STATE_FILE) or {}
            handled = state.get("handled", [])
            handled.append({
                "key": f"{action.get('type', '')}:{action.get('worker', 'global')}:{action.get('item_id', '')}",
                "at": time.time(),
                "result": "executed" if can_auto else "pending",
            })
            state["handled"] = handled[-20:]
            save_json(STATE_FILE, state)
            continue

        # For non-pattern issues, need command + non-none risk to act
        if issue.get("command") and issue.get("suggested_action"):
            # Check if already handled recently
            prev_state = load_json_safe(STATE_FILE) or {}
            handled = prev_state.get("handled", [])
            action_key = f"{issue['type']}:{issue.get('worker', 'global')}"
            already_handled = any(
                h.get("key") == action_key and (time.time() - h.get("at", 0)) < 300
                for h in handled
            )
            if already_handled:
                continue

            if issue.get("risk") == "none":
                continue  # Observation only

            # ─── LLM FALLBACK (unknown pattern) ───
            context = _collect_context(issue, passwords)
            diagnosis, llm_info, confidence = llm_diagnose(issue, context)
            metrics = bump_metric(metrics, "llm_calls")
            if llm_info:
                # Extract cost from llm_info like "~$0.0028 (228in+1332out tokens)"
                try:
                    cost_str = llm_info.split("$")[1].split(" ")[0]
                    metrics["token_cost_total"] = metrics.get("token_cost_total", 0) + float(cost_str)
                except (IndexError, ValueError):
                    pass

            issue["diagnosis"] = diagnosis
            issue["llm_info"] = llm_info or ""
            llm_results.append({"issue": issue.get("type", ""), "diagnosis": diagnosis[:100]})

            can_auto, reason = can_auto_execute(issue, WATCHER_MODE)
            if can_auto:
                actions_auto.append(issue)
            elif WATCHER_MODE != "phase1":
                actions_pending.append(issue)

    # Check for pending approvals
    approval = check_approval()
    executed_results = []

    if approval:
        pending_actions = approval.get("pending_actions", [])
        for action in pending_actions:
            success, output = execute_action(action)
            executed_results.append({
                "action": action.get("description", ""),
                "success": success,
                "output": output[:500] if output else "",
                "diagnosis": action.get("diagnosis", ""),
                "llm_info": action.get("llm_info", ""),
            })
            # Mark as handled
            state = load_json_safe(STATE_FILE) or {}
            handled = state.get("handled", [])
            handled.append({
                "key": f"{action.get('type', '')}:{action.get('worker', 'global')}",
                "at": time.time(),
                "result": "success" if success else "failed",
            })
            state["handled"] = handled[-20:]
            save_json(STATE_FILE, state)

        clear_approval()

    # Execute auto-actions (phase3+)
    for action in actions_auto:
        success, output = execute_action(action)
        executed_results.append({
            "action": action.get("description", ""),
            "success": success,
            "output": output[:500] if output else "",
            "auto": True,
            "reason": can_auto_execute(action, WATCHER_MODE)[1],
            "diagnosis": action.get("diagnosis", ""),
            "llm_info": action.get("llm_info", ""),
        })
        if success:
            metrics = bump_metric(metrics, "actions_auto")

    # Save metrics
    save_metrics(metrics)

    # Build report
    lines.append(f"🔎 **ENCODE WATCHER** — {WATCHER_MODE}")
    lines.append(f"📅 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    lines.append("")

    # Auto-executed actions
    if executed_results:
        lines.append("**✅ Ações executadas:**")
        for r in executed_results:
            prefix = "✅" if r["success"] else "❌"
            auto_tag = " [auto]" if r.get("auto") else ""
            llm_tag = ""
            if r.get("diagnosis") and r["diagnosis"] != "Pattern reconhecido — auto-fix":
                llm_tag = f"\n    🤖 {r['diagnosis'][:200]}"
            if r.get("llm_info"):
                llm_tag += f"\n    📊 {r['llm_info']}"
            lines.append(f"  {prefix} {r['action']}{auto_tag}{llm_tag}")
            if r["output"] and not r["success"]:
                lines.append(f"    {r['output'][:120]}")
        lines.append("")

    # Pending actions needing approval
    if actions_pending:
        lines.append("**⏳ Ações pendentes (aguardando aprovação):**")
        for i, action in enumerate(actions_pending, 1):
            llm_line = ""
            if action.get("diagnosis") and action["diagnosis"] != "Pattern reconhecido — auto-fix":
                llm_line = f"\n    🤖 {action['diagnosis'][:200]}"
            if action.get("llm_info"):
                llm_line += f"\n    📊 {action['llm_info']}"
            lines.append(f"  {i}. **{action['severity'].upper()}** — {action['description']}{llm_line}")
            lines.append(f"     → {action['suggested_action']}")
            lines.append(f"     Risco: {action.get('risk', '?')} | Responda 'aprovar {i}' para executar")
        lines.append("")

        save_json(APPROVAL_FILE.replace("-approve.json", "-pending.json"), {
            "actions": actions_pending,
            "generated_at": time.time(),
        })

    # Observations (no action needed) — exclude issues already consumed by pattern matching
    observations = []
    for i in all_issues:
        if i.get("risk") != "none":
            continue
        if i in [a for a in actions_pending]:
            continue
        issue_key = f"{i.get('type', '')}:{i.get('item_id', '')}:{i.get('worker', '')}"
        if issue_key in matched_issue_keys:
            continue  # Already handled by pattern match
        observations.append(i)
    if observations:
        lines.append("**📋 Observações:**")
        for obs in observations:
            sev_icon = {"warning": "⚠️", "critical": "🔴", "info": "ℹ️"}.get(obs.get("severity", "info"), "ℹ️")
            lines.append(f"  {sev_icon} {obs['description']}")
        lines.append("")

    # Metrics summary (every run)
    total_runs = metrics.get("total_runs", 0)
    llm_calls = metrics.get("llm_calls", 0)
    patterns_matched = metrics.get("patterns_matched", 0)
    token_cost = metrics.get("token_cost_total", 0)
    llm_rate = (llm_calls / total_runs * 100) if total_runs > 0 else 0
    lines.append(f"**📊 Métricas:** {total_runs} runs | {patterns_matched} patterns matched | {llm_calls} LLM calls ({llm_rate:.0f}%) | ~${token_cost:.2f} total")
    lines.append("")

    # Footer
    lines.append("---")
    if WATCHER_MODE == "phase1":
        lines.append("Fase 1: só observação. Nenhuma ação executada.")
    elif WATCHER_MODE == "phase2":
        lines.append("Fase 2: ações precisam de aprovação. Responda 'aprovar <número>'.")
    elif WATCHER_MODE == "phase4":
        lines.append("Fase 4: autonomia total (exceto hard blocks). Patterns conhecidos = auto-fix instantâneo.")

    return "\n".join(lines)


if __name__ == "__main__":
    report = run()
    print(report)
