# Encode Watcher (encode-watcher.py)

**Location:** `/opt/v6-orchestrator/encode-watcher.py`
**Cron:** `encode-watcher-fase1` (job `a427e7efbd87`), every 5min, delivers to Telegram
**Mode:** Phase 4 (autonomia total com hard blocks)

### Pattern Registry (`~/.hermes/encode-watcher-patterns.json`)

Persistent registry of known problem patterns → auto-fix (0 tokens). When a new anomaly is detected, LLM diagnoses and saves repeatable patterns to the registry. Over time, more problems auto-fix without LLM calls.

Default patterns: coordinator_dead, coordinator_duplicate, coordinator_stale_config, tmpfs_high, disk_critical, recoverable_download_fail, download_retry_loop.

### Metrics (`~/.hermes/encode-watcher-metrics.json`)

Tracks: total_runs, llm_calls, patterns_matched, actions_auto, token_cost_total. Goal: patterns_matched/llm_calls ratio → 100%.

### Hard Blocks (NEVER execute)

Only 3 absolute: deletar conversões concluídas, parar webdav/smb, alterar credenciais.

Relaxed in phase4 with conditions:
- Matar ffmpeg → só se comprovado travado (>30min)
- Coordinator → só se duplicata (count > 1)
- config.yaml/rclone → só patches conhecidos
- queue.json → sempre com backup automático

## Architecture

```
coordinator.py (intocado)
       │
       ├── queue.json ──────┐
       ├── workers.json ────┤  lê
       ├── coordinator.log ─┤
       └── work_progress.json ──┘
              ▲
              │ lê + SSH workers
              │
    ┌─────────────────────┐
    │  encode-watcher.py  │
    │                     │
    │  1. Script filter   │  ← threshold checks, 0 tokens when OK
    │  2. Detect anomaly  │
    │  3. LLM diagnose    │  ← qwen3.6-plus via DashScope
    │  4. Auto/propose    │  ← based on phase
    │  5. Report          │  ← Telegram
    └─────────────────────┘
```

## Phase System

| Phase | Behavior | Auto-execute |
|---|---|---|
| **1** | Observe only | Nothing |
| **2** | Propose → approve → execute | Nothing (user must approve) |
| **3** | Limited autonomy | Low risk only |
| **4** | Full autonomy | Low/medium risk, high risk still asks |

## Hard Blocks (NEVER auto-execute, even in phase 4)

- Deletar conversões concluídas
- Parar webdav
- Parar smb
- Alterar credenciais

## Relaxed Rules (phase 4 conditional auto-execute)

| Type | Condition | Auto? |
|---|---|---|
| `coordinator_dead` | Process not running | ✅ Low risk |
| `coordinator_duplicate` | Multiple processes | ✅ Safe kill+restart |
| `kill_stalled_ffmpeg` | Proven stalled (no progress >30min) | ✅ Verified stall |
| Config/rclone patches | Known fix with `safe_patch` flag | ✅ WebDAV URL fix, SMB→WebDAV fallback |
| `queue_json_update` | `backup_before: true` | ✅ Auto-backup first |
| `recoverable_download_fail` | File exists, source_remote mismatch | ✅ Auto-reset with backup |
| `download_retry_loop` | download_failed (dispatched/failed/failed_permanent), retry ≥ 1 | ✅ Auto-reset with backup |
| `coordinator_stale_config` | config.yaml mtime > coordinator process age | ✅ Kill+restart coordinator with backup |

## LLM Integration

- **Model:** qwen3.6-plus via DashScope (`https://coding-intl.dashscope.aliyuncs.com/v1`)
- **Key:** Read from `~/.hermes/.env` (`DASHSCOPE_API_KEY`)
- **Timeout:** 60s (reasoning tokens add 10-30s latency)
- **Cost:** ~$0.003 per diagnosis (~228 input + ~1300 output tokens)
- **Only called when anomaly detected** — 0 cost when everything is healthy

### Diagnosis Flow

1. Script detects anomaly (threshold breach, pattern match)
2. `_collect_context()` gathers: recent coordinator log errors, worker processes, disk usage, queue state
3. `llm_diagnose()` sends context to qwen3.6-plus with system prompt about v6-orchestrator
4. LLM returns root cause + recommended action
5. `can_auto_execute()` decides based on phase + risk level
6. If auto-execute: backup (if flagged) → execute → report
7. If needs approval: report with "aprovar N" instruction

## Key Pitfalls Discovered

### pgrep -f matches Hermes shell wrapper

Hermes runs commands through bash wrappers. `pgrep -f 'python3 coordinator.py'` matches the wrapper's cmdline which contains the command string. Returns false-positive PIDs.

**Fix:** `ps aux | grep 'python3.*coordinator\.py' | grep -v 'bash\|grep\|awk' | awk '{print $2}'`

### awk regex fails over SSH

Commands with awk regex like `df -h | awk 'NR>1 && $5 ~ /%/'` fail over SSH due to shell expansion of `$5` and quoting issues.

**Fix:** Use `grep` for simple patterns over SSH: `df -h | grep '%'`

### Background process spawning in Hermes

Hermes terminal rejects shell-level background wrappers (`&`, `nohup`) in foreground mode.

**Fix:** Use Python subprocess with `start_new_session=True`:
```python
subprocess.Popen(['python3','coordinator.py'], cwd='/opt/v6-orchestrator',
                 stdout=open('/dev/null','w'), stderr=open('/dev/null','w'),
                 start_new_session=True)
```

### Recoverable Download Failure Detection

`detect_recoverable_failures()` auto-diagnoses `failed_permanent` items with `download_failed`:
1. Reads config.yaml to get worker's `source_remotes` mapping
2. Resolves canonical source name
3. SSH to worker → checks if file exists on correct remote
4. If exists → source_remote mismatch → auto-reset item to pending
5. Backs up queue.json before modification

**Example:** The Last Duel failed because gorilla tried `seedbox:` remote but gorilla only has `pve21-webdav:`, `pve21-smb:`, `pve7-smb:`. File existed on `pve21-webdav:` → auto-reset → dispatched to solidvps which has `seedbox:` remote.

## Pattern Registry (Funnel Architecture)

**The core design:** script matches known patterns first (0 tokens). LLM only called for unknown patterns. Over time, LLM-discovered patterns get saved to the registry → future uses cost nothing.

```
Anomaly detected
    │
    ├── Match known pattern? → auto-fix (0 tokens, instant)
    │     Patterns stored in: ~/.hermes/encode-watcher-patterns.json
    │
    └── New pattern? → LLM diagnosis (~$0.003)
          │
          └── LLM finds root cause → suggest adding to registry
```

### Registry location and structure

`~/.hermes/encode-watcher-patterns.json` — auto-created on first run, seeded with 5 defaults:

```json
{
  "coordinator_dead": {
    "key": "coordinator_dead",
    "type": "coordinator_dead",
    "match": {"type": "coordinator_dead"},
    "action": {
      "description": "Restart coordinator",
      "command": "cd /opt/v6-orchestrator && python3 -c \"...\"",
      "risk": "low"
    },
    "times_matched": 0,
    "last_seen": "2026-06-06T..."
  }
}
```

### Default patterns (6 built-in)

| Key | Trigger | Auto-action |
|---|---|---|
| `coordinator_dead` | coordinator.py process missing | Restart via subprocess.Popen |
| `coordinator_duplicate` | Multiple coordinator processes | Kill all, restart one |
| `coordinator_stale_config` | config.yaml modificado após coordinator start | Kill+restart (backup queue.json) |
| `tmpfs_high` | /dev/shm above 70% | Clean VAAPI orphan buffers |
| `disk_critical` | Worker disk >92% | Clean old falhas/ and temp |
| `recoverable_download_fail` | failed_permanent + file exists on correct remote | Reset to pending with backup |
| `download_retry_loop` | Items with download_failed (dispatched/failed/failed_permanent) with retry_count ≥ 1 | Reset to pending with backup (auto-executes in phase4) |

### ⚠️ CRITICAL: Pattern matching MUST run BEFORE risk="none" filter (FIXED Jun 2026)

**Original bug:** For 583 runs, the watcher detected ZERO download failures despite 17+ items `failed_permanent` with `download_failed`. Root cause:

1. `detect_permanent_failures()` returns `risk: "none"` → filtered at line 895 (`if issue.get("risk") == "none": continue`) → issue silently dropped
2. `detect_recoverable_failures()` only triggers if the file EXISTS on the correct remote via rclone check → doesn't detect when the remote is inaccessible, FUSE mount is slow, or the path is wrong
3. No detector checked `worker_status.current_phase == "failed"` from coordinator-report.json

**Fix applied:** Added two new detectors:
- `detect_worker_failed_status()` — reads coordinator-report.json, flags workers in `failed` phase
- `detect_download_retry_loop()` — scans queue.json for items with `download_failed` and retry_count >= 1

Both return `risk: "none"` (report only) — they surface the problem for manual diagnosis. For auto-recovery, the `download_retry_loop` pattern (in registry) auto-resets items to pending with backup, and the coordinator re-dispatches on next cycle.

**⚠️ Auto-reset requires coordinator restart after config fix:** If the root cause is config drift (wrong `source_remotes` on Hermes), the watcher will auto-reset items but the coordinator will re-dispatch with the same wrong remote until restarted. Always restart the coordinator AFTER config changes and BEFORE relying on auto-reset.

### Adding new patterns (LLM-discovered)

When LLM diagnoses a new problem, the pattern can be saved to the registry. The registry is loaded every run and merged with defaults — new patterns added manually or by LLM persist across sessions.

**Structure for adding:**
```python
patterns["new_pattern_key"] = {
    "key": "new_pattern_key",
    "type": "the_issue_type",
    "match": {"type": "the_issue_type"},
    "action": {
        "description": "What it does",
        "command": "bash command with {host}, {item_id} placeholders",
        "risk": "low|medium|high"
    },
    "times_matched": 0,
}
```

Placeholders in command templates: `{host}`, `{item_id}`, `{title}`, `{worker}` — substituted from issue context at match time.

## Metrics

`~/.hermes/encode-watcher-metrics.json` — tracks usage per run:

| Metric | Meaning |
|---|---|
| `total_runs` | How many times the watcher executed |
| `llm_calls` | How many times LLM was invoked (unknown patterns) |
| `patterns_matched` | How many times a known pattern auto-fixed |
| `actions_auto` | Successful auto-executions |
| `token_cost_total` | Cumulative LLM cost in USD |

Reported at the bottom of every Telegram report:
```
📊 Métricas: 42 runs | 38 patterns matched | 4 LLM calls (10%) | ~$0.01 total
```

**Goal:** Over time, `patterns_matched` grows and `llm_calls` shrinks. The funnel gets smarter, not dumber.

## Approval Flow

User responds in Telegram: `"aprovar 1"` → Hermes creates signal file → watcher reads it on next run → executes.

Signal file: `~/.hermes/encode-watcher-approve.json` (auto-deleted after 10min timeout)
