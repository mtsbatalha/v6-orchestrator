#!/usr/bin/env python3
"""fix-source-remote.py — Fix queue items with wrong source_remote paths.

Called by encode-watcher when it detects queue_source_remote_mismatch.
Reads config.yaml to find the correct remote for the item's source_name,
fixes the source_remote in queue.json, and resets the item to pending.

Usage: python3 fix-source-remote.py <item_id>
"""
import json
import os
import shutil
import sys
from datetime import datetime, timezone

CONFIG_PATH = "/opt/v6-orchestrator/config.yaml"
QUEUE_PATH = "/opt/v6-orchestrator/queue.json"

# Local path patterns that only exist on gorilla
GORILLA_ONLY_PATHS = ["/mnt/internal", "/mnt/hostdzire"]


def main():
    if len(sys.argv) < 2:
        print("Usage: fix-source-remote.py <item_id>")
        sys.exit(1)

    item_id = sys.argv[1]

    # Load config
    try:
        import yaml
        with open(CONFIG_PATH) as f:
            cfg = yaml.safe_load(f)
    except Exception as e:
        print(f"Error loading config: {e}")
        sys.exit(1)

    # Load queue
    try:
        with open(QUEUE_PATH) as f:
            q = json.load(f)
    except Exception as e:
        print(f"Error loading queue: {e}")
        sys.exit(1)

    # Find the item
    item = None
    for i in q["items"]:
        if i.get("id") == item_id:
            item = i
            break

    if not item:
        print(f"Item {item_id} not found in queue")
        sys.exit(1)

    current_remote = str(item.get("source_remote", ""))
    source_name = item.get("source_name", "")
    assigned_to = item.get("assigned_to", "")

    # If the source_remote is a gorilla-only path but assigned to a different worker, fix it
    if assigned_to and assigned_to != "gorilla":
        for bad_path in GORILLA_ONLY_PATHS:
            if bad_path in current_remote:
                # Find the correct remote for this worker's source_name
                worker_cfg = cfg.get("workers", {}).get(assigned_to, {})
                worker_remotes = worker_cfg.get("source_remotes", {})

                # Look up canonical source
                source_by_name = {s["name"]: s for s in cfg.get("sources", [])}
                src = source_by_name.get(source_name, {})
                canonical = src.get("canonical_source", source_name)

                correct_remote = worker_remotes.get(canonical) or worker_remotes.get(source_name)

                if correct_remote and correct_remote != current_remote:
                    # Backup queue
                    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                    shutil.copy2(QUEUE_PATH, f"{QUEUE_PATH}.bak.fixsr.{ts}")

                    # Fix the item
                    item["source_remote"] = correct_remote
                    item["source_type"] = "local" if correct_remote.startswith("/") else "webdav"
                    item["status"] = "pending"
                    item["retry_count"] = 0
                    item["error"] = None
                    item["assigned_to"] = None

                    with open(QUEUE_PATH, "w") as f:
                        json.dump(q, f, indent=2)

                    print(f"Fixed {item_id}: {current_remote} -> {correct_remote}")
                    sys.exit(0)
                else:
                    print(f"No correct remote found for {source_name} on {assigned_to}")
                    sys.exit(1)

    # Not a mismatch case — just reset to pending so coordinator re-dispatches
    # (coordinator's dispatch_work will resolve source_remote correctly)
    item["status"] = "pending"
    item["retry_count"] = 0
    item["error"] = None
    item["assigned_to"] = None

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    shutil.copy2(QUEUE_PATH, f"{QUEUE_PATH}.bak.fixsr.{ts}")

    with open(QUEUE_PATH, "w") as f:
        json.dump(q, f, indent=2)

    print(f"Reset {item_id} to pending")


if __name__ == "__main__":
    main()
