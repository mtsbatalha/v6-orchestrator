#!/bin/bash
# Fetch and apply MakeMKV beta key automatically
# Runs weekly via cron

KEY_URL="https://www.makemkv.com/forum/viewtopic.php?f=5&t=1144"
KEY=$(curl -sL --max-time 30 "$KEY_URL" 2>/dev/null | grep -oP 'T-[A-Za-z0-9]{36,}' | head -1)

if [ -n "$KEY" ]; then
    makemkvcon reg "$KEY" > /dev/null 2>&1
    echo "$(date): Key updated successfully: ${KEY:0:10}..."
    exit 0
else
    echo "$(date): Failed to fetch key from forum"
    exit 1
fi
