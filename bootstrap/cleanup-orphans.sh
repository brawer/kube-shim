#!/bin/bash
set -euo pipefail

# Kube-Shim - Manual Orphan Cleanup
#
# A manual, operator-run backstop alongside the automatic orphan scanner
# (src/reconcile/orphan_scan.rs, running every 5 minutes against the real
# UpCloud account while the shim itself is up). Run this when you suspect
# a leak the automatic scanner hasn't caught yet -- e.g. right after the
# shim itself was down for a while, or just to double-check by hand.
#
# "Orphan" means: a volume/server on UpCloud whose title starts with this
# instance's resource_prefix (see src/naming.rs), but which has no
# corresponding row in this instance's own database -- the same
# definition reconcile::orphan_scan.rs uses. This script does NOT delete
# anything the database still tracks, no matter how old.
#
# Requires: curl, jq, sqlite3 (all standard; sqlite3 reads db.sqlite
# directly and read-only -- this script never writes to it).
#
# Usage:
#   UPCLOUD_TOKEN=... ./cleanup-orphans.sh [--yes] [--older-than-days N] \
#       [--resource-prefix kube-shim] [--db /path/to/db.sqlite] [--zone de-fra1]
#
# Without --yes, this only lists what it *would* delete (dry run).

RESOURCE_PREFIX="kube-shim"
DB_PATH="$HOME/kube-shim-data/db.sqlite"
ZONE="de-fra1"
OLDER_THAN_DAYS=7
CONFIRM=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --yes) CONFIRM=1; shift ;;
        --older-than-days) OLDER_THAN_DAYS="$2"; shift 2 ;;
        --resource-prefix) RESOURCE_PREFIX="$2"; shift 2 ;;
        --db) DB_PATH="$2"; shift 2 ;;
        --zone) ZONE="$2"; shift 2 ;;
        *) echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done

if [[ -z "${UPCLOUD_TOKEN:-}" ]]; then
    echo "UPCLOUD_TOKEN must be set in the environment (the same token" >&2
    echo "from config.toml's [upcloud] section)." >&2
    exit 1
fi

if [[ ! -f "$DB_PATH" ]]; then
    echo "Database not found at $DB_PATH (override with --db)." >&2
    exit 1
fi

API="https://api.upcloud.com/1.3"
VOL_PREFIX="${RESOURCE_PREFIX}-vol-"
WORKER_PREFIX="${RESOURCE_PREFIX}-worker-"
NOW_EPOCH=$(date -u +%s)
MIN_AGE_SECS=$((OLDER_THAN_DAYS * 86400))

echo "=== Scanning for orphaned resources (prefix: ${RESOURCE_PREFIX}) ==="
[[ "$CONFIRM" -eq 1 ]] || echo "(dry run -- pass --yes to actually delete anything)"
echo ""

# --- Volumes ---
#
# UpCloud's real GET /storage (list) response does not include a
# creation timestamp for normal-tier storage -- "created" only appears
# on backup-type storage objects (verified against UpCloud's own API
# docs while building this script, not assumed). There is no reliable
# per-volume age signal to filter on via the public API, so volumes are
# listed here by prefix+untracked status alone, with age left
# explicitly "unknown" rather than silently treated as 0 or infinite.
tracked_volume_ids=$(sqlite3 "$DB_PATH" "SELECT provider_volume_id FROM job_volumes WHERE provider_volume_id IS NOT NULL;")

echo "--- Volumes ---"
volumes_json=$(curl -sf -H "Authorization: Bearer ${UPCLOUD_TOKEN}" "${API}/storage")
echo "$volumes_json" | jq -r --arg prefix "$VOL_PREFIX" --arg zone "$ZONE" \
    '.storages.storage[] | select(.title | startswith($prefix)) | select(.zone == $zone) | "\(.uuid)\t\(.title)\t\(.size)"' |
while IFS=$'\t' read -r uuid title size; do
    if echo "$tracked_volume_ids" | grep -qx "$uuid"; then
        continue
    fi
    echo "  ORPHAN volume ${uuid} (${title}, ${size}GB) -- age unknown, UpCloud's API doesn't report it for normal-tier storage"
    if [[ "$CONFIRM" -eq 1 ]]; then
        echo "    deleting..."
        curl -sf -X DELETE -H "Authorization: Bearer ${UPCLOUD_TOKEN}" "${API}/storage/${uuid}" >/dev/null \
            && echo "    deleted." \
            || echo "    FAILED to delete ${uuid}" >&2
    fi
done

echo ""
echo "--- Servers ---"
tracked_server_ids=$(sqlite3 "$DB_PATH" "SELECT worker_vm_id FROM jobs WHERE worker_vm_id IS NOT NULL;")

servers_json=$(curl -sf -H "Authorization: Bearer ${UPCLOUD_TOKEN}" "${API}/server")
echo "$servers_json" | jq -r --arg prefix "$WORKER_PREFIX" --arg zone "$ZONE" \
    '.servers.server[] | select(.title | startswith($prefix)) | select(.zone == $zone) | "\(.uuid)\t\(.title)\t\(.state)"' |
while IFS=$'\t' read -r uuid title state; do
    if echo "$tracked_server_ids" | grep -qx "$uuid"; then
        continue
    fi

    # Unlike the storage list, GET /server/{uuid} (detail, one extra call
    # per candidate -- not in the list response itself) does include a
    # real "created" unix timestamp -- verified against UpCloud's own API
    # docs. Used here, not guessed.
    created=$(curl -sf -H "Authorization: Bearer ${UPCLOUD_TOKEN}" "${API}/server/${uuid}" | jq -r '.server.created // empty')
    if [[ -z "$created" ]]; then
        echo "  ORPHAN server ${uuid} (${title}, ${state}) -- could not determine age, skipping deletion"
        continue
    fi
    age_secs=$((NOW_EPOCH - created))
    age_days=$((age_secs / 86400))
    if [[ "$age_secs" -lt "$MIN_AGE_SECS" ]]; then
        echo "  orphan server ${uuid} (${title}) is only ${age_days}d old, leaving it (< ${OLDER_THAN_DAYS}d threshold)"
        continue
    fi

    echo "  ORPHAN server ${uuid} (${title}, ${state}, ${age_days}d old)"
    if [[ "$CONFIRM" -eq 1 ]]; then
        if [[ "$state" != "stopped" ]]; then
            echo "    stopping (UpCloud refuses to delete a running server)..."
            curl -sf -X POST -H "Authorization: Bearer ${UPCLOUD_TOKEN}" "${API}/server/${uuid}/stop" >/dev/null
            # Same real constraint reconcile::job's handle_vm_terminating
            # already works around: stop is asynchronous.
            for _ in $(seq 1 30); do
                state_now=$(curl -sf -H "Authorization: Bearer ${UPCLOUD_TOKEN}" "${API}/server/${uuid}" | jq -r '.server.state')
                [[ "$state_now" == "stopped" ]] && break
                sleep 2
            done
        fi
        echo "    deleting..."
        curl -sf -X DELETE -H "Authorization: Bearer ${UPCLOUD_TOKEN}" "${API}/server/${uuid}" >/dev/null \
            && echo "    deleted." \
            || echo "    FAILED to delete ${uuid}" >&2
    fi
done

echo ""
echo "=== Done ==="
