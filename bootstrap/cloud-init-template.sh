#!/bin/bash
# kube-shim worker VM startup script (Phase 9).
#
# This is a *template*: src/cloud_init.rs embeds this file at compile time
# (include_str!) and substitutes the __KUBESHIM_..._B64__ placeholders
# below with base64-encoded, job-supplied values before handing the result
# to UpCloud as a real server's `user_data` (executed as root on first
# boot). The placeholders are deliberately NOT the `{{...}}` Go-template
# syntax podman itself uses below (see the exit-code line) -- reusing that
# syntax would make cloud_init.rs's substitution collide with podman's own
# --format templating.
#
# Every job-supplied value (image, command, args, env) arrives here
# already base64-encoded and is decoded with `base64 -d`, never
# interpolated into this script as raw shell text -- see cloud_init.rs's
# own module docs for why (a malicious/malformed image name or env value
# must not be able to inject shell commands into a script that runs as
# root).
set -u

STATUS_FILE=/tmp/job-status.txt
CONTAINER_ID_FILE=/tmp/container-id.txt

echo "Waiting for volume..." > "$STATUS_FILE"

IMAGE=$(echo '__KUBESHIM_IMAGE_B64__' | base64 -d)
mapfile -t COMMAND < <(echo '__KUBESHIM_COMMAND_B64__' | base64 -d)
mapfile -t ARGS < <(echo '__KUBESHIM_ARGS_B64__' | base64 -d)
mapfile -t ENV_LINES < <(echo '__KUBESHIM_ENV_B64__' | base64 -d)

# The job's ephemeral volume, if it requested one, is attached by the shim
# via a separate UpCloud API call made AFTER this VM is already running
# (Phase 9's own real-world ordering constraint: attaching a volume needs
# a server UUID, which doesn't exist until the server itself does) -- so
# it may genuinely not be attached yet even tens of seconds into this
# script's own execution. Poll for it rather than assuming it's already
# there. Device naming depends on which bus UpCloud presents extra
# storage on for this template -- check both of the real possibilities
# rather than hardcoding one and guessing wrong.
DEVICE=""
for _ in $(seq 1 60); do
    for candidate in /dev/vdb /dev/sdb; do
        if [ -b "$candidate" ]; then
            DEVICE="$candidate"
            break 2
        fi
    done
    sleep 5
done

mkdir -p /scratch
if [ -n "$DEVICE" ]; then
    echo "Found device: $DEVICE" >> "$STATUS_FILE"
    echo "Mounting volume..." >> "$STATUS_FILE"
    mkfs.ext4 -F "$DEVICE" >> "$STATUS_FILE" 2>&1
    mount "$DEVICE" /scratch
else
    echo "No volume attached (job requested none, or attach timed out); /scratch is on the boot disk" >> "$STATUS_FILE"
fi

echo "Installing podman..." >> "$STATUS_FILE"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq podman >> "$STATUS_FILE" 2>&1

# ENV_LINES entries are "NAME=VALUE" strings, fed to `podman run` as
# separate -e arguments -- never concatenated into a shell command string,
# so a value containing shell metacharacters can't do anything but be a
# literal environment variable value.
ENV_ARGS=()
for line in "${ENV_LINES[@]}"; do
    [ -n "$line" ] && ENV_ARGS+=(-e "$line")
done

echo "Starting container..." >> "$STATUS_FILE"
CONTAINER_ID=$(podman run -d -v /scratch:/scratch "${ENV_ARGS[@]}" "$IMAGE" "${COMMAND[@]}" "${ARGS[@]}")
echo "$CONTAINER_ID" > "$CONTAINER_ID_FILE"

podman wait "$CONTAINER_ID" > /dev/null
EXIT_CODE=$(podman inspect "$CONTAINER_ID" --format '{{.State.ExitCode}}')
echo "Exit code: $EXIT_CODE" >> "$STATUS_FILE"

# Signals completion to the shim's own reconciliation loop, which has no
# SSH access to this VM yet to read the status file directly (that's
# Phase 10's job -- see docs/IMPLEMENTATION_PLAN.md Phase 9's own notes).
# Powering off is a real, SSH-free completion signal the shim already has
# a primitive for: its `ContainerRunning` handler polls this server's own
# provider-reported state (get_server) and treats the transition away
# from "started" as "the job is done", regardless of the container's exit
# code -- real success/failure determination from this status file is
# deferred to whichever phase actually reads it over SSH.
poweroff
