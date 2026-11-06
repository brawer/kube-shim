#!/bin/bash
# kube-shim worker VM startup script (Phase 9, exit-code signaling
# reworked in Phase 10 -- see the bottom of this file).
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
# Every job-supplied value embedded here (image, resource limits, and
# the run options from src/run_options.rs)
# arrives already base64-encoded and is decoded with `base64 -d`, never
# interpolated into this script as raw shell text -- see cloud_init.rs's
# own module docs for why (a malicious/malformed image name must not be
# able to inject shell commands into a script that runs as root).
#
# The container's env, command and args are deliberately NOT in this
# script (it is the server's user_data, readable through UpCloud's API):
# the shim delivers them over SSH into root-only files under $CONFIG_DIR
# (issue #84), and this script waits for them below.
set -u

STATUS_FILE=/tmp/job-status.txt
CONTAINER_ID_FILE=/tmp/container-id.txt
EXIT_CODE_FILE=/tmp/exit-code

echo "Waiting for volume..." > "$STATUS_FILE"

IMAGE=$(echo '__KUBESHIM_IMAGE_B64__' | base64 -d)
CPU_LIMIT=$(echo '__KUBESHIM_CPU_LIMIT_B64__' | base64 -d)
MEMORY_LIMIT=$(echo '__KUBESHIM_MEMORY_LIMIT_B64__' | base64 -d)
# Run options (issue #86, src/run_options.rs), each empty when unset:
# fsGroup, `--user` value ("uid" or "uid:gid"), "true" for
# readOnlyRootFilesystem, and the scratch volume's `-v` argument.
FS_GROUP=$(echo '__KUBESHIM_FS_GROUP_B64__' | base64 -d)
RUN_AS_USER=$(echo '__KUBESHIM_RUN_AS_USER_B64__' | base64 -d)
READ_ONLY_ROOT=$(echo '__KUBESHIM_READ_ONLY_ROOT_B64__' | base64 -d)
SCRATCH_VOLUME=$(echo '__KUBESHIM_SCRATCH_VOLUME_B64__' | base64 -d)

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

# Kubernetes' fsGroup semantics: the volume's root belongs to that group,
# with the setgid bit so new files inherit it. Without this, a container
# running as non-root can't write to /scratch (root:root 0755). Both the
# fresh ext4 filesystem and the boot-disk directory are empty, so there
# is nothing to chown recursively.
if [ -n "$FS_GROUP" ]; then
    chgrp "$FS_GROUP" /scratch
    chmod 2775 /scratch
fi

echo "Installing podman..." >> "$STATUS_FILE"
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq podman >> "$STATUS_FILE" 2>&1

# Wait for the container config the shim delivers over SSH (its
# `ContainerConfiguring` state, src/cloud_init.rs's UPLOAD_*_COMMAND):
# `env` in `podman run --env-file` format, `secret-env` (variables the
# env file can't hold, e.g. a multi-line PEM key) and `command`/`args`
# as NUL-terminated entries, each written root-only and renamed into place,
# then `ready` last. No timeout of our own: the shim fails and deletes a
# job (and this VM) that gets stuck before its container starts.
CONFIG_DIR=/run/kube-shim
echo "Waiting for container config..." >> "$STATUS_FILE"
until [ -e "$CONFIG_DIR/ready" ]; do
    sleep 2
done
mapfile -d '' -t COMMAND < "$CONFIG_DIR/command"
mapfile -d '' -t ARGS < "$CONFIG_DIR/args"

# Each secret-env NAME/VALUE pair becomes a podman secret, exposed to the
# container as an env var with the exact value (line breaks included).
# The value only ever goes through printf (a builtin) and a pipe, never
# argv, so it stays out of the process list too.
mapfile -d '' -t SECRET_ENV < "$CONFIG_DIR/secret-env"
SECRET_ARGS=()
for ((i = 0; i + 1 < ${#SECRET_ENV[@]}; i += 2)); do
    SECRET_NAME="kube-shim-env-$i"
    printf '%s' "${SECRET_ENV[i+1]}" | podman secret create "$SECRET_NAME" - > /dev/null
    SECRET_ARGS+=(--secret "$SECRET_NAME,type=env,target=${SECRET_ENV[i]}")
done
unset SECRET_ENV

# Empty CPU_LIMIT/MEMORY_LIMIT means the job's spec had no
# resources.limits.cpu/.memory set -- omit the flag entirely rather than
# defaulting to the request or to this VM's own plan size, matching real
# Kubernetes semantics (no limit means unconstrained).
RESOURCE_ARGS=()
[ -n "$CPU_LIMIT" ] && RESOURCE_ARGS+=(--cpus "$CPU_LIMIT")
[ -n "$MEMORY_LIMIT" ] && RESOURCE_ARGS+=(--memory "$MEMORY_LIMIT")

SECURITY_ARGS=()
[ -n "$FS_GROUP" ] && SECURITY_ARGS+=(--group-add "$FS_GROUP")
[ -n "$RUN_AS_USER" ] && SECURITY_ARGS+=(--user "$RUN_AS_USER")
[ "$READ_ONLY_ROOT" = true ] && SECURITY_ARGS+=(--read-only)

echo "Starting container..." >> "$STATUS_FILE"
# --env-file, not one `-e NAME=VALUE` per variable, so Secret values
# never show up in this VM's process list.
CONTAINER_ID=$(podman run -d -v "$SCRATCH_VOLUME" "${RESOURCE_ARGS[@]}" "${SECURITY_ARGS[@]}" --env-file "$CONFIG_DIR/env" "${SECRET_ARGS[@]}" "$IMAGE" "${COMMAND[@]}" "${ARGS[@]}")
echo "$CONTAINER_ID" > "$CONTAINER_ID_FILE"
# podman has copied the env into the container's own config by now.
rm -f "$CONFIG_DIR/env" "$CONFIG_DIR/secret-env"

podman wait "$CONTAINER_ID" > /dev/null
EXIT_CODE=$(podman inspect "$CONTAINER_ID" --format '{{.State.ExitCode}}')
echo "Exit code: $EXIT_CODE" >> "$STATUS_FILE"

# Signals completion to the shim over SSH (Phase 10): its
# `ContainerRunning` handler polls for this exact file, and once it
# exists, fetches the real exit code from it plus the full container
# logs (`podman logs`) before the worker VM is torn down a few states
# later. This is the very last thing the script does, deliberately not
# followed by `poweroff` -- Phase 9's own version of this script powered
# the VM off itself as an SSH-free completion signal (no SSH client
# existed yet); now that one does, the shim needs the VM to stay up long
# enough to actually connect and read this file, so it stays running
# (and billing) until the shim explicitly deletes it. See
# src/reconcile/job.rs's own module docs for the real trade-off this is
# -- a shim that crashes before noticing now leaves the worker running
# indefinitely, where the old mechanism would have self-terminated.
echo "$EXIT_CODE" > "$EXIT_CODE_FILE"
