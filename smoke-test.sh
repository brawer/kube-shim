#!/bin/bash
# CI-friendly smoke test: builds (or reuses) the release binary, starts it
# with a throwaway self-signed cert and an empty database, and asserts a few
# basic properties hold -- most importantly that it actually serves TLS and
# refuses plain HTTP, since that's easy to silently regress.
#
# Exits 0 if every check passes, non-zero (with the failing check printed)
# otherwise. Safe to run repeatedly and in CI: everything happens in a fresh
# temp directory that's removed on exit, and the server always gets killed
# even if a check fails partway through.
#
# Usage: ./smoke-test.sh [--skip-build]
#   --skip-build   Reuse target/release/kube-shim instead of rebuilding it
#                   (useful in CI when a previous step already built it).

set -uo pipefail

SKIP_BUILD=0
if [[ "${1:-}" == "--skip-build" ]]; then
    SKIP_BUILD=1
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BINARY="$REPO_ROOT/target/release/kube-shim"
WORK_DIR="$(mktemp -d)"
SERVER_PID=""
FAILURES=0

find_free_port() {
    # Ask the OS for a currently-free ephemeral port rather than hardcoding
    # one: nothing guarantees any specific port is unused on a CI runner
    # (not even a high, non-standard one -- see PR discussion). This still
    # leaves a small window between "the OS said this port was free" and
    # "our server actually binds it", since a shell script can't hand the
    # already-open socket to the server the way the Rust integration test
    # does with std::net::TcpListener -- but that race is negligible in
    # practice, and far safer than a fixed number.
    python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()'
}

PORT="$(find_free_port)"
if [[ -z "$PORT" ]]; then
    echo "FAIL - could not determine a free port"
    exit 1
fi
BASE_URL="https://127.0.0.1:${PORT}"

cleanup() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill "$SERVER_PID" 2>/dev/null
        wait "$SERVER_PID" 2>/dev/null
    fi
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT

check() {
    local description="$1"
    local result="$2"
    if [[ "$result" == "ok" ]]; then
        echo "  ok - $description"
    else
        echo "  FAIL - $description"
        FAILURES=$((FAILURES + 1))
    fi
}

echo "=== kube-shim smoke test ==="

if [[ "$SKIP_BUILD" -eq 0 ]]; then
    echo "Building release binary..."
    (cd "$REPO_ROOT" && cargo build --release --quiet) || {
        echo "FAIL - build failed"
        exit 1
    }
fi

if [[ ! -x "$BINARY" ]]; then
    echo "FAIL - $BINARY not found (build it first, or drop --skip-build)"
    exit 1
fi

echo "Generating throwaway self-signed cert..."
mkdir -p "$WORK_DIR/certs"
openssl req -x509 -newkey rsa:4096 -keyout "$WORK_DIR/certs/key.pem" -out "$WORK_DIR/certs/cert.pem" \
    -days 1 -nodes -subj "/CN=localhost" >/dev/null 2>&1

TOKEN="smoke-test-token-$(date +%s)"

cat > "$WORK_DIR/config.toml" <<EOF
[server]
host = "127.0.0.1"
port = ${PORT}
tls_cert_path = "${WORK_DIR}/certs/cert.pem"
tls_key_path = "${WORK_DIR}/certs/key.pem"

[[server.api_tokens]]
token = "${TOKEN}"

[database]
path = "${WORK_DIR}/db.sqlite"

[upcloud]
token = "smoke-test-placeholder"
dry_run = true

[reconciliation]
interval_secs = 10
EOF

echo "Starting server..."
"$BINARY" -c "$WORK_DIR/config.toml" > "$WORK_DIR/server.log" 2>&1 &
SERVER_PID=$!

# Poll for readiness instead of a fixed sleep -- CI runners vary in speed.
READY=0
for _ in $(seq 1 50); do
    if curl -sk -o /dev/null -H "Authorization: Bearer ${TOKEN}" "$BASE_URL/health" 2>/dev/null; then
        READY=1
        break
    fi
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        break  # server exited early; stop polling and let the checks below report it
    fi
    sleep 0.1
done

if [[ "$READY" -ne 1 ]]; then
    echo "FAIL - server never became ready. Log:"
    cat "$WORK_DIR/server.log"
    exit 1
fi

echo "Running checks..."

health_body="$(curl -sk -H "Authorization: Bearer ${TOKEN}" "$BASE_URL/health")"
if [[ "$health_body" == '{"status":"healthy"}' ]]; then
    check "GET /health with a valid token returns healthy status" "ok"
else
    check "GET /health with a valid token returns healthy status (got: $health_body)" "fail"
fi

if curl -s -o /dev/null -m 2 "http://127.0.0.1:${PORT}/health" 2>/dev/null; then
    check "plain HTTP on the TLS port is refused" "fail"
else
    check "plain HTTP on the TLS port is refused" "ok"
fi

no_token_status="$(curl -sk -o /dev/null -w '%{http_code}' "$BASE_URL/api/v1")"
if [[ "$no_token_status" == "401" ]]; then
    check "GET /api/v1 with no token is rejected (401)" "ok"
else
    check "GET /api/v1 with no token is rejected (401) (got: $no_token_status)" "fail"
fi

wrong_token_status="$(curl -sk -o /dev/null -w '%{http_code}' -H "Authorization: Bearer wrong-token" "$BASE_URL/api/v1")"
if [[ "$wrong_token_status" == "401" ]]; then
    check "GET /api/v1 with a wrong token is rejected (401)" "ok"
else
    check "GET /api/v1 with a wrong token is rejected (401) (got: $wrong_token_status)" "fail"
fi

v1_kind="$(curl -sk -H "Authorization: Bearer ${TOKEN}" "$BASE_URL/api/v1" | jq -r '.kind' 2>/dev/null)"
if [[ "$v1_kind" == "APIResourceList" ]]; then
    check "GET /api/v1 discovery with a valid token returns APIResourceList" "ok"
else
    check "GET /api/v1 discovery with a valid token returns APIResourceList (got kind: $v1_kind)" "fail"
fi

batch_v1_kind="$(curl -sk -H "Authorization: Bearer ${TOKEN}" "$BASE_URL/apis/batch/v1" | jq -r '.kind' 2>/dev/null)"
if [[ "$batch_v1_kind" == "APIResourceList" ]]; then
    check "GET /apis/batch/v1 discovery with a valid token returns APIResourceList" "ok"
else
    check "GET /apis/batch/v1 discovery with a valid token returns APIResourceList (got kind: $batch_v1_kind)" "fail"
fi

echo ""
if [[ "$FAILURES" -eq 0 ]]; then
    echo "=== All checks passed ==="
    exit 0
else
    echo "=== $FAILURES check(s) failed ==="
    echo "--- server log ---"
    cat "$WORK_DIR/server.log"
    exit 1
fi
