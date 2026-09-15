#!/bin/bash
set -e

BASE_URL="https://127.0.0.1:6443"
INSECURE="-k"  # self-signed cert, not in any trust store
TOKEN="${KUBE_SHIM_TOKEN:-dev-only-insecure-token}"  # matches config-dev.toml
AUTH="Authorization: Bearer $TOKEN"

echo "=== Kube-Shim API Testing ==="
echo ""

# Health check
echo "1. Health check..."
curl $INSECURE -H "$AUTH" $BASE_URL/health
echo ""
echo ""

# Discovery endpoints
echo "2. API v1 discovery..."
curl $INSECURE -H "$AUTH" $BASE_URL/api/v1 | jq .
echo ""

echo "3. Batch v1 discovery..."
curl $INSECURE -H "$AUTH" $BASE_URL/apis/batch/v1 | jq .
echo ""

# Create a secret
echo "4. Creating a secret..."
curl $INSECURE -H "$AUTH" -X POST $BASE_URL/api/v1/namespaces/default/secrets \
  -H "Content-Type: application/json" \
  -d '{
    "api_version": "v1",
    "kind": "Secret",
    "metadata": {"name": "test-secret-123"},
    "data": {"username": "admin", "password": "secret123"}
  }' | jq .
echo ""

# List secrets
echo "5. Listing secrets in default namespace..."
curl $INSECURE -H "$AUTH" $BASE_URL/api/v1/namespaces/default/secrets | jq .
echo ""

# Get specific secret
echo "6. Getting specific secret..."
curl $INSECURE -H "$AUTH" $BASE_URL/api/v1/namespaces/default/secrets/test-secret-123 | jq .
echo ""

# Create a CronJob
echo "7. Creating a CronJob..."
curl $INSECURE -H "$AUTH" -X POST $BASE_URL/apis/batch/v1/namespaces/default/cronjobs \
  -H "Content-Type: application/json" \
  -d '{
    "api_version": "batch/v1",
    "kind": "CronJob",
    "metadata": {"name": "test-cronjob"},
    "spec": {
      "schedule": "0 2 * * *",
      "jobTemplate": {
        "spec": {
          "template": {
            "spec": {
              "containers": [{
                "name": "test",
                "image": "busybox",
                "command": ["echo", "hello"]
              }]
            }
          }
        }
      }
    }
  }' | jq .
echo ""

# List CronJobs
echo "8. Listing CronJobs..."
curl $INSECURE -H "$AUTH" $BASE_URL/apis/batch/v1/namespaces/default/cronjobs | jq .
echo ""

# Check database
echo "9. Checking database state..."
echo "Secrets in database:"
sqlite3 db.sqlite "SELECT id, name, namespace FROM secrets LIMIT 5;"
echo ""
echo "CronJobs in database:"
sqlite3 db.sqlite "SELECT id, name, namespace FROM cronjobs LIMIT 5;"
echo ""

# Delete secret
echo "10. Deleting secret..."
curl $INSECURE -H "$AUTH" -X DELETE $BASE_URL/api/v1/namespaces/default/secrets/test-secret-123
echo "Deleted (204 No Content expected)"
echo ""

# Delete CronJob
echo "11. Deleting CronJob..."
curl $INSECURE -H "$AUTH" -X DELETE $BASE_URL/apis/batch/v1/namespaces/default/cronjobs/test-cronjob
echo "Deleted (204 No Content expected)"
echo ""

# Auth check
echo "12. Confirming requests without a token are rejected..."
STATUS=$(curl $INSECURE -s -o /dev/null -w '%{http_code}' $BASE_URL/api/v1)
if [ "$STATUS" = "401" ]; then
  echo "OK: got 401 as expected"
else
  echo "UNEXPECTED: got $STATUS, expected 401"
  exit 1
fi
echo ""

echo "=== All tests completed ==="
