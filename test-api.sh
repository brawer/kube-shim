#!/bin/bash
set -e

BASE_URL="http://127.0.0.1:6443"
INSECURE=""  # No TLS in Phase 1

echo "=== Kube-Shim API Testing ==="
echo ""

# Health check
echo "1. Health check..."
curl $INSECURE $BASE_URL/health
echo ""
echo ""

# Discovery endpoints
echo "2. API v1 discovery..."
curl $INSECURE $BASE_URL/api/v1 | jq .
echo ""

echo "3. Batch v1 discovery..."
curl $INSECURE $BASE_URL/apis/batch/v1 | jq .
echo ""

# Create a secret
echo "4. Creating a secret..."
curl $INSECURE -X POST $BASE_URL/api/v1/namespaces/default/secrets \
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
curl $INSECURE $BASE_URL/api/v1/namespaces/default/secrets | jq .
echo ""

# Get specific secret
echo "6. Getting specific secret..."
curl $INSECURE $BASE_URL/api/v1/namespaces/default/secrets/test-secret-123 | jq .
echo ""

# Create a CronJob
echo "7. Creating a CronJob..."
curl $INSECURE -X POST $BASE_URL/apis/batch/v1/namespaces/default/cronjobs \
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
curl $INSECURE $BASE_URL/apis/batch/v1/namespaces/default/cronjobs | jq .
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
curl $INSECURE -X DELETE $BASE_URL/api/v1/namespaces/default/secrets/test-secret-123
echo "Deleted (204 No Content expected)"
echo ""

# Delete CronJob
echo "11. Deleting CronJob..."
curl $INSECURE -X DELETE $BASE_URL/apis/batch/v1/namespaces/default/cronjobs/test-cronjob
echo "Deleted (204 No Content expected)"
echo ""

echo "=== All tests completed ==="
