use serde_json::json;
use sqlx::{SqlitePool, Row};
use uuid::Uuid;
use chrono::Utc;

// Test helpers
async fn setup_test_db() -> SqlitePool {
    let pool = kube_shim::db::init_pool(":memory:")
        .await
        .expect("Failed to init test DB");
    pool
}

#[tokio::test]
async fn test_create_and_get_secret() {
    let pool = setup_test_db().await;

    // Create a secret
    let secret_data = json!({
        "api_version": "v1",
        "kind": "Secret",
        "metadata": {"name": "test-secret"},
        "data": {"key": "value"}
    });

    // Insert into database
    let id = Uuid::new_v4().to_string();
    let now = Utc::now().timestamp();
    let data_json = serde_json::to_string(&secret_data["data"])
        .expect("Failed to serialize data");

    sqlx::query(
        r#"
        INSERT INTO secrets (id, name, namespace, data, created_at, updated_at, version)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(&id)
    .bind("test-secret")
    .bind("default")
    .bind(&data_json)
    .bind(now)
    .bind(now)
    .bind(1)
    .execute(&pool)
    .await
    .expect("Failed to insert secret");

    // Verify it's in the database
    let row = sqlx::query("SELECT name, namespace FROM secrets WHERE id = ?")
        .bind(&id)
        .fetch_one(&pool)
        .await
        .expect("Failed to fetch secret");

    let name: String = row.get(0);
    let namespace: String = row.get(1);

    assert_eq!(name, "test-secret");
    assert_eq!(namespace, "default");
}

#[tokio::test]
async fn test_list_secrets_by_namespace() {
    let pool = setup_test_db().await;
    let now = Utc::now().timestamp();

    // Create multiple secrets
    for i in 0..3 {
        let id = Uuid::new_v4().to_string();
        sqlx::query(
            r#"
            INSERT INTO secrets (id, name, namespace, data, created_at, updated_at, version)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(&id)
        .bind(format!("secret-{}", i))
        .bind("default")
        .bind(r#"{"key":"value"}"#)
        .bind(now)
        .bind(now)
        .bind(1)
        .execute(&pool)
        .await
        .expect("Failed to insert secret");
    }

    // Create a secret in different namespace
    let id = Uuid::new_v4().to_string();
    sqlx::query(
        r#"
        INSERT INTO secrets (id, name, namespace, data, created_at, updated_at, version)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(&id)
    .bind("secret-other")
    .bind("other-ns")
    .bind(r#"{"key":"value"}"#)
    .bind(now)
    .bind(now)
    .bind(1)
    .execute(&pool)
    .await
    .expect("Failed to insert secret");

    // Query secrets in default namespace
    let rows = sqlx::query("SELECT COUNT(*) as count FROM secrets WHERE namespace = ?")
        .bind("default")
        .fetch_one(&pool)
        .await
        .expect("Failed to count secrets");

    let count: i64 = rows.get(0);
    assert_eq!(count, 3);

    // Query secrets in other namespace
    let rows = sqlx::query("SELECT COUNT(*) as count FROM secrets WHERE namespace = ?")
        .bind("other-ns")
        .fetch_one(&pool)
        .await
        .expect("Failed to count secrets");

    let count: i64 = rows.get(0);
    assert_eq!(count, 1);
}

#[tokio::test]
async fn test_delete_secret() {
    let pool = setup_test_db().await;
    let now = Utc::now().timestamp();

    // Create a secret
    let id = Uuid::new_v4().to_string();
    sqlx::query(
        r#"
        INSERT INTO secrets (id, name, namespace, data, created_at, updated_at, version)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(&id)
    .bind("test-secret")
    .bind("default")
    .bind(r#"{"key":"value"}"#)
    .bind(now)
    .bind(now)
    .bind(1)
    .execute(&pool)
    .await
    .expect("Failed to insert secret");

    // Verify it exists
    let row = sqlx::query("SELECT COUNT(*) as count FROM secrets WHERE name = ?")
        .bind("test-secret")
        .fetch_one(&pool)
        .await
        .expect("Failed to count secrets");

    let count: i64 = row.get(0);
    assert_eq!(count, 1);

    // Delete it
    let result = sqlx::query("DELETE FROM secrets WHERE name = ?")
        .bind("test-secret")
        .execute(&pool)
        .await
        .expect("Failed to delete secret");

    assert_eq!(result.rows_affected(), 1);

    // Verify it's gone
    let row = sqlx::query("SELECT COUNT(*) as count FROM secrets WHERE name = ?")
        .bind("test-secret")
        .fetch_one(&pool)
        .await
        .expect("Failed to count secrets");

    let count: i64 = row.get(0);
    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_cronjob_crud() {
    let pool = setup_test_db().await;
    let now = Utc::now().timestamp();

    // Create a CronJob
    let id = Uuid::new_v4().to_string();
    let spec = r#"{"schedule":"0 2 * * *","jobTemplate":{}}"#;

    sqlx::query(
        r#"
        INSERT INTO cronjobs (id, name, namespace, spec, created_at, updated_at, schedule, version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(&id)
    .bind("test-cronjob")
    .bind("default")
    .bind(spec)
    .bind(now)
    .bind(now)
    .bind("0 2 * * *")
    .bind(1)
    .execute(&pool)
    .await
    .expect("Failed to insert cronjob");

    // Verify it exists
    let row = sqlx::query("SELECT name, namespace FROM cronjobs WHERE id = ?")
        .bind(&id)
        .fetch_one(&pool)
        .await
        .expect("Failed to fetch cronjob");

    let name: String = row.get(0);
    let namespace: String = row.get(1);

    assert_eq!(name, "test-cronjob");
    assert_eq!(namespace, "default");
}

#[tokio::test]
async fn test_job_status_tracking() {
    let pool = setup_test_db().await;
    let now = Utc::now().timestamp();

    // Create a job
    let id = Uuid::new_v4().to_string();
    let spec = r#"{"image":"test","command":["echo","hello"]}"#;

    sqlx::query(
        r#"
        INSERT INTO jobs (id, name, namespace, spec, status, created_at, updated_at, version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(&id)
    .bind("test-job")
    .bind("default")
    .bind(spec)
    .bind("Created")
    .bind(now)
    .bind(now)
    .bind(1)
    .execute(&pool)
    .await
    .expect("Failed to insert job");

    // Verify initial status
    let row = sqlx::query("SELECT status FROM jobs WHERE id = ?")
        .bind(&id)
        .fetch_one(&pool)
        .await
        .expect("Failed to fetch job");

    let status: String = row.get(0);
    assert_eq!(status, "Created");

    // Update status to simulate reconciliation
    sqlx::query("UPDATE jobs SET status = ?, version = version + 1 WHERE id = ?")
        .bind("VolumePending")
        .bind(&id)
        .execute(&pool)
        .await
        .expect("Failed to update job");

    // Verify status changed
    let row = sqlx::query("SELECT status FROM jobs WHERE id = ?")
        .bind(&id)
        .fetch_one(&pool)
        .await
        .expect("Failed to fetch job");

    let status: String = row.get(0);
    assert_eq!(status, "VolumePending");
}

#[tokio::test]
async fn test_event_emission() {
    let pool = setup_test_db().await;
    let now = Utc::now().timestamp();

    let job_id = Uuid::new_v4().to_string();
    let event_id = Uuid::new_v4().to_string();

    // Emit an event
    sqlx::query(
        r#"
        INSERT INTO events (id, job_id, reason, message, timestamp, type)
        VALUES (?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(&event_id)
    .bind(&job_id)
    .bind("VolumeCreated")
    .bind("Volume kube-shim-vol-test created successfully")
    .bind(now)
    .bind("Normal")
    .execute(&pool)
    .await
    .expect("Failed to insert event");

    // Verify event
    let row = sqlx::query("SELECT reason, message FROM events WHERE job_id = ?")
        .bind(&job_id)
        .fetch_one(&pool)
        .await
        .expect("Failed to fetch event");

    let reason: String = row.get(0);
    let message: String = row.get(1);

    assert_eq!(reason, "VolumeCreated");
    assert!(message.contains("Volume"));
}
