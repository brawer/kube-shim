//! Server-side printing: the `Table` responses `kubectl get` asks for
//! (issue #154's follow-up).
//!
//! `kubectl get` sends
//! `Accept: application/json;as=Table;v=v1;g=meta.k8s.io, ..., application/json`
//! and prints whatever columns the server returns. Given a plain object
//! instead, it falls back to printing only NAME and AGE. [`tables`]
//! turns a handler's JSON response into the `meta.k8s.io` `Table` a
//! Kubernetes API server would return for it, with the same columns,
//! cells and row conditions as upstream's printers
//! (`pkg/printers/internalversion/printers.go`) for each kind kube-shim
//! serves: Jobs, CronJobs, Secrets, Pods, Events and Nodes. Like the
//! API server, every cell is filled in, `-o wide` ones (`priority: 1`)
//! included; `kubectl` decides which ones to show.
//!
//! The conversion works on the JSON a handler already returns, so the
//! table always shows exactly what `kubectl get -o yaml` would, and
//! handlers don't need to know about tables at all. Any other response
//! (an error `Status`, a kind without a printer here, a non-JSON body)
//! passes through unchanged, which `kubectl` accepts too: its `Accept`
//! header ends in plain `application/json`.

use crate::k8s_status;
use axum::body::{to_bytes, Body};
use axum::extract::Request;
use axum::http::{header, HeaderValue, Method, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};

/// Large enough for any list kube-shim returns; a bigger body is passed
/// through as it is rather than buffered.
const MAX_BODY_BYTES: usize = 32 * 1024 * 1024;

/// The `meta.k8s.io` version of `Table` a request asked for, if any.
///
/// Like the API server's content negotiation, the first media range in
/// `Accept` that the server can produce wins. A Table can only be had as
/// JSON here (kubectl never asks for one as protobuf).
pub fn requested_table_version(accept: &str) -> Option<&'static str> {
    for range in accept.split(',') {
        let mut parts = range.split(';').map(str::trim);
        let media_type = parts.next().unwrap_or_default().to_ascii_lowercase();
        let (mut as_kind, mut group, mut version) = (None, None, None);
        for param in parts {
            if let Some((key, value)) = param.split_once('=') {
                let value = value.trim().trim_matches('"');
                match key.trim() {
                    "as" => as_kind = Some(value),
                    "g" => group = Some(value),
                    "v" => version = Some(value),
                    _ => {}
                }
            }
        }
        let servable = matches!(
            media_type.as_str(),
            "application/json"
                | "application/*"
                | "*/*"
                | "application/yaml"
                | "application/vnd.kubernetes.protobuf"
        );
        match as_kind {
            None if servable => return None,
            Some("Table") if media_type == "application/json" && group == Some("meta.k8s.io") => {
                match version {
                    Some("v1") => return Some("v1"),
                    Some("v1beta1") => return Some("v1beta1"),
                    _ => {}
                }
            }
            _ => {}
        }
    }
    None
}

/// `?includeObject=`: what each row's `object` holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncludeObject {
    None,
    Metadata,
    Object,
}

fn include_object(query: Option<&str>) -> Result<IncludeObject, String> {
    let value = query
        .unwrap_or_default()
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find(|(key, _)| *key == "includeObject")
        .map(|(_, value)| value);
    match value {
        None | Some("") | Some("Metadata") => Ok(IncludeObject::Metadata),
        Some("None") => Ok(IncludeObject::None),
        Some("Object") => Ok(IncludeObject::Object),
        Some(other) => Err(format!("unrecognized includeObject value: {other:?}")),
    }
}

/// Middleware: answers a `GET` that asked for a `Table` with one.
pub async fn tables(request: Request, next: Next) -> Response {
    if request.method() != Method::GET {
        return next.run(request).await;
    }
    let Some(version) = request
        .headers()
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .and_then(requested_table_version)
    else {
        return next.run(request).await;
    };
    let include = match include_object(request.uri().query()) {
        Ok(include) => include,
        Err(message) => return k8s_status::bad_request(message),
    };

    let response = next.run(request).await;
    let is_json = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.starts_with("application/json"));
    if response.status() != StatusCode::OK || !is_json {
        return response;
    }

    let (mut parts, body) = response.into_parts();
    let bytes = match to_bytes(body, MAX_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(e) => {
            return k8s_status::status_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalError",
                format!("reading the response to convert to a Table: {e}"),
            )
        }
    };
    let table = serde_json::from_slice::<Value>(&bytes)
        .ok()
        .and_then(|object| to_table(&object, version, include, Utc::now()));
    match table {
        Some(table) => {
            let body = table.to_string();
            parts.headers.remove(header::CONTENT_LENGTH);
            parts.headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
            Response::from_parts(parts, Body::from(body))
        }
        None => Response::from_parts(parts, Body::from(bytes)),
    }
}

/// The `Table` for `object` (a single object or a `...List`), or `None`
/// if there's no printer for its kind.
pub fn to_table(
    object: &Value,
    version: &str,
    include: IncludeObject,
    now: DateTime<Utc>,
) -> Option<Value> {
    let kind = object.get("kind")?.as_str()?;
    let (item_kind, items): (&str, Vec<&Value>) = match kind.strip_suffix("List") {
        Some(item_kind) => (item_kind, object.get("items")?.as_array()?.iter().collect()),
        None => (kind, vec![object]),
    };
    let printer = printer_for(item_kind)?;

    let api_version = format!("meta.k8s.io/{version}");
    let rows: Vec<Value> = items
        .into_iter()
        .map(|item| {
            let (cells, conditions) = (printer.print)(item, now);
            let mut row = Map::new();
            row.insert("cells".into(), Value::Array(cells));
            if let Some(conditions) = conditions {
                row.insert("conditions".into(), conditions);
            }
            let object = match include {
                IncludeObject::None => Value::Null,
                IncludeObject::Object => {
                    let mut item = item.clone();
                    if let Some(map) = item.as_object_mut() {
                        // List items may leave these out; a row's
                        // object is a standalone one.
                        if !map.contains_key("kind") {
                            map.insert("kind".into(), item_kind.into());
                        }
                        if !map.contains_key("apiVersion") {
                            if let Some(v) = object.get("apiVersion") {
                                map.insert("apiVersion".into(), v.clone());
                            }
                        }
                    }
                    item
                }
                IncludeObject::Metadata => json!({
                    "kind": "PartialObjectMetadata",
                    "apiVersion": api_version,
                    "metadata": item.get("metadata").cloned().unwrap_or_else(|| json!({})),
                }),
            };
            row.insert("object".into(), object);
            Value::Object(row)
        })
        .collect();

    let mut metadata = Map::new();
    if item_kind != kind {
        for key in ["resourceVersion", "continue", "remainingItemCount"] {
            if let Some(value) = object.pointer(&format!("/metadata/{key}")) {
                metadata.insert(key.into(), value.clone());
            }
        }
    }

    Some(json!({
        "kind": "Table",
        "apiVersion": api_version,
        "metadata": metadata,
        "columnDefinitions": printer.columns.iter().map(Column::to_json).collect::<Vec<_>>(),
        "rows": rows,
    }))
}

struct Column {
    name: &'static str,
    column_type: &'static str,
    format: &'static str,
    description: &'static str,
    priority: i32,
}

impl Column {
    fn to_json(&self) -> Value {
        json!({
            "name": self.name,
            "type": self.column_type,
            "format": self.format,
            "description": self.description,
            "priority": self.priority,
        })
    }
}

const fn col(name: &'static str, column_type: &'static str, description: &'static str) -> Column {
    Column {
        name,
        column_type,
        format: "",
        description,
        priority: 0,
    }
}

const fn wide(name: &'static str, column_type: &'static str, description: &'static str) -> Column {
    Column {
        priority: 1,
        ..col(name, column_type, description)
    }
}

const NAME_DESCRIPTION: &str = "Name must be unique within a namespace. Is required when creating \
     resources, although some resources may allow a client to request the generation of an \
     appropriate name automatically. Name is primarily intended for creation idempotence and \
     configuration definition. Cannot be updated. More info: \
     https://kubernetes.io/docs/concepts/overview/working-with-objects/names#names";
const AGE_DESCRIPTION: &str = "CreationTimestamp is a timestamp representing the server time when \
     this object was created. It is not guaranteed to be set in happens-before order across \
     separate operations. Clients may not set this value. It is represented in RFC3339 form and \
     is in UTC.\n\nPopulated by the system. Read-only. Null for lists. More info: \
     https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata";

const NAME: Column = Column {
    format: "name",
    ..col("Name", "string", NAME_DESCRIPTION)
};
const AGE: Column = col("Age", "string", AGE_DESCRIPTION);
const CONTAINERS: Column = wide(
    "Containers",
    "string",
    "Names of each container in the template.",
);
const IMAGES: Column = wide(
    "Images",
    "string",
    "Images referenced by each container in the template.",
);
const JOB_SELECTOR: Column = wide(
    "Selector",
    "string",
    "A label query over pods that should match the pod count. Normally, the system sets this \
     field for you. More info: \
     https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors",
);

type PrintFn = fn(&Value, DateTime<Utc>) -> (Vec<Value>, Option<Value>);

struct Printer {
    columns: &'static [Column],
    print: PrintFn,
}

fn printer_for(kind: &str) -> Option<&'static Printer> {
    match kind {
        "Job" => Some(&JOB_PRINTER),
        "CronJob" => Some(&CRONJOB_PRINTER),
        "Secret" => Some(&SECRET_PRINTER),
        "Pod" => Some(&POD_PRINTER),
        "Event" => Some(&EVENT_PRINTER),
        "Node" => Some(&NODE_PRINTER),
        _ => None,
    }
}

// --- helpers mirroring upstream's -----------------------------------

fn str_at<'a>(value: &'a Value, pointer: &str) -> &'a str {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .unwrap_or_default()
}

fn int_at(value: &Value, pointer: &str) -> Option<i64> {
    value.pointer(pointer).and_then(Value::as_i64)
}

fn array_at<'a>(value: &'a Value, pointer: &str) -> &'a [Value] {
    value
        .pointer(pointer)
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default()
}

fn time_at(value: &Value, pointer: &str) -> Option<DateTime<Utc>> {
    let text = value.pointer(pointer)?.as_str()?;
    DateTime::parse_from_rfc3339(text)
        .ok()
        .map(|t| t.with_timezone(&Utc))
}

/// Upstream's `translateTimestampSince`.
fn since(timestamp: Option<DateTime<Utc>>, now: DateTime<Utc>) -> String {
    match timestamp {
        None => "<unknown>".to_string(),
        Some(t) => human_duration(now - t),
    }
}

fn age(object: &Value, now: DateTime<Utc>) -> Value {
    since(time_at(object, "/metadata/creationTimestamp"), now).into()
}

/// `k8s.io/apimachinery/pkg/util/duration.HumanDuration`.
pub fn human_duration(d: chrono::Duration) -> String {
    let seconds = d.num_seconds();
    if seconds < -1 {
        return "<invalid>".to_string();
    } else if seconds < 0 {
        return "0s".to_string();
    } else if seconds < 60 * 2 {
        return format!("{seconds}s");
    }
    let minutes = d.num_minutes();
    if minutes < 10 {
        let s = seconds % 60;
        return if s == 0 {
            format!("{minutes}m")
        } else {
            format!("{minutes}m{s}s")
        };
    } else if minutes < 60 * 3 {
        return format!("{minutes}m");
    }
    let hours = d.num_hours();
    if hours < 8 {
        let m = minutes % 60;
        return if m == 0 {
            format!("{hours}h")
        } else {
            format!("{hours}h{m}m")
        };
    } else if hours < 48 {
        return format!("{hours}h");
    } else if hours < 24 * 8 {
        let h = hours % 24;
        return if h == 0 {
            format!("{}d", hours / 24)
        } else {
            format!("{}d{h}h", hours / 24)
        };
    } else if hours < 24 * 365 * 2 {
        return format!("{}d", hours / 24);
    } else if hours < 24 * 365 * 8 {
        let dy = (hours / 24) % 365;
        return if dy == 0 {
            format!("{}y", hours / 24 / 365)
        } else {
            format!("{}y{dy}d", hours / 24 / 365)
        };
    }
    format!("{}y", hours / 24 / 365)
}

/// Upstream's `hasJobCondition`: the first condition of that type
/// decides.
fn has_condition(conditions: &[Value], condition_type: &str) -> bool {
    conditions
        .iter()
        .find(|c| c.get("type").and_then(Value::as_str) == Some(condition_type))
        .is_some_and(|c| c.get("status").and_then(Value::as_str) == Some("True"))
}

/// Upstream's `layoutContainerCells`.
fn container_cells(containers: &[Value]) -> (Value, Value) {
    let names: Vec<&str> = containers.iter().map(|c| str_at(c, "/name")).collect();
    let images: Vec<&str> = containers.iter().map(|c| str_at(c, "/image")).collect();
    (names.join(",").into(), images.join(",").into())
}

/// `metav1.FormatLabelSelector`.
pub fn format_label_selector(selector: Option<&Value>) -> String {
    let Some(selector) = selector.filter(|s| !s.is_null()) else {
        return "<none>".to_string();
    };
    // (key, rendered) pairs; labels.Selector sorts its requirements by
    // key, keeping their order otherwise.
    let mut requirements: Vec<(String, String)> = Vec::new();
    if let Some(match_labels) = selector.get("matchLabels").and_then(Value::as_object) {
        for (key, value) in match_labels {
            let value = value.as_str().unwrap_or_default();
            requirements.push((key.clone(), format!("{key}={value}")));
        }
    }
    for expression in array_at(selector, "/matchExpressions") {
        let key = str_at(expression, "/key");
        let mut values: Vec<&str> = array_at(expression, "/values")
            .iter()
            .filter_map(Value::as_str)
            .collect();
        values.sort_unstable();
        values.dedup();
        let rendered = match str_at(expression, "/operator") {
            "In" => format!("{key} in ({})", values.join(",")),
            "NotIn" => format!("{key} notin ({})", values.join(",")),
            "Exists" => key.to_string(),
            "DoesNotExist" => format!("!{key}"),
            _ => return "<error>".to_string(),
        };
        requirements.push((key.to_string(), rendered));
    }
    requirements.sort_by(|a, b| a.0.cmp(&b.0));
    if requirements.is_empty() {
        return "<none>".to_string();
    }
    requirements
        .into_iter()
        .map(|(_, rendered)| rendered)
        .collect::<Vec<_>>()
        .join(",")
}

// --- Job -------------------------------------------------------------

const JOB_COLUMNS: &[Column] = &[
    NAME,
    col("Status", "string", "Status of the job."),
    col(
        "Completions",
        "string",
        "The number of pods which reached phase Succeeded. The value increases monotonically \
         for a given spec. However, it may decrease in reaction to scale down of elastic indexed \
         jobs.",
    ),
    col("Duration", "string", "Time required to complete the job."),
    AGE,
    CONTAINERS,
    IMAGES,
    JOB_SELECTOR,
];

static JOB_PRINTER: Printer = Printer {
    columns: JOB_COLUMNS,
    print: print_job,
};

fn print_job(job: &Value, now: DateTime<Utc>) -> (Vec<Value>, Option<Value>) {
    let succeeded = int_at(job, "/status/succeeded").unwrap_or(0);
    let completions = match int_at(job, "/spec/completions") {
        Some(completions) => format!("{succeeded}/{completions}"),
        None => match int_at(job, "/spec/parallelism").unwrap_or(0) {
            parallelism if parallelism > 1 => format!("{succeeded}/1 of {parallelism}"),
            _ => format!("{succeeded}/1"),
        },
    };

    let start = time_at(job, "/status/startTime");
    let duration = match (start, time_at(job, "/status/completionTime")) {
        (None, _) => String::new(),
        (Some(start), None) => human_duration(now - start),
        (Some(start), Some(end)) => human_duration(end - start),
    };

    let conditions = array_at(job, "/status/conditions");
    let status = if has_condition(conditions, "Complete") {
        "Complete"
    } else if has_condition(conditions, "Failed") {
        "Failed"
    } else if job.pointer("/metadata/deletionTimestamp").is_some() {
        "Terminating"
    } else if has_condition(conditions, "Suspended") {
        "Suspended"
    } else if has_condition(conditions, "FailureTarget") {
        "FailureTarget"
    } else if has_condition(conditions, "SuccessCriteriaMet") {
        "SuccessCriteriaMet"
    } else {
        "Running"
    };

    let (names, images) = container_cells(array_at(job, "/spec/template/spec/containers"));
    let cells = vec![
        str_at(job, "/metadata/name").into(),
        status.into(),
        completions.into(),
        duration.into(),
        age(job, now),
        names,
        images,
        format_label_selector(job.pointer("/spec/selector")).into(),
    ];
    (cells, None)
}

// --- CronJob ---------------------------------------------------------

const CRONJOB_COLUMNS: &[Column] = &[
    NAME,
    col(
        "Schedule",
        "string",
        "The schedule in Cron format, see https://en.wikipedia.org/wiki/Cron.",
    ),
    col(
        "Timezone",
        "string",
        "The time zone name for the given schedule, see \
         https://en.wikipedia.org/wiki/List_of_tz_database_time_zones. If not specified, this \
         will default to the time zone of the kube-controller-manager process.",
    ),
    col(
        "Suspend",
        "boolean",
        "This flag tells the controller to suspend subsequent executions, it does not apply to \
         already started executions.  Defaults to false.",
    ),
    col(
        "Active",
        "integer",
        "A list of pointers to currently running jobs.",
    ),
    col(
        "Last Schedule",
        "string",
        "Information when was the last time the job was successfully scheduled.",
    ),
    AGE,
    CONTAINERS,
    IMAGES,
    JOB_SELECTOR,
];

static CRONJOB_PRINTER: Printer = Printer {
    columns: CRONJOB_COLUMNS,
    print: print_cronjob,
};

fn print_cronjob(cronjob: &Value, now: DateTime<Utc>) -> (Vec<Value>, Option<Value>) {
    let last_schedule = match time_at(cronjob, "/status/lastScheduleTime") {
        Some(t) => since(Some(t), now),
        None if cronjob
            .pointer("/status/lastScheduleTime")
            .is_some_and(|v| !v.is_null()) =>
        {
            "<unknown>".to_string()
        }
        None => "<none>".to_string(),
    };
    let time_zone = cronjob
        .pointer("/spec/timeZone")
        .and_then(Value::as_str)
        .unwrap_or("<none>");
    // printBoolPtr.
    let suspend = match cronjob.pointer("/spec/suspend").and_then(Value::as_bool) {
        Some(true) => "True",
        Some(false) => "False",
        None => "<unset>",
    };
    let active = array_at(cronjob, "/status/active").len() as i64;
    let (names, images) = container_cells(array_at(
        cronjob,
        "/spec/jobTemplate/spec/template/spec/containers",
    ));
    let cells = vec![
        str_at(cronjob, "/metadata/name").into(),
        str_at(cronjob, "/spec/schedule").into(),
        time_zone.into(),
        suspend.into(),
        active.into(),
        last_schedule.into(),
        age(cronjob, now),
        names,
        images,
        format_label_selector(cronjob.pointer("/spec/jobTemplate/spec/selector")).into(),
    ];
    (cells, None)
}

// --- Secret ----------------------------------------------------------

const SECRET_COLUMNS: &[Column] = &[
    NAME,
    col(
        "Type",
        "string",
        "Used to facilitate programmatic handling of secret data. More info: \
         https://kubernetes.io/docs/concepts/configuration/secret/#secret-types",
    ),
    col(
        "Data",
        "string",
        "Data contains the secret data. Each key must consist of alphanumeric characters, '-', \
         '_' or '.'. The serialized form of the secret data is a base64 encoded string, \
         representing the arbitrary (possibly non-string) data value here. Described in \
         https://tools.ietf.org/html/rfc4648#section-4",
    ),
    AGE,
];

static SECRET_PRINTER: Printer = Printer {
    columns: SECRET_COLUMNS,
    print: print_secret,
};

fn print_secret(secret: &Value, now: DateTime<Utc>) -> (Vec<Value>, Option<Value>) {
    let data = secret
        .get("data")
        .and_then(Value::as_object)
        .map_or(0, Map::len) as i64;
    // The API server defaults an unset type to Opaque.
    let secret_type = match str_at(secret, "/type") {
        "" => "Opaque",
        t => t,
    };
    let cells = vec![
        str_at(secret, "/metadata/name").into(),
        secret_type.into(),
        data.into(),
        age(secret, now),
    ];
    (cells, None)
}

// --- Pod -------------------------------------------------------------

const POD_COLUMNS: &[Column] = &[
    NAME,
    col(
        "Ready",
        "string",
        "The aggregate readiness state of this pod for accepting traffic.",
    ),
    col(
        "Status",
        "string",
        "The aggregate status of the containers in this pod.",
    ),
    col(
        "Restarts",
        "string",
        "The number of times the containers in this pod have been restarted and when the last \
         container in this pod has restarted.",
    ),
    AGE,
    wide(
        "IP",
        "string",
        "podIP address allocated to the pod. Routable at least within the cluster. Empty if not \
         yet allocated.",
    ),
    wide(
        "Node",
        "string",
        "NodeName indicates in which node this pod is scheduled. If empty, this pod is a \
         candidate for preemption by the scheduler.",
    ),
    wide(
        "Nominated Node",
        "string",
        "nominatedNodeName is set only when this pod preempts other pods on the node, but it \
         cannot be scheduled right away as preemption victims receive their graceful \
         termination periods.",
    ),
    wide(
        "Readiness Gates",
        "string",
        "If specified, all readiness gates will be evaluated for pod readiness.",
    ),
];

static POD_PRINTER: Printer = Printer {
    columns: POD_COLUMNS,
    print: print_pod,
};

fn is_restartable_init_container(container: Option<&Value>) -> bool {
    container.is_some_and(|c| str_at(c, "/restartPolicy") == "Always")
}

fn condition_true(conditions: &[Value], condition_type: &str) -> bool {
    conditions
        .iter()
        .any(|c| str_at(c, "/type") == condition_type && str_at(c, "/status") == "True")
}

/// A finished container's last termination time, if later than `latest`.
fn later_termination(container: &Value, latest: &mut Option<DateTime<Utc>>) {
    if let Some(finished) = time_at(container, "/lastState/terminated/finishedAt") {
        if latest.is_none_or(|l| l < finished) {
            *latest = Some(finished);
        }
    }
}

/// Upstream's `printPod`.
fn print_pod(pod: &Value, now: DateTime<Utc>) -> (Vec<Value>, Option<Value>) {
    let containers = array_at(pod, "/spec/containers");
    let init_containers = array_at(pod, "/spec/initContainers");
    let status_conditions = array_at(pod, "/status/conditions");
    let mut total_containers = containers.len();
    let mut ready_containers = 0;
    let mut restarts = 0;
    let mut restartable_init_restarts = 0;
    let mut last_restart: Option<DateTime<Utc>> = None;
    let mut last_restartable_init_restart: Option<DateTime<Utc>> = None;

    let phase = str_at(pod, "/status/phase");
    let mut reason = match str_at(pod, "/status/reason") {
        "" => phase.to_string(),
        r => r.to_string(),
    };
    for condition in status_conditions {
        if str_at(condition, "/type") == "PodScheduled"
            && str_at(condition, "/reason") == "SchedulingGated"
        {
            reason = "SchedulingGated".to_string();
        }
    }

    let row_conditions = match phase {
        "Succeeded" => Some(json!([{
            "type": "Completed",
            "status": "True",
            "reason": "Succeeded",
            "message": "The pod has completed successfully.",
        }])),
        "Failed" => Some(json!([{
            "type": "Completed",
            "status": "True",
            "reason": "Failed",
            "message": "The pod failed.",
        }])),
        _ => None,
    };

    let init_container = |name: &str| init_containers.iter().find(|c| str_at(c, "/name") == name);
    total_containers += init_containers
        .iter()
        .filter(|c| is_restartable_init_container(Some(c)))
        .count();

    let mut initializing = false;
    let init_statuses = array_at(pod, "/status/initContainerStatuses");
    for (i, container) in init_statuses.iter().enumerate() {
        restarts += int_at(container, "/restartCount").unwrap_or(0);
        later_termination(container, &mut last_restart);
        let spec = init_container(str_at(container, "/name"));
        if is_restartable_init_container(spec) {
            restartable_init_restarts += int_at(container, "/restartCount").unwrap_or(0);
            later_termination(container, &mut last_restartable_init_restart);
        }
        let terminated = container
            .pointer("/state/terminated")
            .filter(|t| t.is_object());
        let waiting_reason = str_at(container, "/state/waiting/reason");
        if terminated.is_some_and(|t| int_at(t, "/exitCode").unwrap_or(0) == 0) {
            continue;
        }
        if is_restartable_init_container(spec)
            && container.get("started").and_then(Value::as_bool) == Some(true)
        {
            if container.get("ready").and_then(Value::as_bool) == Some(true) {
                ready_containers += 1;
            }
            continue;
        }
        if let Some(terminated) = terminated {
            reason = match str_at(terminated, "/reason") {
                "" => match int_at(terminated, "/signal").unwrap_or(0) {
                    0 => format!(
                        "Init:ExitCode:{}",
                        int_at(terminated, "/exitCode").unwrap_or(0)
                    ),
                    signal => format!("Init:Signal:{signal}"),
                },
                r => format!("Init:{r}"),
            };
        } else if !waiting_reason.is_empty() && waiting_reason != "PodInitializing" {
            reason = format!("Init:{waiting_reason}");
        } else {
            reason = format!("Init:{i}/{}", init_containers.len());
        }
        initializing = true;
        break;
    }

    if !initializing || condition_true(status_conditions, "Initialized") {
        restarts = restartable_init_restarts;
        last_restart = last_restartable_init_restart;
        let mut has_running = false;
        for container in array_at(pod, "/status/containerStatuses").iter().rev() {
            restarts += int_at(container, "/restartCount").unwrap_or(0);
            later_termination(container, &mut last_restart);
            let waiting_reason = str_at(container, "/state/waiting/reason");
            let terminated = container
                .pointer("/state/terminated")
                .filter(|t| t.is_object());
            if !waiting_reason.is_empty() {
                reason = waiting_reason.to_string();
            } else if let Some(terminated) = terminated {
                reason = match str_at(terminated, "/reason") {
                    "" => match int_at(terminated, "/signal").unwrap_or(0) {
                        0 => format!("ExitCode:{}", int_at(terminated, "/exitCode").unwrap_or(0)),
                        signal => format!("Signal:{signal}"),
                    },
                    r => r.to_string(),
                };
            } else if container.get("ready").and_then(Value::as_bool) == Some(true)
                && container
                    .pointer("/state/running")
                    .is_some_and(Value::is_object)
            {
                has_running = true;
                ready_containers += 1;
            }
        }
        if reason == "Completed" && has_running {
            reason = if condition_true(status_conditions, "Ready") {
                "Running".to_string()
            } else {
                "NotReady".to_string()
            };
        }
    }

    let deleting = pod
        .pointer("/metadata/deletionTimestamp")
        .is_some_and(|v| !v.is_null());
    if deleting && str_at(pod, "/status/reason") == "NodeLost" {
        reason = "Unknown".to_string();
    } else if deleting && phase != "Failed" && phase != "Succeeded" {
        reason = "Terminating".to_string();
    }

    let restarts = match last_restart {
        Some(last) if restarts != 0 => format!("{restarts} ({} ago)", since(Some(last), now)),
        _ => restarts.to_string(),
    };

    let or_none = |s: &str| {
        if s.is_empty() {
            "<none>".to_string()
        } else {
            s.to_string()
        }
    };
    let pod_ip = array_at(pod, "/status/podIPs")
        .first()
        .map(|ip| str_at(ip, "/ip"))
        .unwrap_or_default();
    let readiness_gates = array_at(pod, "/spec/readinessGates");
    let readiness_gates = if readiness_gates.is_empty() {
        "<none>".to_string()
    } else {
        let true_conditions = readiness_gates
            .iter()
            .filter(|gate| {
                let condition_type = str_at(gate, "/conditionType");
                status_conditions
                    .iter()
                    .find(|c| str_at(c, "/type") == condition_type)
                    .is_some_and(|c| str_at(c, "/status") == "True")
            })
            .count();
        format!("{true_conditions}/{}", readiness_gates.len())
    };

    let cells = vec![
        str_at(pod, "/metadata/name").into(),
        format!("{ready_containers}/{total_containers}").into(),
        reason.into(),
        restarts.into(),
        age(pod, now),
        or_none(pod_ip).into(),
        or_none(str_at(pod, "/spec/nodeName")).into(),
        or_none(str_at(pod, "/status/nominatedNodeName")).into(),
        readiness_gates.into(),
    ];
    (cells, row_conditions)
}

// --- Event -----------------------------------------------------------

const EVENT_COLUMNS: &[Column] = &[
    col(
        "Last Seen",
        "string",
        "The time at which the most recent occurrence of this event was recorded.",
    ),
    col(
        "Type",
        "string",
        "Type of this event (Normal, Warning), new types could be added in the future",
    ),
    col(
        "Reason",
        "string",
        "This should be a short, machine understandable string that gives the reason for the \
         transition into the object's current status.",
    ),
    col("Object", "string", "The object that this event is about."),
    wide(
        "Subobject",
        "string",
        "If referring to a piece of an object instead of an entire object, this string should \
         contain a valid JSON/Go field access statement, such as \
         desiredState.manifest.containers[2].",
    ),
    wide(
        "Source",
        "string",
        "The component reporting this event. Should be a short machine understandable string.",
    ),
    col(
        "Message",
        "string",
        "A human-readable description of the status of this operation.",
    ),
    wide(
        "First Seen",
        "string",
        "The time at which the event was first recorded. (Time of server receipt is in TypeMeta.)",
    ),
    wide(
        "Count",
        "string",
        "The number of times this event has occurred.",
    ),
    Column {
        format: "name",
        ..wide("Name", "string", NAME_DESCRIPTION)
    },
];

static EVENT_PRINTER: Printer = Printer {
    columns: EVENT_COLUMNS,
    print: print_event,
};

/// Upstream's `printEvent`, wide form.
fn print_event(event: &Value, now: DateTime<Utc>) -> (Vec<Value>, Option<Value>) {
    let first = match time_at(event, "/firstTimestamp") {
        Some(t) => since(Some(t), now),
        None => since(time_at(event, "/eventTime"), now),
    };
    let mut last = match time_at(event, "/lastTimestamp") {
        Some(t) => since(Some(t), now),
        None => first.clone(),
    };
    let mut count = int_at(event, "/count").unwrap_or(0);
    if event.get("series").is_some_and(Value::is_object) {
        last = since(time_at(event, "/series/lastObservedTime"), now);
        count = int_at(event, "/series/count").unwrap_or(0);
    } else if count == 0 {
        count = 1;
    }

    let kind = str_at(event, "/involvedObject/kind").to_lowercase();
    let target = match str_at(event, "/involvedObject/name") {
        "" => kind,
        name => format!("{kind}/{name}"),
    };

    let first_non_empty =
        |a: &str, b: &str| -> String { if a.is_empty() { b } else { a }.to_string() };
    let component = first_non_empty(
        str_at(event, "/source/component"),
        str_at(event, "/reportingComponent"),
    );
    let instance = first_non_empty(
        str_at(event, "/source/host"),
        str_at(event, "/reportingInstance"),
    );
    let source = if instance.is_empty() {
        component
    } else {
        format!("{component}, {instance}")
    };

    let cells = vec![
        last.into(),
        str_at(event, "/type").into(),
        str_at(event, "/reason").into(),
        target.into(),
        str_at(event, "/involvedObject/fieldPath").into(),
        source.into(),
        str_at(event, "/message").trim().into(),
        first.into(),
        count.into(),
        str_at(event, "/metadata/name").into(),
    ];
    (cells, None)
}

// --- Node ------------------------------------------------------------

const NODE_COLUMNS: &[Column] = &[
    NAME,
    col("Status", "string", "The status of the node"),
    col("Roles", "string", "The roles of the node"),
    AGE,
    col("Version", "string", "Kubelet Version reported by the node."),
    wide(
        "Internal-IP",
        "string",
        "List of addresses reachable to the node.",
    ),
    wide(
        "External-IP",
        "string",
        "List of addresses reachable to the node.",
    ),
    wide(
        "OS-Image",
        "string",
        "OS Image reported by the node from /etc/os-release (e.g. Debian GNU/Linux 7 (wheezy)).",
    ),
    wide(
        "Kernel-Version",
        "string",
        "Kernel Version reported by the node from 'uname -r' (e.g. 3.16.0-0.bpo.4-amd64).",
    ),
    wide(
        "Container-Runtime",
        "string",
        "ContainerRuntime Version reported by the node through runtime remote API (e.g. \
         containerd://1.4.2).",
    ),
];

static NODE_PRINTER: Printer = Printer {
    columns: NODE_COLUMNS,
    print: print_node,
};

/// Upstream's `printNode`.
fn print_node(node: &Value, now: DateTime<Utc>) -> (Vec<Value>, Option<Value>) {
    let conditions = array_at(node, "/status/conditions");
    // Only Ready counts; the last condition of a type wins, as in
    // upstream's conditionMap.
    let mut status: Vec<String> = Vec::new();
    if let Some(ready) = conditions
        .iter()
        .rev()
        .find(|c| str_at(c, "/type") == "Ready")
    {
        status.push(if str_at(ready, "/status") == "True" {
            "Ready".to_string()
        } else {
            "NotReady".to_string()
        });
    }
    if status.is_empty() {
        status.push("Unknown".to_string());
    }
    if node.pointer("/spec/unschedulable").and_then(Value::as_bool) == Some(true) {
        status.push("SchedulingDisabled".to_string());
    }

    let mut roles: Vec<String> = Vec::new();
    if let Some(labels) = node.pointer("/metadata/labels").and_then(Value::as_object) {
        for (key, value) in labels {
            if let Some(role) = key.strip_prefix("node-role.kubernetes.io/") {
                if !role.is_empty() {
                    roles.push(role.to_string());
                }
            } else if key == "kubernetes.io/role" {
                if let Some(value) = value.as_str().filter(|v| !v.is_empty()) {
                    roles.push(value.to_string());
                }
            }
        }
    }
    roles.sort();
    roles.dedup();
    let roles = if roles.is_empty() {
        "<none>".to_string()
    } else {
        roles.join(",")
    };

    let address = |address_type: &str| -> String {
        array_at(node, "/status/addresses")
            .iter()
            .find(|a| str_at(a, "/type") == address_type)
            .map(|a| str_at(a, "/address").to_string())
            .unwrap_or_else(|| "<none>".to_string())
    };
    let or_unknown = |s: &str| if s.is_empty() { "<unknown>" } else { s }.to_string();

    let cells = vec![
        str_at(node, "/metadata/name").into(),
        status.join(",").into(),
        roles.into(),
        age(node, now),
        str_at(node, "/status/nodeInfo/kubeletVersion").into(),
        address("InternalIP").into(),
        address("ExternalIP").into(),
        or_unknown(str_at(node, "/status/nodeInfo/osImage")).into(),
        or_unknown(str_at(node, "/status/nodeInfo/kernelVersion")).into(),
        or_unknown(str_at(node, "/status/nodeInfo/containerRuntimeVersion")).into(),
    ];
    (cells, None)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    const KUBECTL_ACCEPT: &str = "application/json;as=Table;v=v1;g=meta.k8s.io,\
         application/json;as=Table;v=v1beta1;g=meta.k8s.io,application/json";

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 26, 12, 0, 0).unwrap()
    }

    fn cells(table: &Value) -> Vec<Vec<Value>> {
        table["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row["cells"].as_array().unwrap().clone())
            .collect()
    }

    fn column_names(table: &Value) -> Vec<(String, i64)> {
        table["columnDefinitions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| {
                (
                    c["name"].as_str().unwrap().to_string(),
                    c["priority"].as_i64().unwrap(),
                )
            })
            .collect()
    }

    #[test]
    fn test_requested_table_version() {
        assert_eq!(requested_table_version(KUBECTL_ACCEPT), Some("v1"));
        assert_eq!(
            requested_table_version("application/json;as=Table;v=v1beta1;g=meta.k8s.io"),
            Some("v1beta1")
        );
        assert_eq!(requested_table_version("application/json"), None);
        assert_eq!(requested_table_version(""), None);
        // Plain JSON first wins, as in the API server's negotiation.
        assert_eq!(
            requested_table_version(
                "application/json, application/json;as=Table;v=v1;g=meta.k8s.io"
            ),
            None
        );
        // A Table version or group this server doesn't know is skipped.
        assert_eq!(
            requested_table_version(
                "application/json;as=Table;v=v2;g=meta.k8s.io,application/json;as=Table;v=v1;g=meta.k8s.io"
            ),
            Some("v1")
        );
        assert_eq!(
            requested_table_version(
                "application/json;as=PartialObjectMetadataList;v=v1;g=meta.k8s.io,application/json"
            ),
            None
        );
    }

    #[test]
    fn test_include_object() {
        assert_eq!(include_object(None), Ok(IncludeObject::Metadata));
        assert_eq!(
            include_object(Some("limit=500&includeObject=Object")),
            Ok(IncludeObject::Object)
        );
        assert_eq!(
            include_object(Some("includeObject=None")),
            Ok(IncludeObject::None)
        );
        assert!(include_object(Some("includeObject=Everything")).is_err());
    }

    #[test]
    fn test_human_duration() {
        let cases = [
            (-5, "<invalid>"),
            (-1, "0s"),
            (0, "0s"),
            (119, "119s"),
            (120, "2m"),
            (125, "2m5s"),
            (599, "9m59s"),
            (600, "10m"),
            (3 * 3600 - 1, "179m"),
            (3 * 3600, "3h"),
            (3 * 3600 + 60 * 5, "3h5m"),
            (8 * 3600, "8h"),
            (47 * 3600 + 59 * 60, "47h"),
            (48 * 3600, "2d"),
            (49 * 3600, "2d1h"),
            (8 * 24 * 3600, "8d"),
            (2 * 365 * 24 * 3600, "2y"),
            (2 * 365 * 24 * 3600 + 24 * 3600 * 3, "2y3d"),
            (9 * 365 * 24 * 3600, "9y"),
        ];
        for (seconds, expected) in cases {
            assert_eq!(
                human_duration(chrono::Duration::seconds(seconds)),
                expected,
                "{seconds}s"
            );
        }
    }

    #[test]
    fn test_format_label_selector() {
        assert_eq!(format_label_selector(None), "<none>");
        assert_eq!(format_label_selector(Some(&json!({}))), "<none>");
        assert_eq!(
            format_label_selector(Some(&json!({
                "matchLabels": {"b": "2", "a": "1"},
                "matchExpressions": [
                    {"key": "env", "operator": "In", "values": ["prod", "dev"]},
                    {"key": "tier", "operator": "Exists"},
                    {"key": "zz", "operator": "DoesNotExist"},
                    {"key": "c", "operator": "NotIn", "values": ["x"]},
                ]
            }))),
            "a=1,b=2,c notin (x),env in (dev,prod),tier,!zz"
        );
    }

    fn job(name: &str, status: Value) -> Value {
        json!({
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {
                "name": name,
                "namespace": "default",
                "uid": "u-1",
                "creationTimestamp": "2026-11-26T11:00:00Z",
                "labels": {"app": "x"},
            },
            "spec": {
                "selector": {"matchLabels": {"batch.kubernetes.io/controller-uid": "u-1"}},
                "template": {"spec": {"containers": [
                    {"name": "main", "image": "busybox:latest"},
                    {"name": "side", "image": "alpine:3"},
                ]}},
            },
            "status": status,
        })
    }

    #[test]
    fn test_job_list_table() {
        let list = json!({
            "apiVersion": "batch/v1",
            "kind": "JobList",
            "items": [
                job("done", json!({
                    "succeeded": 1,
                    "conditions": [{"type": "Complete", "status": "True"}],
                    "startTime": "2026-11-26T11:00:00Z",
                    "completionTime": "2026-11-26T11:03:20Z",
                })),
                job("running", json!({
                    "active": 1,
                    "succeeded": 0,
                    "conditions": [],
                    "startTime": "2026-11-26T11:55:00Z",
                })),
                job("broke", json!({
                    "failed": 1,
                    "conditions": [{"type": "Failed", "status": "True"}],
                    "startTime": "2026-11-26T11:00:00Z",
                })),
            ],
        });
        let table = to_table(&list, "v1", IncludeObject::Metadata, now()).unwrap();
        assert_eq!(table["kind"], "Table");
        assert_eq!(table["apiVersion"], "meta.k8s.io/v1");
        assert_eq!(
            column_names(&table),
            [
                ("Name", 0),
                ("Status", 0),
                ("Completions", 0),
                ("Duration", 0),
                ("Age", 0),
                ("Containers", 1),
                ("Images", 1),
                ("Selector", 1),
            ]
            .map(|(n, p)| (n.to_string(), p))
        );
        assert_eq!(table["columnDefinitions"][0]["format"], "name");
        let selector = "batch.kubernetes.io/controller-uid=u-1";
        assert_eq!(
            cells(&table),
            vec![
                vec![
                    json!("done"),
                    json!("Complete"),
                    json!("1/1"),
                    json!("3m20s"),
                    json!("60m"),
                    json!("main,side"),
                    json!("busybox:latest,alpine:3"),
                    json!(selector),
                ],
                vec![
                    json!("running"),
                    json!("Running"),
                    json!("0/1"),
                    json!("5m"),
                    json!("60m"),
                    json!("main,side"),
                    json!("busybox:latest,alpine:3"),
                    json!(selector),
                ],
                vec![
                    json!("broke"),
                    json!("Failed"),
                    json!("0/1"),
                    json!("60m"),
                    json!("60m"),
                    json!("main,side"),
                    json!("busybox:latest,alpine:3"),
                    json!(selector),
                ],
            ]
        );
        let object = &table["rows"][0]["object"];
        assert_eq!(object["kind"], "PartialObjectMetadata");
        assert_eq!(object["apiVersion"], "meta.k8s.io/v1");
        assert_eq!(object["metadata"]["labels"]["app"], "x");
        assert!(object.get("spec").is_none());
    }

    #[test]
    fn test_job_completions_and_status_variants() {
        let mut j = job("j", json!({"succeeded": 2}));
        j["spec"]["completions"] = json!(5);
        assert_eq!(print_job(&j, now()).0[2], "2/5");
        j["spec"].as_object_mut().unwrap().remove("completions");
        j["spec"]["parallelism"] = json!(3);
        assert_eq!(print_job(&j, now()).0[2], "2/1 of 3");

        // No startTime: empty duration.
        assert_eq!(print_job(&j, now()).0[3], "");

        j["metadata"]["deletionTimestamp"] = json!("2026-11-26T11:59:00Z");
        assert_eq!(print_job(&j, now()).0[1], "Terminating");
        j["status"]["conditions"] = json!([{"type": "Complete", "status": "False"}]);
        assert_eq!(print_job(&j, now()).0[1], "Terminating");
        j["status"]["conditions"] = json!([{"type": "Failed", "status": "True"}]);
        assert_eq!(print_job(&j, now()).0[1], "Failed");

        j["metadata"]
            .as_object_mut()
            .unwrap()
            .remove("creationTimestamp");
        assert_eq!(print_job(&j, now()).0[4], "<unknown>");
    }

    #[test]
    fn test_single_job_is_one_row_table() {
        let table = to_table(
            &job("solo", json!({})),
            "v1beta1",
            IncludeObject::Object,
            now(),
        )
        .unwrap();
        assert_eq!(table["apiVersion"], "meta.k8s.io/v1beta1");
        assert_eq!(table["rows"].as_array().unwrap().len(), 1);
        assert_eq!(table["rows"][0]["object"]["kind"], "Job");
        assert_eq!(
            table["rows"][0]["object"]["spec"]["template"]["spec"]["containers"][0]["name"],
            "main"
        );
    }

    #[test]
    fn test_include_object_none() {
        let table = to_table(&job("solo", json!({})), "v1", IncludeObject::None, now()).unwrap();
        assert_eq!(table["rows"][0]["object"], Value::Null);
    }

    #[test]
    fn test_cronjob_table() {
        let list = json!({
            "apiVersion": "batch/v1",
            "kind": "CronJobList",
            "items": [
                {
                    "metadata": {"name": "nightly", "creationTimestamp": "2026-11-20T12:00:00Z"},
                    "spec": {
                        "schedule": "0 3 * * *",
                        "timeZone": "Europe/Zurich",
                        "suspend": false,
                        "jobTemplate": {"spec": {"template": {"spec": {"containers": [
                            {"name": "main", "image": "osm:1"}
                        ]}}}},
                    },
                    "status": {
                        "active": [{"kind": "Job", "name": "nightly-1"}],
                        "lastScheduleTime": "2026-11-26T03:00:00Z",
                    },
                },
                {
                    "metadata": {"name": "fresh", "creationTimestamp": "2026-11-26T11:59:30Z"},
                    "spec": {"schedule": "*/5 * * * *"},
                },
            ],
        });
        let table = to_table(&list, "v1", IncludeObject::Metadata, now()).unwrap();
        let names: Vec<String> = column_names(&table).into_iter().map(|c| c.0).collect();
        assert_eq!(
            names,
            [
                "Name",
                "Schedule",
                "Timezone",
                "Suspend",
                "Active",
                "Last Schedule",
                "Age",
                "Containers",
                "Images",
                "Selector"
            ]
        );
        assert_eq!(
            cells(&table),
            vec![
                vec![
                    json!("nightly"),
                    json!("0 3 * * *"),
                    json!("Europe/Zurich"),
                    json!("False"),
                    json!(1),
                    json!("9h"),
                    json!("6d"),
                    json!("main"),
                    json!("osm:1"),
                    json!("<none>"),
                ],
                vec![
                    json!("fresh"),
                    json!("*/5 * * * *"),
                    json!("<none>"),
                    json!("<unset>"),
                    json!(0),
                    json!("<none>"),
                    json!("30s"),
                    json!(""),
                    json!(""),
                    json!("<none>"),
                ],
            ]
        );
    }

    #[test]
    fn test_secret_table() {
        let list = json!({
            "apiVersion": "v1",
            "kind": "SecretList",
            "items": [
                {
                    "metadata": {"name": "creds", "creationTimestamp": "2026-11-26T09:30:00Z"},
                    "data": {"a": "eA==", "b": "eQ=="},
                    "type": "kubernetes.io/basic-auth",
                },
                {"metadata": {"name": "empty"}, "data": {}},
            ],
        });
        let table = to_table(&list, "v1", IncludeObject::Metadata, now()).unwrap();
        assert_eq!(
            cells(&table),
            vec![
                vec![
                    json!("creds"),
                    json!("kubernetes.io/basic-auth"),
                    json!(2),
                    json!("150m")
                ],
                vec![
                    json!("empty"),
                    json!("Opaque"),
                    json!(0),
                    json!("<unknown>")
                ],
            ]
        );
    }

    #[test]
    fn test_pod_table() {
        let pod = json!({
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "p", "creationTimestamp": "2026-11-26T11:58:00Z"},
            "spec": {"containers": [{"name": "main"}], "nodeName": "vm-1"},
            "status": {
                "phase": "Running",
                "podIPs": [{"ip": "10.0.0.5"}],
                "containerStatuses": [{
                    "name": "main",
                    "ready": true,
                    "restartCount": 2,
                    "state": {"running": {}},
                    "lastState": {"terminated": {"finishedAt": "2026-11-26T11:59:00Z"}},
                }],
            },
        });
        let table = to_table(&pod, "v1", IncludeObject::Metadata, now()).unwrap();
        assert_eq!(
            cells(&table),
            vec![vec![
                json!("p"),
                json!("1/1"),
                json!("Running"),
                json!("2 (60s ago)"),
                json!("2m"),
                json!("10.0.0.5"),
                json!("vm-1"),
                json!("<none>"),
                json!("<none>"),
            ]]
        );
        assert!(table["rows"][0].get("conditions").is_none());

        // kube-shim's own minimal Pod: just a phase.
        let pod = json!({
            "kind": "Pod",
            "metadata": {"name": "q", "creationTimestamp": "2026-11-26T11:00:00Z"},
            "status": {"phase": "Succeeded"},
        });
        let table = to_table(&pod, "v1", IncludeObject::Metadata, now()).unwrap();
        assert_eq!(table["rows"][0]["cells"][1], "0/0");
        assert_eq!(table["rows"][0]["cells"][2], "Succeeded");
        assert_eq!(table["rows"][0]["cells"][3], "0");
        assert_eq!(table["rows"][0]["conditions"][0]["type"], "Completed");
        assert_eq!(table["rows"][0]["conditions"][0]["reason"], "Succeeded");
    }

    #[test]
    fn test_pod_status_reasons() {
        let mut pod = json!({
            "metadata": {"name": "p"},
            "spec": {"containers": [{"name": "main"}]},
            "status": {"phase": "Pending", "containerStatuses": [{
                "name": "main",
                "state": {"waiting": {"reason": "ContainerCreating"}},
            }]},
        });
        assert_eq!(print_pod(&pod, now()).0[2], "ContainerCreating");
        pod["status"]["containerStatuses"][0]["state"] = json!({"terminated": {"exitCode": 3}});
        assert_eq!(print_pod(&pod, now()).0[2], "ExitCode:3");
        pod["status"]["containerStatuses"][0]["state"] =
            json!({"terminated": {"exitCode": 0, "reason": "Completed"}});
        assert_eq!(print_pod(&pod, now()).0[2], "Completed");
        pod["metadata"]["deletionTimestamp"] = json!("2026-11-26T11:00:00Z");
        assert_eq!(print_pod(&pod, now()).0[2], "Terminating");

        let init = json!({
            "metadata": {"name": "p"},
            "spec": {"containers": [{"name": "main"}], "initContainers": [{"name": "a"}, {"name": "b"}]},
            "status": {"phase": "Pending", "initContainerStatuses": [
                {"name": "a", "state": {"terminated": {"exitCode": 0}}},
                {"name": "b", "state": {"running": {}}},
            ]},
        });
        assert_eq!(print_pod(&init, now()).0[2], "Init:1/2");
    }

    #[test]
    fn test_event_table() {
        let list = json!({
            "apiVersion": "v1",
            "kind": "EventList",
            "items": [{
                "metadata": {"name": "e1"},
                "involvedObject": {"kind": "Pod", "name": "p"},
                "reason": "Scheduled",
                "message": " VM created \n",
                "type": "Normal",
                "firstTimestamp": "2026-11-26T11:50:00Z",
                "lastTimestamp": "2026-11-26T11:59:00Z",
                "count": 0,
                "source": {"component": "kube-shim"},
            }],
        });
        let table = to_table(&list, "v1", IncludeObject::Metadata, now()).unwrap();
        let names: Vec<(String, i64)> = column_names(&table);
        assert_eq!(names[0], ("Last Seen".to_string(), 0));
        assert_eq!(names[9], ("Name".to_string(), 1));
        assert_eq!(
            cells(&table),
            vec![vec![
                json!("60s"),
                json!("Normal"),
                json!("Scheduled"),
                json!("pod/p"),
                json!(""),
                json!("kube-shim"),
                json!("VM created"),
                json!("10m"),
                json!(1),
                json!("e1"),
            ]]
        );
    }

    #[test]
    fn test_node_table() {
        let list = json!({
            "apiVersion": "v1",
            "kind": "NodeList",
            "items": [{
                "metadata": {"name": "vm-1", "labels": {"node-role.kubernetes.io/worker": ""}},
                "spec": {"unschedulable": true},
                "status": {
                    "conditions": [{"type": "Ready", "status": "False"}],
                    "addresses": [{"type": "ExternalIP", "address": "203.0.113.7"}],
                },
            }],
        });
        let table = to_table(&list, "v1", IncludeObject::Metadata, now()).unwrap();
        assert_eq!(
            cells(&table),
            vec![vec![
                json!("vm-1"),
                json!("NotReady,SchedulingDisabled"),
                json!("worker"),
                json!("<unknown>"),
                json!(""),
                json!("<none>"),
                json!("203.0.113.7"),
                json!("<unknown>"),
                json!("<unknown>"),
                json!("<unknown>"),
            ]]
        );
    }

    #[test]
    fn test_unknown_kind_is_not_converted() {
        assert!(to_table(
            &json!({"kind": "NodeMetricsList", "items": []}),
            "v1",
            IncludeObject::Metadata,
            now()
        )
        .is_none());
        assert!(to_table(
            &json!({"kind": "Status"}),
            "v1",
            IncludeObject::Metadata,
            now()
        )
        .is_none());
        assert!(to_table(&json!([1, 2]), "v1", IncludeObject::Metadata, now()).is_none());
    }

    #[test]
    fn test_list_metadata_carried_over() {
        let list = json!({
            "kind": "SecretList",
            "metadata": {"resourceVersion": "42", "continue": "abc"},
            "items": [],
        });
        let table = to_table(&list, "v1", IncludeObject::Metadata, now()).unwrap();
        assert_eq!(
            table["metadata"],
            json!({"resourceVersion": "42", "continue": "abc"})
        );
        assert_eq!(table["rows"], json!([]));
    }
}
