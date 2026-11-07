# Vendored Kubernetes protobuf definitions

kube-shim accepts Kubernetes protobuf request bodies
(`application/vnd.kubernetes.protobuf`, issue #106), which is what
client-go, and so the `hashicorp/kubernetes` OpenTofu provider, sends
for built-in types. `src/k8s_protobuf.rs` converts them to the JSON the
handlers already take.

- `k8s.io/…/generated.proto`: copied unchanged from the `k8s.io/api`
  and `k8s.io/apimachinery` versions in `tools/k8s-proto-gen/go.mod`.
  `build.rs` compiles them with [protox](https://crates.io/crates/protox),
  a protobuf compiler written in Rust, so building kube-shim doesn't
  need `protoc`.
- `go-fields.json`: for each message, how Go's `encoding/json` writes
  each field (JSON name, `omitempty`, pointer, inline). Go writes every
  non-pointer field to protobuf whether it was set or not, so the
  protobuf alone can't tell an unset field from a zero one. This table
  lets the converter produce the same JSON Go would.

Don't edit these files by hand. To move to another Kubernetes version,
bump the versions in `tools/k8s-proto-gen/go.mod`, then run

    (cd tools/k8s-proto-gen && go mod tidy && go run .)

This rewrites this directory and the test fixtures in
`tests/testdata/protobuf/`. Then run `cargo test`.
