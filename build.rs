// Compiles the vendored Kubernetes .proto files (proto/, refreshed by
// tools/k8s-proto-gen) into a descriptor set that src/k8s_protobuf.rs
// embeds, so kube-shim can decode protobuf request bodies (issue #106).
// protox is a pure-Rust protobuf compiler: no `protoc` needed in CI,
// the Containerfile or on developer machines.

use std::env;
use std::fs;
use std::path::PathBuf;

const FILES: &[&str] = &[
    "k8s.io/api/core/v1/generated.proto",
    "k8s.io/api/batch/v1/generated.proto",
    "k8s.io/apimachinery/pkg/runtime/generated.proto",
];

fn main() {
    println!("cargo:rerun-if-changed=proto");
    let descriptors = protox::Compiler::new(["proto"])
        .and_then(|mut compiler| {
            compiler.include_imports(true);
            compiler.open_files(FILES)?;
            Ok(compiler.encode_file_descriptor_set())
        })
        .unwrap_or_else(|e| panic!("compiling proto/: {e}"));
    let out = PathBuf::from(env::var("OUT_DIR").unwrap()).join("k8s-descriptors.bin");
    fs::write(&out, descriptors).unwrap();
}
