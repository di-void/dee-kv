fn main() {
    tonic_prost_build::configure()
        .build_client(false)
        .compile_protos(&["proto/store.proto"], &["proto"])
        .unwrap();

    tonic_prost_build::compile_protos("proto/health.proto").unwrap();
}

// https://docs.rs/tonic-build/0.14.2/tonic_build/
// https://docs.rs/tonic-prost-build/0.14.2/tonic_prost_build/
// https://docs.rs/prost-build/latest/prost_build/
