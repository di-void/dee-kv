fn main() {
    tonic_prost_build::configure()
        .build_client(false)
        .compile_protos(&["proto/store.proto"], &["proto"])
        .unwrap();
}

// https://github.com/hyperium/tonic/tree/master/tonic-prost-build
