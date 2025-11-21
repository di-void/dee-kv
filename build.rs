fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_client(false)
        // .out_dir("out/proto")
        .compile_protos(&["proto/hello.proto"], &["proto"])?;
    Ok(())
}
