fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut prost = tonic_prost_build::Config::new();
    prost.protoc_executable(protoc_bin_vendored::protoc_bin_path()?);
    tonic_prost_build::configure().compile_with_config(
        prost,
        &["proto/election.proto"],
        &["proto"],
    )?;
    println!("cargo:rerun-if-changed=proto/election.proto");
    Ok(())
}
