fn main() {
    protobuf_codegen::Codegen::new()
        .cargo_out_dir("protos")
        .include("../proto")
        .input("../proto/wasimoff.proto")
        .run_from_script();
}
