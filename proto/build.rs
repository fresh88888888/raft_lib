
use protobuf_build::Builder;

fn main(){
    let base: String = std::env::var("CARGO_MAINFEST_DIR").unwrap_or_else(|_| ".".to_string());
    Builder::new()
        .search_dir_for_protos(&format!("{}/proto", base))
        .includes(&[format!("{}/include", base),format!("{}/proto", base)])
        .include_google_protos()
        .generate()
}