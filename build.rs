//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
fn main() {
    // Set up the SDK versioning for error messages and http User-Agents
    let out_dir = std::env::var_os("OUT_DIR").unwrap();
    let version = env!("CARGO_PKG_VERSION");
    let ua = format!(
        "NoSQL-RustSDK/{} (rust{}; {}/{})",
        version,
        rustc_version::version().unwrap(),
        std::env::consts::ARCH,
        std::env::consts::OS
    );
    let code = format!(
        "const USER_AGENT: &str = \"{}\";\nconst SDK_VERSION: &str = \"{}\";\n",
        ua, version
    );
    let dest_path = std::path::Path::new(&out_dir).join("ua.rs");
    std::fs::write(&dest_path, &code).unwrap();
    println!("cargo::rerun-if-changed=build.rs");
}
