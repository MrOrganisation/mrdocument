use std::process::Command;

fn main() {
    let hash = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| {
            std::env::var("GIT_COMMIT").unwrap_or_else(|_| "unknown".into())
        });

    println!("cargo:rustc-env=GIT_COMMIT_HASH={}", hash);
}
