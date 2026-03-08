use std::process::Command;
use tracing::{debug, info};

use crate::html_convert::html_to_pdf;

/// Convert RTF bytes to PDF via unrtf (RTF -> HTML) then HTML -> PDF.
pub fn rtf_to_pdf(rtf_bytes: &[u8], filename: &str) -> Result<Vec<u8>, String> {
    debug!(
        "Converting RTF to HTML: {} ({} bytes)",
        filename,
        rtf_bytes.len()
    );

    let output = Command::new("unrtf")
        .arg("--html")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                "unrtf is not installed".to_string()
            } else {
                format!("Failed to start unrtf: {}", e)
            }
        })
        .and_then(|mut child| {
            use std::io::Write;
            if let Some(ref mut stdin) = child.stdin {
                stdin
                    .write_all(rtf_bytes)
                    .map_err(|e| format!("Failed to write to unrtf stdin: {}", e))?;
            }
            drop(child.stdin.take());
            child
                .wait_with_output()
                .map_err(|e| format!("unrtf failed: {}", e))
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("unrtf failed for {}: {}", filename, stderr.trim()));
    }

    let html_bytes = output.stdout;
    if html_bytes.is_empty() {
        return Err(format!("unrtf produced no output for {}", filename));
    }

    info!(
        "Converted RTF to HTML: {} ({} -> {} bytes)",
        filename,
        rtf_bytes.len(),
        html_bytes.len()
    );

    html_to_pdf(&html_bytes, filename)
}
