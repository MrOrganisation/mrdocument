use base64::Engine;
use reqwest::multipart;
use tracing::{debug, error, info, warn};

#[derive(Debug)]
#[allow(dead_code)]
pub struct OcrResult {
    pub pdf_bytes: Vec<u8>,
    pub text: String,
    pub filename: String,
    pub signature_invalidated: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum OcrError {
    #[error("OCR failed: {0}")]
    Failed(String),
    #[error("OCR request error: {0}")]
    Request(#[from] reqwest::Error),
}

pub struct OcrClient {
    base_url: String,
    client: reqwest::Client,
}

impl OcrClient {
    pub fn new(base_url: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
        }
    }

    pub async fn process_pdf(
        &self,
        pdf_bytes: &[u8],
        filename: &str,
        language: &str,
    ) -> Result<OcrResult, OcrError> {
        let url = format!("{}/ocr", self.base_url);

        // Truncate filename to avoid filesystem limits
        let mut fname = filename.to_string();
        if fname.len() > 104 {
            let dot_pos = fname.rfind('.').unwrap_or(fname.len());
            let ext = &fname[dot_pos..];
            fname = format!("{}{}", &fname[..100], ext);
            debug!("Truncated filename to: {}", fname);
        }

        debug!("Sending OCR request to {} for file {}", url, fname);

        let file_part = multipart::Part::bytes(pdf_bytes.to_vec())
            .file_name(fname.clone())
            .mime_str("application/pdf")
            .map_err(|e| OcrError::Failed(e.to_string()))?;

        let form = multipart::Form::new()
            .part("file", file_part)
            .text("language", language.to_string())
            .text("skip_text", "true")
            .text("deskew", "true")
            .text("clean", "true")
            .text("return_text", "true");

        let response = self.client.post(&url).multipart(form).send().await?;

        if response.status() != reqwest::StatusCode::OK {
            let status = response.status();
            match response.json::<serde_json::Value>().await {
                Ok(data) => {
                    let error_msg = data
                        .get("error")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Unknown error");
                    let details = data
                        .get("details")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    error!("OCR service returned error: {} - {}", error_msg, details);
                    return Err(OcrError::Failed(format!(
                        "OCR failed: {}. {}",
                        error_msg, details
                    )));
                }
                Err(_) => {
                    return Err(OcrError::Failed(format!(
                        "OCR failed with status {}",
                        status
                    )));
                }
            }
        }

        let data: serde_json::Value = response.json().await?;

        let pdf_base64 = data
            .get("pdf")
            .and_then(|v| v.as_str())
            .ok_or_else(|| OcrError::Failed("OCR response missing PDF data".to_string()))?;

        let text = data
            .get("text")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let signature_invalidated = data
            .get("signature_invalidated")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let result_filename = data
            .get("filename")
            .and_then(|v| v.as_str())
            .unwrap_or(&fname)
            .to_string();

        let pdf_bytes = base64::engine::general_purpose::STANDARD
            .decode(pdf_base64)
            .map_err(|e| OcrError::Failed(format!("Failed to decode PDF base64: {}", e)))?;

        info!(
            "OCR completed for {}: {} bytes PDF, {} chars text, signature_invalidated={}",
            fname,
            pdf_bytes.len(),
            text.len(),
            signature_invalidated
        );

        Ok(OcrResult {
            pdf_bytes,
            text,
            filename: result_filename,
            signature_invalidated,
        })
    }

    pub async fn health_check(&self) -> bool {
        let url = format!("{}/health", self.base_url);
        match self.client.get(&url).send().await {
            Ok(resp) => {
                let healthy = resp.status() == reqwest::StatusCode::OK;
                if !healthy {
                    warn!("OCR health check failed: status {}", resp.status());
                }
                healthy
            }
            Err(e) => {
                warn!("OCR health check failed: {}", e);
                false
            }
        }
    }
}
