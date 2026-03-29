//! Directus API client for user provisioning.
//!
//! Creates Directus users that map to discovered mrdocument users via
//! `external_identifier`, which the Directus "MrDocument Read Own" policy
//! uses to filter `documents_v2` rows.

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::{json, Value};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};
use uuid::Uuid;

/// Directus API client that authenticates as admin and provisions users.
pub struct DirectusClient {
    url: String,
    admin_email: String,
    admin_password: String,
    client: Client,
    token: Mutex<Option<String>>,
    role_id: Mutex<Option<String>>,
}

impl DirectusClient {
    /// Create a new client from environment variables.
    ///
    /// Returns `None` if `DIRECTUS_ADMIN_EMAIL` is not set (Directus
    /// integration disabled).
    pub fn from_env() -> Option<Arc<Self>> {
        let admin_email = std::env::var("DIRECTUS_ADMIN_EMAIL").ok()?;
        let admin_password = match std::env::var("DIRECTUS_ADMIN_PASSWORD") {
            Ok(v) => v,
            Err(_) => {
                warn!("DIRECTUS_ADMIN_EMAIL set but DIRECTUS_ADMIN_PASSWORD missing — skipping Directus");
                return None;
            }
        };
        let url = std::env::var("DIRECTUS_URL")
            .unwrap_or_else(|_| "http://directus:8055".into());

        Some(Arc::new(Self {
            url,
            admin_email,
            admin_password,
            client: Client::new(),
            token: Mutex::new(None),
            role_id: Mutex::new(None),
        }))
    }

    /// Authenticate with Directus and cache the access token.
    async fn login(&self) -> Result<String> {
        let resp = self
            .client
            .post(format!("{}/auth/login", self.url))
            .json(&json!({
                "email": self.admin_email,
                "password": self.admin_password,
            }))
            .send()
            .await
            .context("Directus login request failed")?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Directus login failed: {} {}", status, body);
        }

        let data: Value = resp.json().await.context("Directus login parse failed")?;
        let access_token = data["data"]["access_token"]
            .as_str()
            .context("No access_token in login response")?
            .to_string();

        let mut cached = self.token.lock().await;
        *cached = Some(access_token.clone());
        Ok(access_token)
    }

    /// Get a valid token, logging in if necessary.
    async fn get_token(&self) -> Result<String> {
        {
            let cached = self.token.lock().await;
            if let Some(ref t) = *cached {
                return Ok(t.clone());
            }
        }
        self.login().await
    }

    /// Make an authenticated GET request, retrying once on 401.
    async fn api_get(&self, path: &str) -> Result<Value> {
        let token = self.get_token().await?;
        let resp = self
            .client
            .get(format!("{}{}", self.url, path))
            .bearer_auth(&token)
            .send()
            .await
            .with_context(|| format!("GET {path}"))?;

        if resp.status().as_u16() == 401 {
            // Token expired — re-login and retry once
            let token = self.login().await?;
            let resp = self
                .client
                .get(format!("{}{}", self.url, path))
                .bearer_auth(&token)
                .send()
                .await
                .with_context(|| format!("GET {path} (retry)"))?;
            let status = resp.status();
            let body: Value = resp.json().await.unwrap_or_default();
            if !status.is_success() {
                anyhow::bail!("GET {path} → {status}: {body}");
            }
            return Ok(body);
        }

        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!("GET {path} → {status}: {body}");
        }
        Ok(body)
    }

    /// Make an authenticated POST request, retrying once on 401.
    async fn api_post(&self, path: &str, payload: &Value) -> Result<Value> {
        let token = self.get_token().await?;
        let resp = self
            .client
            .post(format!("{}{}", self.url, path))
            .bearer_auth(&token)
            .json(payload)
            .send()
            .await
            .with_context(|| format!("POST {path}"))?;

        if resp.status().as_u16() == 401 {
            let token = self.login().await?;
            let resp = self
                .client
                .post(format!("{}{}", self.url, path))
                .bearer_auth(&token)
                .json(payload)
                .send()
                .await
                .with_context(|| format!("POST {path} (retry)"))?;
            let status = resp.status();
            let body: Value = resp.json().await.unwrap_or_default();
            if !status.is_success() {
                anyhow::bail!("POST {path} → {status}: {body}");
            }
            return Ok(body);
        }

        let status = resp.status();
        let body: Value = resp.json().await.unwrap_or_default();
        if !status.is_success() {
            anyhow::bail!("POST {path} → {status}: {body}");
        }
        Ok(body)
    }

    /// Look up the "MrDocument User" role ID, caching the result.
    async fn get_role_id(&self) -> Result<String> {
        {
            let cached = self.role_id.lock().await;
            if let Some(ref id) = *cached {
                return Ok(id.clone());
            }
        }

        let body = self.api_get("/roles").await?;
        let roles = body["data"]
            .as_array()
            .context("Expected roles array")?;

        let role = roles
            .iter()
            .find(|r| r["name"].as_str() == Some("MrDocument User"))
            .context("Role 'MrDocument User' not found — has directus-init run?")?;

        let id = role["id"]
            .as_str()
            .context("Role has no id")?
            .to_string();

        let mut cached = self.role_id.lock().await;
        *cached = Some(id.clone());
        Ok(id)
    }

    /// Ensure a Directus user exists for the given mrdocument username.
    ///
    /// The user is created with:
    /// - `email`: `{username}@mrdocument.local`
    /// - `role`: "MrDocument User"
    /// - `external_identifier`: the PostgreSQL username (used for RLS)
    /// - `password`: random, written to `{user_root}/.directus-password`
    ///
    /// Idempotent — skips creation if a user with the matching
    /// `external_identifier` already exists.
    pub async fn ensure_user(
        &self,
        username: &str,
        user_root: &Path,
    ) -> Result<()> {
        let role_id = self.get_role_id().await?;

        // Check if user already exists by external_identifier
        let path = format!(
            "/users?filter[external_identifier][_eq]={}",
            username
        );
        let body = self.api_get(&path).await?;
        let users = body["data"].as_array();

        if let Some(users) = users {
            if !users.is_empty() {
                debug!("Directus user for '{}' already exists", username);
                return Ok(());
            }
        }

        // Generate password
        let password = format!(
            "{}{}",
            Uuid::new_v4().to_string().replace('-', ""),
            Uuid::new_v4().to_string().replace('-', ""),
        );

        let email = format!("{}@mrdocument.local", username);

        let payload = json!({
            "email": email,
            "password": password,
            "role": role_id,
            "external_identifier": username,
        });

        self.api_post("/users", &payload).await?;

        // Write password to user root
        let password_file = user_root.join(".directus-password");
        if let Some(parent) = password_file.parent() {
            std::fs::create_dir_all(parent).ok();
        }
        std::fs::write(&password_file, &password)
            .with_context(|| format!("Failed to write Directus password to {:?}", password_file))?;

        info!(
            "Created Directus user '{}' ({}), password written to {:?}",
            username, email, password_file
        );
        Ok(())
    }
}
