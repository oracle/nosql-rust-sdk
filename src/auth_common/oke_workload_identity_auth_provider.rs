//
// Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use base64::prelude::{
    Engine as _, BASE64_STANDARD, BASE64_STANDARD_NO_PAD, BASE64_URL_SAFE_NO_PAD,
};
use openssl::bn::BigNum;
use openssl::pkey::{PKey, Private};
use openssl::rsa::Rsa;
use reqwest::{header::HeaderValue, redirect::Policy, Certificate, Client};
use serde_json::{json, Value};
use std::env;
use std::error::Error;
use std::fmt;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use url::{Host, Url};

use super::authentication_provider::AuthenticationProvider;

const DEFAULT_TOKEN_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/token";
const DEFAULT_CERT_PATH: &str = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt";
const TOKEN_PATH: &str = "/resourcePrincipalSessionTokens";
const METADATA_REGION_URL: &str = "http://169.254.169.254/opc/v2/instance/canonicalRegionName";
const AUTH_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub(crate) enum OkeTokenSource {
    File(PathBuf),
    Token(String),
}

impl Default for OkeTokenSource {
    fn default() -> Self {
        Self::File(DEFAULT_TOKEN_PATH.into())
    }
}

impl OkeTokenSource {
    fn read(&self) -> Result<String, Box<dyn Error>> {
        let token = match self {
            Self::File(path) => std::fs::read_to_string(path)
                .map_err(|_| "Kubernetes service account token file unavailable")?,
            Self::Token(token) => token.clone(),
        };
        let token = token.trim();
        token_expiration(&token_claims(token)?)?;
        Ok(token.to_string())
    }
}

/// OKE credentials and the dedicated, verified Kubernetes token-exchange client.
/// This is kept separate from the caller's NoSQL client and TLS overrides.
#[derive(Clone)]
pub(crate) struct OkeWorkloadIdentityAuthProvider {
    source: OkeTokenSource,
    client: Client,
    token_url: Url,
    region: String,
    credentials: OkeCredentials,
}

#[derive(Clone)]
struct OkeCredentials {
    token: String,
    private_key: Rsa<Private>,
    tenancy: String,
    refresh_at: u64,
}

impl fmt::Debug for OkeWorkloadIdentityAuthProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OkeWorkloadIdentityAuthProvider")
            .field("credentials", &"[redacted]")
            .field("source", &"[redacted]")
            .field("region", &self.region)
            .finish()
    }
}

impl AuthenticationProvider for OkeWorkloadIdentityAuthProvider {
    fn tenancy_id(&self) -> &str {
        &self.credentials.tenancy
    }
    fn user_id(&self) -> &str {
        ""
    }
    fn fingerprint(&self) -> &str {
        ""
    }
    fn private_key(&self) -> Result<Rsa<Private>, Box<dyn Error>> {
        Ok(self.credentials.private_key.clone())
    }
    fn key_id(&self) -> String {
        self.credentials.token.clone()
    }
    fn region_id(&self) -> &str {
        &self.region
    }
    fn should_refresh(&self) -> bool {
        now_in_secs() >= self.credentials.refresh_at
    }
}

impl OkeWorkloadIdentityAuthProvider {
    pub(crate) async fn new(
        source: OkeTokenSource,
        region: Option<&str>,
    ) -> Result<Self, Box<dyn Error>> {
        let host = env::var("KUBERNETES_SERVICE_HOST")
            .map_err(|_| "KUBERNETES_SERVICE_HOST must be set for OKE workload identity")?;
        let token_url = token_url(&host)?;
        let cert_path = env::var_os("OCI_KUBERNETES_SERVICE_ACCOUNT_CERT_PATH")
            .map(PathBuf::from)
            .unwrap_or_else(|| DEFAULT_CERT_PATH.into());
        let pem = std::fs::read(cert_path)
            .map_err(|_| "Kubernetes service account CA certificate file unavailable")?;
        let client = token_client(&pem)?;
        let region = match region {
            Some(region) => region.to_string(),
            None => discover_region().await?,
        };
        Self::with_client(source, client, token_url, region).await
    }

    async fn with_client(
        source: OkeTokenSource,
        client: Client,
        token_url: Url,
        region: String,
    ) -> Result<Self, Box<dyn Error>> {
        let credentials = exchange_token(&source, &client, &token_url).await?;
        Ok(Self {
            source,
            client,
            token_url,
            region,
            credentials,
        })
    }

    pub(crate) async fn refresh(&mut self) -> Result<(), Box<dyn Error>> {
        // Re-read projected service account tokens on each exchange. Publish the
        // new token and its matching signing key together, only after validation.
        self.credentials = exchange_token(&self.source, &self.client, &self.token_url).await?;
        Ok(())
    }
}

fn token_client(pem: &[u8]) -> Result<Client, Box<dyn Error>> {
    let certs = Certificate::from_pem_bundle(pem)?;
    if certs.is_empty() {
        return Err("Kubernetes CA bundle contains no certificates".into());
    }
    let mut builder = Client::builder()
        .timeout(AUTH_TIMEOUT)
        .connect_timeout(AUTH_TIMEOUT)
        .https_only(true)
        .no_proxy()
        .redirect(Policy::none())
        .tls_built_in_root_certs(false);
    for cert in certs {
        builder = builder.add_root_certificate(cert);
    }
    // Keep both certificate-chain and hostname verification enabled.
    Ok(builder.build()?)
}

fn token_url(host: &str) -> Result<Url, Box<dyn Error>> {
    let host = match host.parse::<std::net::IpAddr>() {
        Ok(std::net::IpAddr::V6(ip)) => Host::Ipv6(ip),
        _ => Host::parse(host).map_err(|_| "Invalid KUBERNETES_SERVICE_HOST")?,
    };
    Ok(Url::parse(&format!("https://{host}:12250{TOKEN_PATH}"))?)
}

async fn discover_region() -> Result<String, Box<dyn Error>> {
    match env::var("OCI_REGION_METADATA") {
        Ok(metadata) => region_from_metadata(&metadata),
        Err(env::VarError::NotPresent) => {
            let client = Client::builder()
                .timeout(AUTH_TIMEOUT)
                .no_proxy()
                .redirect(Policy::none())
                .build()?;
            let response = client
                .get(METADATA_REGION_URL)
                .header("Authorization", "Bearer Oracle")
                .send()
                .await?;
            if !response.status().is_success() {
                return Err(
                    format!("OKE region metadata request failed: {}", response.status()).into(),
                );
            }
            let region = response.text().await?;
            if region.trim().is_empty() {
                return Err("OKE region metadata is empty".into());
            }
            Ok(region.trim().to_string())
        }
        Err(_) => Err("Invalid OCI_REGION_METADATA".into()),
    }
}

fn region_from_metadata(metadata: &str) -> Result<String, Box<dyn Error>> {
    let value: Value =
        serde_json::from_str(metadata).map_err(|_| "Invalid JSON in OCI_REGION_METADATA")?;
    value
        .get("regionIdentifier")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.trim().to_string())
        .ok_or_else(|| "OCI_REGION_METADATA must contain regionIdentifier".into())
}

async fn exchange_token(
    source: &OkeTokenSource,
    client: &Client,
    url: &Url,
) -> Result<OkeCredentials, Box<dyn Error>> {
    let service_account_token = source.read()?;
    let private_key = Rsa::generate(2048)?;
    // Java's PublicKey.getEncoded() is DER SubjectPublicKeyInfo, not PKCS#1.
    let pod_key = PKey::from_rsa(private_key.clone())?.public_key_to_der()?;
    let mut authorization = HeaderValue::from_str(&format!("Bearer {service_account_token}"))?;
    authorization.set_sensitive(true);
    let mut request_id = [0u8; 16];
    openssl::rand::rand_bytes(&mut request_id)?;
    let request_id: String = request_id
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let response = client
        .post(url.clone())
        .header("Authorization", authorization)
        .header("opc-request-id", &request_id)
        .json(&json!({"podKey": BASE64_STANDARD.encode(pod_key)}))
        .send()
        .await?;
    if !response.status().is_success() {
        // The response body may contain credentials. Never include it in errors.
        return Err(format!(
            "OKE token exchange failed: {}, opc-request-id {request_id}",
            response.status()
        )
        .into());
    }
    decode_response(&response.bytes().await?, private_key)
}

fn decode_response(
    body: &[u8],
    private_key: Rsa<Private>,
) -> Result<OkeCredentials, Box<dyn Error>> {
    // Proxymux returns a JSON string containing base64(JSON({"token":"ST$..."})).
    let encoded: String = serde_json::from_slice(body)
        .map_err(|_| "Invalid OKE token response: expected a JSON string")?;
    let decoded = BASE64_STANDARD
        .decode(encoded)
        .map_err(|_| "Invalid base64 in OKE token response")?;
    let response: Value = serde_json::from_slice(&decoded)
        .map_err(|_| "Invalid JSON in decoded OKE token response")?;
    let token = response
        .get("token")
        .and_then(Value::as_str)
        .ok_or("OKE token response is missing token")?;
    let token = token.strip_prefix("ST$").unwrap_or(token);
    let claims = token_claims(token)?;
    let expiration = token_expiration(&claims)?;
    let jwk = claims
        .get("jwk")
        .ok_or("OKE session token is missing jwk")?;
    // OCI serializes the JWK as a JSON string inside the JWT claims. Also
    // accept an embedded object for compatibility with other token issuers.
    let decoded_jwk;
    let jwk = match jwk {
        Value::String(encoded) => {
            decoded_jwk = serde_json::from_str::<Value>(encoded)
                .map_err(|_| "Invalid JSON in OKE session token JWK")?;
            &decoded_jwk
        }
        _ => jwk,
    };
    let modulus = jwk_integer(jwk, "n")?;
    let exponent = jwk_integer(jwk, "e")?;
    if modulus.as_ref() != private_key.n() || exponent.as_ref() != private_key.e() {
        return Err("OKE session token public key does not match podKey".into());
    }
    // A workload must specify its target compartment; tenancy is informational.
    let tenancy = claims
        .get("res_tenant")
        .and_then(Value::as_str)
        .unwrap_or("");
    // Accommodate short-lived tokens without refreshing on every request.
    let window = 300.min(expiration.saturating_sub(now_in_secs()) / 2);
    Ok(OkeCredentials {
        token: format!("ST${token}"),
        private_key,
        tenancy: tenancy.to_string(),
        refresh_at: expiration.saturating_sub(window),
    })
}

fn token_claims(token: &str) -> Result<Value, Box<dyn Error>> {
    let mut parts = token.split('.');
    let header = parts.next().filter(|s| !s.is_empty());
    let payload = parts.next().filter(|s| !s.is_empty());
    let signature = parts.next().filter(|s| !s.is_empty());
    if header.is_none() || payload.is_none() || signature.is_none() || parts.next().is_some() {
        return Err("Invalid OKE JWT format".into());
    }
    // Claims are read over the authenticated TLS exchange; JWT signatures are
    // verified by OCI. JWT segments use the URL-safe base64 alphabet.
    let decoded = BASE64_URL_SAFE_NO_PAD
        .decode(payload.unwrap().trim_end_matches('='))
        .map_err(|_| "Invalid OKE JWT payload encoding")?;
    serde_json::from_slice(&decoded).map_err(|_| "Invalid OKE JWT payload JSON".into())
}

fn token_expiration(claims: &Value) -> Result<u64, Box<dyn Error>> {
    let expiration = match claims.get("exp") {
        Some(Value::Number(n)) => n.as_u64(),
        Some(Value::String(s)) => s.parse::<u64>().ok(),
        _ => None,
    }
    .ok_or("OKE token must contain a non-negative integer exp claim")?;
    if expiration <= now_in_secs() {
        return Err("OKE token has expired".into());
    }
    Ok(expiration)
}

fn jwk_integer(jwk: &Value, name: &str) -> Result<BigNum, Box<dyn Error>> {
    let value = jwk
        .get(name)
        .and_then(Value::as_str)
        .ok_or("Invalid OKE session token JWK")?;
    // OCI JWK integers may use standard or URL-safe Base64, with or without
    // padding. JWT segments themselves continue to use URL-safe Base64.
    let value = value.trim_end_matches('=');
    let decoded = BASE64_URL_SAFE_NO_PAD
        .decode(value)
        .or_else(|_| BASE64_STANDARD_NO_PAD.decode(value))
        .map_err(|_| "Invalid OKE session token JWK encoding")?;
    Ok(BigNum::from_slice(&decoded)?)
}

fn now_in_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
#[path = "oke_workload_identity_tests.rs"]
mod tests;
