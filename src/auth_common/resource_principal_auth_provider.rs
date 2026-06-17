//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use base64ct::{Base64Unpadded, Encoding};
use openssl::pkey::Private;
use openssl::rsa::Rsa;
use serde_json::Value;
use std::env;
use std::error::Error;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::trace;

use crate::auth_common::authentication_provider::AuthenticationProvider;

static EMPTY_STRING: &str = "";

// supported version for resource principal
static RP_VERSION_2_2: &str = "2.2";

// environment variable that specifies a resource principal version
static RP_VERSION_ENV: &str = "OCI_RESOURCE_PRINCIPAL_VERSION";

// environment variable that specifies a security token or a path to the token file
static RP_RPST_ENV: &str = "OCI_RESOURCE_PRINCIPAL_RPST";

// environment variable that specifies an RSA private key in pem format or a path to the key file
static RP_PRIVATE_PEM_ENV: &str = "OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM";

// environment variable that specifies the passphrase to a key or a path to the passphrase file
static RP_PRIVATE_PEM_PASSPHRASE_ENV: &str = "OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE";

// environment variable that specifies a region
static RP_REGION_ENV: &str = "OCI_RESOURCE_PRINCIPAL_REGION";

// the key used to look up the resource tenancy in an RPST
static TENANCY_CLAIM_KEY: &str = "res_tenant";

// Refresh resource principal material shortly before RPST expiry.
static RP_REFRESH_WINDOW_SECS: u64 = 300;

#[derive(Clone)]
pub struct ResourcePrincipalAuthProvider {
    token: String,
    session_private_key: Rsa<Private>,
    tenancy_id: String,
    region: String,
    expiration_secs: u64,
}

impl fmt::Debug for ResourcePrincipalAuthProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResourcePrincipalAuthProvider")
            .field("token", &"[redacted]")
            .field("session_private_key", &"[redacted]")
            .field("tenancy_id", &self.tenancy_id)
            .field("region", &self.region)
            .finish()
    }
}

impl AuthenticationProvider for ResourcePrincipalAuthProvider {
    fn tenancy_id(&self) -> &str {
        &self.tenancy_id
    }
    fn fingerprint(&self) -> &str {
        EMPTY_STRING
    }
    fn user_id(&self) -> &str {
        EMPTY_STRING
    }
    fn private_key(&self) -> Result<Rsa<Private>, Box<dyn Error>> {
        // TODO: optimize away this clone
        Ok(self.session_private_key.clone())
    }
    fn region_id(&self) -> &str {
        &self.region
    }
    fn key_id(&self) -> String {
        self.token.clone()
    }
    fn should_refresh(&self) -> bool {
        self.expires_within_secs(RP_REFRESH_WINDOW_SECS)
    }
}

fn get_env(var: &str) -> Result<String, Box<dyn Error>> {
    let v = match env::var(var) {
        Ok(val) => val,
        Err(e) => {
            return Err(format!(
                "error reading environment variable '{}': {}",
                var,
                e.to_string()
            )
            .into())
        }
    };
    Ok(v)
}

impl ResourcePrincipalAuthProvider {
    pub fn new() -> Result<ResourcePrincipalAuthProvider, Box<dyn Error>> {
        let rp_version = get_env(RP_VERSION_ENV)?;
        if rp_version != RP_VERSION_2_2 {
            return Err(format!(
                "resource principal version '{}' incorrect: expected {}",
                rp_version, RP_VERSION_2_2
            )
            .as_str()
            .into());
        }

        Self::new_from_values(
            get_env(RP_RPST_ENV)?,
            get_env(RP_PRIVATE_PEM_ENV)?,
            get_env(RP_PRIVATE_PEM_PASSPHRASE_ENV).ok(),
            get_env(RP_REGION_ENV)?,
        )
    }

    pub fn new_from_values(
        rpst: String,
        private_pem: String,
        passphrase: Option<String>,
        region: String,
    ) -> Result<ResourcePrincipalAuthProvider, Box<dyn Error>> {
        // Check the the passphrase and the key are both paths or are both strings
        match &passphrase {
            Some(p) => {
                if is_path(p) != is_path(&private_pem) {
                    return Err(
                        "passphrase and private key must be either both full paths or both values"
                            .into(),
                    );
                }
            }
            None => {}
        }

        // TODO check region is non-empty?

        let session_private_key = {
            if is_path(&private_pem) {
                // load info from path(s) given
                let byte_vec = std::fs::read(&private_pem)?;
                match &passphrase {
                    Some(p) => {
                        let pass_vec = std::fs::read(p)?;
                        Rsa::private_key_from_pem_passphrase(&byte_vec, &pass_vec)?
                    }
                    None => Rsa::private_key_from_pem(&byte_vec)?,
                }
            } else {
                // info given directly
                match passphrase {
                    Some(p) => {
                        Rsa::private_key_from_pem_passphrase(private_pem.as_bytes(), p.as_bytes())?
                    }
                    None => Rsa::private_key_from_pem(private_pem.as_bytes())?,
                }
            }
        };

        // decode token string: if file, read that
        let token = {
            if is_path(&rpst) {
                let byte_vec = std::fs::read(&rpst)?;
                String::from_utf8(byte_vec)?
                    .lines()
                    .next()
                    .ok_or("invalid data in RPST token file")?
                    .to_string()
            } else {
                rpst
            }
        };

        // Note: in Resource Principal, the tenancy is extracted from the given RPST token.
        // In Instance Principal, the tenancy is extracted from the leaf certificate.

        // decode token, get "res_tenant" for tenancyOCID and "exp" for expiration
        // token is a three-part string, dot-separated:
        // header.payload.<something?>
        // the fields we want are in the payload, which is base64-encoded JSON (how fun!)
        let mut parts = token.split('.');
        // header: skip for now
        if parts.next().is_none() {
            return Err("invalid RPST token: missing header".into());
        }
        let payload = match parts.next() {
            Some(p) => p,
            None => return Err("invalid RPST token: missing payload".into()),
        };
        // the payload should not be padded
        let decoded = Base64Unpadded::decode_vec(&payload)?;
        let v: Value = serde_json::from_slice(&decoded)?;
        let tenancy = token_string_claim(&v, TENANCY_CLAIM_KEY)?;
        let expiration_secs = token_expiration_claim(&v)?;
        if expiration_secs <= now_in_secs() {
            return Err("RPST token is expired".into());
        }
        trace!("rpst expiration={}", expiration_secs);
        trace!("using RPST token: len={}", token.chars().count());

        Ok(ResourcePrincipalAuthProvider {
            token: format!("ST${}", token),
            session_private_key: session_private_key,
            tenancy_id: tenancy,
            region: region,
            expiration_secs,
        })
    }

    fn expires_within_secs(&self, window_secs: u64) -> bool {
        now_in_secs() >= self.expiration_secs.saturating_sub(window_secs)
    }
}

fn token_string_claim(v: &Value, claim: &str) -> Result<String, Box<dyn Error>> {
    let claim_value = v
        .get(claim)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("RPST token missing '{}' in payload", claim))?;
    if claim_value.is_empty() {
        return Err(format!("RPST token has empty '{}' in payload", claim).into());
    }
    Ok(claim_value.to_string())
}

fn token_expiration_claim(v: &Value) -> Result<u64, Box<dyn Error>> {
    let exp = v
        .get("exp")
        .ok_or_else(|| "RPST token missing 'exp' in payload".to_string())?;
    match exp {
        Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| "RPST token 'exp' must be a non-negative integer".into()),
        Value::String(s) => s
            .parse::<u64>()
            .map_err(|_| "RPST token 'exp' must be a non-negative integer".into()),
        _ => Err("RPST token 'exp' must be a non-negative integer".into()),
    }
}

fn now_in_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Reversed UNIX time??")
        .as_secs()
}

// By contract for the the content of a resource principal to be considered path, it needs to be
// an absolute path.
fn is_path(val: &str) -> bool {
    std::path::Path::new(val).is_absolute()
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64ct::{Base64Unpadded, Encoding};

    #[test]
    fn debug_redacts_token_and_private_key() {
        let provider = ResourcePrincipalAuthProvider {
            token: "ST$secret-rpst-token".to_string(),
            session_private_key: Rsa::generate(2048).unwrap(),
            tenancy_id: "tenancy".to_string(),
            region: "region".to_string(),
            expiration_secs: now_in_secs() + 3600,
        };

        let debug = format!("{:?}", provider);

        assert!(!debug.contains("secret-rpst-token"));
        assert!(debug.contains("[redacted]"));
    }

    #[test]
    fn new_from_values_stores_expiration_and_uses_refresh_window() {
        let expiration_secs = now_in_secs() + 3600;
        let provider = ResourcePrincipalAuthProvider::new_from_values(
            test_rpst_token("ocid1.tenancy.oc1..abc", expiration_secs),
            test_private_key_pem(),
            None,
            "us-ashburn-1".to_string(),
        )
        .unwrap();

        assert_eq!(provider.expiration_secs, expiration_secs);
        assert!(!provider.should_refresh());
        assert!(provider.expires_within_secs(4000));
    }

    #[test]
    fn new_from_values_rejects_expired_rpst() {
        let err = ResourcePrincipalAuthProvider::new_from_values(
            test_rpst_token("ocid1.tenancy.oc1..abc", now_in_secs() - 1),
            test_private_key_pem(),
            None,
            "us-ashburn-1".to_string(),
        )
        .unwrap_err();

        assert!(err.to_string().contains("expired"));
    }

    fn test_rpst_token(tenancy_id: &str, expiration_secs: u64) -> String {
        let header = Base64Unpadded::encode_string(br#"{"alg":"RS256","typ":"JWT"}"#);
        let payload = Base64Unpadded::encode_string(
            format!(r#"{{"res_tenant":"{tenancy_id}","exp":{expiration_secs}}}"#).as_bytes(),
        );
        format!("{header}.{payload}.signature")
    }

    fn test_private_key_pem() -> String {
        let rsa = Rsa::generate(2048).unwrap();
        String::from_utf8(rsa.private_key_to_pem().unwrap()).unwrap()
    }
}
