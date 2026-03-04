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

#[derive(Debug, Clone)]
pub struct ResourcePrincipalAuthProvider {
    token: String,
    session_private_key: Rsa<Private>,
    tenancy_id: String,
    region: String,
    //expiration: u64, // seconds since the epoch
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
        // TODO: better method for checking these values (not checking for "null")
        let tenancy = format!("{}", v[TENANCY_CLAIM_KEY]).replace("\"", "");
        if tenancy == "null" {
            return Err(
                format!("RPST token missing '{}' in payload", TENANCY_CLAIM_KEY)
                    .as_str()
                    .into(),
            );
        }
        let exp = format!("{}", v["exp"]).replace("\"", "");
        if exp == "null" {
            return Err(format!("RPST token missing 'exp' in payload")
                .as_str()
                .into());
        }
        trace!("rpst expiration={}", exp);
        trace!("using RPST token: {}", token);

        Ok(ResourcePrincipalAuthProvider {
            token: format!("ST${}", token),
            session_private_key: session_private_key,
            tenancy_id: tenancy,
            region: region,
        })
    }
}

// By contract for the the content of a resource principal to be considered path, it needs to be
// an absolute path.
fn is_path(val: &str) -> bool {
    std::path::Path::new(val).is_absolute()
}
