//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
//use itertools::Itertools;
use openssl::pkey::Private;
use openssl::rsa::Rsa;
use openssl::x509::X509;
use reqwest::header::HeaderMap;
use reqwest::Method;
use serde_json::Value;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
#[cfg(test)]
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tracing::{debug, instrument, trace};
use url::Url;

use crate::auth_common::authentication_provider::AuthenticationProvider;
use crate::auth_common::signer;

static METADATA_URL_BASE: &str = "http://169.254.169.254/opc/v2";
static EMPTY_STRING: &str = "";
#[cfg(test)]
static EXPECTED_NEW_WITH_CLIENT: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
pub struct InstancePrincipalAuthProvider {
    token: String,
    session_private_key: Rsa<Private>,
    tenancy_id: String,
    fingerprint: String,
    region: String,
    //expiration: u64, // seconds since the epoch
}

impl fmt::Debug for InstancePrincipalAuthProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InstancePrincipalAuthProvider")
            .field("token", &"[redacted]")
            .field("session_private_key", &"[redacted]")
            .field("tenancy_id", &self.tenancy_id)
            .field("fingerprint", &self.fingerprint)
            .field("region", &self.region)
            .finish()
    }
}

impl AuthenticationProvider for InstancePrincipalAuthProvider {
    fn tenancy_id(&self) -> &str {
        &self.tenancy_id
    }
    fn fingerprint(&self) -> &str {
        &self.fingerprint
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

impl InstancePrincipalAuthProvider {
    #[cfg(test)]
    pub(crate) fn expect_next_new_with_client_for_test(client: &reqwest::Client) {
        EXPECTED_NEW_WITH_CLIENT.store(client as *const reqwest::Client as usize, Ordering::SeqCst);
    }

    #[instrument(skip(client))]
    pub async fn new_with_client(
        client: &reqwest::Client,
    ) -> Result<InstancePrincipalAuthProvider, Box<dyn Error>> {
        #[cfg(test)]
        {
            let expected = EXPECTED_NEW_WITH_CLIENT.swap(0, Ordering::SeqCst);
            if expected != 0 {
                let actual = client as *const reqwest::Client as usize;
                let message = if actual == expected {
                    "test marker: instance principal used supplied client".to_string()
                } else {
                    format!(
                        "test marker: instance principal used unexpected client {:x}, expected {:x}",
                        actual, expected
                    )
                };
                return Err(std::io::Error::new(std::io::ErrorKind::Other, message).into());
            }
        }

        let mut auth_headers: HeaderMap = HeaderMap::new();
        auth_headers.insert("Authorization", "Bearer Oracle".parse()?);

        let leaf_certificate_url: &str = &format!("{}/identity/cert.pem", METADATA_URL_BASE);
        debug!("Getting leaf certificate from {:?}", leaf_certificate_url);
        let leaf_certificate = client
            .get(leaf_certificate_url)
            .headers(auth_headers.clone())
            .timeout(Duration::new(5, 0))
            .send()
            .await?
            .text()
            .await?;
        trace!("received leaf certificate: len={}", leaf_certificate.len());

        let leaf_certificate_private_key_url: &str =
            &format!("{}/identity/key.pem", METADATA_URL_BASE);
        let leaf_certificate_private_key = client
            .get(leaf_certificate_private_key_url)
            .headers(auth_headers.clone())
            .send()
            .await?
            .text()
            .await?;

        let intermediate_certificate_url: &str =
            &format!("{}/identity/intermediate.pem", METADATA_URL_BASE);
        let intermediate_certificates = client
            .get(intermediate_certificate_url)
            .headers(auth_headers.clone())
            .send()
            .await?
            .text()
            .await?;

        // Note: in Instance Principal, the tenancy is extracted from the leaf certificate.
        // In Resource Principal, the tenancy is extracted from the given RPST token.
        let tenancy_id = get_tenancy_id_from_certificate(&leaf_certificate)?;
        let get_region_url: &str = &format!("{}/instance/canonicalRegionName", METADATA_URL_BASE);
        let region =
            get_instance_metadata(client, get_region_url.to_string(), auth_headers.clone()).await?;
        let get_domain_url: &str = &format!(
            "{}/instance/regionInfo/realmDomainComponent",
            METADATA_URL_BASE
        );
        let domain =
            get_instance_metadata(client, get_domain_url.to_string(), auth_headers).await?;
        let (session_public_key, session_private_key) = generate_session_credentials();
        let fingerprint = x509_fingerprint(&leaf_certificate)?;
        let jwt_request_body = serialize_jwt(
            leaf_certificate.clone(),
            session_public_key,
            intermediate_certificates,
        );

        let key_id = format!("{}/fed-x509-sha256/{}", tenancy_id, fingerprint);
        let host: String = format!("https://auth.{}.{}/v1/x509", region, domain);

        // TODO: retries

        let token = get_security_token_from_auth_service(
            client,
            host,
            jwt_request_body,
            leaf_certificate_private_key,
            key_id,
        )
        .await?;

        Ok(InstancePrincipalAuthProvider {
            token: format!("ST${}", token),
            session_private_key: Rsa::private_key_from_pem(session_private_key.as_bytes())?,
            tenancy_id: tenancy_id,
            fingerprint: fingerprint,
            region: region,
        })
    }
}

async fn get_instance_metadata(
    client: &reqwest::Client,
    get_region_url: String,
    auth_headers: HeaderMap,
) -> Result<String, Box<dyn Error>> {
    let response = client
        .get(get_region_url)
        .headers(auth_headers)
        .send()
        .await?
        .text()
        .await?
        .trim()
        .to_lowercase();
    Ok(response)
}

fn get_tenancy_id_from_certificate(cert: &str) -> Result<String, Box<dyn Error>> {
    let cert = cert.as_bytes();
    let cert = X509::from_pem(cert)?;

    // Instance principal certificates carry the tenancy as an X509 subject value
    // such as OU=opc-tenant:ocid1.tenancy.oc1...
    for entry in cert.subject_name().entries() {
        let value = match entry.data().as_utf8() {
            Ok(value) => value.to_string(),
            Err(_) => continue,
        };

        if let Some(tenancy_id) = value.strip_prefix("opc-tenant:") {
            if tenancy_id.starts_with("ocid1.tenancy.") {
                return Ok(tenancy_id.to_string());
            }
        }
    }

    return Err("Cannot find tenancy id in certificate".into());
}

fn sanitize_certificate_string(cert_string: String) -> String {
    return cert_string
        .replace("-----BEGIN CERTIFICATE-----", "")
        .replace("-----END CERTIFICATE-----", "")
        .replace("-----BEGIN PUBLIC KEY-----", "")
        .replace("-----END PUBLIC KEY-----", "")
        .replace("\n", "");
}

fn generate_session_credentials() -> (String, String) {
    // const PUBLIC_EXPONENT: i32 = 65537;
    let key_size = 2048;
    let rsa = Rsa::generate(key_size).unwrap();
    let session_public_key = String::from_utf8(rsa.public_key_to_pem().unwrap()).unwrap();
    let session_private_key = String::from_utf8(rsa.private_key_to_pem().unwrap()).unwrap();
    return (session_public_key, session_private_key);
}

fn x509_fingerprint(cert: &String) -> Result<String, Box<dyn Error>> {
    let cert = cert.as_bytes();
    let cert = X509::from_pem(cert)?;
    let cert = cert.digest(openssl::hash::MessageDigest::sha256())?;
    let mut fp: String = String::default();
    let cert_bytes: &[u8] = &cert;
    for i in cert_bytes {
        fp.push_str(format!("{:02X}", i).as_str());
        fp.push(':');
    }
    // remove last colon
    let _ = fp.pop();
    //let fingerprint = format!("{:02X}", cert.iter().format(":"));
    Ok(fp)
}

fn serialize_jwt(
    leaf_certificate: String,
    public_key: String,
    intermediate_certificate: String,
) -> String {
    let leaf_certificate = sanitize_certificate_string(leaf_certificate);
    let intermediate_certificate = sanitize_certificate_string(intermediate_certificate);
    let public_key = sanitize_certificate_string(public_key);
    let jwt_request_body = String::from(format!(
        "{{\"certificate\":\"{}\",\"intermediateCertificates\":[\"{}\"],\"publicKey\":\"{}\",\"fingerprintAlgorithm\":\"SHA256\",\"purpose\":\"DEFAULT\"}}",
        leaf_certificate, intermediate_certificate, public_key
    ));
    return jwt_request_body;
}

#[instrument(skip(client, jwt_request_body, private_key_pair))]
async fn get_security_token_from_auth_service(
    client: &reqwest::Client,
    host: String,
    jwt_request_body: String,
    private_key_pair: String,
    key_id: String,
) -> Result<String, Box<dyn Error>> {
    let url = Url::parse(&host)?;
    let required_headers = signer::get_required_headers_ext(
        Method::POST,
        &jwt_request_body,
        HeaderMap::new(),
        url.clone(),
        Rsa::private_key_from_pem(private_key_pair.as_bytes())?,
        &key_id,
        HashMap::new(),
        false,
    )?;
    trace!("sending IAM auth token request to {}", url);
    let response = client
        .post(url)
        .body(jwt_request_body)
        .headers(required_headers)
        .send()
        .await?;
    let status = response.status();
    trace!("IAM auth service response status: {}", status);
    if !status.is_success() {
        return Err(format!("IAM auth service returned status {}", status.as_str()).into());
    }

    let rtext = response.text().await?;
    let v: Value = serde_json::from_str(&rtext)?;
    let token = format!("{}", v["token"]).replace("\"", "");
    Ok(token)
}

#[allow(dead_code)]
pub fn now_in_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .expect("Reversed UNIX time??")
        .as_secs()
}

// TODO: unit tests to verify correct parsing of tokens, etc.

//pub async fn test_api_call(host: String) -> Result<String, Box<dyn Error>> {
//let token_signer = instance_principals_security_token_signer().await;
//let url_data = Url::parse(&host)?;
//let sdk_client = sdk_client::SdkClient {
//private_key: token_signer.session_private_key,
//key_id: token_signer.token,
//};
//let response = sdk_client.get(url_data).await;
//response
//}

#[cfg(test)]
mod tests {
    use super::*;
    use openssl::asn1::{Asn1Integer, Asn1Time};
    use openssl::bn::BigNum;
    use openssl::hash::MessageDigest;
    use openssl::nid::Nid;
    use openssl::pkey::PKey;
    use openssl::x509::X509NameBuilder;

    #[test]
    fn debug_redacts_token_and_private_key() {
        let provider = InstancePrincipalAuthProvider {
            token: "ST$secret-security-token".to_string(),
            session_private_key: Rsa::generate(2048).unwrap(),
            tenancy_id: "tenancy".to_string(),
            fingerprint: "fingerprint".to_string(),
            region: "region".to_string(),
        };

        let debug = format!("{:?}", provider);

        assert!(!debug.contains("secret-security-token"));
        assert!(debug.contains("[redacted]"));
    }

    #[test]
    fn tenancy_id_from_certificate_preserves_last_character_before_next_subject_entry() {
        let tenancy_id = "ocid1.tenancy.oc1..abcdefghijklmnopqrstu";
        let cert = test_certificate_with_subject_entries(&[
            (Nid::COMMONNAME, "ocid1.instance.oc1.iad.example"),
            (Nid::ORGANIZATIONALUNITNAME, "opc-certtype:instance"),
            (
                Nid::ORGANIZATIONALUNITNAME,
                &format!("opc-tenant:{tenancy_id}"),
            ),
            (
                Nid::ORGANIZATIONALUNITNAME,
                "opc-instance:ocid1.instance.oc1.iad.example",
            ),
        ]);

        let parsed = get_tenancy_id_from_certificate(&cert).unwrap();

        assert_eq!(parsed, tenancy_id);
    }

    fn test_certificate_with_subject_entries(entries: &[(Nid, &str)]) -> String {
        let rsa = Rsa::generate(2048).unwrap();
        let pkey = PKey::from_rsa(rsa).unwrap();

        let mut name_builder = X509NameBuilder::new().unwrap();
        for (nid, value) in entries {
            name_builder.append_entry_by_nid(*nid, value).unwrap();
        }
        let subject_name = name_builder.build();

        let mut builder = X509::builder().unwrap();
        builder.set_version(2).unwrap();
        let serial_number = BigNum::from_u32(1).unwrap();
        let serial_number = Asn1Integer::from_bn(&serial_number).unwrap();
        builder.set_serial_number(&serial_number).unwrap();
        builder.set_subject_name(&subject_name).unwrap();
        builder.set_issuer_name(&subject_name).unwrap();
        builder.set_pubkey(&pkey).unwrap();
        let not_before = Asn1Time::days_from_now(0).unwrap();
        let not_after = Asn1Time::days_from_now(1).unwrap();
        builder.set_not_before(&not_before).unwrap();
        builder.set_not_after(&not_after).unwrap();
        builder.sign(&pkey, MessageDigest::sha256()).unwrap();

        String::from_utf8(builder.build().to_pem().unwrap()).unwrap()
    }
}
