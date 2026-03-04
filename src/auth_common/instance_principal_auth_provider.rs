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
use std::time::Duration;
use tracing::{debug, instrument, trace};
use url::Url;

use crate::auth_common::authentication_provider::AuthenticationProvider;
use crate::auth_common::signer;

static METADATA_URL_BASE: &str = "http://169.254.169.254/opc/v2";
static EMPTY_STRING: &str = "";

#[derive(Debug, Clone)]
pub struct InstancePrincipalAuthProvider {
    token: String,
    session_private_key: Rsa<Private>,
    tenancy_id: String,
    fingerprint: String,
    region: String,
    //expiration: u64, // seconds since the epoch
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
    pub async fn new() -> Result<InstancePrincipalAuthProvider, Box<dyn Error>> {
        InstancePrincipalAuthProvider::new_with_client(&reqwest::Client::builder().build()?).await
    }

    #[instrument]
    pub async fn new_with_client(
        client: &reqwest::Client,
    ) -> Result<InstancePrincipalAuthProvider, Box<dyn Error>> {
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
        trace!("Leaf certificate: {:?}", leaf_certificate);

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
    let mut subject = String::from_utf8_lossy(&cert.to_text()?).into_owned();

    // In text form, the cert contains a Subject line like this:
    // Subject: CN=ocid1.instance.oc1.eu-zurich-1.an5heljrckmxu5ichjk4nyxrwqg3abrsafgyh4niyl6vs3lmdjfjio3t463a, OU=opc-certtype:instance, OU=opc-compartment:ocid1.tenancy.oc1..aaaaaaaattuxbj75pnn3nksvzyidshdbrfmmeflv4kkemajroz2thvca4kba, OU=opc-instance:ocid1.instance.oc1.eu-zurich-1.an5heljrckmxu5ichjk4nyxrwqg3abrsafgyh4niyl6vs3lmdjfjio3t463a, OU=opc-tenant:ocid1.tenancy.oc1..aaaaaaaattuxbj75pnn3nksvzyidshdbrfmmeflv4kkemajroz2thvca4kba
    // This code attempts to extract the 'opc-tenant:____________' value
    // Note the cert also has the compartment ocid as well, which may be useful for users of this library

    if let Some(off) = subject.find("=opc-tenant:ocid1.tenancy.") {
        // 12 == length of "=opc-tenant:"
        let mut tenancy_id = subject.split_off(off + 12);
        // strip trailing: start at comma, newline, or space
        for i in [',', ' ', '\n', '\r'] {
            if let Some(coff) = tenancy_id.find(i) {
                let _ = tenancy_id.split_off(coff - 1);
            }
        }
        //println!("tenancy='{}'", tenancy_id);
        return Ok(tenancy_id);
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

#[instrument]
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
    trace!(
        "Sending http post request to {} \n with headers : {:?}",
        url,
        required_headers
    );
    let response = client
        .post(url)
        .body(jwt_request_body)
        .headers(required_headers)
        .send()
        .await?;
    trace!("Response received from the service : {:?}", response);
    if !response.status().is_success() {
        return Err(format!(
            "IAM auth service returned status {}",
            response.status().as_str()
        )
        .into());
    }

    let rtext = response.text().await?;
    trace!("response text: {}", rtext);
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
