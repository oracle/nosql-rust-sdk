//
// Copyright (c) 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use super::*;
use crate::handle_builder::{AuthConfig, AuthProvider, AuthType, HandleBuilder, HandleMode};
use openssl::asn1::Asn1Time;
use openssl::hash::MessageDigest;
use openssl::ssl::{SslAcceptor, SslMethod};
use openssl::x509::extension::{
    BasicConstraints, ExtendedKeyUsage, KeyUsage, SubjectAlternativeName,
};
use openssl::x509::{X509NameBuilder, X509};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

fn jwt(claims: Value) -> String {
    format!(
        "e30.{}.c2ln",
        BASE64_URL_SAFE_NO_PAD.encode(claims.to_string())
    )
}

fn service_account_token(subject: &str) -> String {
    jwt(json!({"exp": now_in_secs() + 3600, "sub": subject}))
}

fn response_body(key: &Rsa<openssl::pkey::Public>, expiration: u64, prefix: &str) -> Vec<u8> {
    response_with_claims(
        json!({
            "exp": expiration,
            "res_tenant": "ocid1.tenancy.oc1..oke-test",
            // OCI embeds the JWK as serialized JSON and may use standard Base64.
            "jwk": json!({
                "n": BASE64_STANDARD.encode(key.n().to_vec()),
                "e": BASE64_STANDARD.encode(key.e().to_vec()),
            }).to_string()
        }),
        prefix,
    )
}

fn response_with_claims(claims: Value, prefix: &str) -> Vec<u8> {
    let token = jwt(claims);
    serde_json::to_vec(
        &BASE64_STANDARD.encode(json!({"token": format!("{prefix}{token}")}).to_string()),
    )
    .unwrap()
}

#[test]
fn oke_session_token_accepts_jwk_wire_formats() {
    use base64::prelude::{BASE64_STANDARD_NO_PAD, BASE64_URL_SAFE};

    let key = Rsa::generate(2048).unwrap();
    for engine in [
        BASE64_STANDARD,
        BASE64_STANDARD_NO_PAD,
        BASE64_URL_SAFE,
        BASE64_URL_SAFE_NO_PAD,
    ] {
        let jwk = json!({
            "n": engine.encode(key.n().to_vec()),
            "e": engine.encode(key.e().to_vec()),
        });
        for claim in [Value::String(jwk.to_string()), jwk] {
            let body = response_with_claims(
                json!({
                    "exp": now_in_secs() + 3600,
                    "jwk": claim,
                }),
                "ST$",
            );
            let credentials = decode_response(&body, key.clone()).unwrap();
            assert!(credentials.token.starts_with("ST$e30."));
        }
    }
}

#[test]
fn oke_jwk_modulus_accepts_both_base64_alphabets() {
    // Fixed bytes guarantee that both standard-only characters are exercised,
    // independently of the random RSA keys used by the exchange tests.
    let expected = BigNum::from_slice(&[0xfb, 0xff]).unwrap();
    for modulus in ["+/8=", "+/8", "-_8=", "-_8"] {
        let decoded = jwk_integer(&json!({"n": modulus}), "n").unwrap();
        assert_eq!(decoded, expected);
    }
    assert!(jwk_integer(&json!({"n": "invalid!"}), "n").is_err());
}

#[test]
fn oke_rejects_malformed_serialized_jwk_without_disclosing_it() {
    let key = Rsa::generate(2048).unwrap();
    for jwk in [
        "secret-jwk",
        r#""secret-jwk""#,
        "[]",
        "null",
        "{}",
        r#"{"n":"secret-jwk","e":"AQAB"}"#,
    ] {
        let body = response_with_claims(
            json!({
                "exp": now_in_secs() + 3600,
                "jwk": jwk,
            }),
            "ST$",
        );
        let error = decode_response(&body, key.clone())
            .err()
            .unwrap()
            .to_string();
        assert!(error.contains("JWK"));
        assert!(!error.contains("secret-jwk"));
    }
}

#[test]
fn oke_token_validation_and_redaction() {
    for token in ["", "secret", "a.b.c.d", "a..c", "a.b."] {
        assert!(token_claims(token).is_err());
    }
    for claims in [
        json!({}),
        json!({"exp": -1}),
        json!({"exp": 1.5}),
        json!({"exp": "bad"}),
        json!({"exp": 0}),
        json!({"exp": now_in_secs() - 1}),
    ] {
        assert!(token_expiration(&claims).is_err());
    }
    let expiration = now_in_secs() + 3600;
    assert_eq!(
        token_expiration(&json!({"exp": expiration.to_string()})).unwrap(),
        expiration
    );
    let claims = json!({"exp": expiration, "sub": "\u{ffff}\u{fffe}"});
    assert_eq!(token_claims(&jwt(claims.clone())).unwrap(), claims);

    let key = Rsa::generate(2048).unwrap();
    let public_key = Rsa::public_key_from_der(&key.public_key_to_der().unwrap()).unwrap();
    for prefix in ["ST$", ""] {
        let creds =
            decode_response(&response_body(&public_key, expiration, prefix), key.clone()).unwrap();
        assert!(creds.token.starts_with("ST$e30."));
        assert_eq!(creds.refresh_at, expiration - 300);
    }
    assert!(decode_response(&response_body(&public_key, 1, "ST$"), key.clone()).is_err());
    let other_key = Rsa::generate(2048).unwrap();
    let err = decode_response(&response_body(&public_key, expiration, "ST$"), other_key)
        .err()
        .unwrap();
    assert!(err.to_string().contains("does not match podKey"));

    for body in [
        b"secret-response".to_vec(),
        b"\"invalid-base64\"".to_vec(),
        serde_json::to_vec(&BASE64_STANDARD.encode("secret-response")).unwrap(),
        serde_json::to_vec(&BASE64_STANDARD.encode(r#"{"token":false}"#)).unwrap(),
        serde_json::to_vec(
            &BASE64_STANDARD.encode(json!({"token": jwt(json!({"exp": expiration}))}).to_string()),
        )
        .unwrap(),
    ] {
        let err = decode_response(&body, key.clone())
            .err()
            .unwrap()
            .to_string();
        assert!(!err.contains("secret-response"));
    }
}

#[test]
fn oke_endpoint_and_region_configuration() {
    assert_eq!(
        token_url("10.0.0.1").unwrap().as_str(),
        "https://10.0.0.1:12250/resourcePrincipalSessionTokens"
    );
    assert_eq!(
        token_url("::1").unwrap().as_str(),
        "https://[::1]:12250/resourcePrincipalSessionTokens"
    );
    assert_eq!(token_url("[::1]").unwrap().host_str(), Some("[::1]"));
    for host in [
        "",
        "host/path",
        "user@host",
        "host:443",
        "https://host",
        "host?x",
        "host#x",
    ] {
        assert!(token_url(host).is_err(), "accepted {host}");
    }
    assert_eq!(
        region_from_metadata(r#"{"regionIdentifier":"us-ashburn-1"}"#).unwrap(),
        "us-ashburn-1"
    );
    for metadata in [
        "bad",
        "{}",
        "[]",
        r#"{"regionIdentifier":""}"#,
        r#"{"regionIdentifier":2}"#,
    ] {
        assert!(region_from_metadata(metadata).is_err());
    }
    assert!(token_client(b"").is_err());
    assert!(token_client(b"not a certificate").is_err());
}

#[test]
fn oke_builder_selects_sources_without_exposing_tokens() {
    let builder = HandleBuilder::new().cloud_auth_from_oke().unwrap();
    assert_eq!(builder.auth_type, AuthType::Oke);
    assert_eq!(builder.mode, HandleMode::Cloud);
    assert!(builder.use_https);
    assert!(
        matches!(builder.oke_token_source, OkeTokenSource::File(ref p) if p == &PathBuf::from(DEFAULT_TOKEN_PATH))
    );
    let builder = builder
        .cloud_auth_from_oke_with_token("secret-token")
        .unwrap();
    assert!(!format!("{builder:?}").contains("secret-token"));
    assert!(
        matches!(builder.oke_token_source, OkeTokenSource::Token(ref t) if t == "secret-token")
    );
    let builder = builder
        .cloud_auth_from_oke_with_token_file("/tmp/custom-token")
        .unwrap();
    assert!(
        matches!(builder.oke_token_source, OkeTokenSource::File(ref p) if p == &PathBuf::from("/tmp/custom-token"))
    );
    assert!(HandleBuilder::new()
        .cloud_auth_from_oke_with_token(" ")
        .is_err());
    assert!(HandleBuilder::new()
        .cloud_auth_from_oke_with_token_file("")
        .is_err());
}

struct TestCertificates {
    ca: X509,
    cert: X509,
    key: PKey<Private>,
}

fn certificates(hostname: &str) -> TestCertificates {
    let ca_key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
    let mut name = X509NameBuilder::new().unwrap();
    name.append_entry_by_text("CN", "OKE test CA").unwrap();
    let name = name.build();
    let mut ca = X509::builder().unwrap();
    ca.set_version(2).unwrap();
    ca.set_serial_number(&BigNum::from_u32(1).unwrap().to_asn1_integer().unwrap())
        .unwrap();
    ca.set_subject_name(&name).unwrap();
    ca.set_issuer_name(&name).unwrap();
    ca.set_pubkey(&ca_key).unwrap();
    ca.set_not_before(&Asn1Time::days_from_now(0).unwrap())
        .unwrap();
    ca.set_not_after(&Asn1Time::days_from_now(1).unwrap())
        .unwrap();
    ca.append_extension(BasicConstraints::new().critical().ca().build().unwrap())
        .unwrap();
    ca.append_extension(
        KeyUsage::new()
            .critical()
            .key_cert_sign()
            .crl_sign()
            .build()
            .unwrap(),
    )
    .unwrap();
    ca.sign(&ca_key, MessageDigest::sha256()).unwrap();
    let ca = ca.build();
    let key = PKey::from_rsa(Rsa::generate(2048).unwrap()).unwrap();
    let mut name = X509NameBuilder::new().unwrap();
    name.append_entry_by_text("CN", hostname).unwrap();
    let mut cert = X509::builder().unwrap();
    cert.set_version(2).unwrap();
    cert.set_serial_number(&BigNum::from_u32(2).unwrap().to_asn1_integer().unwrap())
        .unwrap();
    cert.set_subject_name(&name.build()).unwrap();
    cert.set_issuer_name(ca.subject_name()).unwrap();
    cert.set_pubkey(&key).unwrap();
    cert.set_not_before(&Asn1Time::days_from_now(0).unwrap())
        .unwrap();
    cert.set_not_after(&Asn1Time::days_from_now(1).unwrap())
        .unwrap();
    cert.append_extension(BasicConstraints::new().critical().build().unwrap())
        .unwrap();
    cert.append_extension(
        KeyUsage::new()
            .critical()
            .digital_signature()
            .key_encipherment()
            .build()
            .unwrap(),
    )
    .unwrap();
    cert.append_extension(ExtendedKeyUsage::new().server_auth().build().unwrap())
        .unwrap();
    let san = SubjectAlternativeName::new()
        .dns(hostname)
        .build(&cert.x509v3_context(Some(&ca), None))
        .unwrap();
    cert.append_extension(san).unwrap();
    cert.sign(&ca_key, MessageDigest::sha256()).unwrap();
    TestCertificates {
        ca,
        cert: cert.build(),
        key,
    }
}

struct MockRequest {
    headers: String,
    body: Value,
}

// A real local TLS endpoint exercises reqwest's CA and hostname checks. Bound
// waits ensure a failed client does not leave the test waiting indefinitely.
fn tls_server(
    certs: &TestCertificates,
    count: usize,
    respond: impl Fn(MockRequest) -> (u16, Vec<u8>, String) + Send + 'static,
) -> (Url, thread::JoinHandle<usize>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = Url::parse(&format!(
        "https://localhost:{}{TOKEN_PATH}",
        listener.local_addr().unwrap().port()
    ))
    .unwrap();
    let mut acceptor = SslAcceptor::mozilla_intermediate(SslMethod::tls()).unwrap();
    acceptor.set_certificate(&certs.cert).unwrap();
    acceptor.set_private_key(&certs.key).unwrap();
    let acceptor = acceptor.build();
    let thread = thread::spawn(move || {
        let mut requests = 0;
        for _ in 0..count {
            let deadline = Instant::now() + Duration::from_secs(10);
            let socket = loop {
                match listener.accept() {
                    Ok((socket, _)) => break socket,
                    Err(e)
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            && Instant::now() < deadline =>
                    {
                        thread::sleep(Duration::from_millis(10))
                    }
                    Err(e) => panic!("TLS test server accept failed: {e}"),
                }
            };
            socket.set_nonblocking(false).unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            socket
                .set_write_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let Ok(mut stream) = acceptor.accept(socket) else {
                continue;
            };
            let mut request = Vec::new();
            let header_end = loop {
                let mut byte = [0u8];
                // Some backends reject the certificate after completing the
                // server side of the TLS handshake, before sending HTTP.
                if stream.read_exact(&mut byte).is_err() {
                    return requests;
                }
                request.push(byte[0]);
                if request.ends_with(b"\r\n\r\n") {
                    break request.len();
                }
                assert!(request.len() < 65536);
            };
            let headers = String::from_utf8(request[..header_end].to_vec()).unwrap();
            let len: usize = headers
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().unwrap())
                })
                .unwrap();
            let mut body = vec![0u8; len];
            stream.read_exact(&mut body).unwrap();
            let (status, body, extra_headers) = respond(MockRequest {
                headers,
                body: serde_json::from_slice(&body).unwrap(),
            });
            requests += 1;
            write!(stream, "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n{extra_headers}\r\n", body.len()).unwrap();
            stream.write_all(&body).unwrap();
            stream.flush().unwrap();
        }
        requests
    });
    (url, thread)
}

fn pod_key(request: &MockRequest) -> Rsa<openssl::pkey::Public> {
    let der = BASE64_STANDARD
        .decode(request.body["podKey"].as_str().unwrap())
        .unwrap();
    // Enforce the Java wire format (SubjectPublicKeyInfo).
    PKey::public_key_from_der(&der).unwrap().rsa().unwrap()
}

#[tokio::test]
async fn oke_exchange_refresh_rotation_and_signing() {
    use openssl::sign::Verifier;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let certs = certificates("localhost");
    let first = service_account_token("first");
    let second = service_account_token("second");
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("token");
    std::fs::write(&path, format!("{first}\n")).unwrap();
    let expected_tokens = [
        first.clone(),
        second.clone(),
        second.clone(),
        second.clone(),
    ];
    let request_num = AtomicUsize::new(0);
    let (url, server) = tls_server(&certs, 4, move |request| {
        let i = request_num.fetch_add(1, Ordering::SeqCst);
        assert!(request
            .headers
            .starts_with("POST /resourcePrincipalSessionTokens HTTP/1.1"));
        let headers = request.headers.to_lowercase();
        assert!(headers.contains("content-type: application/json"));
        assert!(headers.contains("opc-request-id: "));
        assert!(request
            .headers
            .contains(&format!("Bearer {}", expected_tokens[i])));
        if i == 3 {
            return (401, b"secret-response".to_vec(), String::new());
        }
        (
            200,
            response_body(&pod_key(&request), now_in_secs() + 3600, "ST$"),
            String::new(),
        )
    });
    let provider = OkeWorkloadIdentityAuthProvider::with_client(
        OkeTokenSource::File(path.clone()),
        token_client(&certs.ca.to_pem().unwrap()).unwrap(),
        url,
        "us-ashburn-1".to_string(),
    )
    .await
    .unwrap();
    assert!(!provider.should_refresh());
    assert!(!format!("{provider:?}").contains(&first));
    assert!(!format!("{provider:?}").contains(&provider.key_id()));
    let old_key = provider.private_key().unwrap().public_key_to_der().unwrap();
    let mut builder = HandleBuilder::new().cloud_auth_from_oke().unwrap();
    builder.auth = Arc::new(tokio::sync::Mutex::new(AuthConfig {
        provider: AuthProvider::Oke {
            provider: Box::new(provider),
        },
    }));
    assert!(!builder.refresh_auth_if_needed().await.unwrap());
    std::fs::write(&path, &second).unwrap();
    {
        let mut guard = builder.auth.lock().await;
        let AuthProvider::Oke { provider } = &mut guard.provider else {
            panic!()
        };
        provider.credentials.refresh_at = 0;
    }
    assert!(builder.refresh_auth_if_needed().await.unwrap());
    // Forced refresh after an authentication failure uses the same source and
    // dedicated client, even if the NoSQL client has insecure TLS settings.
    let insecure_client = Client::builder()
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();
    assert!(builder.refresh_auth(&insecure_client).await.unwrap());
    let guard = builder.auth.lock().await;
    let AuthProvider::Oke { provider } = &guard.provider else {
        panic!()
    };
    assert_ne!(
        old_key,
        provider.private_key().unwrap().public_key_to_der().unwrap()
    );
    let key_id = provider.key_id();
    let signing_key = PKey::from_rsa(provider.private_key().unwrap()).unwrap();
    let headers = crate::auth_common::signer::get_required_headers(
        reqwest::Method::POST,
        "",
        reqwest::header::HeaderMap::new(),
        Url::parse("https://nosql.us-ashburn-1.oci.oraclecloud.com/V2/nosql/data").unwrap(),
        provider.as_ref(),
        std::collections::HashMap::new(),
        true,
    )
    .unwrap();
    let authorization = headers["authorization"].to_str().unwrap();
    assert!(authorization.contains(&format!("keyId=\"{key_id}\"")));
    let signature = authorization
        .split("signature=\"")
        .nth(1)
        .unwrap()
        .split('"')
        .next()
        .unwrap();
    let signed_headers = authorization
        .split("headers=\"")
        .nth(1)
        .unwrap()
        .split('"')
        .next()
        .unwrap();
    let signing_string = signed_headers
        .split(' ')
        .map(|name| {
            if name == "(request-target)" {
                "(request-target): post /V2/nosql/data".to_string()
            } else {
                format!("{name}: {}", headers[name].to_str().unwrap())
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    let mut verifier = Verifier::new(MessageDigest::sha256(), &signing_key).unwrap();
    verifier.update(signing_string.as_bytes()).unwrap();
    assert!(verifier
        .verify(&BASE64_STANDARD.decode(signature).unwrap())
        .unwrap());
    drop(guard);
    let error = builder.refresh_auth(&insecure_client).await.unwrap_err();
    assert!(error.to_string().contains("401"));
    assert!(!error.to_string().contains("secret-response"));
    let guard = builder.auth.lock().await;
    let AuthProvider::Oke { provider } = &guard.provider else {
        panic!()
    };
    assert_eq!(
        provider.key_id(),
        key_id,
        "failed refresh must preserve credentials"
    );
    assert_eq!(server.join().unwrap(), 4);
}

#[tokio::test]
async fn oke_tls_rejects_wrong_hostname_and_untrusted_ca() {
    let trusted = certificates("localhost");
    for (certs, trusted_pem) in [
        {
            let wrong = certificates("wrong.example");
            let pem = wrong.ca.to_pem().unwrap();
            (wrong, pem)
        },
        (certificates("localhost"), trusted.ca.to_pem().unwrap()),
    ] {
        let (url, server) = tls_server(&certs, 1, |_| {
            panic!("unverified TLS must not send credentials")
        });
        let result = OkeWorkloadIdentityAuthProvider::with_client(
            OkeTokenSource::Token(service_account_token("tls")),
            token_client(&trusted_pem).unwrap(),
            url,
            "us-ashburn-1".to_string(),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(server.join().unwrap(), 0);
    }
}

#[tokio::test]
async fn oke_token_exchange_rejects_redirects() {
    let certs = certificates("localhost");
    let (url, server) = tls_server(&certs, 1, |_| {
        (
            302,
            b"secret-response".to_vec(),
            "Location: http://127.0.0.1:1/steal\r\n".to_string(),
        )
    });
    let err = OkeWorkloadIdentityAuthProvider::with_client(
        OkeTokenSource::Token(service_account_token("redirect")),
        token_client(&certs.ca.to_pem().unwrap()).unwrap(),
        url,
        "us-ashburn-1".to_string(),
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(err.contains("302"), "{err}");
    assert!(!err.contains("secret-response"));
    assert_eq!(server.join().unwrap(), 1);
}

#[test]
fn oke_token_files_and_short_lived_tokens() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("token");
    let source = OkeTokenSource::File(path.clone());
    assert!(source.read().is_err());
    std::fs::write(&path, jwt(json!({"exp": 1}))).unwrap();
    assert!(source.read().is_err());
    std::fs::write(&path, service_account_token("rotated")).unwrap();
    assert!(source.read().is_ok());
    let key = Rsa::generate(2048).unwrap();
    let public = Rsa::public_key_from_der(&key.public_key_to_der().unwrap()).unwrap();
    let now = now_in_secs();
    let creds = decode_response(&response_body(&public, now + 60, "ST$"), key).unwrap();
    assert!(creds.refresh_at > now);
    assert!(creds.refresh_at <= now + 60);
}
