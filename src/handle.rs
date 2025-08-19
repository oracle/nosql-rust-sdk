//
// Copyright (c) 2024, 2025 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use crate::auth_common::authentication_provider::AuthenticationProvider;
use crate::auth_common::instance_principal_auth_provider::InstancePrincipalAuthProvider;
use crate::auth_common::resource_principal_auth_provider::ResourcePrincipalAuthProvider;
use crate::auth_common::signer;
use crate::handle_builder::AuthConfig;
use crate::handle_builder::AuthType;
use reqwest::header::{HeaderMap, HeaderValue};

use crate::error::NoSQLErrorCode::InternalRetry;
use crate::error::{ia_err, user_agent};
use crate::error::{NoSQLError, NoSQLErrorCode};
use crate::handle_builder::AuthProvider;
use crate::handle_builder::HandleBuilder;
use crate::handle_builder::HandleMode;
use crate::nson::MapWalker;
use crate::reader::Reader;
use crate::writer::Writer;

use std::collections::HashMap;
use std::result::Result;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, trace};
use url::Url;

/// **The main database handle**.
///
/// This should be created once and used
/// throughout the application lifetime, across all threads.
///
/// Note: there is no need to enclose this struct in an `Rc` or [`Arc`], as it uses an
/// [`Arc`] internally, so calling `.clone()` on this struct will always return the
/// same underlying handle.
#[derive(Clone, Debug)]
pub struct Handle {
    // Use an inner Arc so cloning keeps the same contents
    pub(crate) inner: Arc<HandleRef>,
}

#[derive(Debug)]
pub(crate) struct HandleRef {
    pub(crate) client: reqwest::Client,
    pub(crate) endpoint: String,
    pub(crate) serial_version: i16,
    pub(crate) builder: HandleBuilder,
    // session doesn't require a tokio Mutex because it's never held across awaits
    session: std::sync::Mutex<String>,
    request_id: AtomicUsize,
    timeout: Duration,
}

impl Handle {
    /// Create a new [`HandleBuilder`].
    pub fn builder() -> HandleBuilder {
        HandleBuilder::new()
    }

    // Create the new Handle based on builder configuration
    pub(crate) async fn new(b: &HandleBuilder) -> Result<Handle, NoSQLError> {
        if b.auth_type == AuthType::None {
            if b.from_environment {
                return ia_err!("cannot build handle: no auth type specified. set ORACLE_NOSQL_AUTH environment.");
            }
            return ia_err!("cannot build handle: no auth type specified");
            //trace!("defaulting auth config to user-based OCI auth from ~/.oci/config");
            //builder = builder.cloud_auth_from_file("~/.oci/config")?;
        }

        let mut builder = b.clone();
        // default timeout to 30 seconds
        // TODO: connection timeout vs request timeout
        let timeout = {
            if let Some(t) = builder.timeout {
                t.clone()
            } else {
                Duration::new(30, 0)
            }
        };
        let c = {
            if let Some(c) = &builder.client {
                c.clone()
            } else {
                let mut cb = reqwest::Client::builder()
                    .timeout(timeout)
                    .connect_timeout(timeout)
                    //.pool_idle_timeout(timeout)
                    .connection_verbose(true);
                if let Some(cert) = &builder.add_cert {
                    cb = cb.add_root_certificate(cert.clone());
                }
                if builder.accept_invalid_certs {
                    cb = cb.danger_accept_invalid_certs(true);
                }
                cb.build()?
            }
        };
        // create auth provider if not already created
        match builder.auth_type {
            AuthType::Instance => {
                let ifp = InstancePrincipalAuthProvider::new_with_client(&c).await?;
                if builder.region.is_none() {
                    builder = builder.cloud_region(ifp.region_id())?;
                }
                let ap = AuthProvider::Instance {
                    provider: Box::new(ifp),
                };
                builder.auth = Arc::new(tokio::sync::Mutex::new(AuthConfig { provider: ap }));
            }
            AuthType::Resource => {
                let rfp = ResourcePrincipalAuthProvider::new()?;
                if builder.region.is_none() {
                    builder = builder.cloud_region(rfp.region_id())?;
                }
                let ap = AuthProvider::Resource {
                    provider: Box::new(rfp),
                };
                builder.auth = Arc::new(tokio::sync::Mutex::new(AuthConfig { provider: ap }));
            }
            _ => {}
        }
        if builder.endpoint.is_empty() {
            if builder.from_environment {
                return ia_err!("can't determine NoSQL endpoint: set ORACLE_NOSQL_ENDPOINT or ORACLE_NOSQL_REGION");
            } else {
                return ia_err!("can't determine NoSQL endpoint: call HandleBuilder::endpoint() or HandleBuilder::cloud_region()");
            }
        }
        // normalize endpoint to "http[s]://{endpoint}/V2/nosql/data"
        let mut ep = String::from("http");
        if builder.use_https {
            ep.push('s');
        }
        ep.push_str("://");
        ep.push_str(&builder.endpoint);
        ep.push_str("/V2/nosql/data");
        debug!(
            "Creating new Handle: {:?}, {:?}, endpoint={}",
            builder.mode, builder.auth, ep
        );
        Ok(Handle {
            inner: Arc::new(HandleRef {
                client: c,
                endpoint: ep,
                serial_version: 4,
                builder: builder,
                timeout: timeout.clone(),
                session: std::sync::Mutex::new("".to_string()),
                request_id: AtomicUsize::new(1),
            }),
        })
    }

    // geeez, all this to get a stupid usize from an http header....
    fn get_usize_header(headers: &HeaderMap, field: &str) -> Result<usize, NoSQLError> {
        let val = headers.get(field);
        if val.is_none() {
            return ia_err!("missing \"{}\" value in return headers", field);
        }
        let valstr = val.unwrap().to_str();
        if let Err(_) = valstr {
            return ia_err!(
                "\"{}\" value in return headers is not a valid string",
                field
            );
        }
        match valstr.unwrap().parse::<usize>() {
            Ok(v) => {
                return Ok(v);
            }
            Err(_) => {
                return ia_err!("\"{}\" value in return headers is not an integer", field);
            }
        }
    }

    async fn post_data(
        &self,
        data: &Vec<u8>,
        send_options: &mut SendOptions,
    ) -> Result<Vec<u8>, NoSQLError> {
        let request_id = self.inner.request_id.fetch_add(1, Ordering::Relaxed);
        let mut headers = HeaderMap::new();
        headers.insert("x-nosql-request-id", HeaderValue::from(request_id));

        // If there is an oci auth provider, use that to set up required headers
        let mut oci_provider: Option<&Box<dyn AuthenticationProvider>> = None;

        // We need to lock the auth config because it may be asynchronously refreshed elsewhere
        let pguard = self.inner.builder.auth.lock().await;
        match &pguard.provider {
            AuthProvider::Instance { provider } => {
                oci_provider = Some(provider);
            }
            AuthProvider::Resource { provider } => {
                oci_provider = Some(provider);
            }
            AuthProvider::External { provider } => {
                oci_provider = Some(provider);
            }
            AuthProvider::File { provider } => {
                oci_provider = Some(provider);
            }
            AuthProvider::Onprem { provider } => {
                if let Some(p) = provider {
                    p.add_required_headers(&self.inner.client, &mut headers)
                        .await?;
                }
            }
            AuthProvider::None => {}
        }

        if let Some(sp) = oci_provider {
            if !self.inner.builder.default_compartment_id.is_empty() {
                headers.insert(
                    "x-nosql-compartment-id",
                    HeaderValue::from_str(&self.inner.builder.default_compartment_id)?,
                );
            } else {
                headers.insert(
                    "x-nosql-compartment-id",
                    HeaderValue::from_str(sp.tenancy_id())?,
                );
            }
            {
                // If there's a session cookie value, set it into the headers.
                // The lock is needed because another async operation might try to
                // update the session value while we're trying to read it.
                // This is in its own code block so the lock will be released directly afterwards.
                let sguard = self.inner.session.lock().unwrap();
                if sguard.len() > 0 {
                    let s = format!("session={}", sguard.as_str());
                    headers.insert("Cookie", HeaderValue::from_str(s.as_str())?);
                }
            }
            trace!("Adding required headers");
            headers = signer::get_required_headers(
                reqwest::Method::POST,
                "",
                headers,
                Url::parse(&self.inner.endpoint)?,
                sp,
                HashMap::new(),
                true,
            )?;
        } else if self.inner.builder.mode == HandleMode::Onprem {
            // headers added above if necessary
        } else if self.inner.builder.mode == HandleMode::Cloudsim {
            headers.insert("Authorization", HeaderValue::from_str("Bearer rust")?);
        }
        // this will unlock the auth mutex
        core::mem::drop(pguard);

        // let send_options.compartment_id override compartment header
        if !send_options.compartment_id.is_empty() {
            headers.insert(
                "x-nosql-compartment-id",
                HeaderValue::from_str(&send_options.compartment_id)?,
            );
        }

        // let send_options.namespace override namespace header
        if !send_options.namespace.is_empty() {
            headers.insert(
                "x-nosql-default-ns",
                HeaderValue::from_str(&send_options.namespace)?,
            );
        }

        // Set User-Agent
        headers.insert("User-Agent", HeaderValue::from_str(user_agent())?);

        let resp = self
            .inner
            .client
            .post(&self.inner.endpoint)
            // TODO: resolve this clone... Hmmm
            .body(data.clone())
            .timeout(send_options.timeout.clone())
            .headers(headers)
            .send()
            .await?;
        // check resp status for 200, err on others
        if !resp.status().is_success() {
            let status = resp.status().clone();
            let content = resp.text().await?;
            return ia_err!(
                "got unexpected http status: {}, response text: {}",
                status,
                content
            );
        }

        // read request id in return, validate
        match Self::get_usize_header(resp.headers(), "x-nosql-request-id") {
            Ok(rid) => {
                if request_id != rid {
                    // TODO: if rid is less, loop again to read next response
                    // In theory, this should never happen with http 1.1...
                    return ia_err!("expected request_id {}, found {}", request_id, rid);
                }
            }
            Err(e) => {
                return ia_err!("can't get request_id from response: {}", e.to_string());
            }
        }
        //println!("Response status={} headers:", resp.status());
        //for (key, value) in resp.headers().iter() {
        //println!("  {:?}: {:?}", key, value);
        //}
        // get session cookie, if available
        for i in resp.cookies() {
            if i.name() == "session" {
                let mut sguard = self.inner.session.lock().unwrap();
                *sguard = i.value().to_string();
                trace!("Setting session={}", i.value());
            }
        }
        let result = resp.bytes().await?;
        // TODO: some way to avoid this copy
        Ok(result.to_vec())
    }

    // TODO: opCode
    pub(crate) async fn send_and_receive(
        &self,
        w: Writer,
        send_options: &mut SendOptions,
    ) -> Result<Reader, NoSQLError> {
        send_options.retries = 0;
        loop {
            match self.send_and_receive_once(&w, send_options).await {
                Ok(r) => return Ok(r),
                Err(e) => {
                    if e.code == InternalRetry {
                        send_options.retries += 1;
                        //tokio::time::sleep(Duration::from_millis(30)).await;
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    pub(crate) async fn send_and_receive_once(
        &self,
        w: &Writer,
        send_options: &mut SendOptions,
    ) -> Result<Reader, NoSQLError> {
        let bytes = self.post_data(&w.buf, send_options).await?;

        //println!("returned data: len={}", bytes.len());
        let mut r = Reader::new().from_bytes(&bytes);
        let m = MapWalker::check_reader_for_error(&mut r);
        if m.is_ok() {
            return Ok(r);
        }
        let err = m.unwrap_err();
        // this is very specific: If we get a SIU error, and it has a specific string,
        // it's likely that the service should have retried internally but did not for
        // some reason. In this case, delay a bit and retry with the same auth header.
        // allow for up to 4 retries, in case the routing to the service is doing round-robin
        // across instances (typically 3 in NoSQL cloud).
        // TODO: check current nano versus timeout at start of request
        if send_options.retries < 40 && err.code == NoSQLErrorCode::SecurityInfoUnavailable {
            // Note space at end of this message
            if err.message == "NotAuthenticated. " {
                // TODO: check remaining time for request based on timeout
                tokio::time::sleep(Duration::from_millis(30)).await;
                trace!("waited 30ms, now retrying SIU error");
                return Err(NoSQLError::new(InternalRetry, ""));
            }
        }
        // For other auth errors, try refreshing the auth provider. It may have
        // expired credentials.
        if send_options.retries < 4
            && (err.code == NoSQLErrorCode::SecurityInfoUnavailable
                || err.code == NoSQLErrorCode::RetryAuthentication
                || err.code == NoSQLErrorCode::InvalidAuthorization)
        {
            let refreshed = self
                .inner
                .builder
                .refresh_auth(&self.inner.client)
                .await
                .map_err(|e| {
                    NoSQLError::new(
                        err.code,
                        format!(
                            "error trying to refresh authentication provider: {}",
                            e.to_string()
                        )
                        .as_str(),
                    )
                })?;
            if refreshed {
                trace!("Refreshed auth provider: retrying");
                return Err(NoSQLError::new(InternalRetry, ""));
            }
            trace!("attempt to refresh generated no error but did not refresh auth");
        }
        Err(err)
    }

    pub(crate) fn get_timeout(&self, t: &Option<Duration>) -> Duration {
        // if t is given, use that. If not, use handle's timeout
        if let Some(d) = t {
            return d.clone();
        }
        self.inner.timeout.clone()
    }
}

#[derive(Debug, Default)]
pub(crate) struct SendOptions {
    #[allow(dead_code)]
    pub(crate) retryable: bool,
    pub(crate) retries: u16,
    pub(crate) timeout: Duration,
    pub(crate) compartment_id: String,
    pub(crate) namespace: String,
}
