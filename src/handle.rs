//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
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
use crate::rate_limiter::RateLimiter;
use crate::reader::Reader;
use crate::table_request::{GetTableRequest, TableRequest};
use crate::types::{Capacity, TableLimits};
use crate::writer::Writer;

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::result::Result;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, trace};
use url::Url;

const RATE_LIMITER_DURATION_SECS: f64 = 30.0;
const RATE_LIMITER_REFRESH_INTERVAL: Duration = Duration::from_secs(600);
const RATE_LIMITER_RETRY_INTERVAL: Duration = Duration::from_millis(100);
const RATE_LIMITER_METADATA_TIMEOUT: Duration = Duration::from_secs(1);
const SIU_NOT_AUTHENTICATED_MESSAGE: &str = "NotAuthenticated. ";

/// **The main database handle**.
///
/// This should be created once and used
/// throughout the application lifetime, across all threads.
///
/// Note: there is no need to enclose this struct in an `Rc` or [`Arc`], as it uses an
/// [`Arc`] internally, so calling `.clone()` on this struct will always return the
/// same underlying handle.
#[derive(Clone)]
pub struct Handle {
    // Use an inner Arc so cloning keeps the same contents
    pub(crate) inner: Arc<HandleRef>,
}

impl fmt::Debug for Handle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Handle")
            .field("inner", &self.inner)
            .finish()
    }
}

pub(crate) struct HandleRef {
    pub(crate) client: reqwest::Client,
    pub(crate) endpoint: String,
    pub(crate) serial_version: i16,
    pub(crate) builder: HandleBuilder,
    rate_limiter_map: Option<RateLimiterMap>,
    // session doesn't require a tokio Mutex because it's never held across awaits
    session: std::sync::Mutex<String>,
    request_id: AtomicUsize,
    timeout: Duration,
}

impl fmt::Debug for HandleRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HandleRef")
            .field("endpoint", &self.endpoint)
            .field("serial_version", &self.serial_version)
            .field("builder", &self.builder)
            .field("session", &"[redacted]")
            .field("request_id", &self.request_id)
            .field("timeout", &self.timeout)
            .finish()
    }
}

#[derive(Clone, Debug)]
struct RateLimiterEntry {
    read_limiter: Arc<tokio::sync::Mutex<RateLimiter>>,
    write_limiter: Arc<tokio::sync::Mutex<RateLimiter>>,
    read_units: f64,
    write_units: f64,
}

#[derive(Debug)]
struct RateLimiterMap {
    limiters: std::sync::Mutex<HashMap<RateLimiterKey, RateLimiterEntry>>,
    refresh_after: std::sync::Mutex<HashMap<RateLimiterKey, Instant>>,
    refreshing: std::sync::Mutex<HashSet<RateLimiterKey>>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct RateLimiterKey {
    table_name: String,
    compartment_id: String,
}

impl RateLimiterEntry {
    fn new(read_units: f64, write_units: f64) -> Self {
        RateLimiterEntry {
            read_limiter: Arc::new(tokio::sync::Mutex::new(RateLimiter::new_with_duration(
                read_units,
                RATE_LIMITER_DURATION_SECS,
            ))),
            write_limiter: Arc::new(tokio::sync::Mutex::new(RateLimiter::new_with_duration(
                write_units,
                RATE_LIMITER_DURATION_SECS,
            ))),
            read_units,
            write_units,
        }
    }

    fn has_limits(&self, read_units: f64, write_units: f64) -> bool {
        self.read_units == read_units && self.write_units == write_units
    }
}

impl RateLimiterMap {
    fn new() -> Self {
        RateLimiterMap {
            limiters: std::sync::Mutex::new(HashMap::new()),
            refresh_after: std::sync::Mutex::new(HashMap::new()),
            refreshing: std::sync::Mutex::new(HashSet::new()),
        }
    }

    fn key(table_name: &str, compartment_id: &str) -> RateLimiterKey {
        RateLimiterKey {
            table_name: table_name.to_lowercase(),
            compartment_id: compartment_id.to_string(),
        }
    }

    fn get(&self, table_name: &str, compartment_id: &str) -> Option<RateLimiterEntry> {
        if table_name.is_empty() {
            return None;
        }
        self.limiters
            .lock()
            .unwrap()
            .get(&Self::key(table_name, compartment_id))
            .cloned()
    }

    fn needs_refresh(&self, table_name: &str, compartment_id: &str) -> bool {
        if table_name.is_empty() {
            return false;
        }
        let key = Self::key(table_name, compartment_id);
        let guard = self.refresh_after.lock().unwrap();
        match guard.get(&key) {
            Some(refresh_after) => *refresh_after <= Instant::now(),
            None => true,
        }
    }

    fn mark_refreshed(&self, table_name: &str, compartment_id: &str) {
        self.mark_next_refresh(table_name, compartment_id, RATE_LIMITER_REFRESH_INTERVAL);
    }

    fn mark_retry_soon(&self, table_name: &str, compartment_id: &str) {
        self.mark_next_refresh(table_name, compartment_id, RATE_LIMITER_RETRY_INTERVAL);
    }

    fn mark_next_refresh(&self, table_name: &str, compartment_id: &str, delay: Duration) {
        if table_name.is_empty() {
            return;
        }
        self.refresh_after.lock().unwrap().insert(
            Self::key(table_name, compartment_id),
            Instant::now() + delay,
        );
    }

    fn try_start_refresh(&self, table_name: &str, compartment_id: &str) -> bool {
        if table_name.is_empty() {
            return false;
        }
        self.refreshing
            .lock()
            .unwrap()
            .insert(Self::key(table_name, compartment_id))
    }

    fn finish_refresh(&self, table_name: &str, compartment_id: &str) {
        if table_name.is_empty() {
            return;
        }
        self.refreshing
            .lock()
            .unwrap()
            .remove(&Self::key(table_name, compartment_id));
    }

    fn update(
        &self,
        table_name: &str,
        compartment_id: &str,
        limits: Option<&TableLimits>,
        percentage: f64,
    ) -> bool {
        if table_name.is_empty() {
            return false;
        }
        self.mark_refreshed(table_name, compartment_id);

        let key = Self::key(table_name, compartment_id);
        let Some(limits) = limits else {
            self.limiters.lock().unwrap().remove(&key);
            return false;
        };

        if limits.read_units <= 0 && limits.write_units <= 0 {
            self.limiters.lock().unwrap().remove(&key);
            return false;
        }

        let read_units = (limits.read_units as f64 * percentage) / 100.0;
        let write_units = (limits.write_units as f64 * percentage) / 100.0;
        let mut guard = self.limiters.lock().unwrap();
        if guard
            .get(&key)
            .is_some_and(|entry| entry.has_limits(read_units, write_units))
        {
            return true;
        }
        guard.insert(key, RateLimiterEntry::new(read_units, write_units));
        true
    }
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
            "Creating new Handle: mode={:?}, auth_type={:?}, endpoint={}",
            builder.mode, builder.auth_type, ep
        );
        let rate_limiter_map =
            if builder.rate_limiting_enabled && builder.mode != HandleMode::Onprem {
                Some(RateLimiterMap::new())
            } else {
                None
            };
        Ok(Handle {
            inner: Arc::new(HandleRef {
                client: c,
                endpoint: ep,
                serial_version: 4,
                builder: builder,
                rate_limiter_map,
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
        self.inner.builder.refresh_auth_if_needed().await?;

        let request_id = self.inner.request_id.fetch_add(1, Ordering::Relaxed);
        let mut headers = HeaderMap::new();
        headers.insert("x-nosql-request-id", HeaderValue::from(request_id));

        // If there is an oci auth provider, use that to set up required headers
        let mut oci_provider: Option<&Box<dyn AuthenticationProvider>> = None;
        let mut requires_explicit_compartment = false;

        // We need to lock the auth config because it may be asynchronously refreshed elsewhere
        let pguard = self.inner.builder.auth.lock().await;
        match &pguard.provider {
            AuthProvider::Instance { provider } => {
                oci_provider = Some(provider);
                requires_explicit_compartment = true;
            }
            AuthProvider::Resource { provider } => {
                oci_provider = Some(provider);
                requires_explicit_compartment = true;
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
            let compartment_id = Self::effective_compartment_id(
                send_options,
                &self.inner.builder.default_compartment_id,
                sp.tenancy_id(),
                requires_explicit_compartment,
            )?;
            headers.insert(
                "x-nosql-compartment-id",
                HeaderValue::from_str(&compartment_id)?,
            );
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
                trace!("setting session cookie from response");
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
        self.send_and_receive_internal(w, send_options, true).await
    }

    async fn send_and_receive_without_rate_limiting(
        &self,
        w: Writer,
        send_options: &mut SendOptions,
    ) -> Result<Reader, NoSQLError> {
        self.send_and_receive_internal(w, send_options, false).await
    }

    async fn send_and_receive_internal(
        &self,
        w: Writer,
        send_options: &mut SendOptions,
        apply_rate_limiting: bool,
    ) -> Result<Reader, NoSQLError> {
        send_options.retries = 0;
        loop {
            match self
                .send_and_receive_once_internal(&w, send_options, apply_rate_limiting)
                .await
            {
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

    async fn send_and_receive_once_internal(
        &self,
        w: &Writer,
        send_options: &mut SendOptions,
        apply_rate_limiting: bool,
    ) -> Result<Reader, NoSQLError> {
        if apply_rate_limiting {
            self.apply_rate_limiting(send_options).await?;
        }
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
        if Self::should_retry_not_authenticated(send_options, &err) {
            // TODO: check remaining time for request based on timeout
            tokio::time::sleep(Duration::from_millis(30)).await;
            trace!("waited 30ms, now retrying SIU error");
            return Err(NoSQLError::new(InternalRetry, ""));
        }
        // For other auth errors, try refreshing the auth provider. It may have
        // expired credentials.
        if Self::should_refresh_auth_for_retry(send_options, &err) {
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

    fn should_retry_not_authenticated(send_options: &SendOptions, err: &NoSQLError) -> bool {
        send_options.retryable
            && send_options.retries < 40
            && err.code == NoSQLErrorCode::SecurityInfoUnavailable
            && err.message == SIU_NOT_AUTHENTICATED_MESSAGE
    }

    fn should_refresh_auth_for_retry(send_options: &SendOptions, err: &NoSQLError) -> bool {
        send_options.retryable
            && send_options.retries < 4
            && (err.code == NoSQLErrorCode::SecurityInfoUnavailable
                || err.code == NoSQLErrorCode::RetryAuthentication
                || err.code == NoSQLErrorCode::InvalidAuthorization)
    }

    fn effective_compartment_id(
        send_options: &SendOptions,
        default_compartment_id: &str,
        tenancy_id: &str,
        requires_explicit_compartment: bool,
    ) -> Result<String, NoSQLError> {
        if !send_options.compartment_id.is_empty() {
            return Ok(send_options.compartment_id.clone());
        }
        if !default_compartment_id.is_empty() {
            return Ok(default_compartment_id.to_string());
        }
        if requires_explicit_compartment {
            return ia_err!(
                "instance principal and resource principal authentication require an explicit compartment id"
            );
        }
        Ok(tenancy_id.to_string())
    }

    async fn apply_rate_limiting(&self, send_options: &mut SendOptions) -> Result<(), NoSQLError> {
        if !send_options.does_reads && !send_options.does_writes {
            return Ok(());
        }
        let Some(entry) = self
            .rate_limiters_for_table(
                &send_options.table_name,
                &send_options.compartment_id,
                send_options.timeout,
            )
            .await
        else {
            return Ok(());
        };

        if send_options.does_reads {
            send_options.rate_limit_delayed_ms +=
                Self::consume_rate_limiter(&entry.read_limiter, 0, send_options.timeout, true)
                    .await?;
        }
        if send_options.does_writes {
            send_options.rate_limit_delayed_ms +=
                Self::consume_rate_limiter(&entry.write_limiter, 0, send_options.timeout, true)
                    .await?;
        }
        Ok(())
    }

    pub(crate) async fn consume_rate_limited_capacity(
        &self,
        send_options: &mut SendOptions,
        table_name: &str,
        consumed: &Capacity,
    ) {
        if consumed.read_units <= 0 && consumed.write_kb <= 0 {
            return;
        }
        let Some(entry) = self
            .rate_limiters_for_table(
                table_name,
                &send_options.compartment_id,
                send_options.timeout,
            )
            .await
        else {
            return;
        };

        if consumed.read_units > 0 {
            let delay = Self::consume_rate_limiter(
                &entry.read_limiter,
                consumed.read_units as i64,
                send_options.timeout,
                false,
            )
            .await
            .unwrap_or(0);
            send_options.rate_limit_delayed_ms += delay;
        }

        if consumed.write_kb > 0 {
            let delay = Self::consume_rate_limiter(
                &entry.write_limiter,
                consumed.write_kb as i64,
                send_options.timeout,
                false,
            )
            .await
            .unwrap_or(0);
            send_options.rate_limit_delayed_ms += delay;
        }
    }

    async fn consume_rate_limiter(
        limiter: &Arc<tokio::sync::Mutex<RateLimiter>>,
        units: i64,
        timeout: Duration,
        fail_on_timeout: bool,
    ) -> Result<i64, NoSQLError> {
        let timeout_ms = Self::duration_to_millis(timeout);
        let delay_ms = {
            let mut guard = limiter.lock().await;
            guard.reserve_units_with_timeout(units, timeout_ms, false)
        };

        match delay_ms {
            Ok(delay_ms) => {
                if delay_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(delay_ms as u64)).await;
                }
                Ok(delay_ms)
            }
            Err(e) => {
                if timeout_ms > 0 {
                    tokio::time::sleep(Duration::from_millis(timeout_ms as u64)).await;
                }
                if fail_on_timeout {
                    return Err(NoSQLError::new(
                        NoSQLErrorCode::RequestTimeout,
                        &format!(
                            "timed out waiting {}ms due to rate limiting: {}",
                            timeout_ms,
                            e.to_string()
                        ),
                    ));
                }
                Ok(timeout_ms)
            }
        }
    }

    async fn rate_limiters_for_table(
        &self,
        table_name: &str,
        compartment_id: &str,
        timeout: Duration,
    ) -> Option<RateLimiterEntry> {
        let Some(map) = &self.inner.rate_limiter_map else {
            return None;
        };
        if table_name.is_empty() {
            return None;
        }
        let compartment_id = self.effective_rate_limiter_compartment_id(compartment_id);
        if let Some(entry) = map.get(table_name, &compartment_id) {
            if map.needs_refresh(table_name, &compartment_id) {
                self.refresh_rate_limiter_in_background(
                    table_name.to_string(),
                    compartment_id.clone(),
                    timeout,
                );
            }
            return Some(entry);
        }

        if !map.needs_refresh(table_name, &compartment_id) {
            return None;
        }
        if !map.try_start_refresh(table_name, &compartment_id) {
            return None;
        }

        self.refresh_rate_limiter(table_name.to_string(), compartment_id.clone(), timeout)
            .await;
        map.get(table_name, &compartment_id)
    }

    fn refresh_rate_limiter_in_background(
        &self,
        table_name: String,
        compartment_id: String,
        timeout: Duration,
    ) {
        let Some(map) = &self.inner.rate_limiter_map else {
            return;
        };
        if !map.try_start_refresh(&table_name, &compartment_id) {
            return;
        }

        let handle = self.clone();
        tokio::spawn(async move {
            handle
                .refresh_rate_limiter(table_name, compartment_id, timeout)
                .await;
        });
    }

    async fn refresh_rate_limiter(
        &self,
        table_name: String,
        compartment_id: String,
        timeout: Duration,
    ) {
        let Some(map) = &self.inner.rate_limiter_map else {
            return;
        };
        let metadata_timeout = if timeout < RATE_LIMITER_METADATA_TIMEOUT {
            timeout
        } else {
            RATE_LIMITER_METADATA_TIMEOUT
        };
        let mut request = GetTableRequest::new(&table_name).timeout(&metadata_timeout);
        if !compartment_id.is_empty() {
            request = request.compartment_id(&compartment_id);
        }
        let mut w: Writer = Writer::new();
        w.write_i16(self.inner.serial_version);
        request.nson_serialize(&mut w, &metadata_timeout);
        let mut opts = SendOptions {
            timeout: metadata_timeout,
            retryable: true,
            compartment_id: compartment_id.clone(),
            ..Default::default()
        };
        match Box::pin(self.send_and_receive_without_rate_limiting(w, &mut opts)).await {
            Ok(mut r) => match TableRequest::nson_deserialize(&mut r) {
                Ok(resp) => {
                    let result_table_name = if resp.table_name.is_empty() {
                        &table_name
                    } else {
                        &resp.table_name
                    };
                    self.update_rate_limiters(
                        result_table_name,
                        &compartment_id,
                        resp.limits.as_ref(),
                    );
                }
                Err(e) => {
                    trace!(
                        "rate limiter GetTableRequest for table '{}' failed: {}",
                        table_name,
                        e
                    );
                    map.mark_retry_soon(&table_name, &compartment_id);
                }
            },
            Err(e) => {
                trace!(
                    "rate limiter GetTableRequest for table '{}' failed: {}",
                    table_name,
                    e
                );
                map.mark_retry_soon(&table_name, &compartment_id);
            }
        }
        map.finish_refresh(&table_name, &compartment_id);
    }

    pub(crate) fn update_rate_limiters(
        &self,
        table_name: &str,
        compartment_id: &str,
        limits: Option<&TableLimits>,
    ) -> bool {
        let Some(map) = &self.inner.rate_limiter_map else {
            return false;
        };
        let compartment_id = self.effective_rate_limiter_compartment_id(compartment_id);
        map.update(
            table_name,
            &compartment_id,
            limits,
            self.inner.builder.get_rate_limiting_percentage(),
        )
    }

    fn effective_rate_limiter_compartment_id(&self, compartment_id: &str) -> String {
        if !compartment_id.is_empty() {
            return compartment_id.to_string();
        }
        self.inner.builder.default_compartment_id.clone()
    }

    fn duration_to_millis(duration: Duration) -> i64 {
        duration.as_millis().min(i64::MAX as u128) as i64
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
    pub(crate) table_name: String,
    pub(crate) does_reads: bool,
    pub(crate) does_writes: bool,
    #[allow(dead_code)]
    pub(crate) rate_limit_delayed_ms: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limiter_map_keys_limiters_by_compartment() {
        let map = RateLimiterMap::new();
        let limits = TableLimits::provisioned(100, 200, 1);

        assert!(map.update("Users", "compartment-a", Some(&limits), 100.0));
        let compartment_a = map.get("users", "compartment-a").unwrap();
        assert_eq!(compartment_a.read_units, 100.0);
        assert_eq!(compartment_a.write_units, 200.0);

        assert!(map.get("USERS", "compartment-a").is_some());
        assert!(map.get("users", "compartment-b").is_none());
        assert!(!map.needs_refresh("users", "compartment-a"));
        assert!(map.needs_refresh("users", "compartment-b"));
        map.mark_next_refresh("users", "compartment-a", Duration::ZERO);
        assert!(map.get("users", "compartment-a").is_some());
        assert!(map.needs_refresh("users", "compartment-a"));
        assert!(map.try_start_refresh("users", "compartment-a"));
        assert!(!map.try_start_refresh("users", "compartment-a"));
        assert!(map.try_start_refresh("users", "compartment-b"));
        map.finish_refresh("users", "compartment-a");
        assert!(map.try_start_refresh("users", "compartment-a"));
        map.finish_refresh("users", "compartment-a");
        map.finish_refresh("users", "compartment-b");

        assert!(map.update("users", "compartment-a", Some(&limits), 100.0));
        let unchanged_compartment_a = map.get("users", "compartment-a").unwrap();
        assert!(std::sync::Arc::ptr_eq(
            &compartment_a.read_limiter,
            &unchanged_compartment_a.read_limiter
        ));

        let changed_limits = TableLimits::provisioned(101, 200, 1);
        assert!(map.update("users", "compartment-a", Some(&changed_limits), 100.0));
        let changed_compartment_a = map.get("users", "compartment-a").unwrap();
        assert!(!std::sync::Arc::ptr_eq(
            &compartment_a.read_limiter,
            &changed_compartment_a.read_limiter
        ));

        assert!(map.update("users", "compartment-b", Some(&limits), 100.0));
        let compartment_b = map.get("users", "compartment-b").unwrap();
        assert!(!std::sync::Arc::ptr_eq(
            &compartment_a.read_limiter,
            &compartment_b.read_limiter
        ));
    }

    #[test]
    fn handle_ref_debug_redacts_session_cookie() {
        let handle_ref = HandleRef {
            client: reqwest::Client::new(),
            endpoint: "https://example.com/V2/nosql/data".to_string(),
            serial_version: 4,
            builder: HandleBuilder::new(),
            session: std::sync::Mutex::new("secret-session-cookie".to_string()),
            request_id: AtomicUsize::new(1),
            timeout: Duration::new(30, 0),
            rate_limiter_map: None,
        };

        let debug = format!("{:?}", handle_ref);

        assert!(!debug.contains("secret-session-cookie"));
        assert!(debug.contains("[redacted]"));
    }

    #[test]
    fn internal_auth_retries_respect_send_options_retryable() {
        let err = NoSQLError::new(
            NoSQLErrorCode::SecurityInfoUnavailable,
            SIU_NOT_AUTHENTICATED_MESSAGE,
        );
        let non_retryable = SendOptions {
            retryable: false,
            ..Default::default()
        };

        assert!(!Handle::should_retry_not_authenticated(
            &non_retryable,
            &err
        ));
        assert!(!Handle::should_refresh_auth_for_retry(&non_retryable, &err));

        let retryable = SendOptions {
            retryable: true,
            ..Default::default()
        };

        assert!(Handle::should_retry_not_authenticated(&retryable, &err));
        assert!(Handle::should_refresh_auth_for_retry(&retryable, &err));
    }

    #[test]
    fn internal_auth_retry_limits_are_enforced() {
        let err = NoSQLError::new(NoSQLErrorCode::InvalidAuthorization, "expired");

        assert!(Handle::should_refresh_auth_for_retry(
            &SendOptions {
                retryable: true,
                retries: 3,
                ..Default::default()
            },
            &err
        ));
        assert!(!Handle::should_refresh_auth_for_retry(
            &SendOptions {
                retryable: true,
                retries: 4,
                ..Default::default()
            },
            &err
        ));

        let siu = NoSQLError::new(
            NoSQLErrorCode::SecurityInfoUnavailable,
            SIU_NOT_AUTHENTICATED_MESSAGE,
        );
        assert!(Handle::should_retry_not_authenticated(
            &SendOptions {
                retryable: true,
                retries: 39,
                ..Default::default()
            },
            &siu
        ));
        assert!(!Handle::should_retry_not_authenticated(
            &SendOptions {
                retryable: true,
                retries: 40,
                ..Default::default()
            },
            &siu
        ));
    }

    #[test]
    fn effective_compartment_requires_explicit_value_for_instance_and_resource_principals() {
        let err = Handle::effective_compartment_id(
            &SendOptions::default(),
            "",
            "ocid1.tenancy.oc1..root",
            true,
        )
        .unwrap_err();

        assert!(err.message.contains("explicit compartment id"));

        let request_compartment = Handle::effective_compartment_id(
            &SendOptions {
                compartment_id: "ocid1.compartment.oc1..request".to_string(),
                ..Default::default()
            },
            "",
            "ocid1.tenancy.oc1..root",
            true,
        )
        .unwrap();
        assert_eq!(request_compartment, "ocid1.compartment.oc1..request");

        let default_compartment = Handle::effective_compartment_id(
            &SendOptions::default(),
            "ocid1.compartment.oc1..default",
            "ocid1.tenancy.oc1..root",
            true,
        )
        .unwrap();
        assert_eq!(default_compartment, "ocid1.compartment.oc1..default");
    }

    #[test]
    fn effective_compartment_keeps_tenancy_fallback_for_user_principals() {
        let compartment = Handle::effective_compartment_id(
            &SendOptions::default(),
            "",
            "ocid1.tenancy.oc1..root",
            false,
        )
        .unwrap();

        assert_eq!(compartment, "ocid1.tenancy.oc1..root");
    }
}
