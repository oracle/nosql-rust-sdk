//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
//! Builder for creating a [`NoSQL Handle`](crate::Handle)
//!

use base64::prelude::{Engine as _, BASE64_STANDARD};
use std::default::Default;
use std::env;
use std::fmt;
use std::result::Result;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::auth_common::authentication_provider::AuthenticationProvider;
use crate::auth_common::config_file_authentication_provider::ConfigFileAuthenticationProvider;
use crate::auth_common::instance_principal_auth_provider::InstancePrincipalAuthProvider;
use crate::auth_common::resource_principal_auth_provider::ResourcePrincipalAuthProvider;
use crate::error::{ia_err, NoSQLError};
use crate::handle::Handle;
use crate::stats::{StatsHandler, StatsHandlerRef, StatsPercentileMode, StatsProfile};
use reqwest::header::HeaderValue;
use reqwest::Client;
use reqwest::{header::HeaderMap, Certificate};
use serde_derive::Deserialize;

use crate::region::{file_to_string, string_to_region, Region};

/// Builder used to set all the parameters to create a [`NoSQL Handle`](crate::Handle).
///
/// See [Configuring the SDK](index.html#configuring-the-sdk) for a detailed description of creating configurations for
/// various Oracle NoSQL Database instance types (cloud, on-premises, etc.).
///
#[derive(Default, Clone)]
pub struct HandleBuilder {
    pub(crate) endpoint: String,
    pub(crate) timeout: Option<Duration>,
    pub(crate) stats_profile: StatsProfile,
    pub(crate) stats_interval: Option<Duration>,
    pub(crate) stats_pretty_print: bool,
    pub(crate) stats_enable_log: Option<bool>,
    pub(crate) stats_percentile_mode: StatsPercentileMode,
    pub(crate) stats_handler: StatsHandlerRef,
    pub(crate) region: Option<Region>,
    // TODO
    //pub(crate) allow_imds: bool,
    pub(crate) use_https: bool,
    pub(crate) mode: HandleMode,
    pub(crate) add_cert: Option<Certificate>,
    pub(crate) client: Option<Client>,
    pub(crate) accept_invalid_certs: bool,
    pub(crate) auth_type: AuthType,
    // auth uses a tokio Mutex because we occasionally hold a lock across awaits
    pub(crate) auth: Arc<tokio::sync::Mutex<AuthConfig>>,
    // For doc testing
    pub(crate) in_test: bool,
    // For error messaging
    pub(crate) from_environment: bool,
    pub(crate) default_compartment_id: String,
    pub(crate) rate_limiting_enabled: bool,
    pub(crate) default_rate_limiter_percentage: f64,
}

impl fmt::Debug for HandleBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HandleBuilder")
            .field("endpoint", &self.endpoint)
            .field("timeout", &self.timeout)
            .field("region", &self.region)
            .field("use_https", &self.use_https)
            .field("mode", &self.mode)
            .field("add_cert", &self.add_cert.as_ref().map(|_| "[present]"))
            .field("client", &self.client.as_ref().map(|_| "[present]"))
            .field("accept_invalid_certs", &self.accept_invalid_certs)
            .field("auth_type", &self.auth_type)
            .field("auth", &"[redacted]")
            .field("in_test", &self.in_test)
            .field("from_environment", &self.from_environment)
            .field("default_compartment_id", &self.default_compartment_id)
            .finish()
    }
}

#[derive(Default)]
pub(crate) struct AuthConfig {
    pub(crate) provider: AuthProvider,
}

impl fmt::Debug for AuthConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AuthConfig")
            .field("provider", &self.provider)
            .finish()
    }
}

#[derive(Default)]
pub(crate) enum AuthProvider {
    File {
        //path: String,
        //profile: String,
        provider: Box<dyn AuthenticationProvider>,
    },
    Instance {
        provider: Box<dyn AuthenticationProvider>,
        // TODO: last_refresh
        // TODO: tenantId, compartmentId, region, domain, etc...
    },
    Resource {
        provider: Box<dyn AuthenticationProvider>,
        // TODO: is refreshable? expiration, etc
    },
    External {
        provider: Box<dyn AuthenticationProvider>,
    },
    Onprem {
        // TODO: cert paths?
        provider: Option<OnpremAuthProvider>,
    },
    #[default]
    None,
}

impl fmt::Debug for AuthProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AuthProvider::File { .. } => f
                .debug_struct("File")
                .field("provider", &"[redacted]")
                .finish(),
            AuthProvider::Instance { .. } => f
                .debug_struct("Instance")
                .field("provider", &"[redacted]")
                .finish(),
            AuthProvider::Resource { .. } => f
                .debug_struct("Resource")
                .field("provider", &"[redacted]")
                .finish(),
            AuthProvider::External { .. } => f
                .debug_struct("External")
                .field("provider", &"[redacted]")
                .finish(),
            AuthProvider::Onprem { provider } => f
                .debug_struct("Onprem")
                .field("provider", &provider.as_ref().map(|_| "[redacted]"))
                .finish(),
            AuthProvider::None => f.write_str("None"),
        }
    }
}

#[derive(Default, Debug, Clone, PartialEq)]
pub(crate) enum AuthType {
    File,
    Instance,
    Resource,
    External,
    Onprem,
    Cloudsim,
    #[default]
    None,
}

/// The Oracle NoSQL Database mode to use.
#[derive(Default, Debug, Clone, PartialEq)]
pub enum HandleMode {
    /// Connect to the Oracle NoSQL Cloud Service.
    #[default]
    Cloud,
    /// Connect to a local Cloudsim instance (typically for testing purposes).
    Cloudsim,
    /// Connect to an on-premises installation of NoSQL Database Server.
    Onprem,
}

impl HandleBuilder {
    /// Create a new HandleBuilder struct.
    ///
    /// The default HandleBuilder does not set an authentication method. Consider calling
    /// [`from_environment()`](HandleBuilder::from_environment()) to collect all parameters from
    /// the local environment by default.
    pub fn new() -> Self {
        HandleBuilder {
            ..Default::default()
        }
    }
    /// Build a new [`Handle`].
    ///
    /// Note: Internally, if the [`HandleBuilder`] contains
    /// a reference to an existing [`reqwest::Client`], it will clone and
    /// use that. Otherwise, it will create a new [`reqwest::Client`] for its
    /// own internal use. See [`reqwest_client()`](HandleBuilder::reqwest_client()).
    pub async fn build(self) -> Result<Handle, NoSQLError> {
        Handle::new(&self).await
    }
    /// Gather configuration settings from the current envrionment.
    ///
    /// This method will scan the process [`standard environment`](std::env::Vars) to collect and
    /// set the configuration parameters. The values can be overridden in code if this method is
    /// called first and other methods are called afterwards, for example:
    ///```no_run
    /// # use oracle_nosql_rust_sdk::Handle;
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    ///   let builder = Handle::builder()
    ///       .from_environment()?
    ///       .cloud_auth_from_file("~/nosql_oci_config")?;
    /// # Ok(())
    /// # }
    ///```
    /// The following environment variables are used:
    ///
    /// | variable | description |
    /// | -------- | ----------- |
    /// | `ORACLE_NOSQL_ENDPOINT` | The URL endpoint to use. See [`HandleBuilder::endpoint()`]. |
    /// | `ORACLE_NOSQL_REGION` | The OCI region identifier. See [`HandleBuilder::cloud_region()`]. |
    /// | `ORACLE_NOSQL_AUTH` | The auth mechanism. One of: `user`, `instance`, `resource`, `onprem`, `cloudsim`. |
    /// | `ORACLE_NOSQL_AUTH_FILE` | For `user` auth, the path to the OCI config file (see [`HandleBuilder::cloud_auth_from_file()`]). For `onprem` auth, the path to the onprem user/password file (see [`HandleBuilder::onprem_auth_from_file()`]).
    /// | `ORACLE_NOSQL_COMPARTMENT_ID` | For OCI auth, the default compartment id to use (see [`HandleBuilder::compartment_id()`]).
    /// | `ORACLE_NOSQL_CA_CERT` | For `onprem` auth, the path to the certificate file in `pem` format (see [`HandleBuilder::add_cert_from_pemfile()`]). |
    /// | `ORACLE_NOSQL_ACCEPT_INVALID_CERTS` | For `onprem` auth, if this is set to `1` or `true`, do not check certificates (see [`HandleBuilder::danger_accept_invalid_certs()`]). |
    /// | `NOSQL_STATS_PROFILE` / `ORACLE_NOSQL_STATS_PROFILE` | The stats profile. One of: `NONE`, `REGULAR`, `MORE`, `ALL` (see [`HandleBuilder::stats_profile()`]). |
    /// | `NOSQL_STATS_INTERVAL` / `ORACLE_NOSQL_STATS_INTERVAL` | The stats interval in seconds (see [`HandleBuilder::stats_interval()`]). |
    /// | `NOSQL_STATS_PRETTY_PRINT` / `ORACLE_NOSQL_STATS_PRETTY_PRINT` | Whether future stats log output should be pretty-printed (see [`HandleBuilder::stats_pretty_print()`]). |
    /// | `NOSQL_STATS_ENABLE_LOG` / `ORACLE_NOSQL_STATS_ENABLE_LOG` | Whether future stats log output should be enabled (see [`HandleBuilder::stats_enable_log()`]). |
    /// | `NOSQL_STATS_PERCENTILE_MODE` / `ORACLE_NOSQL_STATS_PERCENTILE_MODE` | Percentile calculation mode for `MORE` and `ALL`. One of: `EXACT`, `HDR` (see [`HandleBuilder::stats_percentile_mode()`]). |
    ///
    pub fn from_environment(mut self) -> Result<Self, NoSQLError> {
        self.from_environment = true;
        let mut filename: Option<String> = None;
        if let Some(val) = env::var("ORACLE_NOSQL_AUTH_FILE").ok() {
            // TODO: verify file exists and is readable
            filename = Some(val);
        }
        if let Some(val) = env::var("ORACLE_NOSQL_ENDPOINT").ok() {
            self = self.endpoint(&val)?;
            // TODO: parse region from endpoint?
        }
        if let Some(val) = env::var("ORACLE_NOSQL_REGION").ok() {
            self = self.cloud_region(&val)?;
        }
        if let Some(val) = env::var("ORACLE_NOSQL_COMPARTMENT_ID").ok() {
            self = self.compartment_id(&val)?;
        }
        if let Some(val) = env::var("ORACLE_NOSQL_CA_CERT").ok() {
            self = self.add_cert_from_pemfile(&val)?;
        }
        if let Some(val) = env::var("ORACLE_NOSQL_ACCEPT_INVALID_CERTS").ok() {
            let lv = val.to_lowercase();
            if lv == "true" || lv == "1" {
                self = self.danger_accept_invalid_certs(true)?;
            }
        }
        self = self.apply_stats_environment(|name| env::var(name).ok())?;
        if let Some(val) = env::var("ORACLE_NOSQL_AUTH").ok() {
            let v = val.to_lowercase();
            match v.as_str() {
                "onprem" => {
                    // need user/pass?
                    if let Some(fname) = &filename {
                        self = self.onprem_auth_from_file(fname)?;
                    } else {
                        // TODO: error (need file)?
                        // Need a way to discern between insecure onprem and cloudsim
                    }
                    self.auth_type = AuthType::Onprem;
                }
                "resource" => self = self.cloud_auth_from_resource()?,
                "instance" => self = self.cloud_auth_from_instance()?,
                "user" => {
                    if let Some(fname) = &filename {
                        self = self.cloud_auth_from_file(fname)?;
                    } else {
                        self = self.cloud_auth_from_file("~/.oci/config")?;
                    }
                }
                "cloudsim" => {
                    self.mode = HandleMode::Cloudsim;
                    self.auth_type = AuthType::Cloudsim;
                }
                _ => {
                    return ia_err!("invalid value '{}' for ORACLE_NOSQL_AUTH", v);
                }
            }
        }
        Ok(self)
    }

    fn apply_stats_environment<F>(mut self, mut env_var: F) -> Result<Self, NoSQLError>
    where
        F: FnMut(&str) -> Option<String>,
    {
        if let Some((name, val)) = Self::stats_env_var_from(
            &mut env_var,
            "NOSQL_STATS_PROFILE",
            "ORACLE_NOSQL_STATS_PROFILE",
        ) {
            let profile = Self::parse_stats_profile(&val, &name)?;
            self = self.stats_profile(profile)?;
        }
        if let Some((name, val)) = Self::stats_env_var_from(
            &mut env_var,
            "NOSQL_STATS_INTERVAL",
            "ORACLE_NOSQL_STATS_INTERVAL",
        ) {
            let interval = Self::parse_stats_interval(&val, &name)?;
            self = self.stats_interval(interval)?;
        }
        if let Some((name, val)) = Self::stats_env_var_from(
            &mut env_var,
            "NOSQL_STATS_PRETTY_PRINT",
            "ORACLE_NOSQL_STATS_PRETTY_PRINT",
        ) {
            let pretty_print = Self::parse_stats_bool(&val, &name)?;
            self = self.stats_pretty_print(pretty_print)?;
        }
        if let Some((name, val)) = Self::stats_env_var_from(
            &mut env_var,
            "NOSQL_STATS_ENABLE_LOG",
            "ORACLE_NOSQL_STATS_ENABLE_LOG",
        ) {
            let enable_log = Self::parse_stats_bool(&val, &name)?;
            self = self.stats_enable_log(enable_log)?;
        }
        if let Some((name, val)) = Self::stats_env_var_from(
            &mut env_var,
            "NOSQL_STATS_PERCENTILE_MODE",
            "ORACLE_NOSQL_STATS_PERCENTILE_MODE",
        ) {
            let percentile_mode = Self::parse_stats_percentile_mode(&val, &name)?;
            self = self.stats_percentile_mode(percentile_mode)?;
        }
        Ok(self)
    }

    fn stats_env_var_from<F>(
        env_var: &mut F,
        python_name: &str,
        rust_name: &str,
    ) -> Option<(String, String)>
    where
        F: FnMut(&str) -> Option<String>,
    {
        if let Some(value) = env_var(rust_name) {
            Some((rust_name.to_string(), value))
        } else {
            env_var(python_name).map(|value| (python_name.to_string(), value))
        }
    }

    fn parse_stats_profile(value: &str, variable: &str) -> Result<StatsProfile, NoSQLError> {
        match value.parse::<StatsProfile>() {
            Ok(profile) => Ok(profile),
            Err(_) => ia_err!(
                "invalid value '{}' for {}. expected one of: NONE, REGULAR, MORE, ALL",
                value,
                variable
            ),
        }
    }

    fn parse_stats_interval(value: &str, variable: &str) -> Result<Duration, NoSQLError> {
        match value.trim().parse::<u64>() {
            Ok(seconds) if seconds > 0 => Ok(Duration::from_secs(seconds)),
            _ => ia_err!(
                "invalid value '{}' for {}. expected an integer number of seconds greater than zero",
                value,
                variable
            ),
        }
    }

    fn parse_stats_bool(value: &str, variable: &str) -> Result<bool, NoSQLError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "on" | "yes" => Ok(true),
            "false" | "0" | "off" | "no" => Ok(false),
            _ => ia_err!(
                "invalid value '{}' for {}. expected one of: true, false, 1, 0, on, off, yes, no",
                value,
                variable
            ),
        }
    }

    fn parse_stats_percentile_mode(
        value: &str,
        variable: &str,
    ) -> Result<StatsPercentileMode, NoSQLError> {
        match value.parse::<StatsPercentileMode>() {
            Ok(mode) => Ok(mode),
            Err(_) => ia_err!(
                "invalid value '{}' for {}. expected one of: EXACT, HDR",
                value,
                variable
            ),
        }
    }

    /// Set a specific endpoint connection to use.
    ///
    /// This is typically used when specifying a local cloudsim instance, or an
    /// on-premises instance of the Oracle NoSQL Database Server. It can also be used to
    /// override Cloud Service Region endpoints, or to specify an endpoint for a new
    /// Region that has not been previously added to the SDK internally.
    ///
    /// Examples:
    /// ```text
    ///     // Local cloudsim
    ///     http://localhost:8080
    ///
    ///     // Local on-premises server
    ///     https://<database_host>:8080
    ///
    ///     // Cloud service
    ///     https://nosql.us-ashburn-1.oci.oraclecloud.com
    /// ```
    pub fn endpoint(mut self, endpoint: &str) -> Result<Self, NoSQLError> {
        // normalize to just domain[:port]
        if endpoint.starts_with("https://") {
            self.use_https = true;
            let (_, b) = endpoint.split_at(8);
            self.endpoint = b.to_string();
        } else if endpoint.starts_with("http://") {
            self.use_https = false;
            let (_, b) = endpoint.split_at(7);
            self.endpoint = b.to_string();
        } else {
            self.endpoint = endpoint.to_string();
        }
        Ok(self)
    }
    /// Set the mode for the handle.
    ///
    /// Use [`HandleMode::Cloudsim`] to specify connection to a local cloudsim instance.
    ///
    /// Use [`HandleMode::Onprem`] when connecting to an on-premises NoSQL Server.
    ///
    /// By default, HandleBuilder assumes [`HandleMode::Cloud`].
    pub fn mode(mut self, mode: HandleMode) -> Result<Self, NoSQLError> {
        self.mode = mode;
        if self.mode == HandleMode::Cloudsim {
            self.auth_type = AuthType::Cloudsim;
        } else if self.mode == HandleMode::Onprem {
            self.auth_type = AuthType::Onprem;
        }
        Ok(self)
    }
    #[doc(hidden)]
    pub fn cloud_auth(
        mut self,
        provider: Box<dyn AuthenticationProvider>,
    ) -> Result<Self, NoSQLError> {
        // TODO: simple validation of provider?
        let ap = AuthProvider::External { provider: provider };
        self.auth = Arc::new(tokio::sync::Mutex::new(AuthConfig { provider: ap }));
        self.use_https = true;
        self.mode = HandleMode::Cloud;
        self.auth_type = AuthType::External;
        Ok(self)
    }
    /// Specify an OCI config file to use with user-based authentication.
    ///
    /// This method allows the use of a file other than the default `~/.oci/config` file.
    /// See [SDK and CLI Configuration File](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm) for details.
    /// This method assumes the use of the `"DEFAULT"` profile.
    pub fn cloud_auth_from_file(self, config_file: &str) -> Result<Self, NoSQLError> {
        self.cloud_auth_from_file_with_profile(config_file, "DEFAULT")
    }
    /// Specify an OCI config file to use with user-based authentication.
    ///
    /// This method allows the use of a file other than the default `~/.oci/config` file.
    /// See [SDK and CLI Configuration File](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm) for details.
    pub fn cloud_auth_from_file_with_profile(
        mut self,
        config_file: &str,
        profile: &str,
    ) -> Result<Self, NoSQLError> {
        let cfp = ConfigFileAuthenticationProvider::new_from_file(config_file, profile)?;
        if self.region.is_none() && !cfp.region_id().is_empty() {
            self = self.cloud_region(cfp.region_id())?;
        }
        let ap = AuthProvider::File {
            //path: config_file.to_string(),
            //profile: profile.to_string(),
            provider: Box::new(cfp),
        };
        self.auth = Arc::new(tokio::sync::Mutex::new(AuthConfig { provider: ap }));
        self.auth_type = AuthType::File;
        self.use_https = true;
        self.mode = HandleMode::Cloud;
        Ok(self)
    }
    // TODO: cloud_auth_from_session
    /// Specify using OCI Instance Principal for authentication.
    ///
    /// Instance Principal is an IAM service feature that enables instances to be authorized actors (or _principals_) to perform actions on service resources.
    /// If the application is running on an OCI compute instance in the Oracle Cloud,
    /// the SDK can make use of the instance environment to determine its credentials (no config file is required).
    /// Each compute instance has its own identity, and it authenticates using the certificates that are added to it.
    /// See [Calling Services from an Instance](https://docs.oracle.com/en-us/iaas/Content/Identity/Tasks/callingservicesfrominstances.htm) for prerequisite steps to set up Instance Principal.
    ///
    pub fn cloud_auth_from_instance(mut self) -> Result<Self, NoSQLError> {
        //let ifp = InstancePrincipalAuthProvider::new().await?;
        //let ap = AuthProvider::Instance {
        //provider: Box::new(ifp),
        //};
        //self.auth = Arc::new(tokio::sync::Mutex::new(AuthConfig { provider: ap }));
        self.auth_type = AuthType::Instance;
        self.use_https = true;
        self.mode = HandleMode::Cloud;
        Ok(self)
    }
    // TODO: cloud_auth_from_oke
    /// Specify using OCI Resource Principal for authentication.
    ///
    /// Resource Principal is an IAM service feature that enables the resources to be authorized actors
    /// (or _principals_) to perform actions on service resources. You may use Resource Principal when calling
    /// Oracle NoSQL Database Cloud Service from other Oracle Cloud service resources such as
    /// [Functions](https://docs.cloud.oracle.com/en-us/iaas/Content/Functions/Concepts/functionsoverview.htm).
    /// See [Accessing Other Oracle Cloud Infrastructure Resources from Running Functions](https://docs.cloud.oracle.com/en-us/iaas/Content/Functions/Tasks/functionsaccessingociresources.htm) for how to set up Resource Principal.
    pub fn cloud_auth_from_resource(mut self) -> Result<Self, NoSQLError> {
        //let rfp = ResourcePrincipalAuthProvider::new()?;
        //let ap = AuthProvider::Resource {
        //provider: Box::new(rfp),
        //};
        //self.auth = Arc::new(tokio::sync::Mutex::new(AuthConfig { provider: ap }));
        self.auth_type = AuthType::Resource;
        self.use_https = true;
        self.mode = HandleMode::Cloud;
        Ok(self)
    }
    /// Specify a region identifier for the NoSQL Cloud Service.
    ///
    /// This method is only required if using cloud user file-based authentication and the
    /// given config file does not have a `region` specification. The value should be a
    /// cloud-standard identifier for the region, such as `us-ashburn-1`. For more information
    /// on regions, see [Regions and Availability Domains](https://docs.cloud.oracle.com/en-us/iaas/Content/General/Concepts/regions.htm).
    ///
    /// The NoSQL rust SDK maintains an internal list of regions where the NoSQL service is available.
    /// The region identifier passed to this method is validated against the internal list. If the region
    /// identifier is not found, it is then compared to the region metadata contained in the `OCI_REGION_METADATA`
    /// environment variable (if set), and to region metadata that may exist in a `~/.oci/regions-config.json` file.
    /// See [Adding Regions](https://docs.oracle.com/en-us/iaas/Content/API/Concepts/sdk_adding_new_region_endpoints.htm) for details of these settings. In this way, new regions where NoSQL has been added may
    /// be used without needing to update to the latest NoSQL rust SDK.
    pub fn cloud_region(mut self, region: &str) -> Result<Self, NoSQLError> {
        let r = string_to_region(region)?;
        if self.endpoint.is_empty() {
            self.endpoint = r.nosql_endpoint();
        }
        self.region = Some(r);
        self.use_https = true;
        self.mode = HandleMode::Cloud;
        Ok(self)
    }
    /// Cloud Service only: set the name or id of a compartment to be used for all operations.
    ///
    /// If the associated handle authenticated as an Instance Principal, this value must be an OCID.
    /// In all other cases, the value may be specified as either a name (or path for nested compartments) or as an OCID.
    ///
    /// This value may be overridden on a per-request basis.
    ///
    /// If no compartment is given, user-based OCI authentication uses the root compartment of the tenancy.
    /// Instance-principal and resource-principal authentication require either this default compartment
    /// or a per-request compartment id.
    pub fn compartment_id(mut self, compartment_id: &str) -> Result<Self, NoSQLError> {
        self.default_compartment_id = compartment_id.to_string();
        Ok(self)
    }

    /// Cloud Service only: enable or disable internal rate limiting.
    ///
    /// When enabled, the handle maintains table-scoped read and write rate limiters based on
    /// the table limits returned by `GetTableRequest` or `TableRequest`.
    ///
    /// Rate limiting is disabled by default.
    pub fn rate_limiting_enabled(mut self, enabled: bool) -> Result<Self, NoSQLError> {
        self.rate_limiting_enabled = enabled;
        Ok(self)
    }

    /// Cloud Service only: set the percentage of table limits this handle should use.
    ///
    /// This only applies when [`rate_limiting_enabled()`](HandleBuilder::rate_limiting_enabled)
    /// is set to `true`. The default is 100.0, which allows this handle to consume the full
    /// table limits.
    pub fn rate_limiting_percentage(mut self, percentage: f64) -> Result<Self, NoSQLError> {
        if percentage <= 0.0 || !percentage.is_finite() {
            return ia_err!("rate limiting percentage must be positive");
        }
        self.default_rate_limiter_percentage = percentage;
        Ok(self)
    }

    pub(crate) fn get_rate_limiting_percentage(&self) -> f64 {
        if self.default_rate_limiter_percentage == 0.0 {
            return 100.0;
        }
        self.default_rate_limiter_percentage
    }

    /// Specify credentials for use with a secure On-premises NoSQL Server.
    ///
    /// When using a secure server, a username and password are required. Use this method
    /// to specify the values.
    ///
    /// Calling this method will also internally set the `HandleMode` to `Onprem`.
    pub fn onprem_auth(mut self, username: &str, passwd: &str) -> Result<Self, NoSQLError> {
        if !username.is_empty() {
            let ap = AuthProvider::Onprem {
                provider: Some(OnpremAuthProvider::new(&self, username, passwd)),
            };
            self.auth = Arc::new(tokio::sync::Mutex::new(AuthConfig { provider: ap }));
        } else {
            let ap = AuthProvider::Onprem { provider: None };
            self.auth = Arc::new(tokio::sync::Mutex::new(AuthConfig { provider: ap }));
        }
        self.mode = HandleMode::Onprem;
        self.auth_type = AuthType::Onprem;
        Ok(self)
    }
    /// Specify credentials for use with a secure On-premises NoSQL Server from a local file.
    ///
    /// When using a secure server, a username and password are required. Use this method
    /// to specify the values from a file. The format of the file is one value per line, using
    /// a `key=value` pair syntax, such as:
    ///```text
    /// username=testuser
    /// password=1234567
    ///```
    ///
    /// Calling this method will also internally set the `HandleMode` to `Onprem`.
    pub fn onprem_auth_from_file(mut self, filename: &str) -> Result<Self, NoSQLError> {
        // read user/pass from file
        let mut user = "".to_string();
        let mut pass = "".to_string();
        let data = file_to_string(filename)?;
        // format: one k/v per line, k=v pairs
        for line in data.split("\n") {
            if let Some((k, v)) = line.split_once("=") {
                if k == "username" {
                    user = v.to_string();
                } else if k == "password" {
                    pass = v.to_string();
                }
            }
        }
        // TODO: is password a required field?
        if user.is_empty() {
            return ia_err!("username field missing from onprem auth file {}", filename);
        }
        self = self.onprem_auth(&user, &pass)?;
        self.use_https = true;
        Ok(self)
    }
    /// Add a certificate to use for on-premises https connections from a file.
    ///
    /// The file must contain an x509 certificate in `PEM` file format.
    pub fn add_cert_from_pemfile(self, pemfile: &str) -> Result<Self, NoSQLError> {
        let buf = file_to_string(pemfile)?.into_bytes();
        match reqwest::Certificate::from_pem(&buf) {
            Ok(cert) => {
                return self.add_cert(cert);
            }
            Err(e) => {
                return ia_err!(
                    "error getting certificate from pemfile {}: {}",
                    pemfile,
                    e.to_string()
                );
            }
        }
    }

    /// Add a certificate to use for on-premises https connections.
    pub fn add_cert(mut self, cert: Certificate) -> Result<Self, NoSQLError> {
        self.add_cert = Some(cert);
        Ok(self)
    }
    // see https://docs.rs/reqwest/latest/reqwest/struct.ClientBuilder.html#method.danger_accept_invalid_certs
    /// Allow https connection without validating certificates.
    ///
    /// **Warning:** This is only recommended for local testing purposes. Its use is insecure. See [`reqwest::ClientBuilder::danger_accept_invalid_certs()`] for details.
    ///
    pub fn danger_accept_invalid_certs(
        mut self,
        accept_invalid_certs: bool,
    ) -> Result<Self, NoSQLError> {
        self.accept_invalid_certs = accept_invalid_certs;
        Ok(self)
    }
    /// Specify a [`reqwest::Client`] to use for all http/s connections.
    ///
    /// By default, the [`NoSQL Handle`](crate::Handle) creates an internal [`reqwest::Client`] to use for
    /// all communications. If your application already has a reqwest Client, you can pass that
    /// into the HandleBuilder to avoid creating multiple connection pools.
    pub fn reqwest_client(mut self, client: &Client) -> Result<Self, NoSQLError> {
        // TODO: validate client is open/operational?
        self.client = Some(client.clone());
        Ok(self)
    }
    /// Specify the timeout used for operations.
    ///
    /// Currently this is used for both connection and request timeouts.
    /// Note that the request timeout can be set on a per-request basis.
    ///
    /// The default timeout is 30 seconds.
    pub fn timeout(mut self, timeout: Duration) -> Result<Self, NoSQLError> {
        // TODO: validate timeout
        self.timeout = Some(timeout);
        Ok(self)
    }

    /// Set the statistics collection profile.
    ///
    /// The default is [`StatsProfile::None`], matching the Java and Python SDKs.
    /// Non-`None` profiles start the stats collection gate when the handle is
    /// built. [`StatsProfile::All`] also enables query-level aggregation.
    pub fn stats_profile(mut self, profile: StatsProfile) -> Result<Self, NoSQLError> {
        self.stats_profile = profile;
        Ok(self)
    }

    /// Set the statistics reporting interval.
    ///
    /// The Java and Python SDKs default to 600 seconds. The value is stored on
    /// the runtime [`StatsControl`](crate::StatsControl).
    pub fn stats_interval(mut self, interval: Duration) -> Result<Self, NoSQLError> {
        if interval < Duration::from_secs(1) {
            return ia_err!("stats interval must be at least 1 second");
        }
        self.stats_interval = Some(interval);
        Ok(self)
    }

    /// Configure whether future statistics log output should be pretty-printed.
    ///
    /// The default is `false`, matching the Java and Python SDKs.
    pub fn stats_pretty_print(mut self, pretty_print: bool) -> Result<Self, NoSQLError> {
        self.stats_pretty_print = pretty_print;
        Ok(self)
    }

    /// Configure whether future statistics log output should be enabled.
    ///
    /// Java enables stats logging by default. The value is stored for stats
    /// runtime reporting.
    pub fn stats_enable_log(mut self, enable_log: bool) -> Result<Self, NoSQLError> {
        self.stats_enable_log = Some(enable_log);
        Ok(self)
    }

    /// Set the percentile calculation mode used by [`StatsProfile::More`] and
    /// [`StatsProfile::All`].
    ///
    /// [`StatsPercentileMode::Exact`] matches the Java SDK by storing samples
    /// and sorting at interval emission time. [`StatsPercentileMode::Hdr`] uses
    /// a bounded-memory HDR-style histogram for high-throughput clients.
    pub fn stats_percentile_mode(
        mut self,
        percentile_mode: StatsPercentileMode,
    ) -> Result<Self, NoSQLError> {
        self.stats_percentile_mode = percentile_mode;
        Ok(self)
    }

    /// Configure an application callback for future statistics snapshots.
    ///
    /// The handler is stored on the runtime stats control.
    pub fn stats_handler<H>(mut self, handler: H) -> Result<Self, NoSQLError>
    where
        H: StatsHandler,
    {
        self.stats_handler = StatsHandlerRef::new(handler);
        Ok(self)
    }

    // for doc testing use only
    #[doc(hidden)]
    pub fn in_test(mut self, in_test: bool) -> Self {
        self.in_test = in_test;
        self
    }

    // Return true if the auth has been updated/refreshed.
    // Return false if there are no errors, but nothing was refreshed.
    pub(crate) async fn refresh_auth(&self, client: &Client) -> Result<bool, NoSQLError> {
        // It is safe to keep this mutex lock across await because we're using tokio::sync::Mutex
        let mut pguard = self.auth.lock().await;
        match &mut pguard.provider {
            AuthProvider::Instance { provider: _ } => {
                // create an entirely new IP auth, as currently IP Auth has no methods to refresh itself
                let ifp = InstancePrincipalAuthProvider::new_with_client(client).await?;
                pguard.provider = AuthProvider::Instance {
                    provider: Box::new(ifp),
                };
                return Ok(true);
            }
            AuthProvider::Resource { provider: _ } => {
                let rfp = ResourcePrincipalAuthProvider::new()?;
                pguard.provider = AuthProvider::Resource {
                    provider: Box::new(rfp),
                };
                return Ok(true);
            }
            AuthProvider::Onprem { provider } => {
                if let Some(prov) = provider {
                    let _ = prov.generate_token(client, true).await?;
                }
            }
            // TODO: maybe refresh file-based auth?
            _ => {}
        }
        Ok(false)
    }

    pub(crate) async fn refresh_auth_if_needed(&self) -> Result<bool, NoSQLError> {
        let mut pguard = self.auth.lock().await;
        match &mut pguard.provider {
            AuthProvider::Resource { provider } if provider.should_refresh() => {
                let rfp = ResourcePrincipalAuthProvider::new()?;
                pguard.provider = AuthProvider::Resource {
                    provider: Box::new(rfp),
                };
                Ok(true)
            }
            _ => Ok(false),
        }
    }
}

#[cfg(test)]
mod stats_env_tests {
    use super::*;

    #[test]
    fn stats_environment_parses_settings() {
        let builder = builder_from_stats_env(&[
            ("NOSQL_STATS_PROFILE", "more"),
            ("NOSQL_STATS_INTERVAL", "42"),
            ("NOSQL_STATS_PRETTY_PRINT", "yes"),
            ("NOSQL_STATS_ENABLE_LOG", "off"),
            ("NOSQL_STATS_PERCENTILE_MODE", "hdr"),
        ])
        .unwrap();

        assert_eq!(builder.stats_profile, StatsProfile::More);
        assert_eq!(builder.stats_interval, Some(Duration::from_secs(42)));
        assert!(builder.stats_pretty_print);
        assert_eq!(builder.stats_enable_log, Some(false));
        assert_eq!(builder.stats_percentile_mode, StatsPercentileMode::Hdr);
    }

    #[test]
    fn oracle_stats_env_vars_take_precedence_over_nosql_aliases() {
        let builder = builder_from_stats_env(&[
            ("NOSQL_STATS_PROFILE", "REGULAR"),
            ("ORACLE_NOSQL_STATS_PROFILE", "ALL"),
            ("NOSQL_STATS_INTERVAL", "30"),
            ("ORACLE_NOSQL_STATS_INTERVAL", "60"),
            ("NOSQL_STATS_PRETTY_PRINT", "false"),
            ("ORACLE_NOSQL_STATS_PRETTY_PRINT", "true"),
            ("NOSQL_STATS_ENABLE_LOG", "true"),
            ("ORACLE_NOSQL_STATS_ENABLE_LOG", "false"),
            ("NOSQL_STATS_PERCENTILE_MODE", "EXACT"),
            ("ORACLE_NOSQL_STATS_PERCENTILE_MODE", "HDR"),
        ])
        .unwrap();

        assert_eq!(builder.stats_profile, StatsProfile::All);
        assert_eq!(builder.stats_interval, Some(Duration::from_secs(60)));
        assert!(builder.stats_pretty_print);
        assert_eq!(builder.stats_enable_log, Some(false));
        assert_eq!(builder.stats_percentile_mode, StatsPercentileMode::Hdr);
    }

    #[test]
    fn stats_bool_env_parsing_accepts_supported_forms() {
        assert!(HandleBuilder::parse_stats_bool("true", "TEST").unwrap());
        assert!(HandleBuilder::parse_stats_bool("1", "TEST").unwrap());
        assert!(HandleBuilder::parse_stats_bool("on", "TEST").unwrap());
        assert!(HandleBuilder::parse_stats_bool("yes", "TEST").unwrap());
        assert!(!HandleBuilder::parse_stats_bool("false", "TEST").unwrap());
        assert!(!HandleBuilder::parse_stats_bool("0", "TEST").unwrap());
        assert!(!HandleBuilder::parse_stats_bool("off", "TEST").unwrap());
        assert!(!HandleBuilder::parse_stats_bool("no", "TEST").unwrap());
    }

    #[test]
    fn stats_env_parsing_rejects_invalid_values() {
        let error = builder_from_stats_env(&[("NOSQL_STATS_PROFILE", "LOUD")]).unwrap_err();

        assert!(error.message.contains("NOSQL_STATS_PROFILE"));
        assert!(error.message.contains("NONE, REGULAR, MORE, ALL"));

        assert!(HandleBuilder::parse_stats_interval("0", "INTERVAL").is_err());
        assert!(HandleBuilder::parse_stats_interval("abc", "INTERVAL").is_err());
        assert!(HandleBuilder::parse_stats_bool("maybe", "BOOL").is_err());
        assert!(HandleBuilder::parse_stats_percentile_mode("sketch", "MODE").is_err());
    }

    fn builder_from_stats_env(vars: &[(&str, &str)]) -> Result<HandleBuilder, NoSQLError> {
        HandleBuilder::new().apply_stats_environment(|name| {
            vars.iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).to_string())
        })
    }
}

// On premises auth
#[derive(Default, Clone)]
pub(crate) struct OnpremAuthProvider {
    pub(crate) inner: Arc<OnpremAuthProviderRef>,
}

impl fmt::Debug for OnpremAuthProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OnpremAuthProvider")
            .field("inner", &"[redacted]")
            .finish()
    }
}

#[derive(Default)]
pub(crate) struct OnpremAuthProviderRef {
    username: String,
    password: String,
    endpoint: String,
    // We use a tokio Mutex because we occasionally hold a lock across awaits
    token: tokio::sync::Mutex<OnpremToken>,
}

impl fmt::Debug for OnpremAuthProviderRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OnpremAuthProviderRef")
            .field("username", &"[redacted]")
            .field("password", &"[redacted]")
            .field("endpoint", &self.endpoint)
            .field("token", &"[redacted]")
            .finish()
    }
}

#[derive(Default, Deserialize)]
struct OnpremToken {
    token: String,
    #[serde(rename = "expireAt")]
    expire_at: i64,
}

impl fmt::Debug for OnpremToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OnpremToken")
            .field("token", &"[redacted]")
            .field("expire_at", &self.expire_at)
            .finish()
    }
}

impl OnpremAuthProvider {
    pub fn new(builder: &HandleBuilder, user: &str, pass: &str) -> OnpremAuthProvider {
        // normalize endpoint to "http[s]://{endpoint}/V2/nosql/security"
        let mut ep = String::from("http");
        if builder.use_https {
            ep.push('s');
        }
        ep.push_str("://");
        ep.push_str(&builder.endpoint);
        ep.push_str("/V2/nosql/security");
        OnpremAuthProvider {
            inner: Arc::new(OnpremAuthProviderRef {
                username: user.to_string(),
                password: pass.to_string(),
                endpoint: ep,
                token: tokio::sync::Mutex::new(OnpremToken::default()),
            }),
        }
        // TODO: should new() attempt to connect to the service? Or wait until
        // first time needing headers?
    }
    pub async fn add_required_headers(
        &self,
        client: &Client,
        headers: &mut HeaderMap,
    ) -> Result<(), NoSQLError> {
        let mut bearer = self.generate_token(client, false).await?;
        bearer.insert_str(0, "Bearer ");
        headers.insert("Authorization", HeaderValue::from_str(&bearer)?);
        Ok(())
    }
    async fn generate_token(&self, client: &Client, force: bool) -> Result<String, NoSQLError> {
        let mut tguard = self.inner.token.lock().await;
        if !force && !tguard.token.is_empty() && (tguard.expire_at - 10000) > Self::now() {
            return Ok(tguard.token.clone());
        }

        let mut headers = HeaderMap::new();
        headers.insert("Accept", HeaderValue::from_str("application/json")?);
        let mut ep = self.inner.endpoint.clone();
        let bup = {
            if tguard.token.is_empty() {
                ep.push_str("/login");
                // set Authorization: Basic base64(username:password)
                let up = format!("{}:{}", &self.inner.username, &self.inner.password);
                format!("Basic {}", BASE64_STANDARD.encode(up))
            } else {
                ep.push_str("/renew");
                // set Authorization: Bearer {token}
                format!("Bearer {}", tguard.token)
            }
        };
        headers.insert("Authorization", HeaderValue::from_str(&bup)?);
        let resp = client.get(ep).headers(headers).send().await?;
        // parse returned JSON
        let result = resp.text().await?;
        let nt: Result<OnpremToken, serde_json::Error> = serde_json::from_str(&result);
        if let Ok(new_token) = nt {
            tguard.token = new_token.token;
            tguard.expire_at = new_token.expire_at;
            return Ok(tguard.token.clone());
        }
        ia_err!("error from onprem login service: {}", result)
    }

    fn now() -> i64 {
        let umillis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let imillis: i64 = umillis.try_into().unwrap();
        imillis
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64ct::{Base64Unpadded, Encoding};
    use openssl::rsa::Rsa;
    use std::env;

    #[derive(Clone, Debug)]
    struct TestAuthProvider;

    impl AuthenticationProvider for TestAuthProvider {
        fn tenancy_id(&self) -> &str {
            "tenancy"
        }

        fn fingerprint(&self) -> &str {
            "fingerprint"
        }

        fn user_id(&self) -> &str {
            "user"
        }

        fn private_key(
            &self,
        ) -> Result<openssl::rsa::Rsa<openssl::pkey::Private>, Box<dyn std::error::Error>> {
            Ok(openssl::rsa::Rsa::generate(2048)?)
        }

        fn region_id(&self) -> &str {
            "region"
        }
    }

    #[derive(Clone, Debug)]
    struct ExpiringTestAuthProvider;

    impl AuthenticationProvider for ExpiringTestAuthProvider {
        fn tenancy_id(&self) -> &str {
            "tenancy"
        }

        fn fingerprint(&self) -> &str {
            "fingerprint"
        }

        fn user_id(&self) -> &str {
            "user"
        }

        fn private_key(
            &self,
        ) -> Result<openssl::rsa::Rsa<openssl::pkey::Private>, Box<dyn std::error::Error>> {
            Ok(openssl::rsa::Rsa::generate(2048)?)
        }

        fn region_id(&self) -> &str {
            "region"
        }

        fn should_refresh(&self) -> bool {
            true
        }
    }

    static RESOURCE_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    struct ResourcePrincipalEnvGuard {
        _lock: std::sync::MutexGuard<'static, ()>,
        saved: Vec<(&'static str, Option<String>)>,
    }

    impl Drop for ResourcePrincipalEnvGuard {
        fn drop(&mut self) {
            for (key, value) in &self.saved {
                match value {
                    Some(value) => env::set_var(key, value),
                    None => env::remove_var(key),
                }
            }
        }
    }

    fn set_resource_principal_env(tenancy_id: &str) -> ResourcePrincipalEnvGuard {
        let lock = RESOURCE_ENV_LOCK.lock().unwrap();
        let keys = [
            "OCI_RESOURCE_PRINCIPAL_VERSION",
            "OCI_RESOURCE_PRINCIPAL_RPST",
            "OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM",
            "OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE",
            "OCI_RESOURCE_PRINCIPAL_REGION",
        ];
        let saved = keys.iter().map(|key| (*key, env::var(key).ok())).collect();

        env::set_var("OCI_RESOURCE_PRINCIPAL_VERSION", "2.2");
        env::set_var(
            "OCI_RESOURCE_PRINCIPAL_RPST",
            test_rpst_token(tenancy_id, now_secs() + 3600),
        );
        env::set_var("OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM", test_private_key_pem());
        env::remove_var("OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE");
        env::set_var("OCI_RESOURCE_PRINCIPAL_REGION", "us-ashburn-1");

        ResourcePrincipalEnvGuard { _lock: lock, saved }
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

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    async fn assert_resource_provider_tenancy(builder: &HandleBuilder, tenancy_id: &str) {
        let guard = builder.auth.lock().await;
        match &guard.provider {
            AuthProvider::Resource { provider } => {
                assert_eq!(provider.tenancy_id(), tenancy_id);
            }
            provider => panic!("expected resource provider, got {:?}", provider),
        }
    }

    #[test]
    fn onprem_debug_redacts_credentials_and_token() {
        let auth_ref = OnpremAuthProviderRef {
            username: "secret-user".to_string(),
            password: "secret-password".to_string(),
            endpoint: "https://example.com/V2/nosql/security".to_string(),
            token: tokio::sync::Mutex::new(OnpremToken {
                token: "secret-token".to_string(),
                expire_at: 123,
            }),
        };

        let debug = format!("{:?}", auth_ref);

        assert!(!debug.contains("secret-user"));
        assert!(!debug.contains("secret-password"));
        assert!(!debug.contains("secret-token"));
        assert!(debug.contains("[redacted]"));
    }

    #[test]
    fn builder_and_auth_config_debug_do_not_format_auth_provider_internals() {
        let mut builder = HandleBuilder::new();
        builder.endpoint = "example.com".to_string();
        builder.auth_type = AuthType::Onprem;
        let provider = OnpremAuthProvider::new(&builder, "secret-user", "secret-password");
        builder.auth = Arc::new(tokio::sync::Mutex::new(AuthConfig {
            provider: AuthProvider::Onprem {
                provider: Some(provider.clone()),
            },
        }));

        let builder_debug = format!("{:?}", builder);
        let auth_debug = format!(
            "{:?}",
            AuthConfig {
                provider: AuthProvider::Onprem {
                    provider: Some(provider),
                },
            }
        );

        assert!(!builder_debug.contains("secret-user"));
        assert!(!builder_debug.contains("secret-password"));
        assert!(!auth_debug.contains("secret-user"));
        assert!(!auth_debug.contains("secret-password"));
        assert!(builder_debug.contains("[redacted]"));
        assert!(auth_debug.contains("[redacted]"));
    }

    #[tokio::test]
    async fn resource_principal_refresh_reloads_env_material_after_auth_error() {
        let tenancy_id = "ocid1.tenancy.oc1..refreshed";
        let _env_guard = set_resource_principal_env(tenancy_id);
        let client = Client::builder().build().unwrap();
        let mut builder = HandleBuilder::new();
        builder.auth_type = AuthType::Resource;
        builder.auth = Arc::new(tokio::sync::Mutex::new(AuthConfig {
            provider: AuthProvider::Resource {
                provider: Box::new(TestAuthProvider),
            },
        }));

        let refreshed = builder.refresh_auth(&client).await.unwrap();

        assert!(refreshed);
        assert_resource_provider_tenancy(&builder, tenancy_id).await;
    }

    #[tokio::test]
    async fn resource_principal_refresh_if_needed_reloads_expiring_provider() {
        let tenancy_id = "ocid1.tenancy.oc1..preemptive";
        let _env_guard = set_resource_principal_env(tenancy_id);
        let mut builder = HandleBuilder::new();
        builder.auth_type = AuthType::Resource;
        builder.auth = Arc::new(tokio::sync::Mutex::new(AuthConfig {
            provider: AuthProvider::Resource {
                provider: Box::new(ExpiringTestAuthProvider),
            },
        }));

        let refreshed = builder.refresh_auth_if_needed().await.unwrap();

        assert!(refreshed);
        assert_resource_provider_tenancy(&builder, tenancy_id).await;
    }

    #[tokio::test]
    async fn instance_principal_refresh_uses_supplied_client() {
        let client = Client::builder().build().unwrap();
        InstancePrincipalAuthProvider::expect_next_new_with_client_for_test(&client);
        let mut builder = HandleBuilder::new();
        builder.auth_type = AuthType::Instance;
        builder.auth = Arc::new(tokio::sync::Mutex::new(AuthConfig {
            provider: AuthProvider::Instance {
                provider: Box::new(TestAuthProvider),
            },
        }));

        let err = builder.refresh_auth(&client).await.unwrap_err();

        assert!(
            err.message
                .contains("test marker: instance principal used supplied client"),
            "{}",
            err
        );
    }
}
