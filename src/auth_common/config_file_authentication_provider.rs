//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use crate::auth_common::authentication_provider::AuthenticationProvider;
use crate::auth_common::file_utils::expand_user_home;
use crate::auth_common::private_key_supplier::FilePrivateKeySupplier;
use crate::auth_common::simple_authentication_provider::SimpleAuthenticationProvider;
use ini::Ini;
use openssl::pkey::Private;
use openssl::rsa::Rsa;
use std::error::Error;

#[allow(dead_code)]
const DEFAULT_CONFIG_FILE_PATH: &str = "~/.oci/config";
const TENANCY: &str = "tenancy";
const USER: &str = "user";
const PASS_PHRASE: &str = "pass_phrase";
const KEY_FILE: &str = "key_file";
const FINGERPRINT: &str = "fingerprint";
const REGION: &str = "region";

/// Struct defining a user-based Authentication Provider that reads all authentication information from config file
#[derive(Debug, Clone)]
pub struct ConfigFileAuthenticationProvider {
    auth_provider: SimpleAuthenticationProvider,
}

fn get_section_variable(
    file: &str,
    props: &ini::Properties,
    var: &str,
) -> Result<String, Box<dyn Error>> {
    match props.get(var) {
        Some(v) => Ok(v.to_string()),
        None => Err(format!(
            "error reading OCI config file '{}': missing field '{}'",
            file, var
        )
        .as_str()
        .into()),
    }
}

impl ConfigFileAuthenticationProvider {
    /// Creates a new ConfigFileAuthenticationProvider using the file from the path provided using the profile name provided.
    ///
    /// # Arguments
    ///
    /// * `config_file_path` : The path of the config file to use
    /// * `profile_name`: The name of the profile to use
    ///
    /// # Returns
    ///
    /// An instance of ConfigFileAuthenticationProvider
    ///
    pub fn new_from_file(
        config_file_path: &str,
        profile_name: &str,
    ) -> Result<Self, Box<dyn Error>> {
        tracing::debug!(
            "Creating a ConfigFileAuthenticationProvider from file {} and profile {}",
            config_file_path,
            profile_name
        );
        // Fix path
        let file_path = expand_user_home(config_file_path);
        // Read config file
        let config = match Ini::load_from_file(&file_path) {
            Ok(c) => c,
            Err(e) => {
                return Err(format!(
                    "error reading OCI config file '{}': {}",
                    file_path,
                    e.to_string()
                )
                .as_str()
                .into());
            }
        };
        // Get Profile
        let profile_data = match config.section(Some(profile_name)) {
            Some(p) => p,
            None => {
                return Err(format!(
                    "error reading OCI config file '{}': missing profile '{}'",
                    file_path, profile_name
                )
                .as_str()
                .into());
            }
        };

        // Read required keys from profile data
        let tenancy_id = get_section_variable(&file_path, profile_data, TENANCY)?;
        let user_id = get_section_variable(&file_path, profile_data, USER)?;
        let fingerprint = get_section_variable(&file_path, profile_data, FINGERPRINT)?;
        let pem_file_path = get_section_variable(&file_path, profile_data, KEY_FILE)?;

        // region_id is optional
        let region_id =
            get_section_variable(&file_path, profile_data, REGION).unwrap_or("".to_string());
        if region_id.is_empty() {
            tracing::debug!("no '{}' specified in OCI config file", REGION);
        }

        let passphrase = profile_data.get(PASS_PHRASE);
        match passphrase {
            Some(pass) => {
                let supplier = Box::new(FilePrivateKeySupplier::new_with_passphrase(
                    pem_file_path,
                    Some(pass.chars().collect()),
                ));
                Ok(ConfigFileAuthenticationProvider {
                    auth_provider: SimpleAuthenticationProvider::new(
                        tenancy_id,
                        user_id,
                        fingerprint,
                        region_id,
                        supplier,
                    ),
                })
            }
            None => {
                let supplier = Box::new(FilePrivateKeySupplier::new(pem_file_path));
                Ok(ConfigFileAuthenticationProvider {
                    auth_provider: SimpleAuthenticationProvider::new(
                        tenancy_id,
                        user_id,
                        fingerprint,
                        region_id,
                        supplier,
                    ),
                })
            }
        }
    }

    /// Creates a new ConfigFileAuthenticationProvider using the default oci config path using the profile name provided.
    ///
    /// # Arguments
    ///
    /// * `profile_name`: The name of the profile to use
    ///
    /// # Returns
    ///
    /// An instance of ConfigFileAuthenticationProvider
    ///
    #[allow(dead_code)]
    pub fn new(profile_name: &str) -> Result<Self, Box<dyn Error>> {
        Self::new_from_file(DEFAULT_CONFIG_FILE_PATH, profile_name)
    }
}

impl AuthenticationProvider for ConfigFileAuthenticationProvider {
    fn tenancy_id(&self) -> &str {
        self.auth_provider.tenancy_id()
    }
    fn fingerprint(&self) -> &str {
        self.auth_provider.fingerprint()
    }
    fn user_id(&self) -> &str {
        self.auth_provider.user_id()
    }
    fn private_key(&self) -> Result<Rsa<Private>, Box<dyn Error>> {
        self.auth_provider.private_key()
    }
    fn region_id(&self) -> &str {
        self.auth_provider.region_id()
    }
    // Note: key_id() uses default implementation in AuthenticationProvider
}

#[cfg(test)]
mod test {
    use std::io::Write;

    use super::*;

    // TODO: rewrite these to not expect panics
    #[test]
    #[should_panic]
    fn test_config_auth_provider_with_invalid_file_path() {
        let _auth_provider =
            ConfigFileAuthenticationProvider::new_from_file("~/invalid_path", "DOES_NOT_EXIST")
                .expect("");
    }

    #[test]
    #[should_panic]
    fn test_config_auth_provider_with_invalid_profile() {
        // Create a directory inside of `std::env::temp_dir()`
        let dir = tempfile::tempdir().expect("Error creating temp dir");
        let file_path = dir.path().join("oci_config_file");
        let mut file =
            std::fs::File::create(file_path.to_str().unwrap()).expect("Error creating file");
        let config_file_contents = r#"
        [DEFAULT]
        Some Key = Some value
        "#;
        file.write_all(config_file_contents.as_bytes())
            .expect("Error writing to config file");

        let _auth_provider = ConfigFileAuthenticationProvider::new_from_file(
            file_path.to_str().unwrap(),
            "DOES_NOT_EXIST",
        )
        .expect("");
        drop(file);
        _ = dir.close();
    }

    #[test]
    #[should_panic]
    fn test_config_auth_provider_with_missing_keys() {
        // Create a directory inside of `std::env::temp_dir()`
        let dir = tempfile::tempdir().expect("Error creating temp dir");
        let file_path = dir.path().join("oci_config_file-2");
        let mut file =
            std::fs::File::create(file_path.to_str().unwrap()).expect("Error creating file");
        let config_file_contents = r#"
        [DEFAULT]
        tenancy = some tenancy
        user = some user
        fingerprint = some fingerprint
        region = some region
        "#;
        file.write_all(config_file_contents.as_bytes())
            .expect("Error writing to config file");

        let _auth_provider =
            ConfigFileAuthenticationProvider::new_from_file(file_path.to_str().unwrap(), "DEFAULT")
                .expect("");
        drop(file);
        _ = dir.close();
    }
}
