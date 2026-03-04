//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use crate::auth_common::authentication_provider::AuthenticationProvider;
use crate::auth_common::private_key_supplier::Supplier;
use openssl::pkey::Private;
use openssl::rsa::Rsa;
use std::error::Error;

/// An authentication details provider that contains user authentication information and region information.
/// This is an ideal provider to be used if customer authentication information is not read from config file.
#[derive(Debug, Clone)]
pub struct SimpleAuthenticationProvider {
    tenancy_id: String,
    user_id: String,
    fingerprint: String,
    region_id: String,
    supplier: Box<dyn Supplier>,
}

impl SimpleAuthenticationProvider {
    /// Creates a new SimpleAuthenticationProvider using the values passed in the arguments.
    ///
    /// # Arguments
    ///
    /// * `tenancy_id` : The tenancy ocid of the tenancy to use for user-based authentication
    /// * `user_id`: The user ocid of the tenancy to use for user-based authentication
    /// * `fingerprint`: The fingerprint of the private key to be used for the user-based authentication
    /// * `region_id`: The region-id to associate with this Authentication Provider.
    /// * `supplier`: The supplier that provides the private key to be used for user-based authentication
    ///
    /// # Returns
    ///
    /// An instance of SimpleAuthenticationProvider
    ///
    pub fn new(
        tenancy_id: String,
        user_id: String,
        fingerprint: String,
        region_id: String,
        supplier: Box<dyn Supplier + Send + Sync>,
    ) -> Self {
        SimpleAuthenticationProvider {
            tenancy_id,
            user_id,
            fingerprint,
            region_id,
            supplier,
        }
    }
}

impl AuthenticationProvider for SimpleAuthenticationProvider {
    fn tenancy_id(&self) -> &str {
        &self.tenancy_id
    }
    fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
    fn user_id(&self) -> &str {
        &self.user_id
    }
    fn private_key(&self) -> Result<Rsa<Private>, Box<dyn Error>> {
        Ok(self.supplier.get_key()?)
    }
    fn region_id(&self) -> &str {
        &self.region_id
    }
}
