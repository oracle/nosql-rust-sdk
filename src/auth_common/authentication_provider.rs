//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use openssl::pkey::Private;
use openssl::rsa::Rsa;
use std::error::Error;
use std::fmt::Debug;

/// Trait defining an Authentication Provider
pub trait AuthenticationProvider: Send + Sync + Debug + AuthenticationProviderClone {
    /// Returns the Tenancy OCID associated with this AuthenticationProvider
    fn tenancy_id(&self) -> &str;
    /// Returns the User OCID associated with this AuthenticationProvider
    fn user_id(&self) -> &str;
    /// Returns the Fingerprint associated with the Private Key of this AuthenticationProvider
    fn fingerprint(&self) -> &str;
    /// Returns the Private Key associated with this AuthenticationProvider
    fn private_key(&self) -> Result<Rsa<Private>, Box<dyn Error>>;
    /// Returns the key id associated with this AuthenticationProvider to be used for signing requests
    fn key_id(&self) -> String {
        let key_id = format!(
            "{}/{}/{}",
            self.tenancy_id(),
            self.user_id(),
            self.fingerprint()
        );
        key_id
    }
    /// Returns the region-id associated with this AuthenticationProvider
    fn region_id(&self) -> &str;
}

// This allows users of this library to clone a Box<dyn AuthenticationProvider>
pub trait AuthenticationProviderClone {
    fn clone_box(&self) -> Box<dyn AuthenticationProvider>;
}

impl<T> AuthenticationProviderClone for T
where
    T: 'static + AuthenticationProvider + Clone,
{
    fn clone_box(&self) -> Box<dyn AuthenticationProvider> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn AuthenticationProvider> {
    fn clone(&self) -> Box<dyn AuthenticationProvider> {
        self.clone_box()
    }
}
