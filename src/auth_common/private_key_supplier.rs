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

use crate::auth_common::file_utils::expand_user_home;

pub trait Supplier: Send + Sync + Debug + SupplierClone {
    fn get_key(&self) -> Result<Rsa<Private>, Box<dyn Error>>;
}

pub trait SupplierClone {
    fn clone_box(&self) -> Box<dyn Supplier>;
}

impl<T> SupplierClone for T
where
    T: 'static + Supplier + Clone,
{
    fn clone_box(&self) -> Box<dyn Supplier> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn Supplier> {
    fn clone(&self) -> Box<dyn Supplier> {
        self.clone_box()
    }
}

#[derive(Debug, Clone)]
pub struct PrivateKeySupplier {
    key_content: String,
    passphrase: Option<Vec<char>>,
}

impl PrivateKeySupplier {
    #[allow(dead_code)]
    pub fn new(key_content: String) -> Self {
        PrivateKeySupplier {
            key_content,
            passphrase: None,
        }
    }

    #[allow(dead_code)]
    pub fn new_with_passphrase(key_content: String, passphrase: Option<Vec<char>>) -> Self {
        PrivateKeySupplier {
            key_content,
            passphrase,
        }
    }
}

impl Supplier for PrivateKeySupplier {
    fn get_key(&self) -> Result<Rsa<Private>, Box<dyn Error>> {
        match self.passphrase.as_ref() {
            Some(pass) => {
                let pass_bytes = pass.iter().map(|c| *c as u8).collect::<Vec<_>>();
                Ok(Rsa::private_key_from_pem_passphrase(
                    self.key_content.as_bytes(),
                    &pass_bytes,
                )?)
            }
            None => Ok(Rsa::private_key_from_pem(self.key_content.as_bytes())?),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FilePrivateKeySupplier {
    key_path: String,
    passphrase: Option<Vec<char>>,
}

impl FilePrivateKeySupplier {
    pub fn new(key_path: String) -> Self {
        FilePrivateKeySupplier {
            key_path,
            passphrase: None,
        }
    }

    pub fn new_with_passphrase(key_path: String, passphrase: Option<Vec<char>>) -> Self {
        FilePrivateKeySupplier {
            key_path,
            passphrase,
        }
    }
}

impl Supplier for FilePrivateKeySupplier {
    fn get_key(&self) -> Result<Rsa<Private>, Box<dyn Error>> {
        // TODO: cache key in memory, don't go to disk for every request
        let key_content = match std::fs::read_to_string(&expand_user_home(&self.key_path)) {
            Ok(kc) => kc,
            Err(e) => {
                return Err(format!(
                    "Unable to read private fey file '{}': {}",
                    self.key_path,
                    e.to_string()
                )
                .as_str()
                .into());
            }
        };
        match self.passphrase.as_ref() {
            Some(pass) => {
                let pass_bytes = pass.iter().map(|c| *c as u8).collect::<Vec<_>>();
                Ok(Rsa::private_key_from_pem_passphrase(
                    key_content.as_bytes(),
                    &pass_bytes,
                )?)
            }
            None => Ok(Rsa::private_key_from_pem(key_content.as_bytes())?),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    #[should_panic]
    fn test_file_private_key_supplier_with_key_file_missing() {
        let file_key_provider = FilePrivateKeySupplier::new(String::from("~/invalid_path"));
        file_key_provider.get_key().unwrap();
    }

    #[test]
    #[should_panic]
    fn test_private_key_supplier_with_invalid_key() {
        let key_supplier = PrivateKeySupplier::new(String::from("invalid key"));
        key_supplier.get_key().unwrap();
    }

    #[test]
    #[should_panic]
    fn test_private_key_with_invalid_passphrase() {
        let rsa = Rsa::generate(4096).unwrap();
        let private_key_bytes = rsa
            .private_key_to_pem_passphrase(
                openssl::symm::Cipher::aes_256_cbc(),
                "some_passphrase".as_bytes(),
            )
            .expect("Error generating Private key with passphrase");
        let private_key_supplier = PrivateKeySupplier::new_with_passphrase(
            String::from_utf8(private_key_bytes).expect("Invalid key"),
            Some("Invalid passphrase".chars().collect::<Vec<_>>()),
        );
        private_key_supplier.get_key().unwrap();
    }

    #[test]
    fn test_private_key_with_valid_passphrase() {
        let rsa = Rsa::generate(4096).unwrap();
        let private_key_bytes = rsa
            .private_key_to_pem_passphrase(
                openssl::symm::Cipher::aes_256_cbc(),
                "some_passphrase".as_bytes(),
            )
            .expect("Error generating Private key with passphrase");
        let private_key_supplier = PrivateKeySupplier::new_with_passphrase(
            String::from_utf8(private_key_bytes).expect("Invalid key"),
            Some("some_passphrase".chars().collect::<Vec<_>>()),
        );
        private_key_supplier.get_key().unwrap();
    }
}
