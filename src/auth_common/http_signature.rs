//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use openssl::hash::MessageDigest;
use openssl::pkey::{PKey, Private};
use openssl::rsa::Rsa;
use openssl::sign::Signer;

pub fn sign(key: Rsa<Private>, data_to_sign: &[u8]) -> Vec<u8> {
    let pkey = PKey::from_rsa(key).unwrap();
    let mut signer = Signer::new(MessageDigest::sha256(), &pkey).unwrap();
    signer.update(data_to_sign).unwrap();
    let signature = signer.sign_to_vec().unwrap();
    signature
}
