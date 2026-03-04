//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use dirs;
use std::path::PathBuf;

/// Generates the absolute file path of the path provided based on the underlying platform
///
/// # Arguments
///
/// * `file_path`: The file path that needs to be corrected
///
/// # Returns
///
/// The absolute file path to be used based on the platform being used
///
pub(crate) fn expand_user_home(file_path: &str) -> String {
    if file_path.starts_with("~/") || file_path.starts_with("~\\") {
        if let Some(home_dir) = dirs::home_dir() {
            let full_path = home_dir.join(PathBuf::from(correct_path(&file_path[2..])));
            String::from(format!("{}", full_path.display()))
        } else {
            panic!("Error reading home directory");
        }
    } else {
        String::from(file_path)
    }
}

/// Generates the correct file path based on the platform
///
/// # Arguments
///
/// * `file_path`: The file path that needs to be corrected
///
/// # Returns
///
/// The correct the file path to be used based on the platform being used
///
fn correct_path(file_path: &str) -> String {
    if cfg!(target_os = "windows") {
        let file_path_corrected = file_path.replace("/", "\\");
        String::from(file_path_corrected)
    } else {
        String::from(file_path)
    }
}
