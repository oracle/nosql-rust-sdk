# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/).

## Unreleased

### Added
- Latest OCI region codes

## 0.1.4 06/22/2026

### Changed
- Now requires Rust version 1.88 or higher

### Added
- Support for UNION operator
- Internal rate limiting
- Support for a few edge-case query types that did not make it into previous releases

### Fixed

- Several fixes for issues found from AI scanning:
    - Debug/trace logging can expose credentials, auth headers, tokens
    - Multi-delete field ranges can become unbounded deletes
    - System request op codes are shifted by a missing DropIndex slot
    - Advanced-query internal fetches drop per-request compartment and limits
    - Internal auth/SIU retry can replay non-idempotent bodies
    - Namespace setters are dropped for table DDL and get-indexes
    - Delete subrequests in write-multiple are not finalized as NSON maps
    - NSON map and array length backpatching omits the high byte
    - Instance-principal refresh bypasses the handle client used for initial credential acquisition
    - Table usage start index is serialized under the limit field name
    - Response decoders can panic or allocate from malformed packed integers and unchecked counts
    - Instance-principal tenancy parsing can truncate the default compartment id
    - Resource-principal token expiration is parsed but not enforced or refreshed
    - Instance and resource principals default missing compartment to tenancy/root

## 0.1.3 05/18/2026

### Added
- Added latest OCI region codes

### Changed
- Updated copyrights to 2026
- Enabled QTF tests to run against a cloud instance

### Fixed
- Fixed compiler warnings, security issues, formatting
- Fixed prepared queries for 22.4 and lower servers
- Fixed various QTF tests


## 0.1.2 08/13/2025

### Added
- Added support for default compartment id for a handle

## 0.1.1 01/07/2025

### Fixed

- Fixed case where onprem use mistakently required ORACLE_NOSQL_AUTH=onprem environment setting.

## 0.1.0 10/04/2024

- First released version
