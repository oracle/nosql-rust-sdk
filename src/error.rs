//
// Copyright (c) 2024, 2026 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use num_enum::TryFromPrimitive;

include!(concat!(env!("OUT_DIR"), "/ua.rs"));

pub(crate) fn sdk_version() -> &'static str {
    SDK_VERSION
}

pub(crate) fn user_agent() -> &'static str {
    USER_AGENT
}

/// Enumeration of all possible errors returned by this library.
#[derive(Debug, Clone)]
pub struct NoSQLError {
    pub code: NoSQLErrorCode,
    pub message: String,
}

impl std::error::Error for NoSQLError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }
}

impl std::fmt::Display for NoSQLError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        return write!(f, "code={:?} message=\"{}\"", self.code, self.message);
    }
}

impl NoSQLError {
    pub fn new(code: NoSQLErrorCode, msg: &str) -> NoSQLError {
        NoSQLError {
            code,
            message: msg.to_string(),
        }
    }

    pub fn from_int(icode: i32, msg: &str) -> NoSQLError {
        if let Ok(code) = NoSQLErrorCode::try_from(icode) {
            return NoSQLError {
                code: code,
                message: msg.to_string(),
            };
        }
        NoSQLError {
            code: NoSQLErrorCode::UnknownError,
            message: format!("Invalid integer error code {}", icode),
        }
    }
}

//pub(crate) fn ia(msg: String) -> NoSQLError {
//NoSQLError {
//code: NoSQLErrorCode::IllegalArgument,
//message: msg,
//}
//}
//pub(crate) fn ias(msg: &str) -> NoSQLError {
//NoSQLError {
//code: NoSQLErrorCode::IllegalArgument,
//message: msg.to_string(),
//}
//}

macro_rules! ia_error {
    ($($t:tt)*) => {{
        let m = format!($($t)*);
        NoSQLError {
            code: crate::error::NoSQLErrorCode::IllegalArgument,
            message: format!("{} ({})", m, crate::error::sdk_version()),
        }
    }};
}

pub(crate) use ia_error;

macro_rules! ia_err {
    ($($t:tt)*) => {{
        let m = format!($($t)*);
        Err(NoSQLError {
            code: crate::error::NoSQLErrorCode::IllegalArgument,
            message: format!("{} ({})", m, crate::error::sdk_version()),
        })
    }};
}

pub(crate) use ia_err;

impl From<reqwest::Error> for NoSQLError {
    fn from(e: reqwest::Error) -> Self {
        let mut code = NoSQLErrorCode::ServerError;
        if e.is_timeout() {
            code = NoSQLErrorCode::RequestTimeout;
        }
        // TODO: others?
        NoSQLError {
            code: code,
            message: format!(
                "reqwest error: {} ({})",
                e.to_string(),
                crate::error::sdk_version()
            ),
        }
    }
}

impl From<reqwest::header::InvalidHeaderValue> for NoSQLError {
    fn from(e: reqwest::header::InvalidHeaderValue) -> Self {
        ia_error!("invalid header value: {}", e.to_string())
    }
}

impl From<url::ParseError> for NoSQLError {
    fn from(e: url::ParseError) -> Self {
        ia_error!("error parsing url: {}", e.to_string())
    }
}

impl From<chrono::ParseError> for NoSQLError {
    fn from(e: chrono::ParseError) -> Self {
        ia_error!("invalid datetime value: {}", e.to_string())
    }
}

// TODO: remove this and write From(s) for all other errors
impl From<Box<dyn std::error::Error>> for NoSQLError {
    fn from(e: Box<dyn std::error::Error>) -> Self {
        ia_error!("{}", e.to_string())
    }
}

// NoSQLErrorCode represents the error code.
// Error codes are divided into categories as follows:
//
// 1. Error codes for user-generated errors, range from 1 to 50(exclusive).
// These include illegal arguments, exceeding size limits for some objects,
// resource not found, etc.
//
// 2. Error codes for user throttling, range from 50 to 100(exclusive).
//
// 3. Error codes for server issues, range from 100 to 150(exclusive).
//
// 3.1 Retryable server issues, range from 100 to 125(exclusive), that represent
// internal problems, presumably temporary, and need to be sent back to the
// application for retry.
//
// 3.2 Other server issues, begin from 125.
// These include server illegal state, unknown server error, etc.
// They might be retryable, or not.
//
#[derive(Debug, Clone, Copy, Eq, PartialEq, TryFromPrimitive)]
#[repr(i32)]
pub enum NoSQLErrorCode {
    /// NoError represents there is no error.
    NoError = 0,

    /// UnknownOperation error represents the operation attempted is unknown.
    UnknownOperation = 1,

    /// TableNotFound error represents the operation attempted to access a table
    /// that does not exist or is not in a visible state.
    TableNotFound = 2,

    /// IndexNotFound error represents the operation attempted to access an index
    /// that does not exist or is not in a visible state.
    IndexNotFound = 3,

    /// IllegalArgument error represents the application provided an illegal
    /// argument for the operation.
    IllegalArgument = 4,

    /// RowSizeLimitExceeded error represents an attempt has been made to create
    /// a row with a size that exceeds the system defined limit.
    ///
    /// This is used for cloud service only.
    RowSizeLimitExceeded = 5,

    /// KeySizeLimitExceeded error represents an attempt has been made to create
    /// a row with a primary key or index key size that exceeds the system defined limit.
    ///
    /// This is used for cloud service only.
    KeySizeLimitExceeded = 6,

    /// BatchOpNumberLimitExceeded error represents that the number of operations
    /// included in Client.WriteMultiple operation exceeds the system defined limit.
    ///
    /// This is used for cloud service only.
    BatchOpNumberLimitExceeded = 7,

    /// RequestSizeLimitExceeded error represents that the size of a request
    /// exceeds the system defined limit.
    ///
    /// This is used for cloud service only.
    RequestSizeLimitExceeded = 8,

    /// TableExists error represents the operation attempted to create a table
    /// but the named table already exists.
    TableExists = 9,

    /// IndexExists error represents the operation attempted to create an index
    /// for a table but the named index already exists.
    IndexExists = 10,

    /// InvalidAuthorization error represents the client provides an invalid
    /// authorization string in the request header.
    InvalidAuthorization = 11,

    /// InsufficientPermission error represents an application does not have
    /// sufficient permission to perform a request.
    InsufficientPermission = 12,

    /// ResourceExists error represents the operation attempted to create a
    /// resource but it already exists.
    ResourceExists = 13,

    /// ResourceNotFound error represents the operation attempted to access a
    /// resource that does not exist or is not in a visible state.
    ResourceNotFound = 14,

    /// TableLimitExceeded error represents an attempt has been made to create a
    /// number of tables that exceeds the system defined limit.
    ///
    /// This is used for cloud service only.
    TableLimitExceeded = 15,

    /// IndexLimitExceeded error represents an attempt has been made to create
    /// more indexes on a table than the system defined limit.
    ///
    /// This is used for cloud service only.
    IndexLimitExceeded = 16,

    /// BadProtocolMessage error represents there is an error in the protocol
    /// used by client and server to exchange informations.
    /// This error is not visible to applications. It is wrapped as an IllegalArgument
    /// error and returned to applications.
    BadProtocolMessage = 17,

    /// EvolutionLimitExceeded error represents an attempt has been made to evolve
    /// the schema of a table more times than allowed by the system defined limit.
    ///
    /// This is used for cloud service only.
    EvolutionLimitExceeded = 18,

    /// TableDeploymentLimitExceeded error represents an attempt has been made to
    /// create or modify a table using limits that exceed the maximum allowed for
    /// a single table.
    ///
    /// This is system-defined limit, used for cloud service only.
    TableDeploymentLimitExceeded = 19,

    /// TenantDeploymentLimitExceeded error represents an attempt has been made to
    /// create or modify a table using limits that cause the tenant's aggregate
    /// resources to exceed the maximum allowed for a tenant.
    ///
    /// This is system-defined limit, used for cloud service only.
    TenantDeploymentLimitExceeded = 20,

    /// OperationNotSupported error represents the operation attempted is not supported.
    /// This may be related to on-premise vs cloud service configurations.
    OperationNotSupported = 21,

    /// EtagMismatch is used only by the cloud REST service.
    EtagMismatch = 22,

    /// CannotCancelWorkRequest is used only by the cloud REST service.
    CannotCancelWorkRequest = 23,

    /// UnsupportedProtocol error indicates the server does not support the
    /// given driver protocol version. The driver should decrement its internal
    /// protocol version (and accompanying logic) and try again.
    UnsupportedProtocol = 24,

    /// ReadLimitExceeded error represents that the provisioned read throughput
    /// has been exceeded.
    ///
    /// Operations resulting in this error can be retried but it is recommended
    /// that callers use a delay before retrying in order to minimize the chance
    /// that a retry will also be throttled. Applications should attempt to avoid
    /// throttling errors by rate limiting themselves to the degree possible.
    ///
    /// Retries and behavior related to throttling can be managed by configuring
    /// the DefaultRetryHandler for client or by providing a custom implementation
    /// of the RetryHandler interface for client.
    ///
    /// This is used for cloud service only.
    ReadLimitExceeded = 50,

    /// WriteLimitExceeded error represents that the provisioned write throughput
    /// has been exceeded.
    ///
    /// Operations resulting in this error can be retried but it is recommended
    /// that callers use a delay before retrying in order to minimize the chance
    /// that a retry will also be throttled. Applications should attempt to avoid
    /// throttling errors by rate limiting themselves to the degree possible.
    ///
    /// Retries and behavior related to throttling can be managed by configuring
    /// the DefaultRetryHandler for client or by providing a custom implementation
    /// of the RetryHandler interface for client.
    ///
    /// This is used for cloud service only.
    WriteLimitExceeded = 51,

    /// SizeLimitExceeded error represents a table size limit has been exceeded
    /// by writing more data than the table can support.
    /// This error is not retryable because the conditions that lead to it being
    /// retuned, while potentially transient, typically require user intervention.
    SizeLimitExceeded = 52,

    /// OperationLimitExceeded error represents the operation attempted has exceeded
    /// the allowed limits for non-data operations defined by the system.
    ///
    /// This error is returned when a non-data operation is throttled.
    /// This can happen if an application attempts too many control operations
    /// such as table creation, deletion, or similar methods. Such operations
    /// do not use throughput or capacity provisioned for a given table but they
    /// consume system resources and their use is limited.
    ///
    /// Operations resulting in this error can be retried but it is recommended
    /// that callers use a relatively large delay before retrying in order to
    /// minimize the chance that a retry will also be throttled.
    ///
    /// This is used for cloud service only.
    OperationLimitExceeded = 53,

    /// RequestTimeout error represents the request cannot be processed or does
    /// not complete when the specified timeout duration elapses.
    ///
    /// If a retry handler is configured for the client it is possible that the
    /// request has been retried a number of times before the timeout occurs.
    RequestTimeout = 100,

    /// ServerError represents there is an internal system problem.
    /// Most system problems are temporary.
    /// The operation that leads to this error may need to retry.
    ServerError = 101,

    /// ServiceUnavailable error represents the requested service is currently unavailable.
    /// This is usually a temporary error.
    /// The operation that leads to this error may need to retry.
    ServiceUnavailable = 102,

    /// TableBusy error represents the table is in use or busy.
    /// This error may be returned when a table operation fails.
    /// Note that only one modification operation at a time is allowed on a table.
    TableBusy = 103,

    /// SecurityInfoUnavailable error represents the security information is not
    /// ready in the system.
    /// This error will occur as the system acquires security information and
    /// must be retried in order for authorization to work properly.
    ///
    /// This is used for cloud service only.
    SecurityInfoUnavailable = 104,

    /// RetryAuthentication error represents the authentication failed and may need to retry.
    /// This may be returned by kvstore.AccessTokenProvider in the following cases:
    ///
    /// 1. Authentication information was not provided in the request header.
    /// 2. The user session has expired. By default kvstore.AccessTokenProvider
    /// will automatically retry authentication with user credentials provided.
    ///
    RetryAuthentication = 105,

    /// UnknownError represents an unknown error has occurred on the server.
    UnknownError = 125,

    /// IllegalState error represents an illegal state.
    IllegalState = 126,

    /// InternalRetry is used internally for retry logic.
    InternalRetry = 1001,
}
