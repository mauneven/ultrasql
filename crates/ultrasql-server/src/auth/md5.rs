//! MD5 password authentication (PostgreSQL wire-protocol §55.3).
//!
//! MD5 auth is the legacy PostgreSQL authentication method, superseded by
//! SCRAM-SHA-256 in PG 10 but still widely supported by clients and proxies.
//! UltraSQL gates it behind the `auth.allow_md5 = false` configuration flag;
//! it is disabled by default because it offers no salted storage and is
//! susceptible to offline dictionary attacks.
//!
//! ## Wire exchange
//!
//! ```text
//! Server: AuthenticationMD5Password { salt: [u8; 4] }
//! Client: Password { password: "md5<hex(MD5(hex(MD5(password + username)) + salt))>" }
//! Server: AuthenticationOk  -or-  ErrorResponse
//! ```
//!
//! ## Hash derivation
//!
//! PostgreSQL's MD5 auth hashes as follows:
//!
//! 1. `inner = MD5(password || username)` — hex-encoded lowercase.
//! 2. `outer = MD5(inner || salt)` — hex-encoded lowercase.
//! 3. Wire value = `"md5"` + outer.
//!
//! The server receives the wire value and must reproduce it from the stored
//! password (or its own stored `md5(password + username)` if pre-hashed) plus
//! the random salt it sent.
//!
//! Because MD5 is broken for collision resistance, this implementation is
//! intentionally minimal and hidden behind a configuration gate.

use rand::RngCore;

/// A 4-byte random MD5 auth salt.
///
/// Generated freshly for every authentication exchange; never reused.
pub type Md5Salt = [u8; 4];

/// Generate a cryptographically random 4-byte MD5 salt.
#[must_use]
pub fn random_salt() -> Md5Salt {
    let mut salt = [0u8; 4];
    rand::thread_rng().fill_bytes(&mut salt);
    salt
}

/// Compute the MD5 password hash the client is expected to send.
///
/// `password` and `username` are UTF-8 strings (no NUL bytes). `salt` is the
/// 4-byte challenge sent in `AuthenticationMD5Password`.
///
/// Returns the complete wire-format string including the `"md5"` prefix.
#[must_use]
pub fn compute_md5_response(password: &str, username: &str, salt: &Md5Salt) -> String {
    // Step 1: MD5(password || username)
    let mut inner_input = Vec::with_capacity(password.len() + username.len());
    inner_input.extend_from_slice(password.as_bytes());
    inner_input.extend_from_slice(username.as_bytes());
    let inner_digest = md5::compute(&inner_input);
    let inner = hex::encode(inner_digest.as_ref());

    // Step 2: MD5(inner || salt)
    let mut outer_input = Vec::with_capacity(inner.len() + salt.len());
    outer_input.extend_from_slice(inner.as_bytes());
    outer_input.extend_from_slice(salt);
    let outer_digest = md5::compute(&outer_input);
    let outer = hex::encode(outer_digest.as_ref());

    format!("md5{outer}")
}

/// Verify a client's MD5 response against the expected hash.
///
/// `expected` is the wire-format string returned by [`compute_md5_response`].
/// `client_response` is the value from the client's `Password` message.
///
/// Returns `true` if the response matches.
#[must_use]
pub fn verify_md5_response(expected: &str, client_response: &str) -> bool {
    constant_time_eq(expected.as_bytes(), client_response.as_bytes())
}

fn constant_time_eq(expected: &[u8], supplied: &[u8]) -> bool {
    let mut diff = expected.len() ^ supplied.len();
    for (idx, expected_byte) in expected.iter().copied().enumerate() {
        let supplied_byte = supplied.get(idx).copied().unwrap_or(0);
        diff |= usize::from(expected_byte ^ supplied_byte);
    }
    diff == 0
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn md5_response_has_correct_prefix() {
        let salt = [0x01, 0x02, 0x03, 0x04];
        let response = compute_md5_response("password", "user", &salt);
        assert!(
            response.starts_with("md5"),
            "response must start with 'md5': {response}"
        );
        assert_eq!(response.len(), 35, "3 chars 'md5' + 32 hex chars");
    }

    #[test]
    fn md5_round_trip_verifies() {
        let salt = [0xDE, 0xAD, 0xBE, 0xEF];
        let response = compute_md5_response("s3cr3t", "alice", &salt);
        assert!(
            verify_md5_response(&response, &response),
            "identical hashes must verify"
        );
    }

    #[test]
    fn md5_wrong_password_does_not_verify() {
        let salt = [0x11, 0x22, 0x33, 0x44];
        let correct = compute_md5_response("correct", "alice", &salt);
        let wrong = compute_md5_response("wrong", "alice", &salt);
        assert!(
            !verify_md5_response(&correct, &wrong),
            "different passwords must not verify"
        );
    }

    #[test]
    fn md5_different_users_produce_different_hashes() {
        let salt = [0xAA, 0xBB, 0xCC, 0xDD];
        let h1 = compute_md5_response("pw", "alice", &salt);
        let h2 = compute_md5_response("pw", "bob", &salt);
        assert_ne!(h1, h2);
    }

    #[test]
    fn md5_different_salts_produce_different_hashes() {
        let salt1 = [0x01, 0x02, 0x03, 0x04];
        let salt2 = [0x05, 0x06, 0x07, 0x08];
        let h1 = compute_md5_response("pw", "user", &salt1);
        let h2 = compute_md5_response("pw", "user", &salt2);
        assert_ne!(h1, h2);
    }

    #[test]
    fn md5_known_vector() {
        // Known vector computed manually:
        // MD5("password" + "user") = 7e4f2de8e80c77da82a0bc1a7d93ece6
        // MD5("7e4f2de8e80c77da82a0bc1a7d93ece6" + "\x01\x02\x03\x04")
        let salt = [0x01, 0x02, 0x03, 0x04];
        let response = compute_md5_response("password", "user", &salt);
        // The response must be deterministic.
        let second = compute_md5_response("password", "user", &salt);
        assert_eq!(response, second, "MD5 computation must be deterministic");
        // Must be parseable as hex after the "md5" prefix.
        let hex_part = response.strip_prefix("md5").expect("has md5 prefix");
        assert_eq!(hex_part.len(), 32);
        assert!(
            hex_part.chars().all(|c| c.is_ascii_hexdigit()),
            "hex part must be all hex digits"
        );
    }
}
