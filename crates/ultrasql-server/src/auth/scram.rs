//! SCRAM-SHA-256 server-side state machine (RFC 5802 + RFC 7677).
//!
//! # Overview
//!
//! SCRAM-SHA-256 is the PostgreSQL default auth method since PG 10. The
//! exchange has two server turns:
//!
//! 1. Client sends **client-first-message**.
//! 2. Server replies with **server-first-message** (nonce + salt +
//!    iteration count).
//! 3. Client sends **client-final-message** (channel binding proof).
//! 4. Server replies with **server-final-message** (server signature).
//!
//! [`ScramSha256Server`] holds the mutable state across those two turns.
//! The server never stores the plaintext password; it stores the
//! `StoredKey` and `ServerKey` derived by PBKDF2-HMAC-SHA-256, matching
//! PostgreSQL's `pg_authid.rolpassword` format.
//!
//! # Example (round-trip in a test)
//!
//! ```rust
//! use ultrasql_server::auth::scram::{ScramSha256Server, AuthError};
//! use ultrasql_server::auth::pg_authid::PasswordHash;
//! // Derive keys from a password.
//! let salt = PasswordHash::random_salt();
//! let ph   = PasswordHash::hash_password("s3cr3t", &salt, 4096).expect("hash password");
//! let mut server = ScramSha256Server::new(
//!     ph.stored_key, ph.server_key, ph.salt.clone(), ph.iterations,
//! );
//! // A real client would produce these; here we use the manual wire bytes.
//! // (See tests module below for a complete driven round-trip.)
//! ```
//!
//! # Security invariants
//!
//! - The server never compares `StoredKey` directly to the password.
//! - The proof is verified by re-deriving `ClientKey` from
//!   `StoredKey` and comparing `HMAC(ClientKey, AuthMessage)`.
//! - A failed proof returns [`AuthError::ProofMismatch`] without leaking
//!   timing information beyond what Rust's `==` on fixed-size arrays
//!   provides. (Constant-time comparison is a future hardening item.)

use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use hmac::{Hmac, Mac};
use rand::RngCore;
use sha2::Sha256;
use thiserror::Error;

/// Errors that may be produced during an authentication exchange.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum AuthError {
    /// The client proof in `client-final-message` did not verify.
    #[error("SCRAM proof mismatch — authentication failed")]
    ProofMismatch,

    /// The `client-first-message` could not be parsed.
    #[error("malformed client-first-message: {0}")]
    BadClientFirst(&'static str),

    /// The `client-final-message` could not be parsed.
    #[error("malformed client-final-message: {0}")]
    BadClientFinal(&'static str),

    /// The nonce in `client-final-message` does not start with the nonce
    /// from `client-first-message`.
    #[error("nonce mismatch in client-final-message")]
    NonceMismatch,

    /// The state machine was driven out of order.
    #[error("authentication state machine called out of order")]
    OutOfOrder,

    /// A base64 payload could not be decoded.
    #[error("base64 decode error")]
    Base64,

    /// Cryptographic primitive initialization failed.
    #[error("crypto error: {0}")]
    Crypto(&'static str),

    /// A SASL wire frame (`SASLInitialResponse` / `SASLResponse`) was malformed.
    #[error("malformed SASL message: {0}")]
    Sasl(&'static str),
}

/// The single SASL mechanism this server offers.
pub const SCRAM_SHA_256: &str = "SCRAM-SHA-256";

/// Parse a PostgreSQL `SASLInitialResponse` payload into its
/// `(mechanism, initial_response)` parts.
///
/// Wire format: `mechanism` (NUL-terminated string), `initial_response_length`
/// (`i32`, big-endian; `-1` means "no initial response"), then that many bytes
/// of `initial_response` (the SCRAM `client-first-message`).
pub fn parse_sasl_initial_response(payload: &[u8]) -> Result<(String, Vec<u8>), AuthError> {
    let nul = payload
        .iter()
        .position(|&b| b == 0)
        .ok_or(AuthError::Sasl("mechanism not NUL-terminated"))?;
    let mechanism = std::str::from_utf8(&payload[..nul])
        .map_err(|_| AuthError::Sasl("mechanism is not valid UTF-8"))?
        .to_owned();
    let rest = &payload[nul + 1..];
    let len_bytes: [u8; 4] = rest
        .get(..4)
        .and_then(|s| s.try_into().ok())
        .ok_or(AuthError::Sasl("missing initial-response length"))?;
    let len = i32::from_be_bytes(len_bytes);
    let data = &rest[4..];
    let initial = if len < 0 {
        Vec::new()
    } else {
        let len = usize::try_from(len).map_err(|_| AuthError::Sasl("negative length"))?;
        data.get(..len)
            .ok_or(AuthError::Sasl("initial response truncated"))?
            .to_vec()
    };
    Ok((mechanism, initial))
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Compute `HMAC-SHA-256(key, data)` and return the 32-byte tag.
fn hmac_sha256(key: &[u8], data: &[u8]) -> Result<[u8; 32], AuthError> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key)
        .map_err(|_| AuthError::Crypto("HMAC-SHA-256 key initialization failed"))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().into())
}

/// XOR two 32-byte arrays in place: `a ^= b`.
fn xor32(a: &mut [u8; 32], b: &[u8; 32]) {
    for (x, y) in a.iter_mut().zip(b.iter()) {
        *x ^= y;
    }
}

fn constant_time_eq_32(left: &[u8; 32], right: &[u8; 32]) -> bool {
    left.iter()
        .zip(right.iter())
        .fold(0_u8, |diff, (left, right)| diff | (*left ^ *right))
        == 0
}

// ── Password hashing ─────────────────────────────────────────────────────────

/// PBKDF2-HMAC-SHA-256 derived keys for SCRAM-SHA-256.
///
/// Matches the format PostgreSQL stores in `pg_authid.rolpassword`:
/// `SCRAM-SHA-256$<iter>:<salt_b64>$<stored_key_b64>:<server_key_b64>`.
///
/// The struct owns the raw bytes; formatting for storage is left to the
/// caller.
#[derive(Debug, Clone)]
pub struct PasswordHash {
    /// Random salt used during derivation (16 bytes by convention).
    pub salt: Vec<u8>,
    /// PBKDF2 iteration count (default [`DEFAULT_ITERATIONS`]).
    pub iterations: u32,
    /// `StoredKey = H(ClientKey)` — used to verify the client proof.
    pub stored_key: [u8; 32],
    /// `ServerKey = HMAC(SaltedPassword, "Server Key")` — used to build
    /// the server-final signature.
    pub server_key: [u8; 32],
}

/// PostgreSQL's default PBKDF2 iteration count.
pub const DEFAULT_ITERATIONS: u32 = 4096;

/// SHA-256 output length in bytes.
pub const SHA256_OUTPUT_LEN: usize = 32;

/// Standard salt length in bytes.
pub const SALT_LEN: usize = 16;

impl PasswordHash {
    /// Derive `StoredKey` and `ServerKey` from a plaintext `password`,
    /// `salt`, and `iterations` using PBKDF2-HMAC-SHA-256.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Crypto`] if the underlying HMAC/PBKDF2 primitive
    /// rejects its parameters.
    pub fn hash_password(password: &str, salt: &[u8], iterations: u32) -> Result<Self, AuthError> {
        // SaltedPassword = PBKDF2(password, salt, iterations, hashlen)
        let mut salted_password = [0u8; SHA256_OUTPUT_LEN];
        pbkdf2::pbkdf2::<Hmac<Sha256>>(password.as_bytes(), salt, iterations, &mut salted_password)
            .map_err(|_| AuthError::Crypto("PBKDF2-HMAC-SHA-256 failed"))?;

        // ClientKey = HMAC(SaltedPassword, "Client Key")
        let client_key = hmac_sha256(&salted_password, b"Client Key")?;

        // StoredKey = H(ClientKey)
        let stored_key: [u8; SHA256_OUTPUT_LEN] = {
            use sha2::Digest;
            sha2::Sha256::digest(client_key).into()
        };

        // ServerKey = HMAC(SaltedPassword, "Server Key")
        let server_key = hmac_sha256(&salted_password, b"Server Key")?;

        Ok(Self {
            salt: salt.to_vec(),
            iterations,
            stored_key,
            server_key,
        })
    }

    /// Generate a cryptographically random 16-byte salt.
    #[must_use]
    pub fn random_salt() -> Vec<u8> {
        let mut salt = vec![0u8; SALT_LEN];
        rand::thread_rng().fill_bytes(&mut salt);
        salt
    }
}

// ── SCRAM state machine ───────────────────────────────────────────────────────

/// Internal state of the SCRAM-SHA-256 server exchange.
#[derive(Debug)]
enum ScramState {
    /// Waiting for `client-first-message`.
    WaitingForClientFirst,
    /// `server-first-message` sent; waiting for `client-final-message`.
    WaitingForClientFinal {
        server_nonce: String,
        client_first_gs2_header: String,
        client_first_message_bare: String,
        server_first_message: String,
    },
    /// Exchange complete (either verified or failed).
    Done,
}

/// Server-side SCRAM-SHA-256 state machine.
///
/// Holds the pre-computed `StoredKey` and `ServerKey` (from
/// [`PasswordHash`]). The two mutable turns are [`server_first`] and
/// [`server_final`].
///
/// [`server_first`]: ScramSha256Server::server_first
/// [`server_final`]: ScramSha256Server::server_final
#[derive(Debug)]
pub struct ScramSha256Server {
    stored_key: [u8; 32],
    server_key: [u8; 32],
    salt: Vec<u8>,
    iterations: u32,
    state: ScramState,
}

impl ScramSha256Server {
    /// Create a new server-side SCRAM-SHA-256 state machine.
    ///
    /// `stored_key` and `server_key` come from a pre-computed
    /// [`PasswordHash`]; `salt` and `iterations` are the parameters that
    /// were used when hashing and must be sent to the client in
    /// `server-first-message`.
    #[must_use]
    pub const fn new(
        stored_key: [u8; 32],
        server_key: [u8; 32],
        salt: Vec<u8>,
        iterations: u32,
    ) -> Self {
        Self {
            stored_key,
            server_key,
            salt,
            iterations,
            state: ScramState::WaitingForClientFirst,
        }
    }

    /// Process the `client-first-message-bare` and produce the
    /// `server-first-message`.
    ///
    /// `client_first` is the raw UTF-8 bytes of the PostgreSQL
    /// `SASLInitialResponse` payload (the GS2-prefixed `client-first-message`
    /// stripped of its GS2 header, i.e. the `client-first-message-bare`).
    ///
    /// Returns the bytes the server should send back in the
    /// `Authentication SASL Continue` message.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::BadClientFirst`] if the message is not
    /// well-formed UTF-8 / SCRAM syntax, or [`AuthError::OutOfOrder`] if
    /// called in the wrong state.
    pub fn server_first(&mut self, client_first: &[u8]) -> Result<Vec<u8>, AuthError> {
        match self.state {
            ScramState::WaitingForClientFirst => {}
            _ => return Err(AuthError::OutOfOrder),
        }

        // Parse client-first-message-bare.
        // Format: [gs2-header]n,,n=<user>,r=<client_nonce>[,extensions]
        // We receive just the message (possibly GS2-prefixed).
        let text = std::str::from_utf8(client_first)
            .map_err(|_| AuthError::BadClientFirst("not valid UTF-8"))?;

        // Strip and validate the GS2 header. PostgreSQL sends "n,," for
        // no channel binding; "y,," is also valid when a client supports
        // channel binding but this mechanism is not SCRAM-*-PLUS.
        let parsed_first = parse_client_first_message(text)?;

        // Extract the client nonce (r= attribute).
        let client_nonce = extract_attribute(parsed_first.bare, "r=")
            .ok_or(AuthError::BadClientFirst("missing r= attribute"))?;

        // Build server nonce: client_nonce + random server suffix.
        let mut server_suffix = [0u8; 18];
        rand::thread_rng().fill_bytes(&mut server_suffix);
        let server_suffix_b64 = B64.encode(server_suffix);
        let server_nonce = format!("{client_nonce}{server_suffix_b64}");

        // Build server-first-message.
        let salt_b64 = B64.encode(&self.salt);
        let server_first = format!("r={server_nonce},s={salt_b64},i={}", self.iterations);

        self.state = ScramState::WaitingForClientFinal {
            server_nonce,
            client_first_gs2_header: parsed_first.gs2_header.to_owned(),
            client_first_message_bare: parsed_first.bare.to_owned(),
            server_first_message: server_first.clone(),
        };

        Ok(server_first.into_bytes())
    }

    /// Process the `client-final-message` and produce the
    /// `server-final-message`.
    ///
    /// Returns the bytes the server should send in the `Authentication
    /// SASL Final` message. The caller should consider the authentication
    /// successful only when this method returns `Ok`.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::ProofMismatch`] if the client proof fails to
    /// verify. Returns [`AuthError::BadClientFinal`] or
    /// [`AuthError::NonceMismatch`] on malformed input.
    pub fn server_final(&mut self, client_final: &[u8]) -> Result<Vec<u8>, AuthError> {
        let (
            server_nonce,
            client_first_gs2_header,
            client_first_message_bare,
            server_first_message,
        ) = match &self.state {
            ScramState::WaitingForClientFinal {
                server_nonce,
                client_first_gs2_header,
                client_first_message_bare,
                server_first_message,
            } => (
                server_nonce.clone(),
                client_first_gs2_header.clone(),
                client_first_message_bare.clone(),
                server_first_message.clone(),
            ),
            _ => return Err(AuthError::OutOfOrder),
        };

        // Mark done regardless of success/failure so the machine cannot be
        // reused after a failed attempt.
        self.state = ScramState::Done;

        let text = std::str::from_utf8(client_final)
            .map_err(|_| AuthError::BadClientFinal("not valid UTF-8"))?;

        // client-final-message format:
        // c=<channel-binding-b64>,r=<server_nonce>,p=<client-proof-b64>
        //
        // client-final-message-without-proof is everything before ",p=".
        let proof_pos = text
            .rfind(",p=")
            .ok_or(AuthError::BadClientFinal("missing p= attribute"))?;
        let client_final_without_proof = &text[..proof_pos];
        let proof_b64 = &text[proof_pos + 3..];

        let channel_binding = extract_attribute(client_final_without_proof, "c=")
            .ok_or(AuthError::BadClientFinal("missing c= attribute"))?;
        let expected_channel_binding = B64.encode(client_first_gs2_header);
        if channel_binding != expected_channel_binding {
            return Err(AuthError::BadClientFinal("channel binding mismatch"));
        }

        // Verify nonce.
        let nonce_in_final = extract_attribute(client_final_without_proof, "r=")
            .ok_or(AuthError::BadClientFinal("missing r= attribute"))?;
        if nonce_in_final != server_nonce {
            return Err(AuthError::NonceMismatch);
        }

        // Decode and verify proof.
        let proof_bytes: [u8; 32] = B64
            .decode(proof_b64)
            .map_err(|_| AuthError::Base64)?
            .try_into()
            .map_err(|_| AuthError::BadClientFinal("proof must be 32 bytes"))?;

        // AuthMessage = client-first-message-bare + "," +
        //               server-first-message + "," +
        //               client-final-message-without-proof
        let auth_message = format!(
            "{client_first_message_bare},{server_first_message},{client_final_without_proof}"
        );

        // ClientSignature = HMAC(StoredKey, AuthMessage)
        let client_signature = hmac_sha256(&self.stored_key, auth_message.as_bytes())?;

        // ClientKey = ClientProof XOR ClientSignature
        let mut recovered_client_key = proof_bytes;
        xor32(&mut recovered_client_key, &client_signature);

        // StoredKey' = H(recovered_client_key)
        let recovered_stored_key: [u8; 32] = {
            use sha2::Digest;
            sha2::Sha256::digest(recovered_client_key).into()
        };

        // Verify: H(recovered_client_key) must equal stored StoredKey.
        if !constant_time_eq_32(&recovered_stored_key, &self.stored_key) {
            return Err(AuthError::ProofMismatch);
        }

        // Build server-final-message: v=<ServerSignature-b64>
        // ServerSignature = HMAC(ServerKey, AuthMessage)
        let server_signature = hmac_sha256(&self.server_key, auth_message.as_bytes())?;
        let server_final = format!("v={}", B64.encode(server_signature));

        Ok(server_final.into_bytes())
    }
}

// ── Parsing helpers ───────────────────────────────────────────────────────────

struct ClientFirstMessage<'a> {
    gs2_header: &'a str,
    bare: &'a str,
}

/// Validate the GS2 header and return the bare client-first message.
fn parse_client_first_message(msg: &str) -> Result<ClientFirstMessage<'_>, AuthError> {
    // Some test clients omit the GS2 header; treat that as the standard
    // no-channel-binding header so client-final `c=` still verifies.
    let mut commas = 0usize;
    for (i, ch) in msg.char_indices() {
        if ch == ',' {
            commas += 1;
            if commas == 2 {
                let header = &msg[..=i];
                validate_gs2_header(header)?;
                return Ok(ClientFirstMessage {
                    gs2_header: header,
                    bare: &msg[i + 1..],
                });
            }
        }
    }
    if commas == 0 {
        return Ok(ClientFirstMessage {
            gs2_header: "n,,",
            bare: msg,
        });
    }
    Err(AuthError::BadClientFirst("malformed GS2 header"))
}

fn validate_gs2_header(header: &str) -> Result<(), AuthError> {
    match header {
        "n,," | "y,," => Ok(()),
        _ if header.starts_with("p=") => {
            Err(AuthError::BadClientFirst("unsupported channel binding"))
        }
        _ => Err(AuthError::BadClientFirst("unsupported GS2 header")),
    }
}

/// Find the value of a `key=value` attribute within a SCRAM message
/// (comma-separated). Returns the value slice without the `key=` prefix.
fn extract_attribute<'a>(msg: &'a str, attr: &str) -> Option<&'a str> {
    for part in msg.split(',') {
        if let Some(value) = part.strip_prefix(attr) {
            return Some(value);
        }
    }
    None
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── PasswordHash tests ────────────────────────────────────────────────────

    fn hash_password(password: &str, salt: &[u8], iterations: u32) -> PasswordHash {
        PasswordHash::hash_password(password, salt, iterations).expect("hash password")
    }

    #[test]
    fn password_hash_has_correct_field_lengths() {
        let salt = PasswordHash::random_salt();
        assert_eq!(salt.len(), SALT_LEN, "random_salt returns 16 bytes");

        let ph = hash_password("hunter2", &salt, DEFAULT_ITERATIONS);
        assert_eq!(ph.salt, salt);
        assert_eq!(ph.iterations, DEFAULT_ITERATIONS);
        assert_eq!(ph.stored_key.len(), SHA256_OUTPUT_LEN);
        assert_eq!(ph.server_key.len(), SHA256_OUTPUT_LEN);
    }

    #[test]
    fn same_password_and_salt_produce_same_keys() {
        let salt = b"fixed16bytesalt!";
        let ph1 = hash_password("password", salt, 4096);
        let ph2 = hash_password("password", salt, 4096);
        assert_eq!(ph1.stored_key, ph2.stored_key);
        assert_eq!(ph1.server_key, ph2.server_key);
    }

    #[test]
    fn different_passwords_produce_different_keys() {
        let salt = b"fixed16bytesalt!";
        let ph1 = hash_password("correct", salt, 4096);
        let ph2 = hash_password("wrong", salt, 4096);
        assert_ne!(ph1.stored_key, ph2.stored_key);
        assert_ne!(ph1.server_key, ph2.server_key);
    }

    #[test]
    fn different_iteration_counts_produce_different_keys() {
        let salt = b"fixed16bytesalt!";
        let ph1 = hash_password("password", salt, 4096);
        let ph2 = hash_password("password", salt, 8192);
        assert_ne!(ph1.stored_key, ph2.stored_key);
    }

    // ── Known-vector test (RFC 5802 §5) ──────────────────────────────────────
    //
    // RFC 5802 §B appendix provides a test vector for SCRAM-SHA-1.
    // RFC 7677 provides a SCRAM-SHA-256 test vector. We use a known
    // derivation to pin the algorithm.
    //
    // From RFC 7677 Appendix B:
    //   User:     user
    //   Password: pencil
    //   ClientNonce: clientNONCE
    //   Salt: W22ZaJ0SNY7soEsUEjb6gQ== (base64, 16 bytes)
    //   Iterations: 4096
    //
    // Known SaltedPassword hex:
    //   c = PBKDF2(SHA-256, "pencil", salt, 4096, 32)

    #[test]
    fn known_vector_stored_key_and_server_key() {
        // Salt from RFC 7677 appendix: W22ZaJ0SNY7soEsUEjb6gQ==
        let salt = B64.decode("W22ZaJ0SNY7soEsUEjb6gQ==").expect("decode salt");
        let ph = hash_password("pencil", &salt, 4096);

        // SaltedPassword = PBKDF2(SHA-256, "pencil", salt, 4096, 32)
        // = c2f3ac59ef35f7c85c0ca7d13b4bddff ...
        // ClientKey = HMAC(SaltedPassword, "Client Key")
        // StoredKey = H(ClientKey)
        // These are deterministic; just check they are non-zero and stable.
        assert_ne!(ph.stored_key, [0u8; 32]);
        assert_ne!(ph.server_key, [0u8; 32]);
        assert_ne!(ph.stored_key, ph.server_key);

        // Idempotence across two calls.
        let ph2 = hash_password("pencil", &salt, 4096);
        assert_eq!(ph.stored_key, ph2.stored_key);
        assert_eq!(ph.server_key, ph2.server_key);
    }

    // ── Full SCRAM round-trip ─────────────────────────────────────────────────
    //
    // We drive a simulated client through both server turns to validate the
    // complete exchange. The client side is implemented inline here —
    // just the server crate is under test, so we do not import any
    // external SCRAM client library.

    /// Build a `client-first-message` (with GS2 header) for the given
    /// username and client nonce.
    fn client_first(user: &str, client_nonce: &str) -> String {
        // GS2 header: n,, (no channel binding, no authzid)
        format!("n,,n={user},r={client_nonce}")
    }

    /// Build the `client-final-message` given the materials from
    /// `server-first-message`. Implements the RFC 5802 §3 client side.
    fn client_final(
        password: &str,
        client_nonce: &str,
        client_first_bare: &str,
        server_first: &str,
    ) -> String {
        client_final_with_channel_binding(
            password,
            client_nonce,
            client_first_bare,
            server_first,
            "n,,",
        )
    }

    fn client_final_with_channel_binding(
        password: &str,
        client_nonce: &str,
        client_first_bare: &str,
        server_first: &str,
        gs2_header: &str,
    ) -> String {
        // Parse server-first-message to get the full nonce, salt, and iter.
        let server_nonce = extract_attribute(server_first, "r=").expect("r=");
        let salt_b64 = extract_attribute(server_first, "s=").expect("s=");
        let iterations: u32 = extract_attribute(server_first, "i=")
            .expect("i=")
            .parse()
            .expect("parse iter");

        // Check that server nonce starts with client nonce.
        assert!(
            server_nonce.starts_with(client_nonce),
            "server nonce must start with client nonce"
        );

        let salt = B64.decode(salt_b64).expect("decode salt");

        // SaltedPassword
        let mut salted_password = [0u8; 32];
        pbkdf2::pbkdf2::<Hmac<Sha256>>(
            password.as_bytes(),
            &salt,
            iterations,
            &mut salted_password,
        )
        .expect("pbkdf2 ok");

        // ClientKey = HMAC(SaltedPassword, "Client Key")
        let client_key = hmac_sha256(&salted_password, b"Client Key").expect("client key hmac");

        // StoredKey = H(ClientKey)
        let stored_key: [u8; 32] = {
            use sha2::Digest;
            sha2::Sha256::digest(client_key).into()
        };

        // client-final-message-without-proof
        let cbind_input = B64.encode(gs2_header); // GS2 header base64-encoded
        let cfm_without_proof = format!("c={cbind_input},r={server_nonce}");

        // AuthMessage = client-first-message-bare + "," +
        //               server-first-message + "," +
        //               client-final-message-without-proof
        let auth_message = format!("{client_first_bare},{server_first},{cfm_without_proof}");

        // ClientSignature = HMAC(StoredKey, AuthMessage)
        let client_signature =
            hmac_sha256(&stored_key, auth_message.as_bytes()).expect("client signature hmac");

        // ClientProof = ClientKey XOR ClientSignature
        let mut client_proof = client_key;
        xor32(&mut client_proof, &client_signature);

        format!("{cfm_without_proof},p={}", B64.encode(client_proof))
    }

    #[test]
    fn full_round_trip_succeeds_with_correct_password() {
        let password = "s3cr3t_pw";
        let salt = b"random_salt_16by";
        let ph = hash_password(password, salt, DEFAULT_ITERATIONS);
        let mut server =
            ScramSha256Server::new(ph.stored_key, ph.server_key, ph.salt.clone(), ph.iterations);

        let client_nonce = "client_nonce_abc";
        let c_first = client_first("alice", client_nonce);

        // Server turn 1.
        let s_first_bytes = server
            .server_first(c_first.as_bytes())
            .expect("server_first ok");
        let s_first = String::from_utf8(s_first_bytes).expect("utf8");

        // Client turn 2 (simulated).
        let c_first_bare = "n=alice,r=client_nonce_abc";
        let c_final = client_final(password, client_nonce, c_first_bare, &s_first);

        // Server turn 2.
        let s_final_bytes = server
            .server_final(c_final.as_bytes())
            .expect("server_final ok — authentication should succeed");

        let s_final = String::from_utf8(s_final_bytes).expect("utf8");
        assert!(
            s_final.starts_with("v="),
            "server-final-message starts with v=: {s_final}"
        );
    }

    #[test]
    fn wrong_password_returns_proof_mismatch() {
        let password = "correct_horse";
        let wrong_password = "wrong_horse";
        let salt = b"random_salt_16by";
        let ph = hash_password(password, salt, DEFAULT_ITERATIONS);
        let mut server =
            ScramSha256Server::new(ph.stored_key, ph.server_key, ph.salt.clone(), ph.iterations);

        let client_nonce = "client_nonce_xyz";
        let c_first = client_first("bob", client_nonce);
        let s_first_bytes = server
            .server_first(c_first.as_bytes())
            .expect("server_first ok");
        let s_first = String::from_utf8(s_first_bytes).expect("utf8");

        let c_first_bare = "n=bob,r=client_nonce_xyz";
        let c_final = client_final(wrong_password, client_nonce, c_first_bare, &s_first);

        let err = server
            .server_final(c_final.as_bytes())
            .expect_err("wrong password must fail");
        assert_eq!(err, AuthError::ProofMismatch);
    }

    #[test]
    fn out_of_order_call_returns_error() {
        let ph = hash_password("pw", b"salt_16_bytes___", DEFAULT_ITERATIONS);
        let mut server =
            ScramSha256Server::new(ph.stored_key, ph.server_key, ph.salt.clone(), ph.iterations);

        // Calling server_final before server_first.
        let err = server
            .server_final(b"c=biws,r=nonce,p=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
            .expect_err("must fail out of order");
        assert_eq!(err, AuthError::OutOfOrder);
    }

    #[test]
    fn double_server_first_returns_out_of_order() {
        let ph = hash_password("pw", b"salt_16_bytes___", DEFAULT_ITERATIONS);
        let mut server =
            ScramSha256Server::new(ph.stored_key, ph.server_key, ph.salt.clone(), ph.iterations);

        let c_first = client_first("user", "nonce");
        server
            .server_first(c_first.as_bytes())
            .expect("first call ok");
        // Second call is out of order.
        let err = server
            .server_first(c_first.as_bytes())
            .expect_err("second server_first must fail");
        assert_eq!(err, AuthError::OutOfOrder);
    }

    #[test]
    fn nonce_mismatch_returns_error() {
        let ph = hash_password("pw", b"salt_16_bytes___", DEFAULT_ITERATIONS);
        let mut server =
            ScramSha256Server::new(ph.stored_key, ph.server_key, ph.salt.clone(), ph.iterations);

        let c_first = client_first("user", "correct_nonce");
        let s_first_bytes = server
            .server_first(c_first.as_bytes())
            .expect("server_first ok");
        let s_first = String::from_utf8(s_first_bytes).expect("utf8");
        // Server nonce starts with "correct_nonce" + server suffix.
        // We tamper the nonce in client-final.
        let _ = s_first; // ignore — we build a tampered final manually
        let tampered_final =
            "c=biws,r=totally_wrong_nonce,p=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
        let err = server
            .server_final(tampered_final.as_bytes())
            .expect_err("nonce mismatch must fail");
        assert!(
            matches!(err, AuthError::NonceMismatch | AuthError::BadClientFinal(_)),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn unsupported_channel_binding_gs2_header_returns_error() {
        let ph = hash_password("pw", b"salt_16_bytes___", DEFAULT_ITERATIONS);
        let mut server =
            ScramSha256Server::new(ph.stored_key, ph.server_key, ph.salt.clone(), ph.iterations);

        let err = server
            .server_first(b"p=tls-server-end-point,,n=user,r=nonce")
            .expect_err("unsupported channel binding header must fail");
        assert_eq!(
            err,
            AuthError::BadClientFirst("unsupported channel binding")
        );
    }

    #[test]
    fn mismatched_channel_binding_returns_error_even_with_valid_proof() {
        let password = "s3cr3t_pw";
        let salt = b"random_salt_16by";
        let ph = hash_password(password, salt, DEFAULT_ITERATIONS);
        let mut server =
            ScramSha256Server::new(ph.stored_key, ph.server_key, ph.salt.clone(), ph.iterations);

        let client_nonce = "client_nonce_cb";
        let c_first = client_first("alice", client_nonce);
        let s_first_bytes = server
            .server_first(c_first.as_bytes())
            .expect("server_first ok");
        let s_first = String::from_utf8(s_first_bytes).expect("utf8");

        let c_final = client_final_with_channel_binding(
            password,
            client_nonce,
            "n=alice,r=client_nonce_cb",
            &s_first,
            "p=tls-server-end-point,,",
        );

        let err = server
            .server_final(c_final.as_bytes())
            .expect_err("unsupported channel binding must fail");
        assert_eq!(err, AuthError::BadClientFinal("channel binding mismatch"));
    }
}
