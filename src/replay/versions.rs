//! Frozen identity pins for the Jev precompute contract.
//!
//! These values are part of the evidence boundary: a cached judgment is only
//! reusable when the complete tuple matches.  The question-document digest is
//! deliberately computed from the checked-in question loader source rather
//! than inferred from a display label.

use serde::{Deserialize, Serialize};

/// Model identifier used by the frozen Lead-Lag V1 request.
pub const MODEL_ID_V1: &str = "jev-latest";
/// Provider model/version pin used by the frozen Lead-Lag V1 request.
pub const MODEL_VERSION_V1: &str = "jev-latest";
/// Strategy implementation pin from the precompute contract.
pub const STRATEGY_VERSION_V1: &str = "v1-lead-lag";
/// Question wording pin from the precompute contract.
pub const PROMPT_VERSION_V1: &str = "v1-lead-lag";
/// Question schema pin from the precompute contract.
pub const QUESTION_SCHEMA_VERSION_V1: &str = "v1";
/// Feature-builder implementation pin.  Any state-builder change increments it.
pub const FEATURE_BUILDER_VERSION_V1: &str = "v1-feature-builder";
/// Numeric normalization pin for the frozen state builder.
pub const NORMALIZATION_VERSION_V1: &str = "v1-normalization";
/// Canonical JSON serialization pin for the frozen state identity.
pub const SERIALIZATION_VERSION_V1: &str = "v1-canonical-json";
/// The single eight-output question variant written by phase 1.
pub const VARIANT_V1: &str = "CONTROL";

/// SHA-256 of the current `src/jev/questions_md.rs` source.
///
/// Keep this explicit and reviewable.  [`question_document_sha256`] computes
/// the same value from bytes at runtime, and the unit test below prevents a
/// wording-loader edit from silently retaining the old pin.
pub const QUESTION_DOCUMENT_SHA256_V1: &str =
    "d77a229d4b87fcc4c7a09b1ba0ece650a88998157eacc299eaa8cbca8290baf9";

/// The complete cache/evaluation identity tuple.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionPins {
    pub model_id: String,
    pub model_version: String,
    pub strategy_version: String,
    pub prompt_version: String,
    pub question_document_sha256: String,
    pub question_schema_version: String,
    pub feature_builder_version: String,
    pub normalization_version: String,
    pub serialization_version: String,
    pub variant: String,
}

impl VersionPins {
    /// Builds the checked-in V1 tuple, computing the question-loader digest.
    #[must_use]
    pub fn current_v1() -> Self {
        Self {
            model_id: MODEL_ID_V1.to_owned(),
            model_version: MODEL_VERSION_V1.to_owned(),
            strategy_version: STRATEGY_VERSION_V1.to_owned(),
            prompt_version: PROMPT_VERSION_V1.to_owned(),
            question_document_sha256: question_document_sha256(),
            question_schema_version: QUESTION_SCHEMA_VERSION_V1.to_owned(),
            feature_builder_version: FEATURE_BUILDER_VERSION_V1.to_owned(),
            normalization_version: NORMALIZATION_VERSION_V1.to_owned(),
            serialization_version: SERIALIZATION_VERSION_V1.to_owned(),
            variant: VARIANT_V1.to_owned(),
        }
    }

    /// The legacy value used when an older cache entry has no pin metadata.
    #[must_use]
    pub fn unknown() -> Self {
        let unknown = "unknown".to_owned();
        Self {
            model_id: unknown.clone(),
            model_version: unknown.clone(),
            strategy_version: unknown.clone(),
            prompt_version: unknown.clone(),
            question_document_sha256: unknown.clone(),
            question_schema_version: unknown.clone(),
            feature_builder_version: unknown.clone(),
            normalization_version: unknown.clone(),
            serialization_version: unknown.clone(),
            variant: unknown,
        }
    }
}

/// Computes the pinned digest from the question-loader source itself.
#[must_use]
pub fn question_document_sha256() -> String {
    sha256_hex(include_bytes!("../jev/questions_md.rs"))
}

/// Small dependency-free SHA-256 implementation for cache identity.
///
/// The project intentionally keeps the precompute identity self-contained: no
/// command invocation, network access, or new dependency is needed to hash a
/// state or the embedded question loader.
#[must_use]
pub fn sha256_hex(input: &[u8]) -> String {
    let mut hash = Sha256::new();
    hash.update(input);
    hash.finish()
}

struct Sha256 {
    state: [u32; 8],
    buffer: [u8; 64],
    buffer_len: usize,
    bit_len: u64,
}

impl Sha256 {
    fn new() -> Self {
        Self {
            state: [
                0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
                0x5be0cd19,
            ],
            buffer: [0; 64],
            buffer_len: 0,
            bit_len: 0,
        }
    }

    fn update(&mut self, mut input: &[u8]) {
        self.bit_len = self.bit_len.wrapping_add((input.len() as u64) * 8);
        if self.buffer_len > 0 {
            let needed = 64 - self.buffer_len;
            if input.len() < needed {
                self.buffer[self.buffer_len..self.buffer_len + input.len()].copy_from_slice(input);
                self.buffer_len += input.len();
                return;
            }
            self.buffer[self.buffer_len..].copy_from_slice(&input[..needed]);
            let block = self.buffer;
            self.compress(&block);
            self.buffer_len = 0;
            input = &input[needed..];
        }
        while input.len() >= 64 {
            self.compress(input[..64].try_into().expect("SHA-256 block is 64 bytes"));
            input = &input[64..];
        }
        self.buffer[..input.len()].copy_from_slice(input);
        self.buffer_len = input.len();
    }

    fn finish(mut self) -> String {
        self.buffer[self.buffer_len] = 0x80;
        self.buffer_len += 1;
        if self.buffer_len > 56 {
            self.buffer[self.buffer_len..].fill(0);
            let block = self.buffer;
            self.compress(&block);
            self.buffer_len = 0;
        }
        self.buffer[self.buffer_len..56].fill(0);
        self.buffer[56..].copy_from_slice(&self.bit_len.to_be_bytes());
        let block = self.buffer;
        self.compress(&block);

        let mut out = String::with_capacity(64);
        for word in self.state {
            use std::fmt::Write as _;
            write!(&mut out, "{word:08x}").expect("writing to a String cannot fail");
        }
        out
    }

    fn compress(&mut self, block: &[u8; 64]) {
        const K: [u32; 64] = [
            0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
            0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
            0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
            0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
            0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
            0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
            0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
            0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
            0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
            0xc67178f2,
        ];
        let mut words = [0_u32; 64];
        for (index, chunk) in block.as_chunks::<4>().0.iter().enumerate() {
            words[index] = u32::from_be_bytes(*chunk);
        }
        for i in 16..64 {
            let s0 = words[i - 15].rotate_right(7)
                ^ words[i - 15].rotate_right(18)
                ^ (words[i - 15] >> 3);
            let s1 = words[i - 2].rotate_right(17)
                ^ words[i - 2].rotate_right(19)
                ^ (words[i - 2] >> 10);
            words[i] = words[i - 16]
                .wrapping_add(s0)
                .wrapping_add(words[i - 7])
                .wrapping_add(s1);
        }

        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = self.state;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = h
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(words[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);
            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }
        self.state[0] = self.state[0].wrapping_add(a);
        self.state[1] = self.state[1].wrapping_add(b);
        self.state[2] = self.state[2].wrapping_add(c);
        self.state[3] = self.state[3].wrapping_add(d);
        self.state[4] = self.state[4].wrapping_add(e);
        self.state[5] = self.state[5].wrapping_add(f);
        self.state[6] = self.state[6].wrapping_add(g);
        self.state[7] = self.state[7].wrapping_add(h);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_matches_known_vectors() {
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn current_digest_matches_the_explicit_pin() {
        assert_eq!(question_document_sha256(), QUESTION_DOCUMENT_SHA256_V1);
        assert_eq!(VersionPins::current_v1().model_id, MODEL_ID_V1);
    }
}
