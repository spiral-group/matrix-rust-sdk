// Copyright 2026 the Spiral authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Native MatrixRTC sessions.
//!
//! This module owns everything a client needs to take part in a MatrixRTC call
//! without embedding a web application: publishing and refreshing the
//! per-device membership, scheduling the server-side leave event, discovering
//! and authorising against the SFU that carries the media, and distributing the
//! per-participant media keys over Olm-encrypted to-device messages.
//!
//! What this module deliberately does *not* do is touch media. Callers connect
//! the returned [`TransportCredentials`] to an SFU client of their choosing and
//! apply the keys carried by [`MatrixRtcUpdate`]; the protocol never hands raw
//! Matrix events or Olm material to that layer.
//!
//! # Retries and the wire dialect
//!
//! Every request this module sends goes through the client's [`HttpClient`],
//! which already retries `429` (honouring `Retry-After`) and `5xx` with an
//! exponential back-off; nothing here re-implements that. Network failures
//! are NOT retried on the join path on purpose — a user waiting on a dial tone
//! is better served by a fast failure than by the client's fifteen-minute
//! budget — and the delayed-leave heartbeat re-stages on any failure.
//!
//! The membership is refreshed by re-publishing it with a NEW `created_ts`
//! rather than the join-time one. That is the only way the write survives
//! Synapse's deduplication of an unchanged state event, and it is the reading
//! the peers on this deployment apply: a moved `created_ts` on a known device
//! is a renewal that re-shares the media key, never a leaver that rotates it.
//! (The upstream JavaScript SDK keeps `created_ts` fixed and grows `expires`
//! instead; the two are not interoperable on this point, and this deployment
//! does not need them to be.)
//!
//! [`HttpClient`]: crate::http_client::HttpClient
//!
//! ```no_run
//! # async {
//! # let room: matrix_sdk::Room = unimplemented!();
//! use matrix_sdk::matrixrtc::{JoinConfiguration, MatrixRtcUpdate};
//!
//! let session = room.matrix_rtc_session()?;
//! let mut updates = session.subscribe();
//!
//! session.join(JoinConfiguration::default()).await?;
//! let credentials = session.transport_credentials().await?;
//!
//! while let Ok(update) = updates.recv().await {
//!     match update {
//!         MatrixRtcUpdate::MembershipsChanged { memberships } => { /* … */ }
//!         MatrixRtcUpdate::OwnEncryptionKeyChanged { key } => { /* … */ }
//!         MatrixRtcUpdate::EncryptionKeyReceived { key } => { /* … */ }
//!         MatrixRtcUpdate::Ended { .. } => break,
//!     }
//! }
//!
//! session.leave().await?;
//! # anyhow::Ok(()) };
//! ```

use thiserror::Error;

pub mod keys;
pub mod membership;
pub mod session;
pub mod transport;

pub use keys::{ReceivedRtcEncryptionKey, RtcEncryptionKey};
pub use membership::{LiveKitFocus, RtcMembership};
pub use session::{
    JoinConfiguration, LeaveReason, MatrixRtcSession, MatrixRtcUpdate, RtcCallIntent,
    RtcNotificationType,
};
pub use transport::{LiveKitTransport, TransportCredentials};

/// Result alias for MatrixRTC operations.
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Failures that can happen while driving a MatrixRTC session.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// The client is not logged in, so it has no identity to join with.
    #[error("the client is not logged in")]
    NotLoggedIn,

    /// The operation requires a joined session.
    #[error("the session has not been joined")]
    NotJoined,

    /// No usable MatrixRTC transport is advertised for this homeserver.
    #[error("no MatrixRTC transport is available")]
    NoTransport,

    /// The MatrixRTC authorisation service refused to issue credentials.
    ///
    /// The message never carries request material, so it is safe to surface.
    #[error("the MatrixRTC authorization service failed: {0}")]
    TransportAuthorization(String),

    /// Talking to the homeserver failed.
    #[error(transparent)]
    Sdk(Box<crate::Error>),

    /// An HTTP request to the homeserver failed.
    #[error(transparent)]
    Http(Box<crate::HttpError>),

    /// A payload could not be built or parsed.
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),
}

impl Error {
    pub(crate) fn from_sdk(error: impl Into<crate::Error>) -> Self {
        Self::Sdk(Box::new(error.into()))
    }

    pub(crate) fn from_http(error: impl Into<crate::HttpError>) -> Self {
        Self::Http(Box::new(error.into()))
    }
}
