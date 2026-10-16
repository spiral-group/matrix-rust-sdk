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

//! Bindings for native MatrixRTC sessions.
//!
//! The bindings expose typed snapshots only: no Matrix event, Olm payload or
//! access token crosses this boundary. Media keys do cross it, because the
//! media stack on the other side is the component that has to apply them.

use std::{sync::Arc, time::Duration};

use matrix_sdk::matrixrtc::{
    self, JoinConfiguration as SdkJoinConfiguration, LeaveReason as SdkLeaveReason,
    MatrixRtcUpdate as SdkMatrixRtcUpdate, ReceivedRtcEncryptionKey,
    RtcCallIntent as SdkRtcCallIntent, RtcEncryptionKey, RtcMembership as SdkRtcMembership,
    RtcNotificationType as SdkRtcNotificationType, TransportCredentials as SdkTransportCredentials,
};
use matrix_sdk_common::{SendOutsideWasm, SyncOutsideWasm};
use tokio::sync::broadcast::error::RecvError;
use tracing::warn;

use crate::{error::ClientError, room::Room, runtime::get_runtime_handle, task_handle::TaskHandle};

#[matrix_sdk_ffi_macros::export]
impl Room {
    /// Get a handle on our participation in this room's MatrixRTC session.
    ///
    /// The returned session is inert until it is joined, and each call returns
    /// an independent handle, so a caller that keeps one alive per call screen
    /// cannot accidentally share membership state between two calls.
    pub fn matrix_rtc_session(&self) -> Result<Arc<MatrixRtcSession>, ClientError> {
        let inner = self.inner.matrix_rtc_session()?;
        Ok(Arc::new(MatrixRtcSession { inner: Arc::new(inner) }))
    }
}

/// Our participation in the MatrixRTC session of a room.
#[derive(uniffi::Object)]
pub struct MatrixRtcSession {
    pub(crate) inner: Arc<matrixrtc::MatrixRtcSession>,
}

/// How a session should be joined.
#[derive(Clone, Debug, uniffi::Record)]
pub struct MatrixRtcJoinConfiguration {
    /// Lifetime of the published membership, in milliseconds.
    pub membership_expiry_ms: u64,

    /// Whether media must be end-to-end encrypted.
    pub encrypt_media: bool,

    /// Optional notification sent after publishing the initial membership.
    pub notification_type: Option<MatrixRtcNotificationType>,

    /// Soft indication of whether the call starts as audio or video.
    pub call_intent: Option<MatrixRtcCallIntent>,
}

impl From<MatrixRtcJoinConfiguration> for SdkJoinConfiguration {
    fn from(value: MatrixRtcJoinConfiguration) -> Self {
        Self {
            membership_expiry: Duration::from_millis(value.membership_expiry_ms),
            encrypt_media: value.encrypt_media,
            notification_type: value.notification_type.map(Into::into),
            call_intent: value.call_intent.map(Into::into),
        }
    }
}

/// How an outgoing call should notify room members.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum MatrixRtcNotificationType {
    /// Ring with an audible sound.
    Ring,
    /// Display a visual notification without ringing.
    Notification,
}

impl From<MatrixRtcNotificationType> for SdkRtcNotificationType {
    fn from(value: MatrixRtcNotificationType) -> Self {
        match value {
            MatrixRtcNotificationType::Ring => Self::Ring,
            MatrixRtcNotificationType::Notification => Self::Notification,
        }
    }
}

/// The media intent advertised by an outgoing call notification.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum MatrixRtcCallIntent {
    /// Start as an audio call.
    Audio,
    /// Start as a video call.
    Video,
}

impl From<MatrixRtcCallIntent> for SdkRtcCallIntent {
    fn from(value: MatrixRtcCallIntent) -> Self {
        match value {
            MatrixRtcCallIntent::Audio => Self::Audio,
            MatrixRtcCallIntent::Video => Self::Video,
        }
    }
}

/// A participant of the session.
#[derive(Clone, Debug, uniffi::Record)]
pub struct MatrixRtcMembership {
    /// The Matrix user the participant belongs to.
    pub user_id: String,

    /// The device the participant publishes from.
    pub device_id: String,

    /// The identity this participant uses on the SFU.
    pub identity: String,
}

impl From<&SdkRtcMembership> for MatrixRtcMembership {
    fn from(value: &SdkRtcMembership) -> Self {
        Self {
            user_id: value.user_id.to_string(),
            device_id: value.device_id.to_string(),
            identity: value.livekit_identity(),
        }
    }
}

/// Credentials for the SFU carrying the session's media.
#[derive(Clone, Debug, uniffi::Record)]
pub struct MatrixRtcTransportCredentials {
    /// The SFU websocket URL.
    pub url: String,

    /// The SFU access token. Treat as a secret: never log or persist it.
    pub token: String,

    /// The identity the SFU assigns to this connection.
    pub identity: String,
}

impl From<SdkTransportCredentials> for MatrixRtcTransportCredentials {
    fn from(value: SdkTransportCredentials) -> Self {
        Self { url: value.url, token: value.token, identity: value.identity }
    }
}

/// A media key belonging to a participant.
#[derive(Clone, uniffi::Record)]
pub struct MatrixRtcEncryptionKey {
    /// The participant the key applies to.
    pub participant_identity: String,

    /// Position of the key in the participant's key ring.
    pub index: u8,

    /// The raw key material.
    pub key: Vec<u8>,
}

/// Hand-written so the key material cannot reach a log. The SDK-side
/// `RtcEncryptionKey` redacts itself the same way; deriving `Debug` here put
/// the raw AES key back into every `MatrixRtcSessionUpdate` dump.
impl std::fmt::Debug for MatrixRtcEncryptionKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MatrixRtcEncryptionKey")
            .field("participant_identity", &self.participant_identity)
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}

impl MatrixRtcEncryptionKey {
    fn own(identity: String, key: &RtcEncryptionKey) -> Self {
        Self { participant_identity: identity, index: key.index, key: key.key.clone() }
    }
}

impl From<&ReceivedRtcEncryptionKey> for MatrixRtcEncryptionKey {
    fn from(value: &ReceivedRtcEncryptionKey) -> Self {
        Self {
            participant_identity: value.participant_identity.clone(),
            index: value.key.index,
            key: value.key.key.clone(),
        }
    }
}

/// Why a session ended.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum MatrixRtcLeaveReason {
    /// The local user hung up.
    UserHungUp,
    /// The session could not be kept alive.
    Error,
}

impl From<SdkLeaveReason> for MatrixRtcLeaveReason {
    fn from(value: SdkLeaveReason) -> Self {
        match value {
            SdkLeaveReason::UserHungUp => Self::UserHungUp,
            SdkLeaveReason::Error => Self::Error,
        }
    }
}

/// An update emitted by a joined session.
#[derive(Clone, Debug, uniffi::Enum)]
pub enum MatrixRtcSessionUpdate {
    /// The participants of the session changed.
    MembershipsChanged {
        /// Every current participant, including ourselves.
        memberships: Vec<MatrixRtcMembership>,
    },

    /// Outgoing media must be encrypted with this key from now on.
    OwnEncryptionKeyChanged {
        /// The key to publish with.
        key: MatrixRtcEncryptionKey,
    },

    /// A participant shared the key their media is encrypted with.
    EncryptionKeyReceived {
        /// The key and the participant it belongs to.
        key: MatrixRtcEncryptionKey,
    },

    /// The session ended.
    Ended {
        /// Why the session ended.
        reason: MatrixRtcLeaveReason,
    },
}

/// Receives session updates.
#[matrix_sdk_ffi_macros::export(callback_interface)]
pub trait MatrixRtcSessionListener: SyncOutsideWasm + SendOutsideWasm {
    /// Called for every update, in order.
    fn on_update(&self, update: MatrixRtcSessionUpdate);
}

#[matrix_sdk_ffi_macros::export]
impl MatrixRtcSession {
    /// The identity this client uses on the SFU.
    pub fn own_identity(&self) -> String {
        self.inner.own_identity()
    }

    /// Whether we are currently joined.
    pub async fn is_joined(&self) -> bool {
        self.inner.is_joined().await
    }

    /// The `m.rtc.notification` event this join sent, if it rang the room.
    ///
    /// Callers subscribe to MSC4310 decline events for this notification to
    /// learn that the callee refused the call.
    pub async fn rtc_notification_event_id(&self) -> Option<String> {
        self.inner.rtc_notification_event_id().await.map(|event_id| event_id.to_string())
    }

    /// Publish our membership and start driving the session.
    ///
    /// Calling this twice is a no-op, so a retry cannot produce a duplicate
    /// membership.
    pub async fn join(&self, configuration: MatrixRtcJoinConfiguration) -> Result<(), ClientError> {
        Ok(self.inner.join(configuration.into()).await?)
    }

    /// Update the media intent advertised by our live membership.
    pub async fn update_call_intent(
        &self,
        call_intent: MatrixRtcCallIntent,
    ) -> Result<(), ClientError> {
        Ok(self.inner.update_call_intent(call_intent.into()).await?)
    }

    /// Withdraw our membership and stop driving the session.
    pub async fn leave(&self) -> Result<(), ClientError> {
        Ok(self.inner.leave().await?)
    }

    /// Get credentials for the SFU carrying this session.
    pub async fn transport_credentials(
        &self,
    ) -> Result<MatrixRtcTransportCredentials, ClientError> {
        Ok(self.inner.transport_credentials().await?.into())
    }

    /// The participants currently published in the room.
    pub async fn memberships(&self) -> Result<Vec<MatrixRtcMembership>, ClientError> {
        Ok(self.inner.memberships().await?.iter().map(Into::into).collect())
    }

    /// Subscribe to session updates.
    ///
    /// The returned handle must be kept alive for as long as updates are
    /// wanted; dropping or cancelling it stops the subscription.
    pub fn subscribe(
        self: Arc<Self>,
        listener: Box<dyn MatrixRtcSessionListener>,
    ) -> Arc<TaskHandle> {
        let mut updates = self.inner.subscribe();
        let own_identity = self.inner.own_identity();
        let session = self.inner.clone();

        Arc::new(TaskHandle::new(get_runtime_handle().spawn(async move {
            loop {
                match updates.recv().await {
                    Ok(update) => {
                        if let Some(update) = convert_update(&own_identity, update) {
                            listener.on_update(update);
                        }
                    }
                    Err(RecvError::Lagged(count)) => {
                        // Dropping updates would desynchronise the media layer
                        // from the protocol state, so make it loud — and put
                        // back the one loss that is silent for the rest of
                        // the call: an `OwnEncryptionKeyChanged` the media
                        // layer never saw leaves us encrypting with a key the
                        // peers no longer hold. Memberships re-emit on their
                        // own with the next room change; the own key does
                        // not (audit 2026-09-05, SDK P3).
                        warn!(count, "The MatrixRTC update listener fell behind");
                        if let Some(key) = session.own_encryption_key().await {
                            listener.on_update(MatrixRtcSessionUpdate::OwnEncryptionKeyChanged {
                                key: MatrixRtcEncryptionKey::own(own_identity.clone(), &key),
                            });
                        }
                    }
                    Err(RecvError::Closed) => break,
                }
            }
        })))
    }
}

fn convert_update(
    own_identity: &str,
    update: SdkMatrixRtcUpdate,
) -> Option<MatrixRtcSessionUpdate> {
    Some(match update {
        SdkMatrixRtcUpdate::MembershipsChanged { memberships } => {
            MatrixRtcSessionUpdate::MembershipsChanged {
                memberships: memberships.iter().map(Into::into).collect(),
            }
        }
        SdkMatrixRtcUpdate::OwnEncryptionKeyChanged { key } => {
            MatrixRtcSessionUpdate::OwnEncryptionKeyChanged {
                key: MatrixRtcEncryptionKey::own(own_identity.to_owned(), &key),
            }
        }
        SdkMatrixRtcUpdate::EncryptionKeyReceived { key } => {
            MatrixRtcSessionUpdate::EncryptionKeyReceived {
                key: MatrixRtcEncryptionKey::from(&key),
            }
        }
        SdkMatrixRtcUpdate::Ended { reason } => {
            MatrixRtcSessionUpdate::Ended { reason: reason.into() }
        }
    })
}

impl From<matrixrtc::Error> for ClientError {
    fn from(error: matrixrtc::Error) -> Self {
        // The MatrixRTC errors are already redacted: the transport variant
        // carries a status, never request material.
        ClientError::from_err(error)
    }
}
