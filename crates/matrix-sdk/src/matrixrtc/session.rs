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

//! The lifecycle of our own participation in a MatrixRTC session.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use matrix_sdk_base::deserialized_responses::RawAnySyncOrStrippedState;
use matrix_sdk_common::executor::{JoinHandle, spawn};
use ruma::{
    MilliSecondsSinceUnixEpoch, OwnedDeviceId, OwnedEventId, OwnedUserId,
    api::client::delayed_events::{
        DelayParameters, delayed_state_event,
        update_delayed_event::{self, UpdateAction},
    },
    events::{
        AnyToDeviceEvent, Mentions, StateEventType,
        relation::Reference,
        rtc::notification::{CallIntent, NotificationType, RtcNotificationEventContent},
    },
    serde::Raw,
};
use serde::Deserialize;
use serde_json::value::RawValue as RawJsonValue;
use tokio::sync::{Mutex, broadcast};
use tracing::{debug, info, instrument, warn};

use super::{
    Error, Result,
    keys::{
        ENCRYPTION_KEYS_EVENT_TYPE, EncryptionKeysContent, ReceivedRtcEncryptionKey,
        RtcEncryptionKey, parse_received_key,
    },
    membership::{
        ActiveFocus, CALL_MEMBER_EVENT_TYPE, DEFAULT_MEMBERSHIP_EXPIRY, LiveKitFocus,
        RtcMembership, RtcMembershipContent, membership_id, membership_state_key, parse_membership,
    },
    transport::{LiveKitTransport, TransportCredentials},
};
use crate::{Room, event_handler::EventHandlerDropGuard};

/// How long a freshly created key stays valid for a joining participant before
/// we rotate instead of re-sharing. Mirrors Element Call's grace period.
const KEY_ROTATION_GRACE_PERIOD: Duration = Duration::from_secs(10);

/// How long we keep publishing with the previous key after distributing a new
/// one, so that receivers have time to install it.
const USE_KEY_DELAY: Duration = Duration::from_secs(1);

/// How long the server waits before publishing our leave event if we stop
/// heart-beating.
const DELAYED_LEAVE_TIMEOUT: Duration = Duration::from_secs(10);

/// How often the delayed leave event is restarted while we are in the call.
const DELAYED_LEAVE_HEARTBEAT: Duration = Duration::from_secs(5);

/// How long after a join an absent own membership is still "the echo has
/// not synced yet" rather than "the server wiped it".
const SELF_HEAL_ECHO_GRACE: Duration = Duration::from_secs(30);

/// How many times one membership refresh is retried before giving up on
/// this interval, and how long between attempts.
const REFRESH_RETRIES: u32 = 3;
const REFRESH_RETRY_DELAY: Duration = Duration::from_secs(10);

/// How often a session still owing its key to a peer tries again on its own.
/// Without this the owed round only ran on a `RoomInfo` change, so in a quiet
/// room a peer whose device keys landed a moment late stayed silent until
/// somebody sent a message or a receipt (pre-merge review 2026-09-08, SDK P2).
const OWED_KEY_RETRY_INTERVAL: Duration = Duration::from_secs(15);

/// How long receivers should consider an outgoing call notification valid.
const NOTIFICATION_LIFETIME: Duration = Duration::from_secs(30);

/// Why a session ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LeaveReason {
    /// The local user left the call.
    UserHungUp,
    /// We could not keep the session alive.
    Error,
}

/// How an outgoing call should notify room members.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RtcNotificationType {
    /// Ring with an audible sound.
    Ring,
    /// Display a visual notification without ringing.
    Notification,
}

impl From<RtcNotificationType> for NotificationType {
    fn from(value: RtcNotificationType) -> Self {
        match value {
            RtcNotificationType::Ring => Self::Ring,
            RtcNotificationType::Notification => Self::Notification,
        }
    }
}

/// The media intent advertised by a session membership and outgoing call
/// notification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RtcCallIntent {
    /// Start as an audio call.
    Audio,
    /// Start as a video call.
    Video,
}

impl From<RtcCallIntent> for CallIntent {
    fn from(value: RtcCallIntent) -> Self {
        match value {
            RtcCallIntent::Audio => Self::Audio,
            RtcCallIntent::Video => Self::Video,
        }
    }
}

/// An update emitted by a joined [`MatrixRtcSession`].
#[derive(Clone, Debug)]
pub enum MatrixRtcUpdate {
    /// The set of remote participants changed.
    MembershipsChanged {
        /// All non-expired memberships, including our own.
        memberships: Vec<RtcMembership>,
    },

    /// Our own media key changed and must be used for outgoing media.
    OwnEncryptionKeyChanged {
        /// The key to encrypt with.
        key: RtcEncryptionKey,
    },

    /// A participant shared the key their media is encrypted with.
    EncryptionKeyReceived {
        /// The key and the participant it belongs to.
        key: ReceivedRtcEncryptionKey,
    },

    /// The session ended.
    Ended {
        /// Why the session ended.
        reason: LeaveReason,
    },
}

/// Configuration for joining a session.
#[derive(Clone, Debug)]
pub struct JoinConfiguration {
    /// Lifetime published in our membership.
    pub membership_expiry: Duration,

    /// Whether media is end-to-end encrypted. When `false`, no keys are
    /// generated or distributed.
    pub encrypt_media: bool,

    /// Optional notification sent after publishing the initial membership.
    pub notification_type: Option<RtcNotificationType>,

    /// Soft indication of whether the call starts as audio or video.
    pub call_intent: Option<RtcCallIntent>,
}

impl Default for JoinConfiguration {
    fn default() -> Self {
        Self {
            membership_expiry: DEFAULT_MEMBERSHIP_EXPIRY,
            encrypt_media: true,
            notification_type: None,
            call_intent: None,
        }
    }
}

/// Our participation in the MatrixRTC session of a room.
///
/// The session owns the protocol state: membership publication and expiry, the
/// delayed leave event, transport authorisation and media key distribution.
/// Callers receive typed updates and never touch Matrix events directly.
#[derive(Debug)]
pub struct MatrixRtcSession {
    room: Room,
    inner: Arc<SessionInner>,
}

#[derive(Debug)]
struct SessionInner {
    user_id: OwnedUserId,
    device_id: OwnedDeviceId,
    state: Mutex<SessionState>,
    /// Serialises membership reconciliation end to end.
    ///
    /// `state` cannot do this: reconciliation deliberately RELEASES it to
    /// distribute keys over the network and to wait out `USE_KEY_DELAY`. Two
    /// passes overlapping there (the observer task fires on every RoomInfo
    /// change while `join` runs one directly) could publish an older key's
    /// `OwnEncryptionKeyChanged` after a newer one, leaving the media layer on
    /// a superseded key with nothing to correct it.
    reconcile: Mutex<()>,
    /// Serialises call-intent membership updates without holding `state`
    /// across the homeserver request.
    call_intent_update: Mutex<()>,
    updates: broadcast::Sender<MatrixRtcUpdate>,
}

#[derive(Debug, Default)]
enum SessionState {
    #[default]
    Idle,
    /// The network handshake is running and holds NO lock, so a hang-up, a
    /// roster read or the membership observer can proceed meanwhile. A
    /// `leave()` here takes the session back to `Idle`, and the finishing
    /// join sees that and undoes its own membership instead of installing it.
    Joining,
    Joined(Box<JoinedState>),
}

#[derive(Debug)]
struct JoinedState {
    transport: LiveKitTransport,
    delay_id: Option<String>,
    /// Kept so a membership that a spuriously fired delayed leave wiped
    /// can be re-published with the same parameters.
    membership_expiry: Duration,
    membership_created_ts: MilliSecondsSinceUnixEpoch,
    /// Our own membership has been read back from the room at least once —
    /// the echo of the PUT has synced. Until then, "absent from the roster"
    /// means "not synced yet", not "wiped" (audit 2026-09-05, SDK P3): the
    /// self-heal used to republish on the very first reconciliation of
    /// every join — three extra requests, a moved `created_ts`, a second
    /// heartbeat — because `join()` reconciles before its own echo lands.
    own_membership_seen: bool,
    /// When the join completed, for the fallback below.
    joined_at: std::time::Instant,
    /// The state event our join published. A cancelled join withdraws the
    /// membership only while THIS event is still what the room holds under
    /// our state key: the FFI hands out one session per call, so a redial's
    /// fresh session may already have published its own by the time a
    /// slow first join comes back (audit 2026-09-05, SDK P2).
    membership_event_id: OwnedEventId,
    call_intent: Option<RtcCallIntent>,
    /// The `m.rtc.notification` this join sent, if it rang the room. Callers
    /// need it to observe MSC4310 decline events for their own ring.
    rtc_notification_event_id: Option<OwnedEventId>,
    own_key: Option<RtcEncryptionKey>,
    own_key_created_at: MilliSecondsSinceUnixEpoch,
    encrypt_media: bool,
    /// Every other participant we have shared our key with, keyed by LiveKit
    /// identity, with the `created_ts` of the membership we shared it under.
    /// A device that publishes a NEW `created_ts` — a renewal, or a leave
    /// and rejoin that the state observer coalesced into one change — needs
    /// the key again: its media layer may have been torn down in between,
    /// and nothing else will re-send it.
    known_memberships: BTreeMap<String, MilliSecondsSinceUnixEpoch>,
    /// Participants in `known_memberships` that the last distribution round
    /// did NOT reach — a device the crypto store had not seen yet (its
    /// `/keys/query` lands a sync or two after its membership), a device
    /// lookup error, or a to-device send the server refused. They are owed
    /// the key and are treated as joiners again on the next round; without
    /// this they were recorded as shared-with and heard silence for the rest
    /// of the call, because a round only runs when the roster changes.
    unshared_memberships: BTreeSet<String>,
    /// In-flight membership re-publish, if any. Single-flight AND tracked:
    /// see `synchronise_memberships`.
    republish: Option<JoinHandle<()>>,
    tasks: Vec<JoinHandle<()>>,
    /// Held for its `Drop`: uninstalls the to-device key handler with the
    /// joined state.
    _drop_guards: Vec<EventHandlerDropGuard>,
}

impl Drop for JoinedState {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
        // A re-publish that outlived the session would resurrect the
        // membership after the leave event went out.
        if let Some(republish) = &self.republish {
            republish.abort();
        }
    }
}

impl MatrixRtcSession {
    /// Create a session for the given room.
    pub(crate) fn new(room: Room) -> Result<Self> {
        let client = room.client();
        let user_id = client.user_id().ok_or(Error::NotLoggedIn)?.to_owned();
        let device_id = client.device_id().ok_or(Error::NotLoggedIn)?.to_owned();
        // 256, not 64 (audit 2026-09-05, SDK P3): `MembershipsChanged` is
        // emitted on every RoomInfo change, and a main-actor consumer stalled
        // behind a burst of them used to lag past the one update it cannot
        // afford to miss.
        let (updates, _) = broadcast::channel(256);

        Ok(Self {
            room,
            inner: Arc::new(SessionInner {
                reconcile: Mutex::new(()),
                call_intent_update: Mutex::new(()),
                user_id,
                device_id,
                state: Mutex::new(SessionState::Idle),
                updates,
            }),
        })
    }

    /// The event id of the membership the room currently holds under our
    /// state key, from the state store — `None` when nothing has synced.
    async fn current_own_membership_event_id(&self) -> Option<OwnedEventId> {
        let state_key = membership_state_key(&self.inner.user_id, &self.inner.device_id);
        let event = self
            .room
            .get_state_event(StateEventType::from(CALL_MEMBER_EVENT_TYPE), &state_key)
            .await
            .ok()
            .flatten()?;
        let RawAnySyncOrStrippedState::Sync(raw) = event else { return None };
        raw.get_field::<OwnedEventId>("event_id").ok().flatten()
    }

    /// Subscribe to session updates.
    pub fn subscribe(&self) -> broadcast::Receiver<MatrixRtcUpdate> {
        self.inner.updates.subscribe()
    }

    /// The media key we currently encrypt with, if joined with encryption.
    ///
    /// For a subscriber that fell behind (`Lagged`) and may have missed an
    /// `OwnEncryptionKeyChanged`: encrypting with a key the peers no longer
    /// hold is silence for the rest of the call, so the FFI re-announces
    /// this after a lag (audit 2026-09-05, SDK P3).
    pub async fn own_encryption_key(&self) -> Option<RtcEncryptionKey> {
        match &*self.inner.state.lock().await {
            SessionState::Joined(joined) => joined.own_key.clone(),
            SessionState::Idle | SessionState::Joining => None,
        }
    }

    /// The identity this client uses on the SFU.
    pub fn own_identity(&self) -> String {
        membership_id(&self.inner.user_id, &self.inner.device_id)
    }

    /// Whether we are currently joined.
    pub async fn is_joined(&self) -> bool {
        matches!(*self.inner.state.lock().await, SessionState::Joined(_))
    }

    /// The `m.rtc.notification` event this join sent, if it rang the room.
    ///
    /// Callers subscribe to MSC4310 decline events for this notification to
    /// learn that the callee refused the call.
    pub async fn rtc_notification_event_id(&self) -> Option<OwnedEventId> {
        match &*self.inner.state.lock().await {
            SessionState::Joined(joined) => joined.rtc_notification_event_id.clone(),
            SessionState::Idle | SessionState::Joining => None,
        }
    }

    /// The memberships currently published in the room, ours included.
    pub async fn memberships(&self) -> Result<Vec<RtcMembership>> {
        read_memberships(&self.room).await
    }

    /// Join the session.
    ///
    /// Joining twice is a no-op, so a duplicate call from a retrying caller
    /// cannot publish a second membership or a second key.
    #[instrument(skip_all, fields(room_id = %self.room.room_id()))]
    pub async fn join(&self, configuration: JoinConfiguration) -> Result<()> {
        {
            let mut state = self.inner.state.lock().await;
            match *state {
                SessionState::Joined(_) => {
                    debug!("Already joined, ignoring the duplicate join request");
                    return Ok(());
                }
                SessionState::Joining => {
                    debug!("A join is already in progress, ignoring the duplicate request");
                    return Ok(());
                }
                SessionState::Idle => *state = SessionState::Joining,
            }
        }

        // Everything below runs UNLOCKED: discovery, the membership PUT, the
        // notification and the delayed-leave staging are four network
        // round-trips, and holding the session lock across them made every
        // other entry point — `leave()` above all — wait them out. A user
        // hanging up during a slow handshake had to wait for the handshake.
        let joined = match self.perform_join(&configuration).await {
            Ok(joined) => joined,
            Err(error) => {
                let mut state = self.inner.state.lock().await;
                if matches!(*state, SessionState::Joining) {
                    *state = SessionState::Idle;
                }
                return Err(error);
            }
        };

        {
            let mut state = self.inner.state.lock().await;
            if !matches!(*state, SessionState::Joining) {
                // `leave()` ran while we were joining. Its own teardown could
                // not reach a membership that did not exist yet, so undo ours.
                drop(state);
                warn!("The session was left while joining; withdrawing the membership");
                // The delayed leave staged for THIS membership would fire
                // ~10 s from now against whatever membership is current by
                // then — a redial's, typically (SDK F2). Retire it first.
                let stale_delay_id = joined.delay_id.clone();
                let our_event_id = joined.membership_event_id.clone();
                drop(joined);
                if let Some(delay_id) = stale_delay_id
                    && let Err(error) =
                        self.update_delayed_event(delay_id, UpdateAction::Cancel).await
                {
                    warn!("Could not cancel the delayed leave of a cancelled join: {error}");
                }
                // Only OUR membership is ours to withdraw. A redial through
                // a fresh session handle may have published its own under
                // the same state key while this join was in flight; wiping
                // it would end the redial for every peer and the agent.
                // With no echo in the store yet there is nothing to compare
                // against and the withdrawal proceeds as before — the
                // residual is a redial whose PUT beat this join's return AND
                // whose echo has not synced, which the redial's own
                // self-heal then repairs.
                match self.current_own_membership_event_id().await {
                    Some(current) if current != our_event_id => {
                        info!(
                            "A newer membership ({current}) replaced the cancelled join's ({our_event_id}); leaving it standing"
                        );
                    }
                    _ => {
                        if let Err(error) = self.publish_leave().await {
                            warn!(
                                "Could not withdraw the membership after a cancelled join: {error}"
                            );
                        }
                    }
                }
                return Ok(());
            }
            *state = SessionState::Joined(Box::new(joined));
        }

        info!("Joined the MatrixRTC session");

        // Distribute the first key to everybody already in the call.
        self.synchronise_memberships().await?;

        Ok(())
    }

    /// Update the media intent advertised by our live membership.
    ///
    /// Intent belongs to this participant, not to the room: one participant
    /// may publish video while another remains audio-only.
    pub async fn update_call_intent(&self, call_intent: RtcCallIntent) -> Result<()> {
        let _intent_update = self.inner.call_intent_update.lock().await;
        let (transport, expiry, created_ts, current_intent) = {
            let state = self.inner.state.lock().await;
            let SessionState::Joined(joined) = &*state else {
                return Err(Error::NotJoined);
            };
            (
                joined.transport.clone(),
                joined.membership_expiry,
                joined.membership_created_ts,
                joined.call_intent,
            )
        };

        if current_intent == Some(call_intent) {
            return Ok(());
        }

        self.publish_membership(&transport, expiry, Some(created_ts), Some(call_intent)).await?;

        let mut state = self.inner.state.lock().await;
        let SessionState::Joined(joined) = &mut *state else {
            // A leave raced the PUT. Withdraw the membership we may have
            // resurrected rather than leaving a ghost for the full expiry.
            drop(state);
            self.publish_leave().await?;
            return Ok(());
        };
        joined.call_intent = Some(call_intent);

        Ok(())
    }

    /// The unlocked half of [`Self::join`]: everything that talks to the
    /// network, plus the background tasks the joined state owns.
    async fn perform_join(&self, configuration: &JoinConfiguration) -> Result<JoinedState> {
        let client = self.room.client();
        let transport = LiveKitTransport::discover(&client).await?;
        let membership_created_ts = MilliSecondsSinceUnixEpoch::now();

        // Installed BEFORE the membership is published (call audit
        // 2026-09-05, SDK F3): a peer that is already in the call — the
        // voice agent above all, which reacts to our membership on the same
        // host — answers the PUT with its key over to-device within an RTT,
        // and a key processed before the handler existed was gone: nothing
        // re-requests it, and the peer only re-sends on a roster change.
        // Dropping the guard on a failed join uninstalls the handler.
        let mut drop_guards = Vec::new();
        if configuration.encrypt_media
            && let Some(guard) = self.observe_encryption_keys()
        {
            drop_guards.push(guard);
        }

        let membership_event_id = match self
            .publish_membership(
                &transport,
                configuration.membership_expiry,
                None,
                configuration.call_intent,
            )
            .await
        {
            Ok(event_id) => event_id,
            Err(error) => {
                // A PUT that failed on OUR side may still have been applied
                // on the server (a client-side timeout is the common case),
                // and with no delayed leave staged yet that membership would
                // stand for its full 4 h expiry (pre-merge review 2026-09-08,
                // SDK P2). Withdraw best-effort: a membership that never
                // landed answers with a harmless empty write.
                warn!("The membership PUT failed; withdrawing in case it was applied");
                if let Err(leave_error) = self.publish_leave().await {
                    debug!("Could not withdraw the possibly-applied membership: {leave_error}");
                }
                return Err(error);
            }
        };

        // From here on the membership is on the wire. A step that fails
        // below must take it back (SDK F1): `join()` returns `Err` and flips
        // to `Idle`, after which `leave()` has nothing to leave — so without
        // this the room showed a live call for the whole membership expiry
        // (4 h) and the agent joined a call with no human in it.
        let rtc_notification_event_id = match configuration.notification_type {
            Some(notification_type) => {
                match self
                    .send_notification(
                        membership_event_id.clone(),
                        notification_type,
                        configuration.call_intent,
                    )
                    .await
                {
                    Ok(event_id) => Some(event_id),
                    Err(error) => {
                        warn!("The call notification failed; withdrawing the membership");
                        if let Err(leave_error) = self.publish_leave().await {
                            warn!("Could not withdraw the membership either: {leave_error}");
                        }
                        return Err(error);
                    }
                }
            }
            None => None,
        };

        // Best effort: a homeserver without MSC4140 support simply means we
        // rely on the membership expiry instead of a server-side leave.
        let delay_id = match self.schedule_delayed_leave().await {
            Ok(delay_id) => Some(delay_id),
            Err(error) => {
                warn!("Could not schedule the delayed leave event: {error}");
                None
            }
        };

        let mut joined = JoinedState {
            transport,
            delay_id: delay_id.clone(),
            membership_expiry: configuration.membership_expiry,
            membership_created_ts,
            own_membership_seen: false,
            joined_at: std::time::Instant::now(),
            membership_event_id: membership_event_id.clone(),
            call_intent: configuration.call_intent,
            rtc_notification_event_id,
            own_key: None,
            own_key_created_at: MilliSecondsSinceUnixEpoch::now(),
            encrypt_media: configuration.encrypt_media,
            known_memberships: BTreeMap::new(),
            unshared_memberships: BTreeSet::new(),
            republish: None,
            tasks: Vec::new(),
            _drop_guards: drop_guards,
        };

        if configuration.encrypt_media {
            let key = RtcEncryptionKey::generate(0);
            joined.own_key = Some(key.clone());
            joined.own_key_created_at = MilliSecondsSinceUnixEpoch::now();
            let _ = self.inner.updates.send(MatrixRtcUpdate::OwnEncryptionKeyChanged { key });
        }

        joined.tasks.push(self.spawn_membership_observer());

        if let Some(delay_id) = delay_id {
            joined.tasks.push(self.spawn_delayed_leave_heartbeat(delay_id));
        }

        joined.tasks.push(self.spawn_membership_refresh(configuration.membership_expiry));
        if configuration.encrypt_media {
            joined.tasks.push(self.spawn_owed_key_retry());
        }

        Ok(joined)
    }

    /// Leave the session.
    ///
    /// Leaving when not joined is a no-op so that a hang-up racing with a
    /// failed join cannot leave a stale membership behind.
    #[instrument(skip_all, fields(room_id = %self.room.room_id()))]
    pub async fn leave(&self) -> Result<()> {
        let mut state = self.inner.state.lock().await;
        let SessionState::Joined(joined) = std::mem::take(&mut *state) else {
            debug!("Not joined, ignoring the leave request");
            return Ok(());
        };
        drop(state);

        let delay_id = joined.delay_id.clone();
        // Dropping the joined state aborts every background task.
        drop(joined);

        // Firing the pre-staged leave is the most reliable path when we have
        // one: the server publishes it even if our own send fails afterwards.
        //
        // Having none is not the same as having nothing to do, which is what
        // this used to assume. Staging is best effort — any failure at join
        // time (a homeserver without MSC4140, a 5xx, a rate limit, an expired
        // token) leaves `delay_id` as `None` — and the old `None => Ok(())`
        // arm then skipped the direct send too, so `leave()` published
        // nothing and still reported success. The membership stood for its
        // full expiry, peers kept rendering a participant who had hung up,
        // and the caller was told it had gone cleanly.
        match delay_id {
            Some(delay_id) => {
                if let Err(error) = self.update_delayed_event(delay_id, UpdateAction::Send).await {
                    warn!(
                        "Could not fire the delayed leave event, sending the leave directly: \
                         {error}"
                    );
                    self.publish_leave().await?;
                }
            }
            None => {
                debug!("No delayed leave was staged, sending the leave directly");
                self.publish_leave().await?;
            }
        }

        let _ = self.inner.updates.send(MatrixRtcUpdate::Ended { reason: LeaveReason::UserHungUp });

        info!("Left the MatrixRTC session");

        Ok(())
    }

    /// Obtain credentials for the SFU carrying this session.
    #[instrument(skip_all, fields(room_id = %self.room.room_id()))]
    pub async fn transport_credentials(&self) -> Result<TransportCredentials> {
        let transport = {
            let state = self.inner.state.lock().await;
            match &*state {
                SessionState::Joined(joined) => joined.transport.clone(),
                // A join still in flight has no credentials to hand out yet.
                SessionState::Idle | SessionState::Joining => return Err(Error::NotJoined),
            }
        };

        TransportCredentials::request(
            &self.room.client(),
            &transport,
            self.room.room_id(),
            self.own_identity(),
        )
        .await
    }

    async fn publish_membership(
        &self,
        transport: &LiveKitTransport,
        expiry: Duration,
        created_ts: Option<MilliSecondsSinceUnixEpoch>,
        call_intent: Option<RtcCallIntent>,
    ) -> Result<OwnedEventId> {
        let content = RtcMembershipContent::new(
            &self.inner.user_id,
            &self.inner.device_id,
            ActiveFocus::oldest_membership(),
            vec![LiveKitFocus::new(
                transport.service_url.clone(),
                Some(self.room.room_id().to_string()),
            )],
            expiry,
            created_ts,
            call_intent.map(Into::into),
        );

        let state_key = membership_state_key(&self.inner.user_id, &self.inner.device_id);

        let response = self
            .room
            .send_state_event_raw(
                CALL_MEMBER_EVENT_TYPE,
                &state_key,
                serde_json::to_value(&content).map_err(Error::Serialization)?,
            )
            .await
            .map_err(Error::from_sdk)?;

        Ok(response.event_id)
    }

    async fn send_notification(
        &self,
        membership_event_id: OwnedEventId,
        notification_type: RtcNotificationType,
        call_intent: Option<RtcCallIntent>,
    ) -> Result<OwnedEventId> {
        let mut content = RtcNotificationEventContent::new(
            MilliSecondsSinceUnixEpoch::now(),
            NOTIFICATION_LIFETIME,
            notification_type.into(),
        );
        content.mentions = Some(Mentions::with_room_mention());
        content.relates_to = Some(Reference::new(membership_event_id));
        content.call_intent = call_intent.map(Into::into);

        let result = self.room.send(content).await.map_err(Error::from_sdk)?;

        Ok(result.response.event_id)
    }

    async fn publish_leave(&self) -> Result<()> {
        let state_key = membership_state_key(&self.inner.user_id, &self.inner.device_id);

        self.room
            .send_state_event_raw(CALL_MEMBER_EVENT_TYPE, &state_key, serde_json::json!({}))
            .await
            .map_err(Error::from_sdk)?;

        Ok(())
    }

    async fn schedule_delayed_leave(&self) -> Result<String> {
        let state_key = membership_state_key(&self.inner.user_id, &self.inner.device_id);
        let body = Raw::new(&serde_json::json!({})).map_err(Error::Serialization)?;

        let request = delayed_state_event::unstable::Request::new_raw(
            self.room.room_id().to_owned(),
            state_key,
            StateEventType::from(CALL_MEMBER_EVENT_TYPE),
            DelayParameters::Timeout { timeout: DELAYED_LEAVE_TIMEOUT },
            body.cast_unchecked(),
        );

        let response = self.room.client().send(request).await.map_err(Error::from_http)?;

        Ok(response.delay_id)
    }

    async fn update_delayed_event(&self, delay_id: String, action: UpdateAction) -> Result<()> {
        let request = update_delayed_event::unstable_v1::Request::new(delay_id, action);
        self.room.client().send(request).await.map_err(Error::from_http)?;

        Ok(())
    }

    fn spawn_membership_observer(&self) -> JoinHandle<()> {
        let session = self.weak_clone();
        let room = self.room.clone();

        spawn(async move {
            let mut subscriber = room.subscribe_info();

            while subscriber.next().await.is_some() {
                let Some(session) = session.upgrade() else { break };

                if let Err(error) = session.synchronise_memberships().await {
                    warn!("Could not process a MatrixRTC membership change: {error}");
                }
            }
        })
    }

    fn spawn_delayed_leave_heartbeat(&self, delay_id: String) -> JoinHandle<()> {
        let session = self.weak_clone();

        spawn(async move {
            let mut delay_id = delay_id;
            loop {
                matrix_sdk_common::sleep::sleep(DELAYED_LEAVE_HEARTBEAT).await;

                let Some(session) = session.upgrade() else { break };

                // A re-publish staged a fresh leave and started its own
                // heartbeat; this one would keep restarting a cancelled id,
                // read the `M_NOT_FOUND` as a fired leave, and restage a
                // second, competing one.
                {
                    let state = session.inner.state.lock().await;
                    let SessionState::Joined(joined) = &*state else { break };
                    if joined.delay_id.as_deref() != Some(delay_id.as_str()) {
                        debug!("The delayed leave event was replaced; stopping this heartbeat");
                        break;
                    }
                }

                if let Err(error) =
                    session.update_delayed_event(delay_id.clone(), UpdateAction::Restart).await
                {
                    // A single failed restart — a token-refresh 401 is enough —
                    // lets the delay elapse and the server publish the staged
                    // leave, consuming the delay id. A dead guard must not
                    // keep heart-beating a consumed id: stage a fresh one so
                    // the eventual hang-up still has a server-side leave, and
                    // let the membership observer heal the published leave.
                    warn!("Could not restart the delayed leave event, restaging: {error}");

                    match session.schedule_delayed_leave().await {
                        Ok(new_delay_id) => {
                            // The old one may still be armed — the restart
                            // failed on a transient, not because the leave
                            // had fired. Left standing it fires at its
                            // original deadline and wipes the membership
                            // the new one now guards; peers see a blip and
                            // the self-heal republishes (audit 2026-09-05,
                            // SDK P3). Best-effort: a consumed id answers
                            // `M_NOT_FOUND`, which is the outcome we want.
                            if let Err(error) = session
                                .update_delayed_event(delay_id.clone(), UpdateAction::Cancel)
                                .await
                            {
                                debug!(
                                    "The replaced delayed leave could not be cancelled: {error}"
                                );
                            }
                            let mut state = session.inner.state.lock().await;
                            let SessionState::Joined(joined) = &mut *state else { break };
                            // A republish may have staged ITS OWN leave (and
                            // started its own heartbeat) while this restage
                            // was in flight. Overwriting its id here left
                            // that leave armed with nobody beating it, and
                            // it wiped the fresh membership ~10 s later
                            // (pre-merge review 2026-09-08, SDK P2). The
                            // newer owner wins; this one retires.
                            if !restage_still_owns_the_leave(joined.delay_id.as_deref(), &delay_id)
                            {
                                drop(state);
                                debug!(
                                    "The delayed leave was replaced during the restage; \
                                     cancelling this one and stopping"
                                );
                                if let Err(error) = session
                                    .update_delayed_event(new_delay_id, UpdateAction::Cancel)
                                    .await
                                {
                                    debug!(
                                        "The superseded restage could not be cancelled: {error}"
                                    );
                                }
                                break;
                            }
                            joined.delay_id = Some(new_delay_id.clone());
                            delay_id = new_delay_id;
                        }
                        Err(error) => {
                            warn!("Could not restage the delayed leave event: {error}");
                        }
                    }
                }
            }
        })
    }

    /// A quiet-room retry for peers a distribution round did not reach.
    /// `synchronise_memberships` already treats them as owed (an unrotated
    /// round, never a rotation); this only makes sure it runs.
    fn spawn_owed_key_retry(&self) -> JoinHandle<()> {
        let session = self.weak_clone();

        spawn(async move {
            loop {
                matrix_sdk_common::sleep::sleep(OWED_KEY_RETRY_INTERVAL).await;

                let Some(session) = session.upgrade() else { break };
                let owes_anybody = {
                    let state = session.inner.state.lock().await;
                    match &*state {
                        SessionState::Joined(joined) => owes_a_key(&joined.unshared_memberships),
                        SessionState::Idle | SessionState::Joining => break,
                    }
                };
                if !owes_anybody {
                    continue;
                }

                debug!("Retrying the media key for peers the last round did not reach");
                if let Err(error) = session.synchronise_memberships().await {
                    warn!("Could not retry the owed media key: {error}");
                }
            }
        })
    }

    fn spawn_membership_refresh(&self, expiry: Duration) -> JoinHandle<()> {
        let session = self.weak_clone();
        // Refresh well before the published expiry so a slow request cannot
        // let the membership lapse mid-call.
        let interval = expiry.mul_f32(0.5).max(Duration::from_secs(60));

        spawn(async move {
            loop {
                matrix_sdk_common::sleep::sleep(interval).await;

                let Some(session) = session.upgrade() else { break };

                // Re-anchor the window on now. Re-sending the ORIGINAL
                // `created_ts`, which this used to do, meant a refresh could
                // not move the deadline it exists to move: peers resolve the
                // absolute expiry as `(created_ts ?? origin_server_ts) +
                // expires`, so a fixed `created_ts` pins it to join time and
                // every call outlasting the expiry blipped out of the
                // participant list no matter how faithfully we refreshed.
                //
                // Stamping it — rather than dropping it to `None` and letting
                // `origin_server_ts` serve — is what makes the refresh
                // survive Synapse, which deduplicates a state write whose
                // content already matches the current state. With `None` the
                // second and every later refresh would be byte-identical to
                // the first, accepted, and change nothing.
                let created_ts = MilliSecondsSinceUnixEpoch::now();

                // Bounded retry (audit 2026-09-05, SDK P3): one failed
                // refresh — a token-refresh 401 is enough — used to put the
                // NEXT attempt exactly on the expiry deadline, and two put
                // it past it: peers dropped us, the agent ended the
                // session, the self-heal republished into a call that had
                // already ended for everyone else. A few quick retries
                // cover the transient; a persistent failure still logs and
                // waits for the next interval.
                //
                // The intent lock is taken per attempt, never across the
                // retry sleeps: held for the whole loop it blocked a camera
                // toggle for up to four PUTs' worth of retries (pre-merge
                // review 2026-09-08, SDK P2). Each attempt re-reads the
                // intent under the lock so a toggle that landed in between is
                // what gets refreshed, not a stale copy.
                let mut attempt = 0;
                loop {
                    let outcome = {
                        let _intent_update = session.inner.call_intent_update.lock().await;
                        let (transport, call_intent) = {
                            let state = session.inner.state.lock().await;
                            match &*state {
                                SessionState::Joined(joined) => {
                                    (joined.transport.clone(), joined.call_intent)
                                }
                                SessionState::Idle | SessionState::Joining => return,
                            }
                        };
                        session
                            .refresh_membership_once(&transport, expiry, created_ts, call_intent)
                            .await
                    };
                    match outcome {
                        Ok(()) => break,
                        Err(error) if attempt < REFRESH_RETRIES => {
                            attempt += 1;
                            warn!(
                                "Could not refresh the MatrixRTC membership (attempt {attempt}), retrying: {error}"
                            );
                            matrix_sdk_common::sleep::sleep(REFRESH_RETRY_DELAY).await;
                        }
                        Err(error) => {
                            warn!("Could not refresh the MatrixRTC membership: {error}");
                            break;
                        }
                    }
                }
            }
        })
    }

    /// One refresh pass: re-anchor the expiry window, then remember the new
    /// anchor.
    ///
    /// The write-back is the half that used to be missing.
    /// `update_call_intent` republishes from the cached field, so a camera
    /// toggle after a refresh re-sent the JOIN-time stamp and rewound the
    /// resolved expiry by a whole refresh interval — leaving the membership
    /// riding on a single PUT landing exactly on time, which is the failure
    /// the refresh exists to prevent.
    ///
    /// Split out of the loop above so a test can drive it without waiting out
    /// the interval; a test of the publish alone cannot see the write-back at
    /// all, which is how it shipped missing.
    async fn refresh_membership_once(
        &self,
        transport: &LiveKitTransport,
        expiry: Duration,
        created_ts: MilliSecondsSinceUnixEpoch,
        call_intent: Option<RtcCallIntent>,
    ) -> Result<()> {
        self.publish_membership(transport, expiry, Some(created_ts), call_intent).await?;

        let mut state = self.inner.state.lock().await;
        if let SessionState::Joined(joined) = &mut *state {
            joined.membership_created_ts = created_ts;
        }
        Ok(())
    }

    fn observe_encryption_keys(&self) -> Option<EventHandlerDropGuard> {
        let client = self.room.client();
        let room_id = self.room.room_id().to_owned();
        let updates = self.inner.updates.clone();
        let own_user_id = self.inner.user_id.clone();
        let own_device_id = self.inner.device_id.clone();

        let handle = client.add_event_handler(
            async move |raw: Raw<AnyToDeviceEvent>,
                        encryption_info: Option<
                matrix_sdk_common::deserialized_responses::EncryptionInfo,
            >| {
                let Ok(Some(event_type)) = raw.get_field::<String>("type") else { return };
                if event_type != ENCRYPTION_KEYS_EVENT_TYPE {
                    return;
                }

                // Media keys are only trustworthy when they arrived through
                // Olm; a clear-text key could be injected by the homeserver.
                if encryption_info.is_none() {
                    warn!("Dropping a MatrixRTC media key that arrived unencrypted");
                    return;
                }

                let Ok(envelope) = raw.deserialize_as_unchecked::<ToDeviceEnvelope>() else {
                    return;
                };

                if envelope.sender == own_user_id {
                    // Our own other devices publish their own media; we never
                    // apply their keys to ourselves.
                    if let Ok(content) =
                        serde_json::from_str::<EncryptionKeysContent>(envelope.content.get())
                        && content.member.claimed_device_id == own_device_id
                    {
                        return;
                    }
                }

                let Ok(content) =
                    serde_json::from_str::<EncryptionKeysContent>(envelope.content.get())
                else {
                    return;
                };

                // The crypto layer tells us which device the Olm session
                // actually belongs to; the payload only CLAIMS one.
                let sending_device_id =
                    encryption_info.as_ref().and_then(|info| info.sender_device.as_deref());
                if let Some(key) =
                    parse_received_key(&envelope.sender, sending_device_id, &room_id, content)
                {
                    let _ = updates.send(MatrixRtcUpdate::EncryptionKeyReceived { key });
                }
            },
        );

        Some(client.event_handler_drop_guard(handle))
    }

    /// Re-read the memberships and reconcile our key state with them.
    async fn synchronise_memberships(&self) -> Result<()> {
        // One reconciliation at a time (see `SessionInner::reconcile`).
        let _reconcile = self.inner.reconcile.lock().await;

        let memberships = read_memberships(&self.room).await?;

        let mut state = self.inner.state.lock().await;
        let SessionState::Joined(joined) = &mut *state else {
            return Ok(());
        };

        let own_identity = membership_id(&self.inner.user_id, &self.inner.device_id);

        // A delayed leave the server fired spuriously (a failed heartbeat
        // tick is enough) wipes our membership while we still consider the
        // call live: peers rotate their keys away from us and we go silent
        // for them. Re-assert the membership instead of lingering as a
        // ghost.
        let own_present =
            memberships.iter().any(|membership| membership.livekit_identity() == own_identity);
        // Re-publishing is SINGLE-FLIGHT and TRACKED. Untracked, it outlived
        // `leave()` and re-created the membership seconds after the user hung
        // up — a ghost that peers see for the whole membership expiry.
        // Unbounded, it span a fresh HTTP PUT on every RoomInfo change
        // for as long as the publish kept failing (no permission,
        // server 5xx).
        let republish_in_flight = joined.republish.as_ref().is_some_and(|task| !task.is_finished());
        if own_present {
            joined.own_membership_seen = true;
        }
        // A wipe is only a wipe once the membership has been seen — or, as
        // the fallback for an echo that never comes, once the join is old
        // enough that a sync must have carried it.
        let wiped = !own_present
            && (joined.own_membership_seen || joined.joined_at.elapsed() > SELF_HEAL_ECHO_GRACE);
        if wiped && !republish_in_flight {
            warn!("Own MatrixRTC membership disappeared while joined; re-publishing it");
            let session = self.weak_clone();
            joined.republish = Some(spawn(async move {
                let Some(session) = session.upgrade() else { return };
                session.republish_membership().await;
            }));
        }

        let present: BTreeMap<String, MilliSecondsSinceUnixEpoch> = memberships
            .iter()
            .filter(|membership| membership.livekit_identity() != own_identity)
            .map(|membership| (membership.livekit_identity(), membership.created_ts))
            .collect();

        // A leaver is a device that is GONE. A joiner is a device we have not
        // shared with under its current `created_ts`: brand new, renewed, or
        // left-and-back inside one observer tick. Renewals and rejoins get the
        // key re-sent without a rotation — the same reading the voice agent
        // runtime applies to us, so both sides agree on what a moved
        // `created_ts` means.
        let anybody_left =
            joined.known_memberships.keys().any(|identity| !present.contains_key(identity));
        let anybody_new = present.iter().any(|(identity, created_ts)| {
            joined.known_memberships.get(identity) != Some(created_ts)
        });
        // A device we have never shared with at all — as opposed to one whose
        // `created_ts` merely moved. Only THIS forces a rotation: a renewal
        // re-shares the key it was already entitled to, and rotating for it
        // would put an undecryptable window under every peer every time
        // somebody's membership refreshed (pre-merge review 2026-09-08; the
        // rule the doc and the renewal test always claimed).
        let anybody_brand_new =
            present.keys().any(|identity| !joined.known_memberships.contains_key(identity));
        // Owed from a previous round: re-sending them the key they were
        // already entitled to is what a successful first round would have
        // done. It must NOT count as a joiner for the rotation decision —
        // a permanently unreachable device (one that never uploaded keys)
        // would otherwise rotate the call every tick past the grace period,
        // an undecryptable window for every peer on every room event.
        let anybody_owed =
            present.keys().any(|identity| joined.unshared_memberships.contains(identity));
        let anybody_joined = anybody_new || anybody_owed;
        // Who an UNROTATED round sends to: the new and the owed. Everybody
        // else already holds this key, and re-sending it to the whole roster
        // on every room event — with a permanently unknown device present,
        // on every message and receipt — was one to-device send per
        // participant per event for nothing (audit 2026-09-05, SDK P3).
        let needs_current_key = recipients_of_an_unrotated_round(
            &present,
            &joined.known_memberships,
            &joined.unshared_memberships,
        );
        // Kept until the distribution below has actually happened: a round
        // that fails as a whole must leave the leaver detectable by the next
        // one, or the departed device keeps decrypting us for the rest of
        // the call — the rotation it forced would otherwise be forgotten
        // with it.
        let previous_known = std::mem::replace(&mut joined.known_memberships, present);
        joined
            .unshared_memberships
            .retain(|identity| joined.known_memberships.contains_key(identity));

        let _ = self
            .inner
            .updates
            .send(MatrixRtcUpdate::MembershipsChanged { memberships: memberships.clone() });

        if !joined.encrypt_media || (!anybody_left && !anybody_joined) {
            return Ok(());
        }

        let key_age_ms =
            MilliSecondsSinceUnixEpoch::now().get().saturating_sub(joined.own_key_created_at.get());
        let key_is_fresh = u64::from(key_age_ms) < KEY_ROTATION_GRACE_PERIOD.as_millis() as u64;

        // A participant that left must not be able to decrypt what we send
        // afterwards, so a departure always forces a new key. A joiner can
        // reuse a key that is young enough to carry no meaningful history.
        //
        // Fresh material goes to the NEXT ring index. Frames carry their
        // index, so receivers keep decrypting the previous slot until we
        // switch senders (`USE_KEY_DELAY` below) and follow us the moment we
        // do — no window in which the two sides disagree on what one slot
        // holds. Publishing every rotation at index 0 instead (the rule
        // until 2026-09-12) put exactly that window under every rotation:
        // a receiver installed the new key in slot 0 within half a second
        // while we kept sending with the old one for a full second, and
        // device day run 4 heard it as ~1 s of silence per agent leave.
        // The index wraps with the u8 the wire carries; both clients ring
        // 256 slots. A sender that cannot move off index 0 (LiveKit Swift
        // before the `spiral/2.16.0` fork) must not run this code — it
        // would tag frames 0 while peers hold only the bumped slot.
        let rotated = anybody_left || (anybody_brand_new && !key_is_fresh);
        // The new key is NOT committed to the session yet: distribution can
        // fail, and a session that already believes in a key the peers never
        // received leaves the media layer encrypting with the old one while
        // nothing announces the difference.
        let key = if rotated {
            let next_index = joined.own_key.as_ref().map_or(0, |key| key.index.wrapping_add(1));
            RtcEncryptionKey::generate(next_index)
        } else {
            let Some(existing) = joined.own_key.clone() else { return Ok(()) };
            existing
        };
        drop(state);

        let recipients: Vec<RtcMembership> = if rotated {
            memberships.clone()
        } else {
            memberships
                .iter()
                .filter(|membership| needs_current_key.contains(&membership.livekit_identity()))
                .cloned()
                .collect()
        };
        let unreached = match self.distribute_key(&recipients, &key).await {
            Ok(unreached) => unreached,
            Err(error) => {
                let mut state = self.inner.state.lock().await;
                if let SessionState::Joined(joined) = &mut *state {
                    joined.known_memberships = previous_known;
                }
                return Err(error);
            }
        };

        // Distribution succeeded: commit, then announce. Announcing is driven
        // by what actually CHANGED rather than by `rotated` — an unrotated
        // round still re-sends the current key to a joiner, and if the media
        // layer had never been told about it (a previous round failed after
        // committing) the joiner would hold a key we never encrypt with.
        let mut state = self.inner.state.lock().await;
        let SessionState::Joined(joined) = &mut *state else {
            // Left while we were distributing; nothing to commit.
            return Ok(());
        };
        joined.unshared_memberships = unreached;
        let is_new_to_us = joined.own_key.as_ref() != Some(&key);
        joined.own_key = Some(key.clone());
        if rotated {
            joined.own_key_created_at = MilliSecondsSinceUnixEpoch::now();
        }
        drop(state);

        if is_new_to_us {
            // Give receivers time to install the key before we start
            // publishing media they could not decrypt.
            matrix_sdk_common::sleep::sleep(USE_KEY_DELAY).await;
            let _ = self.inner.updates.send(MatrixRtcUpdate::OwnEncryptionKeyChanged { key });
        }

        Ok(())
    }

    /// Re-assert a membership the server wiped from under us.
    ///
    /// The delayed leave staged for the OLD membership is retired first: it
    /// is the thing that fired spuriously, and left standing it would fire
    /// again at the next missed heartbeat and wipe the re-published
    /// membership too. A fresh one is staged for the new membership so the
    /// eventual hang-up still has a server-side leave, and the heartbeat is
    /// pointed at it. The new publication stamps its own `created_ts`, and
    /// the cached anchor follows it so a later call-intent update does not
    /// republish the join-time stamp and rewind the expiry.
    async fn republish_membership(&self) {
        let _intent_update = self.inner.call_intent_update.lock().await;
        // Re-check under the lock: `leave()` may have run while this task was
        // waiting to be scheduled.
        let (transport, expiry, call_intent, stale_delay_id) = {
            let state = self.inner.state.lock().await;
            let SessionState::Joined(joined) = &*state else { return };
            (
                joined.transport.clone(),
                joined.membership_expiry,
                joined.call_intent,
                joined.delay_id.clone(),
            )
        };

        if let Some(stale_delay_id) = stale_delay_id
            && let Err(error) =
                self.update_delayed_event(stale_delay_id, UpdateAction::Cancel).await
        {
            // Already consumed (it fired) or otherwise gone: nothing to cancel.
            debug!("Could not cancel the stale delayed leave event: {error}");
        }

        let created_ts = MilliSecondsSinceUnixEpoch::now();
        if let Err(error) =
            self.publish_membership(&transport, expiry, Some(created_ts), call_intent).await
        {
            warn!("Could not re-publish the MatrixRTC membership: {error}");
            return;
        }

        let new_delay_id = match self.schedule_delayed_leave().await {
            Ok(delay_id) => Some(delay_id),
            Err(error) => {
                warn!("Could not restage the delayed leave event after re-publishing: {error}");
                None
            }
        };

        let mut state = self.inner.state.lock().await;
        let SessionState::Joined(joined) = &mut *state else { return };
        joined.membership_created_ts = created_ts;
        joined.delay_id = new_delay_id.clone();
        if let Some(delay_id) = new_delay_id {
            // The old heartbeat is beating a cancelled id; replace it.
            let heartbeat = self.spawn_delayed_leave_heartbeat(delay_id);
            joined.tasks.push(heartbeat);
        }
    }

    #[instrument(skip_all, fields(key_index = key.index))]
    /// Returns the LiveKit identities the key did NOT reach; the caller
    /// keeps them as joiners for the next round.
    async fn distribute_key(
        &self,
        memberships: &[RtcMembership],
        key: &RtcEncryptionKey,
    ) -> Result<BTreeSet<String>> {
        let client = self.room.client();
        let encryption = client.encryption();

        let mut devices = Vec::new();
        let mut unreached = BTreeSet::new();
        for membership in memberships {
            if membership.user_id == self.inner.user_id
                && membership.device_id == self.inner.device_id
            {
                continue;
            }

            match encryption.get_device(&membership.user_id, &membership.device_id).await {
                Ok(Some(device)) => devices.push(device),
                Ok(None) => {
                    warn!(
                        user_id = %membership.user_id,
                        "Skipping a MatrixRTC participant with an unknown device; retrying next round"
                    );
                    unreached.insert(membership.livekit_identity());
                }
                Err(error) => {
                    warn!(
                        user_id = %membership.user_id,
                        "Could not look up a MatrixRTC participant's device: {error}"
                    );
                    unreached.insert(membership.livekit_identity());
                }
            }
        }

        if devices.is_empty() {
            debug!("No other participant to share the media key with");
            return Ok(unreached);
        }

        let content = EncryptionKeysContent::new(
            &self.inner.user_id,
            &self.inner.device_id,
            self.room.room_id(),
            key,
        );
        let content = Raw::new(&content).map_err(Error::Serialization)?;

        // The call's media key is protected by nothing else, so it follows
        // the SAME recipient policy the client is configured with
        // (`ClientBuilder::with_room_key_recipient_strategy`) rather than a
        // constant. Hard-coding `AllDevices` here shipped the key to every
        // unsigned device of every participant even for a user who had asked
        // to exclude insecure devices — upstream marks that variant "not
        // recommended, per the guidance of MSC4153".
        let strategy = self.room.client().base_client().room_key_recipient_strategy.clone();
        let failures = match encryption
            .encrypt_and_send_raw_to_device(
                devices.iter().collect(),
                ENCRYPTION_KEYS_EVENT_TYPE,
                content.clone().cast_unchecked(),
                strategy.clone(),
            )
            .await
        {
            Ok(failures) => failures,
            Err(error) => {
                // A verified-user problem (an unsigned device, a changed
                // identity) fails the WHOLE round under the strict strategy,
                // which used to leave every healthy peer keyless because of
                // one device of one participant (pre-merge review 2026-09-08,
                // SDK P1). Exclude the offenders — they stay owed, so the
                // round after the user fixes the trust reaches them — and
                // send to everybody else.
                let Some(offenders) = verified_user_problem_devices(&error) else {
                    return Err(Error::from_sdk(error));
                };
                let (devices, excluded) = exclude_offenders(devices, &offenders);
                warn!(
                    excluded = excluded.len(),
                    "Excluding devices with a verified-user problem from the media key round"
                );
                unreached.extend(excluded);
                if devices.is_empty() {
                    return Ok(unreached);
                }
                encryption
                    .encrypt_and_send_raw_to_device(
                        devices.iter().collect(),
                        ENCRYPTION_KEYS_EVENT_TYPE,
                        content.cast_unchecked(),
                        strategy,
                    )
                    .await
                    .map_err(Error::from_sdk)?
            }
        };

        if !failures.is_empty() {
            // Failing to reach a device is not fatal: that participant simply
            // cannot decrypt us until the next distribution round — which is
            // why they are handed back as owed, so there IS a next round.
            warn!(count = failures.len(), "Could not deliver the media key to every device");
            unreached.extend(
                failures.iter().map(|(user_id, device_id)| membership_id(user_id, device_id)),
            );
        }

        Ok(unreached)
    }

    fn weak_clone(&self) -> WeakSession {
        WeakSession { room: self.room.clone(), inner: Arc::downgrade(&self.inner) }
    }
}

/// A handle that does not keep the session alive, used by background tasks so
/// that dropping the session stops them.
struct WeakSession {
    room: Room,
    inner: std::sync::Weak<SessionInner>,
}

impl WeakSession {
    fn upgrade(&self) -> Option<MatrixRtcSession> {
        Some(MatrixRtcSession { room: self.room.clone(), inner: self.inner.upgrade()? })
    }
}

#[derive(Deserialize)]
struct ToDeviceEnvelope {
    sender: OwnedUserId,
    content: Box<RawJsonValue>,
}

#[derive(Deserialize)]
struct StateEventEnvelope {
    state_key: String,
    sender: OwnedUserId,
    origin_server_ts: MilliSecondsSinceUnixEpoch,
    content: Box<RawJsonValue>,
}

/// The devices a strict recipient strategy refused the whole round over, or
/// `None` when the error is anything else. `VerifiedUserChangedIdentity`
/// names users, not devices: every device of such a user is an offender.
fn verified_user_problem_devices(error: &crate::Error) -> Option<VerifiedUserProblem> {
    use matrix_sdk_base::crypto::SessionRecipientCollectionError as E;
    let crate::Error::OlmError(olm) = error else { return None };
    let matrix_sdk_base::crypto::OlmError::SessionRecipientCollectionError(collection) =
        olm.as_ref()
    else {
        return None;
    };
    match collection {
        E::VerifiedUserHasUnsignedDevice(map) => Some(VerifiedUserProblem {
            devices: map
                .iter()
                .flat_map(|(user, devices)| {
                    devices.iter().map(move |device| (user.clone(), device.clone()))
                })
                .collect(),
            users: BTreeSet::new(),
        }),
        E::VerifiedUserChangedIdentity(users) => Some(VerifiedUserProblem {
            devices: BTreeSet::new(),
            users: users.iter().cloned().collect(),
        }),
        _ => None,
    }
}

/// What a strict strategy refused: specific devices, or whole users.
#[derive(Debug, Default, PartialEq, Eq)]
struct VerifiedUserProblem {
    devices: BTreeSet<(OwnedUserId, OwnedDeviceId)>,
    users: BTreeSet<OwnedUserId>,
}

impl VerifiedUserProblem {
    fn covers(&self, user_id: &ruma::UserId, device_id: &ruma::DeviceId) -> bool {
        self.users.contains(user_id)
            || self.devices.contains(&(user_id.to_owned(), device_id.to_owned()))
    }
}

/// Split a round's recipients into the ones to send to and the LiveKit
/// identities to hand back as owed.
fn exclude_offenders(
    devices: Vec<crate::encryption::identities::Device>,
    offenders: &VerifiedUserProblem,
) -> (Vec<crate::encryption::identities::Device>, BTreeSet<String>) {
    let mut kept = Vec::with_capacity(devices.len());
    let mut excluded = BTreeSet::new();
    for device in devices {
        if offenders.covers(device.user_id(), device.device_id()) {
            excluded.insert(membership_id(device.user_id(), device.device_id()));
        } else {
            kept.push(device);
        }
    }
    (kept, excluded)
}

/// Whether a heartbeat that just restaged a delayed leave may install its new
/// id: only if the session still points at the id THIS heartbeat was beating.
/// A republish that ran meanwhile staged its own leave and heartbeat; taking
/// over from it would leave that leave armed with nobody restarting it.
fn restage_still_owns_the_leave(current_delay_id: Option<&str>, beaten_delay_id: &str) -> bool {
    current_delay_id == Some(beaten_delay_id)
}

/// Whether the quiet-room retry has anything to do: a session that owes its
/// key to somebody still present. Idle sessions and full rosters skip.
fn owes_a_key(unshared: &BTreeSet<String>) -> bool {
    !unshared.is_empty()
}

/// The identities an UNROTATED distribution round sends the current key to:
/// memberships we have not shared with under their present `created_ts`
/// (new or renewed) and the ones a previous round did not reach.
fn recipients_of_an_unrotated_round(
    present: &BTreeMap<String, MilliSecondsSinceUnixEpoch>,
    known: &BTreeMap<String, MilliSecondsSinceUnixEpoch>,
    unshared: &BTreeSet<String>,
) -> BTreeSet<String> {
    present
        .iter()
        .filter(|(identity, created_ts)| {
            known.get(*identity) != Some(*created_ts) || unshared.contains(*identity)
        })
        .map(|(identity, _)| identity.clone())
        .collect()
}

/// Read every non-expired membership published in the room.
async fn read_memberships(room: &Room) -> Result<Vec<RtcMembership>> {
    let events = room
        .get_state_events(StateEventType::from(CALL_MEMBER_EVENT_TYPE))
        .await
        .map_err(Error::from_sdk)?;

    let mut memberships = Vec::new();

    for event in events {
        let json = match &event {
            RawAnySyncOrStrippedState::Sync(raw) => raw.json(),
            // A stripped state event carries no timestamp, so it can never be
            // aged out; those rooms are not joined and have no live call.
            RawAnySyncOrStrippedState::Stripped(_) => continue,
        };

        let Ok(envelope) = serde_json::from_str::<StateEventEnvelope>(json.get()) else {
            continue;
        };

        memberships.extend(parse_membership(
            &envelope.state_key,
            &envelope.sender,
            envelope.origin_server_ts,
            &envelope.content,
        ));
    }

    memberships.sort_by_key(|membership| membership.livekit_identity());

    Ok(memberships)
}

impl Room {
    /// Get a handle on our participation in this room's MatrixRTC session.
    pub fn matrix_rtc_session(&self) -> Result<MatrixRtcSession> {
        MatrixRtcSession::new(self.clone())
    }
}

#[cfg(all(test, not(target_family = "wasm")))]
mod tests {
    use matrix_sdk_test::{JoinedRoomBuilder, async_test, event_factory::EventFactory};
    use ruma::{event_id, room_id};
    use serde_json::json;
    use wiremock::{
        Mock, ResponseTemplate,
        matchers::{method, path_regex, query_param},
    };

    use super::*;
    use crate::test_utils::mocks::MatrixMockServer;

    const SERVICE_URL: &str = "https://rtc.example.org/livekit/jwt";

    async fn mock_transports(server: &MatrixMockServer) {
        Mock::given(method("GET"))
            .and(path_regex(r"^/_matrix/client/unstable/org.matrix.msc4143/rtc/transports"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "rtc_transports": [{ "type": "livekit", "livekit_service_url": SERVICE_URL }]
            })))
            .named("rtc transports")
            .mount(server.server())
            .await;
    }

    #[test]
    fn leave_reason_is_copyable_for_callbacks() {
        let reason = LeaveReason::UserHungUp;
        let copy = reason;
        assert_eq!(reason, copy);
    }

    #[test]
    fn default_join_configuration_matches_element_call() {
        let configuration = JoinConfiguration::default();
        assert_eq!(configuration.membership_expiry, DEFAULT_MEMBERSHIP_EXPIRY);
        assert!(configuration.encrypt_media);
        assert_eq!(configuration.notification_type, None);
        assert_eq!(configuration.call_intent, None);
    }

    /// The deployed Spiral homeserver answers the MSC4143 endpoint with a bare
    /// JSON array instead of the `{ "rtc_transports": [...] }` object the MSC
    /// specifies. That endpoint is an optimisation, so a payload we cannot
    /// parse must fall through to `.well-known` rather than fail the call.
    #[async_test]
    async fn test_discovery_falls_back_when_the_transports_endpoint_is_malformed() {
        let server = MatrixMockServer::new().await;

        Mock::given(method("GET"))
            .and(path_regex(r"^/_matrix/client/unstable/org.matrix.msc4143/rtc/transports"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
                { "type": "livekit", "livekit_service_url": SERVICE_URL }
            ])))
            .named("malformed rtc transports")
            .mount(server.server())
            .await;

        let client = server.client_builder().build().await;

        let result = LiveKitTransport::discover(&client).await;

        // Before the endpoint became best-effort this surfaced the
        // deserialization error and the call never started. Discovery may still
        // end without a transport, but it must get as far as `.well-known`,
        // whose contents depend on the environment the test runs in.
        assert!(
            !matches!(result, Err(Error::Http(_))),
            "a malformed transports response must not abort discovery"
        );
    }

    #[async_test]
    async fn test_join_publishes_a_membership_and_stages_the_leave_event() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        mock_transports(&server).await;

        let room_id = room_id!("!room:localhost");
        let room = server.sync_joined_room(&client, room_id).await;

        // The staged leave event must be an empty membership, so that whoever
        // publishes it removes us from the call.
        let delayed = Mock::given(method("PUT"))
            .and(path_regex(r"^/_matrix/client/v3/rooms/.*/state/.*/.*"))
            .and(query_param("org.matrix.msc4140.delay", "10000"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "delay_id": "delay-1" })),
            )
            .named("delayed leave")
            .expect(1)
            .mount_as_scoped(server.server())
            .await;

        server
            .mock_room_send_state()
            .body_matches_partial_json(json!({
                "application": "m.call",
                "call_id": "",
                "scope": "m.room",
                "device_id": "DEVICEID",
                "membershipID": "@example:localhost:DEVICEID",
                "expires": 14_400_000,
                "m.call.intent": "audio",
                "focus_active": { "type": "livekit", "focus_selection": "oldest_membership" },
                "foci_preferred": [{
                    "type": "livekit",
                    "livekit_service_url": SERVICE_URL,
                    "livekit_alias": "!room:localhost",
                }],
            }))
            .ok(event_id!("$membership"))
            .expect(1)
            .mount()
            .await;

        let session = room.matrix_rtc_session().unwrap();
        session
            .join(JoinConfiguration {
                call_intent: Some(RtcCallIntent::Audio),
                ..Default::default()
            })
            .await
            .unwrap();

        assert!(session.is_joined().await);
        assert_eq!(session.own_identity(), "@example:localhost:DEVICEID");
        // A silent join sends no notification, so there is nothing to decline.
        assert!(session.rtc_notification_event_id().await.is_none());

        drop(delayed);
    }

    #[async_test]
    async fn test_joining_twice_publishes_a_single_membership() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        mock_transports(&server).await;

        let room = server.sync_joined_room(&client, room_id!("!room:localhost")).await;

        let _delayed = Mock::given(method("PUT"))
            .and(path_regex(r"^/_matrix/client/v3/rooms/.*/state/.*/.*"))
            .and(query_param("org.matrix.msc4140.delay", "10000"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "delay_id": "delay-1" })),
            )
            .expect(1)
            .mount_as_scoped(server.server())
            .await;

        server.mock_room_send_state().ok(event_id!("$membership")).expect(1).mount().await;

        let session = room.matrix_rtc_session().unwrap();
        session.join(JoinConfiguration::default()).await.unwrap();
        // A retrying caller must not end up with two memberships or two keys.
        session.join(JoinConfiguration::default()).await.unwrap();
    }

    #[async_test]
    async fn test_call_intent_can_change_while_joined() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        mock_transports(&server).await;

        let room = server.sync_joined_room(&client, room_id!("!room:localhost")).await;

        let _delayed = Mock::given(method("PUT"))
            .and(path_regex(r"^/_matrix/client/v3/rooms/.*/state/.*/.*"))
            .and(query_param("org.matrix.msc4140.delay", "10000"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "delay_id": "delay-1" })),
            )
            .mount_as_scoped(server.server())
            .await;

        server
            .mock_room_send_state()
            .body_matches_partial_json(json!({ "m.call.intent": "audio" }))
            .ok(event_id!("$audio-membership"))
            .expect(1)
            .mount()
            .await;
        server
            .mock_room_send_state()
            .body_matches_partial_json(json!({ "m.call.intent": "video" }))
            .ok(event_id!("$video-membership"))
            .expect(1)
            .mount()
            .await;

        let session = room.matrix_rtc_session().unwrap();
        session
            .join(JoinConfiguration {
                call_intent: Some(RtcCallIntent::Audio),
                ..Default::default()
            })
            .await
            .unwrap();

        session.update_call_intent(RtcCallIntent::Video).await.unwrap();
    }

    #[async_test]
    async fn test_join_can_ring_room_members() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        mock_transports(&server).await;
        server.mock_room_state_encryption().plain().mount().await;

        let room = server.sync_joined_room(&client, room_id!("!room:localhost")).await;

        let _delayed = Mock::given(method("PUT"))
            .and(path_regex(r"^/_matrix/client/v3/rooms/.*/state/.*/.*"))
            .and(query_param("org.matrix.msc4140.delay", "10000"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "delay_id": "delay-1" })),
            )
            .mount_as_scoped(server.server())
            .await;

        server.mock_room_send_state().ok(event_id!("$membership")).expect(1).mount().await;
        server
            .mock_room_send()
            .body_matches_partial_json(json!({
                "m.mentions": { "room": true },
                "m.relates_to": {
                    "rel_type": "m.reference",
                    "event_id": "$membership",
                },
                "notification_type": "ring",
                "m.call.intent": "audio",
                "lifetime": 30_000,
            }))
            .ok(event_id!("$notification"))
            .expect(1)
            .mount()
            .await;

        let session = room.matrix_rtc_session().unwrap();
        session
            .join(JoinConfiguration {
                notification_type: Some(RtcNotificationType::Ring),
                call_intent: Some(RtcCallIntent::Audio),
                ..Default::default()
            })
            .await
            .unwrap();

        // The caller needs the notification event id to observe declines.
        assert_eq!(
            session.rtc_notification_event_id().await.as_deref(),
            Some(event_id!("$notification"))
        );
    }

    /// SDK F1 (call audit 2026-09-05): the membership PUT succeeded and the
    /// notification then failed. `join` returns `Err` and the session is
    /// `Idle`, so a later `leave()` has nothing to do — the membership must
    /// therefore be withdrawn HERE, or the room shows a live call for the
    /// whole 4 h expiry and the agent joins a call with nobody in it.
    #[async_test]
    async fn test_a_join_that_fails_after_the_membership_put_withdraws_it() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        mock_transports(&server).await;
        server.mock_room_state_encryption().plain().mount().await;
        let room = server.sync_joined_room(&client, room_id!("!room:localhost")).await;

        server
            .mock_room_send_state()
            .body_matches_partial_json(json!({ "application": "m.call" }))
            .ok(event_id!("$membership"))
            .expect(1)
            .mount()
            .await;
        // The withdrawal: an empty membership on the same state key.
        server
            .mock_room_send_state()
            .body_matches_partial_json(json!({}))
            .ok(event_id!("$withdrawn"))
            .expect(1)
            .mount()
            .await;
        server.mock_room_send().error500().expect(1).mount().await;

        let session = room.matrix_rtc_session().unwrap();
        let result = session
            .join(JoinConfiguration {
                notification_type: Some(RtcNotificationType::Ring),
                ..Default::default()
            })
            .await;
        assert!(result.is_err(), "a failed notification fails the join");
        assert!(!session.is_joined().await);
    }

    /// Pre-merge review 2026-09-08 (SDK P2): the membership PUT failed on OUR
    /// side — a client-side timeout is the common case — but the server may
    /// have applied it, and no delayed leave exists yet. Without a withdrawal
    /// that membership stands for its full 4 h expiry as a ghost.
    #[async_test]
    async fn test_a_failed_membership_put_is_withdrawn_in_case_it_was_applied() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        mock_transports(&server).await;
        server.mock_room_state_encryption().plain().mount().await;
        let room = server.sync_joined_room(&client, room_id!("!room:localhost")).await;

        server
            .mock_room_send_state()
            .body_matches_partial_json(json!({ "application": "m.call" }))
            .error500()
            .expect(1)
            .mount()
            .await;
        // The withdrawal: an empty membership on the same state key, even
        // though we never saw the PUT succeed.
        server
            .mock_room_send_state()
            .body_matches_partial_json(json!({}))
            .ok(event_id!("$withdrawn"))
            .expect(1)
            .mount()
            .await;

        let session = room.matrix_rtc_session().unwrap();
        let result = session.join(JoinConfiguration::default()).await;
        assert!(result.is_err(), "a failed membership PUT fails the join");
        assert!(!session.is_joined().await);
    }

    /// The silent counterpart (ADR-0079 D3): a call started in a public room
    /// or a large group must reach the wire as `notification_type:
    /// "notification"` — a banner on every member's phone, never a ring. The
    /// receiving NSE keys on exactly this string to decide between CallKit
    /// and a plain notification.
    #[async_test]
    async fn test_join_can_notify_room_members_without_ringing() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        mock_transports(&server).await;
        server.mock_room_state_encryption().plain().mount().await;

        let room = server.sync_joined_room(&client, room_id!("!room:localhost")).await;

        let _delayed = Mock::given(method("PUT"))
            .and(path_regex(r"^/_matrix/client/v3/rooms/.*/state/.*/.*"))
            .and(query_param("org.matrix.msc4140.delay", "10000"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "delay_id": "delay-1" })),
            )
            .mount_as_scoped(server.server())
            .await;

        server.mock_room_send_state().ok(event_id!("$membership")).expect(1).mount().await;
        server
            .mock_room_send()
            .body_matches_partial_json(json!({
                "m.mentions": { "room": true },
                "m.relates_to": {
                    "rel_type": "m.reference",
                    "event_id": "$membership",
                },
                "notification_type": "notification",
                "m.call.intent": "video",
            }))
            .ok(event_id!("$notification"))
            .expect(1)
            .mount()
            .await;

        let session = room.matrix_rtc_session().unwrap();
        session
            .join(JoinConfiguration {
                notification_type: Some(RtcNotificationType::Notification),
                call_intent: Some(RtcCallIntent::Video),
                ..Default::default()
            })
            .await
            .unwrap();

        assert_eq!(
            session.rtc_notification_event_id().await.as_deref(),
            Some(event_id!("$notification"))
        );
    }

    #[async_test]
    async fn test_leaving_fires_the_staged_leave_event() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        mock_transports(&server).await;

        let room = server.sync_joined_room(&client, room_id!("!room:localhost")).await;

        let _delayed = Mock::given(method("PUT"))
            .and(path_regex(r"^/_matrix/client/v3/rooms/.*/state/.*/.*"))
            .and(query_param("org.matrix.msc4140.delay", "10000"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "delay_id": "delay-1" })),
            )
            .mount_as_scoped(server.server())
            .await;

        server.mock_room_send_state().ok(event_id!("$membership")).mount().await;

        let fired = Mock::given(method("POST"))
            .and(path_regex(
                r"^/_matrix/client/unstable/org.matrix.msc4140/delayed_events/delay-1$",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .named("fire staged leave")
            .expect(1)
            .mount_as_scoped(server.server())
            .await;

        let session = room.matrix_rtc_session().unwrap();
        session.join(JoinConfiguration::default()).await.unwrap();

        let mut updates = session.subscribe();
        session.leave().await.unwrap();

        assert!(!session.is_joined().await);
        assert_matches::assert_matches!(
            updates.try_recv(),
            Ok(MatrixRtcUpdate::Ended { reason: LeaveReason::UserHungUp })
        );

        drop(fired);
    }

    /// Hanging up must actually publish the leave when nothing was staged.
    ///
    /// Staging a delayed leave is best effort: a homeserver without MSC4140,
    /// a 5xx, a rate limit or an expired token all leave `delay_id` as
    /// `None`, and the join proceeds anyway. `leave()` used to treat that as
    /// nothing-to-do and return `Ok(())` having published nothing, so the
    /// membership stood for its full expiry while the caller was told the
    /// hang-up succeeded. Peers keep rendering someone who has gone.
    #[async_test]
    async fn test_leaving_publishes_directly_when_no_delayed_leave_was_staged() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        mock_transports(&server).await;

        let room = server.sync_joined_room(&client, room_id!("!room:localhost")).await;

        // Staging fails, exactly as it does against a homeserver with no
        // MSC4140 support. The join must still succeed — that is the
        // documented best-effort behaviour, and it is what makes the
        // no-delay-id state reachable at all.
        let _staging_refused = Mock::given(method("PUT"))
            .and(path_regex(r"^/_matrix/client/v3/rooms/.*/state/.*/.*"))
            .and(query_param("org.matrix.msc4140.delay", "10000"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "errcode": "M_UNRECOGNIZED",
                "error": "Unknown endpoint",
            })))
            .named("delayed events unsupported")
            .mount_as_scoped(server.server())
            .await;

        server.mock_room_send_state().ok(event_id!("$membership")).mount().await;

        let session = room.matrix_rtc_session().unwrap();
        session.join(JoinConfiguration::default()).await.unwrap();
        assert!(
            session.is_joined().await,
            "a homeserver without delayed events must not block joining"
        );

        // Two state sends must have happened by the end: the membership at
        // join, and the empty-content leave now. Counting from here isolates
        // the second.
        let sends_before = server
            .server()
            .received_requests()
            .await
            .map(|requests| {
                requests
                    .iter()
                    .filter(|r| {
                        r.method == wiremock::http::Method::PUT
                            && r.url.path().contains("/state/")
                            && r.url.query().is_none_or(|q| !q.contains("msc4140.delay"))
                    })
                    .count()
            })
            .expect("mock server records requests");

        session.leave().await.unwrap();

        let sends_after = server
            .server()
            .received_requests()
            .await
            .map(|requests| {
                requests
                    .iter()
                    .filter(|r| {
                        r.method == wiremock::http::Method::PUT
                            && r.url.path().contains("/state/")
                            && r.url.query().is_none_or(|q| !q.contains("msc4140.delay"))
                    })
                    .count()
            })
            .expect("mock server records requests");

        assert_eq!(
            sends_after,
            sends_before + 1,
            "leaving with no staged delay id must publish the leave itself, not silently succeed"
        );
        assert!(!session.is_joined().await);
    }

    /// A refresh has to move the deadline it exists to move.
    ///
    /// Peers resolve a membership's absolute expiry as
    /// `(content.created_ts ?? event.origin_server_ts) + content.expires`.
    /// Re-sending the ORIGINAL `created_ts` — which the refresh loop used to
    /// do — pins that sum to join time, so the membership lapses on schedule
    /// however many times we refresh, and any call outliving the expiry
    /// blinks out of the participant list.
    ///
    /// Stamping the current time also keeps the content changing, which is
    /// what gets the write past Synapse's deduplication of a state event
    /// whose content already matches the current state.
    #[async_test]
    async fn test_membership_refresh_moves_the_expiry_window() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        mock_transports(&server).await;
        let room = server.sync_joined_room(&client, room_id!("!room:localhost")).await;

        let _delayed = Mock::given(method("PUT"))
            .and(path_regex(r"^/_matrix/client/v3/rooms/.*/state/.*/.*"))
            .and(query_param("org.matrix.msc4140.delay", "10000"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "delay_id": "delay-1" })),
            )
            .mount_as_scoped(server.server())
            .await;
        server.mock_room_send_state().ok(event_id!("$membership")).mount().await;

        let before = MilliSecondsSinceUnixEpoch::now();
        let session = room.matrix_rtc_session().unwrap();
        session.join(JoinConfiguration::default()).await.unwrap();

        // Read back what a peer would see on the join publish.
        let joined_created_ts = {
            let state = session.inner.state.lock().await;
            match &*state {
                SessionState::Joined(joined) => joined.membership_created_ts,
                _ => panic!("expected to be joined"),
            }
        };
        assert!(joined_created_ts >= before, "sanity: the join stamps a creation time");

        // Drive one refresh publish the way the loop does, without waiting
        // out the real interval.
        let (transport, call_intent) = {
            let state = session.inner.state.lock().await;
            match &*state {
                SessionState::Joined(joined) => (joined.transport.clone(), joined.call_intent),
                _ => panic!("expected to be joined"),
            }
        };
        // A tick of real time so the two stamps cannot collide.
        tokio::time::sleep(Duration::from_millis(5)).await;
        let refresh_created_ts = MilliSecondsSinceUnixEpoch::now();
        session
            .refresh_membership_once(
                &transport,
                Duration::from_secs(4 * 60 * 60),
                refresh_created_ts,
                call_intent,
            )
            .await
            .unwrap();

        assert!(
            refresh_created_ts > joined_created_ts,
            "a refresh must publish a LATER created_ts than the join, or the expiry \
             window never moves: join={joined_created_ts:?} refresh={refresh_created_ts:?}"
        );

        // And it must be REMEMBERED. This is the half the first version of
        // this test could not see: it compared two local variables, so it
        // passed with the cached anchor still on join time — and every later
        // publisher, `update_call_intent` among them, reads that field and
        // would have rewound the window this refresh just moved.
        let cached_created_ts = {
            let state = session.inner.state.lock().await;
            match &*state {
                SessionState::Joined(joined) => joined.membership_created_ts,
                _ => panic!("expected to be joined"),
            }
        };
        assert_eq!(
            cached_created_ts, refresh_created_ts,
            "the refreshed anchor must be written back, or the next publish undoes it"
        );

        session.leave().await.unwrap();
    }

    #[async_test]
    async fn test_leaving_without_joining_is_a_no_op() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        let room = server.sync_joined_room(&client, room_id!("!room:localhost")).await;

        // No endpoint is mocked: a stray hang-up must not touch the network.
        room.matrix_rtc_session().unwrap().leave().await.unwrap();
    }

    /// The join handshake is four network round-trips. Holding the session
    /// lock across them made every other entry point wait it out — including
    /// `leave()`, i.e. the user hanging up during a slow connection had to
    /// wait for that connection to finish first.
    #[async_test]
    async fn test_the_session_stays_responsive_while_joining() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        let room = server.sync_joined_room(&client, room_id!("!room:localhost")).await;

        // Discovery never answers: the join parks on its first round-trip.
        Mock::given(method("GET"))
            .and(path_regex(r"^/_matrix/client/unstable/org.matrix.msc4143/rtc/transports"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(30))
                    .set_body_json(json!({ "rtc_transports": [] })),
            )
            .named("slow rtc transports")
            .mount(server.server())
            .await;

        let session = Arc::new(room.matrix_rtc_session().unwrap());
        let joining = spawn({
            let session = session.clone();
            async move { session.join(JoinConfiguration::default()).await }
        });

        // Give the join time to reach its first await.
        matrix_sdk_common::sleep::sleep(Duration::from_millis(100)).await;

        // The lock is free: reads answer, and the hang-up returns without
        // waiting for the handshake. Both would have blocked for 30s before.
        assert!(session.rtc_notification_event_id().await.is_none());
        tokio::time::timeout(Duration::from_secs(2), session.leave())
            .await
            .expect("leaving must not wait for the join handshake")
            .unwrap();

        joining.abort();
    }

    /// Audit 2026-09-05 (SDK P2): session A's join is slow past its
    /// membership PUT; the user hangs up (A goes Idle) and redials through
    /// a FRESH session handle B, whose membership lands and syncs. When A's
    /// join finally returns it must NOT withdraw the membership — that is
    /// B's now, under the same state key.
    #[async_test]
    async fn test_a_cancelled_join_leaves_a_redials_membership_standing() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        mock_transports(&server).await;
        server.mock_room_state_encryption().plain().mount().await;
        let room = server.sync_joined_room(&client, room_id!("!room:localhost")).await;

        // A's membership PUT: accepted, but slow to answer.
        let slow_put = Mock::given(method("PUT"))
            .and(path_regex(r"^/_matrix/client/v3/rooms/.*/state/.*/.*"))
            .and(query_param("org.matrix.msc4140.delay", "10000"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "delay_id": "delay-a" })),
            )
            .mount_as_scoped(server.server())
            .await;
        server
            .mock_room_send_state()
            .body_matches_partial_json(json!({ "application": "m.call" }))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(600))
                    .set_body_json(json!({ "event_id": "$membership-a" })),
            )
            .mount()
            .await;
        // The withdrawal A must NOT send.
        server
            .mock_room_send_state()
            .body_matches_partial_json(json!({}))
            .ok(event_id!("$withdrawn"))
            .expect(0)
            .mount()
            .await;
        let _cancel_any = Mock::given(method("POST"))
            .and(path_regex(r"^/_matrix/client/unstable/org.matrix.msc4140/delayed_events/delay-"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .mount_as_scoped(server.server())
            .await;

        let session_a = Arc::new(room.matrix_rtc_session().unwrap());
        let joining = spawn({
            let session = session_a.clone();
            async move { session.join(JoinConfiguration::default()).await }
        });
        matrix_sdk_common::sleep::sleep(Duration::from_millis(100)).await;

        // Hang up while A's PUT is in flight, then redial: B's membership is
        // what the room holds under our state key now.
        session_a.leave().await.unwrap();
        let f = EventFactory::new();
        let ours_b = f
            .call_membership_state(
                client.user_id().unwrap().to_owned(),
                client.device_id().unwrap().to_string(),
            )
            .set_livekit_focus(room_id!("!room:localhost").to_string(), SERVICE_URL.to_owned())
            .state_key(membership_state_key(client.user_id().unwrap(), client.device_id().unwrap()))
            .event_id(event_id!("$membership-b"));
        server
            .sync_room(
                &client,
                JoinedRoomBuilder::new(room_id!("!room:localhost")).add_state_event(ours_b),
            )
            .await;

        // A's join returns into a session that was left.
        joining.await.unwrap().unwrap();
        assert!(!session_a.is_joined().await);
        drop(slow_put);
        // The `expect(0)` on the withdrawal mock verifies on drop.
    }

    #[async_test]
    async fn test_transport_credentials_require_a_joined_session() {
        let server = MatrixMockServer::new().await;
        let client = server.client_builder().build().await;
        let room = server.sync_joined_room(&client, room_id!("!room:localhost")).await;

        let session = room.matrix_rtc_session().unwrap();
        assert_matches::assert_matches!(
            session.transport_credentials().await,
            Err(Error::NotJoined)
        );
    }
}

#[cfg(all(test, not(target_family = "wasm")))]
mod reconciliation_tests {
    //! Brief 0067 (ADR-0079 D5): the reconciliation paths the first audit
    //! found untested — who counts as a joiner, what a renewed membership
    //! triggers, and how a wiped membership is re-asserted.

    use matrix_sdk_test::{JoinedRoomBuilder, async_test, event_factory::EventFactory};
    use ruma::{event_id, owned_user_id, room_id};
    use serde_json::json;
    use wiremock::{
        Mock, ResponseTemplate,
        matchers::{method, path_regex, query_param},
    };

    use super::*;
    use crate::test_utils::mocks::MatrixMockServer;

    const SERVICE_URL: &str = "https://rtc.example.org/livekit/jwt";
    fn room() -> &'static ruma::RoomId {
        room_id!("!room:localhost")
    }

    async fn mock_transports(server: &MatrixMockServer) {
        Mock::given(method("GET"))
            .and(path_regex(r"^/_matrix/client/unstable/org.matrix.msc4143/rtc/transports"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "rtc_transports": [{ "type": "livekit", "livekit_service_url": SERVICE_URL }]
            })))
            .mount(server.server())
            .await;
    }

    /// Stage a delayed leave with a fixed id; returns the scoped mock so a
    /// test can swap it for a second id later.
    async fn mock_delayed_leave(server: &MatrixMockServer, delay_id: &str) -> wiremock::MockGuard {
        Mock::given(method("PUT"))
            .and(path_regex(r"^/_matrix/client/v3/rooms/.*/state/.*/.*"))
            .and(query_param("org.matrix.msc4140.delay", "10000"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "delay_id": delay_id })))
            .mount_as_scoped(server.server())
            .await
    }

    async fn joined_session(server: &MatrixMockServer) -> (crate::Client, Room, MatrixRtcSession) {
        let client = server.client_builder().build().await;
        mock_transports(server).await;
        let room = server.sync_joined_room(&client, room()).await;
        server.mock_room_send_state().ok(event_id!("$membership")).mount().await;
        let session = room.matrix_rtc_session().unwrap();
        session.join(JoinConfiguration::default()).await.unwrap();
        (client, room, session)
    }

    /// A membership as this module publishes it: the state key carries the
    /// `_m.call` application suffix that `membership_state_key` writes and
    /// `parse_membership` requires; ruma's factory default omits it.
    fn bob_membership(
        f: &EventFactory,
        event_id: &ruma::EventId,
    ) -> matrix_sdk_test::event_factory::EventBuilder<
        ruma::events::call::member::CallMemberEventContent,
    > {
        let bob = owned_user_id!("@bob:localhost");
        f.call_membership_state(bob.clone(), "BOB".to_owned())
            .set_livekit_focus(room().to_string(), SERVICE_URL.to_owned())
            .state_key(membership_state_key(&bob, ruma::device_id!("BOB")))
            .event_id(event_id)
    }

    fn unshared(session: &MatrixRtcSession) -> BTreeSet<String> {
        let state = session.inner.state.try_lock().expect("state is not held across awaits");
        match &*state {
            SessionState::Joined(joined) => joined.unshared_memberships.clone(),
            _ => panic!("expected to be joined"),
        }
    }

    fn known(session: &MatrixRtcSession) -> BTreeMap<String, MilliSecondsSinceUnixEpoch> {
        let state = session.inner.state.try_lock().expect("state is not held across awaits");
        match &*state {
            SessionState::Joined(joined) => joined.known_memberships.clone(),
            _ => panic!("expected to be joined"),
        }
    }

    /// The first time a device shows up it is a joiner; the next observer
    /// tick with the SAME membership is not — nothing is re-sent for a
    /// RoomInfo change that did not touch the roster.
    #[async_test]
    async fn test_a_membership_is_a_joiner_once_until_its_created_ts_moves() {
        let server = MatrixMockServer::new().await;
        let _delayed = mock_delayed_leave(&server, "delay-1").await;
        let (client, _room, session) = joined_session(&server).await;

        let f = EventFactory::new();
        let bob = bob_membership(&f, event_id!("$bob-1"));
        server.sync_room(&client, JoinedRoomBuilder::new(room()).add_state_event(bob)).await;
        session.synchronise_memberships().await.unwrap();

        let first = known(&session);
        assert_eq!(first.len(), 1, "bob is known after his first membership");
        let bob_ts = first["@bob:localhost:BOB"];

        // A second sync that changes nothing about the roster.
        server.sync_room(&client, JoinedRoomBuilder::new(room())).await;
        session.synchronise_memberships().await.unwrap();
        assert_eq!(known(&session)["@bob:localhost:BOB"], bob_ts, "the anchor is unchanged");
    }

    /// Pre-merge review 2026-09-08 (SDK P2): a heartbeat whose restart failed
    /// restages a leave, but a republish may have staged ITS OWN in between.
    /// The heartbeat may only install its id when the session still points
    /// at the one it was beating; otherwise it retires and the republish's
    /// heartbeat keeps the newer leave alive.
    #[test]
    fn a_restage_installs_its_id_only_while_it_still_owns_the_leave() {
        assert!(restage_still_owns_the_leave(Some("delay-1"), "delay-1"));
        assert!(!restage_still_owns_the_leave(Some("delay-republished"), "delay-1"));
        assert!(!restage_still_owns_the_leave(None, "delay-1"), "a leave that is gone is not ours");
    }

    /// Pre-merge review 2026-09-08 (SDK P2): the owed round used to run only
    /// on a RoomInfo change; in a quiet room a peer stayed keyless. The retry
    /// task fires whenever somebody is owed and stays idle otherwise.
    #[test]
    fn the_quiet_room_retry_runs_only_while_somebody_is_owed() {
        assert!(!owes_a_key(&BTreeSet::new()));
        assert!(owes_a_key(&BTreeSet::from(["@bob:localhost:BOB".to_owned()])));
    }

    /// The retry reuses `synchronise_memberships`, so a tick with an unchanged
    /// roster and an owed peer must re-send WITHOUT rotating — the property
    /// `test_an_unreached_joiner_is_retried_on_the_next_round` pins. This
    /// pins that the owed set survives a no-change tick (the retry's input).
    #[async_test]
    async fn test_an_owed_peer_stays_owed_across_a_quiet_tick() {
        let server = MatrixMockServer::new().await;
        let _delayed = mock_delayed_leave(&server, "delay-1").await;
        let (client, _room, session) = joined_session(&server).await;

        let f = EventFactory::new();
        let bob = bob_membership(&f, event_id!("$bob-1"));
        server.sync_room(&client, JoinedRoomBuilder::new(room()).add_state_event(bob)).await;
        session.synchronise_memberships().await.unwrap();
        assert!(unshared(&session).contains("@bob:localhost:BOB"));

        // The retry task's tick: no sync at all, just another reconcile.
        session.synchronise_memberships().await.unwrap();
        assert!(
            unshared(&session).contains("@bob:localhost:BOB"),
            "still unreachable (no device keys on the mock server), still owed"
        );
    }

    /// Audit 2026-09-05 (SDK P3): an unrotated round addresses only who needs
    /// the key — the new, the renewed, the owed — never the whole roster.
    #[test]
    fn an_unrotated_round_sends_only_to_the_new_and_the_owed() {
        let ts = |n: u64| MilliSecondsSinceUnixEpoch(ruma::UInt::from(n as u32));
        let present = BTreeMap::from([
            ("@bob:localhost:BOB".to_owned(), ts(1)),
            ("@carol:localhost:CAROL".to_owned(), ts(5)),
            ("@dave:localhost:DAVE".to_owned(), ts(1)),
            ("@erin:localhost:ERIN".to_owned(), ts(1)),
        ]);
        // bob: shared under this created_ts — done. carol: renewed (ts
        // moved). dave: owed from a failed round. erin: brand new.
        let known = BTreeMap::from([
            ("@bob:localhost:BOB".to_owned(), ts(1)),
            ("@carol:localhost:CAROL".to_owned(), ts(1)),
            ("@dave:localhost:DAVE".to_owned(), ts(1)),
        ]);
        let unshared = BTreeSet::from(["@dave:localhost:DAVE".to_owned()]);

        let recipients = recipients_of_an_unrotated_round(&present, &known, &unshared);
        assert_eq!(
            recipients,
            BTreeSet::from([
                "@carol:localhost:CAROL".to_owned(),
                "@dave:localhost:DAVE".to_owned(),
                "@erin:localhost:ERIN".to_owned(),
            ])
        );
    }

    /// A joiner whose device the crypto store does not know yet (its
    /// `/keys/query` lands a sync after its membership) is recorded in the
    /// roster — it IS present, and its departure must still rotate — but
    /// stays OWED the key: the next reconciliation, roster change or not,
    /// treats it as a joiner and tries again. Recording it as shared-with,
    /// which is what shipped, left it decrypting silence for the whole call.
    #[async_test]
    async fn test_an_unreached_joiner_is_retried_on_the_next_round() {
        let server = MatrixMockServer::new().await;
        let _delayed = mock_delayed_leave(&server, "delay-1").await;
        let (client, _room, session) = joined_session(&server).await;

        let f = EventFactory::new();
        let bob = bob_membership(&f, event_id!("$bob-1"));
        server.sync_room(&client, JoinedRoomBuilder::new(room()).add_state_event(bob)).await;
        session.synchronise_memberships().await.unwrap();
        assert_eq!(known(&session).len(), 1, "bob is in the roster");
        assert!(unshared(&session).contains("@bob:localhost:BOB"), "but still owed the key");

        // Age the key past the grace period: a genuinely NEW joiner would
        // now rotate; a retry to an owed one must not.
        {
            let mut state = session.inner.state.lock().await;
            let SessionState::Joined(joined) = &mut *state else { panic!("expected to be joined") };
            let aged = joined.own_key_created_at.get().saturating_sub(
                ruma::UInt::try_from(KEY_ROTATION_GRACE_PERIOD.as_millis() as u64 + 1_000).unwrap(),
            );
            joined.own_key_created_at = MilliSecondsSinceUnixEpoch(aged);
        }

        // A tick that changes nothing about the roster still tries again.
        let mut updates = session.subscribe();
        server.sync_room(&client, JoinedRoomBuilder::new(room())).await;
        session.synchronise_memberships().await.unwrap();
        assert!(unshared(&session).contains("@bob:localhost:BOB"), "still unknown, still owed");
        // No leaver, no new joiner: the retry must not rotate on every tick.
        while let Ok(update) = updates.try_recv() {
            assert!(
                !matches!(update, MatrixRtcUpdate::OwnEncryptionKeyChanged { .. }),
                "a retry to an unreached joiner must not rotate the key"
            );
        }
    }

    /// A renewal — same device, later `created_ts` — is a joiner again, so
    /// the key is re-shared with it. It is NOT a leaver: no rotation, no
    /// undecryptable window for everybody else. This is the reading the voice
    /// agent runtime applies to our own renewals (`e2ee/membership.rs`).
    #[async_test]
    async fn test_a_renewed_membership_is_re_shared_with_but_rotates_nothing() {
        let server = MatrixMockServer::new().await;
        let _delayed = mock_delayed_leave(&server, "delay-1").await;
        let (client, _room, session) = joined_session(&server).await;
        let mut updates = session.subscribe();

        let f = EventFactory::new();
        let bob = bob_membership(&f, event_id!("$bob-1"));
        server.sync_room(&client, JoinedRoomBuilder::new(room()).add_state_event(bob)).await;
        session.synchronise_memberships().await.unwrap();
        let first_ts = known(&session)["@bob:localhost:BOB"];

        // Age the key past the grace period: a key young enough to carry no
        // history is re-used whatever happens, so a renewal test on a fresh
        // key proves nothing about renewals (pre-merge review 2026-09-08).
        {
            let mut state = session.inner.state.lock().await;
            let SessionState::Joined(joined) = &mut *state else { panic!("expected to be joined") };
            let aged = joined.own_key_created_at.get().saturating_sub(
                ruma::UInt::try_from(KEY_ROTATION_GRACE_PERIOD.as_millis() as u64 + 1_000).unwrap(),
            );
            joined.own_key_created_at = MilliSecondsSinceUnixEpoch(aged);
        }
        // Drain what the first pass announced.
        let mut announced_before = 0;
        while let Ok(update) = updates.try_recv() {
            if matches!(update, MatrixRtcUpdate::OwnEncryptionKeyChanged { .. }) {
                announced_before += 1;
            }
        }

        // The renewal carries a later timestamp.
        tokio::time::sleep(Duration::from_millis(5)).await;
        let bob_renewed = bob_membership(&f, event_id!("$bob-2"));
        server
            .sync_room(&client, JoinedRoomBuilder::new(room()).add_state_event(bob_renewed))
            .await;
        session.synchronise_memberships().await.unwrap();

        let renewed_ts = known(&session)["@bob:localhost:BOB"];
        assert!(renewed_ts > first_ts, "the renewal moved the anchor we remember for bob");

        // Re-shared, not rotated: the key the media layer holds is unchanged,
        // so nothing new is announced to it.
        let mut announced_after = 0;
        while let Ok(update) = updates.try_recv() {
            if matches!(update, MatrixRtcUpdate::OwnEncryptionKeyChanged { .. }) {
                announced_after += 1;
            }
        }
        assert_eq!(
            announced_after, 0,
            "a renewal must not rotate our key (got {announced_before} before)"
        );
    }

    /// A device that is GONE is a leaver: the key rotates and the media layer
    /// is told about the new one. Forward secrecy on a real exit is what the
    /// renewal rule above must not cost.
    #[async_test]
    async fn test_a_departed_device_rotates_the_key() {
        let server = MatrixMockServer::new().await;
        let _delayed = mock_delayed_leave(&server, "delay-1").await;
        let (client, _room, session) = joined_session(&server).await;

        let f = EventFactory::new();
        let bob = bob_membership(&f, event_id!("$bob-1"));
        server.sync_room(&client, JoinedRoomBuilder::new(room()).add_state_event(bob)).await;
        session.synchronise_memberships().await.unwrap();
        assert_eq!(known(&session).len(), 1);

        let mut updates = session.subscribe();
        let bob_left = f
            .call_membership_left_state(owned_user_id!("@bob:localhost"), "BOB".to_owned())
            .state_key(membership_state_key(
                &owned_user_id!("@bob:localhost"),
                ruma::device_id!("BOB"),
            ))
            .event_id(event_id!("$bob-left"));
        server.sync_room(&client, JoinedRoomBuilder::new(room()).add_state_event(bob_left)).await;
        session.synchronise_memberships().await.unwrap();

        assert!(known(&session).is_empty(), "bob is forgotten once his device is gone");
        let mut rotated_to = None;
        while let Ok(update) = updates.try_recv() {
            if let MatrixRtcUpdate::OwnEncryptionKeyChanged { key } = update {
                rotated_to = Some(key.index);
            }
        }
        // The join's key sat at index 0; the rotation must announce the NEXT
        // slot, or a receiver that installs it overwrites the key our frames
        // are still tagged with for the whole `USE_KEY_DELAY` (device day
        // run 4, 2026-09-12: an audible ~1 s gap per rotation).
        assert_eq!(
            rotated_to,
            Some(1),
            "a leaver must rotate our key to the next ring index and announce it to the media layer"
        );
    }

    /// The ring index is the u8 the wire carries: a rotation off 255 wraps
    /// to 0 rather than failing or sticking.
    #[async_test]
    async fn test_a_rotation_off_the_last_ring_index_wraps_to_zero() {
        let server = MatrixMockServer::new().await;
        let _delayed = mock_delayed_leave(&server, "delay-1").await;
        let (client, _room, session) = joined_session(&server).await;

        let f = EventFactory::new();
        let bob = bob_membership(&f, event_id!("$bob-1"));
        server.sync_room(&client, JoinedRoomBuilder::new(room()).add_state_event(bob)).await;
        session.synchronise_memberships().await.unwrap();
        {
            let mut state = session.inner.state.lock().await;
            let SessionState::Joined(joined) = &mut *state else { panic!("expected to be joined") };
            joined.own_key = Some(RtcEncryptionKey::generate(u8::MAX));
        }

        let mut updates = session.subscribe();
        let bob_left = f
            .call_membership_left_state(owned_user_id!("@bob:localhost"), "BOB".to_owned())
            .state_key(membership_state_key(
                &owned_user_id!("@bob:localhost"),
                ruma::device_id!("BOB"),
            ))
            .event_id(event_id!("$bob-left"));
        server.sync_room(&client, JoinedRoomBuilder::new(room()).add_state_event(bob_left)).await;
        session.synchronise_memberships().await.unwrap();

        let mut rotated_to = None;
        while let Ok(update) = updates.try_recv() {
            if let MatrixRtcUpdate::OwnEncryptionKeyChanged { key } = update {
                rotated_to = Some(key.index);
            }
        }
        assert_eq!(rotated_to, Some(0), "index 255 rotates to 0, the u8 wrap the wire allows");
    }

    /// Our own membership vanished (a delayed leave the server fired on a
    /// missed heartbeat): it is re-published under a NEW `created_ts`, the
    /// stale delayed leave is cancelled, and a fresh one is staged — the old
    /// one would otherwise fire again and wipe the re-publish too.
    #[async_test]
    async fn test_a_wiped_membership_is_republished_with_a_fresh_delayed_leave() {
        let server = MatrixMockServer::new().await;
        // Mocks are matched in mount order, so the staging mock must be up
        // before the generic state-send mock `joined_session` mounts. One
        // mock answers both stagings: `delay-1` for the join, `delay-2` for
        // the re-publish.
        let stagings = std::sync::atomic::AtomicUsize::new(0);
        let _delayed = Mock::given(method("PUT"))
            .and(path_regex(r"^/_matrix/client/v3/rooms/.*/state/.*/.*"))
            .and(query_param("org.matrix.msc4140.delay", "10000"))
            .respond_with(move |_: &wiremock::Request| {
                let n = stagings.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                ResponseTemplate::new(200)
                    .set_body_json(json!({ "delay_id": format!("delay-{}", n + 1) }))
            })
            .mount_as_scoped(server.server())
            .await;
        let _cancel_any = Mock::given(method("POST"))
            .and(path_regex(r"^/_matrix/client/unstable/org.matrix.msc4140/delayed_events/delay-"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .mount_as_scoped(server.server())
            .await;
        let (client, _room, session) = joined_session(&server).await;
        // The mock server stores no state, so our own membership is absent
        // from the very first reconciliation — which no longer republishes
        // (the echo has not synced; audit 2026-09-05). Sync the echo so the
        // membership counts as seen; the scenario under test is the wipe.
        {
            let state = session.inner.state.lock().await;
            let SessionState::Joined(joined) = &*state else { panic!("expected to be joined") };
            assert!(joined.republish.is_none(), "a fresh join must not self-heal before its echo");
        }
        let f = EventFactory::new();
        let ours = f
            .call_membership_state(
                client.user_id().unwrap().to_owned(),
                client.device_id().unwrap().to_string(),
            )
            .set_livekit_focus(room().to_string(), SERVICE_URL.to_owned())
            .state_key(membership_state_key(client.user_id().unwrap(), client.device_id().unwrap()))
            .event_id(event_id!("$ours"));
        server.sync_room(&client, JoinedRoomBuilder::new(room()).add_state_event(ours)).await;
        session.synchronise_memberships().await.unwrap();
        let (join_created_ts, delay_id_before) = {
            let state = session.inner.state.lock().await;
            match &*state {
                SessionState::Joined(joined) => {
                    (joined.membership_created_ts, joined.delay_id.clone())
                }
                _ => panic!("expected to be joined"),
            }
        };
        let delay_id_before = delay_id_before.expect("a delayed leave is staged after join");
        let cancels_of = |requests: &[wiremock::Request], delay_id: &str| {
            requests
                .iter()
                .filter(|r| {
                    r.method == wiremock::http::Method::POST
                        && r.url.path().ends_with(&format!("/delayed_events/{delay_id}"))
                        && String::from_utf8_lossy(&r.body).contains("cancel")
                })
                .count()
        };
        let cancels_before = cancels_of(
            &server.server().received_requests().await.unwrap_or_default(),
            &delay_id_before,
        );

        // Our own membership is absent from the roster the server now holds.
        let ours_left = f
            .call_membership_left_state(
                client.user_id().unwrap().to_owned(),
                client.device_id().unwrap().to_string(),
            )
            .state_key(membership_state_key(client.user_id().unwrap(), client.device_id().unwrap()))
            .event_id(event_id!("$ours-left"));
        server.sync_room(&client, JoinedRoomBuilder::new(room()).add_state_event(ours_left)).await;
        tokio::time::sleep(Duration::from_millis(5)).await;
        session.synchronise_memberships().await.unwrap();

        // The re-publish runs on its own task; wait for it to settle.
        let republish = {
            let mut state = session.inner.state.lock().await;
            match &mut *state {
                SessionState::Joined(joined) => joined.republish.take(),
                _ => panic!("expected to be joined"),
            }
        };
        republish.expect("a re-publish was spawned").await.unwrap();

        let state = session.inner.state.lock().await;
        let SessionState::Joined(joined) = &*state else { panic!("expected to be joined") };
        assert!(
            joined.membership_created_ts > join_created_ts,
            "the re-publish stamps a later created_ts and remembers it"
        );
        let delay_id_after = joined.delay_id.clone().expect("a fresh delayed leave is staged");
        assert_ne!(delay_id_after, delay_id_before, "the re-publish staged a NEW delayed leave");
        drop(state);
        let cancels_after = cancels_of(
            &server.server().received_requests().await.unwrap_or_default(),
            &delay_id_before,
        );
        assert_eq!(cancels_after, cancels_before + 1, "the stale delayed leave was cancelled");
    }
}

#[cfg(test)]
mod key_round_offender_tests {
    use std::collections::BTreeMap;

    use matrix_sdk_base::crypto::{OlmError, SessionRecipientCollectionError};
    use ruma::{device_id, owned_device_id, owned_user_id, user_id};

    use super::*;

    fn collection_error(inner: SessionRecipientCollectionError) -> crate::Error {
        OlmError::SessionRecipientCollectionError(inner).into()
    }

    /// Pre-merge review 2026-09-08, SDK P1: under `ErrorOnVerifiedUserProblem`
    /// one unsigned device of one verified peer failed the whole key round,
    /// so every healthy peer stayed keyless. The offenders are now named…
    #[test]
    fn an_unsigned_device_of_a_verified_user_names_that_device() {
        let error =
            collection_error(SessionRecipientCollectionError::VerifiedUserHasUnsignedDevice(
                BTreeMap::from([(owned_user_id!("@bob:hs"), vec![owned_device_id!("BAD")])]),
            ));
        let problem = verified_user_problem_devices(&error).expect("a verified-user problem");
        assert!(problem.covers(user_id!("@bob:hs"), device_id!("BAD")));
        assert!(
            !problem.covers(user_id!("@bob:hs"), device_id!("GOOD")),
            "bob's signed device stays"
        );
        assert!(
            !problem.covers(user_id!("@carol:hs"), device_id!("BAD")),
            "another user is untouched"
        );
    }

    /// …and a changed identity covers every device of that user.
    #[test]
    fn a_changed_identity_names_every_device_of_that_user() {
        let error =
            collection_error(SessionRecipientCollectionError::VerifiedUserChangedIdentity(vec![
                owned_user_id!("@bob:hs"),
            ]));
        let problem = verified_user_problem_devices(&error).expect("a verified-user problem");
        assert!(problem.covers(user_id!("@bob:hs"), device_id!("ANY")));
        assert!(!problem.covers(user_id!("@carol:hs"), device_id!("ANY")));
    }

    /// Anything else still fails the round as before — the retry is only for
    /// the strict strategy's refusals, never for transport or store errors.
    #[test]
    fn other_errors_are_not_a_verified_user_problem() {
        let error = collection_error(SessionRecipientCollectionError::CrossSigningNotSetup);
        assert!(verified_user_problem_devices(&error).is_none());
        let error: crate::Error =
            crate::Error::UnknownError(Box::new(std::io::Error::other("net")));
        assert!(verified_user_problem_devices(&error).is_none());
    }
}
