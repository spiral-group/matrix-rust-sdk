// Copyright 2026 the Spiral authors.
//
// SPDX-License-Identifier: Apache-2.0
//
// SPIRAL (Brief 0066, ADR-0007): the three FFI additions an Interactive Post
// needs and upstream does not expose.
//
// 1. The timeline must let the post through `default_event_filter`, which
//    drops an `m.room.message` whose `msgtype` it does not know. An
//    Interactive Post IS a message — the same kind of thing as a photo — so
//    it travels as `m.room.message` with `msgtype: com.spiral.app` and a
//    `body` that every other client can render. The ACTION type is a custom
//    event and deliberately NOT let through: actions are read via relations,
//    never rendered as rows, and upstream's filter drops them for free.
// 2. A client must be able to collect every action related to a post no
//    matter how far back the post is. `Room::load_or_fetch_event_with_relations`
//    exists in `matrix-sdk` but has no UniFFI surface; this file exports it,
//    returning raw (decrypted) event JSON — the same shape the client already
//    parses for `com.spiral.*` from `debugInfo.originalJSON`.
// 3. A client must learn, live, that an action has arrived. Because (1) keeps
//    actions out of the timeline, no `TimelineDiff` is ever produced for one,
//    so there is nothing to observe up there. `subscribe_to_events_of_type`
//    listens to the ROOM EVENT CACHE instead, which sits below the timeline's
//    filter and above decryption.
//
// Why the event cache and not the timeline: the cache is populated by
// `response_processors::timeline`, which collects every event with no type
// filtering at all, and stores it already decrypted. The timeline's filter
// runs several frames later, on a consumer of that same cache. Subscribing
// underneath it therefore sees what the timeline is not allowed to show.
//
// The property that decided it: the cache RE-EMITS an event once a late room
// key arrives (`event_cache::redecryptor` replaces the UTD and broadcasts a
// `Set` diff with `EventsOrigin::Cache`). An action that could not be
// decrypted on arrival is therefore not lost, which a timeline-based signal
// could never offer — the filter discards it before anything can notice.
//
// The fold over actions (LWW per path, tiebreak) lives in the client, not
// here: `matrix-sdk-ui`'s `AggregationKind` is a closed enum and this fork
// stays upstream-shaped in the timeline (Brief 0063 posture).

use std::{collections::HashMap, sync::Arc};

use eyeball_im::VectorDiff;
use matrix_sdk::{
    QueueWedgeError as SdkQueueWedgeError,
    encryption::LocalTrust,
    event_cache::{EventsOrigin, RoomEventCache, RoomEventCacheUpdate, TimelineVectorDiffs},
    ruma::{
        EventId, OwnedDeviceId, UserId,
        events::{AnyMessageLikeEventContent, AnySyncTimelineEvent, relation::RelationType},
        room_version_rules::RoomVersionRules,
    },
};
use matrix_sdk_base::event_cache::Event as CachedEvent;
use matrix_sdk_common::{SendOutsideWasm, SyncOutsideWasm};
use tokio::sync::broadcast::error::RecvError;
use tracing::{error, warn};

use crate::{
    client::Client,
    error::{ClientError, QueueWedgeError},
    room::Room,
    runtime::get_runtime_handle,
    task_handle::TaskHandle,
};

/// The Interactive Post message type. Rendered inline as a timeline row.
///
/// A `msgtype` rather than an event type, so a post is a MESSAGE: the room
/// list, push rules and every other client's fallback rendering all work on
/// `m.room.message` without being taught anything. Only the timeline filter
/// has to be told, because it allows a closed set of msgtypes.
pub(crate) const APP_MSGTYPE: &str = "com.spiral.app";
/// A generated Interactive Post the owner has not published yet: the CODE,
/// as an ordinary message in the owner's DM thread with the agent (Brief
/// 0066 step 6). A message rather than its own event type so it travels
/// the same path a post does — filter, timeline item, row — and so a
/// client that does not know it still shows its `body`.
pub(crate) const DRAFT_MSGTYPE: &str = "com.spiral.app.draft";
/// The agent's «generating» marker (notebook § 5.71): sent into the thread
/// the moment the model is asked for a draft, shown by the client as a card
/// where the draft will land. Only the filter needs to know it — it reaches
/// the client as `MessageType::Other` with its `body`.
pub(crate) const GENERATING_MSGTYPE: &str = "com.spiral.app.generating";
/// A client-rendered notice (Brief 0073): the server sends the facts under
/// the `com.spiral.notice` key, the client the words in the reader's
/// language. Reaches the client as `MessageType::Other` with its `data`.
pub(crate) const NOTICE_MSGTYPE: &str = "com.spiral.notice";

/// Timeline filter for the Spiral fork: upstream's default plus the
/// Interactive Post types and the agent-tools fallback
/// (`spiral_agents::is_agent_tools_fallback`). Compose, never patch
/// `default_event_filter`.
///
/// Both timeline-builder arms the client uses (`All` and `EventFilter`)
/// must go through this — the client's live room timeline arrives via
/// `EventFilter`, and a post that only passes `All` never shows in chat
/// (device day run 4, 2026-09-13, for the session events).
pub(crate) fn spiral_event_filter(event: &AnySyncTimelineEvent, rules: &RoomVersionRules) -> bool {
    matrix_sdk_ui::timeline::default_event_filter(event, rules)
        || is_spiral_app_post(event)
        || crate::spiral_agents::is_agent_tools_fallback(event)
}

/// Whether this is an Interactive Post.
///
/// Reads the `msgtype` off the original content. A redacted post has no
/// content and is not one: there is nothing left to run.
fn is_spiral_app_post(event: &AnySyncTimelineEvent) -> bool {
    let AnySyncTimelineEvent::MessageLike(message_like) = event else {
        return false;
    };

    let Some(AnyMessageLikeEventContent::RoomMessage(content)) = message_like.original_content()
    else {
        return false;
    };

    let msgtype = content.msgtype.msgtype();
    msgtype == APP_MSGTYPE
        || msgtype == DRAFT_MSGTYPE
        || msgtype == GENERATING_MSGTYPE
        || msgtype == NOTICE_MSGTYPE
}

/// Relation type filter for [`Room::load_or_fetch_event_with_relations`].
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum RelationTypeFilter {
    /// `m.reference` — what Interactive Post actions use.
    Reference,
    /// `m.annotation` (reactions).
    Annotation,
    /// `m.replace` (edits).
    Replacement,
    /// `m.thread`.
    Thread,
}

impl From<RelationTypeFilter> for RelationType {
    fn from(filter: RelationTypeFilter) -> Self {
        match filter {
            RelationTypeFilter::Reference => RelationType::Reference,
            RelationTypeFilter::Annotation => RelationType::Annotation,
            RelationTypeFilter::Replacement => RelationType::Replacement,
            RelationTypeFilter::Thread => RelationType::Thread,
        }
    }
}

/// An event plus every event related to it, as raw (decrypted) JSON.
#[derive(Clone, Debug, uniffi::Record)]
pub struct EventWithRelations {
    /// The target event, as it would appear in `/sync` (no `room_id`).
    pub event_json: String,
    /// The related events, in the order the server / cache returned them.
    /// The caller must order by `origin_server_ts` before folding.
    pub relations_json: Vec<String>,
}

#[matrix_sdk_ffi_macros::export]
impl Room {
    /// Load `event_id` and every event that relates to it, from the event
    /// cache when possible and from the homeserver otherwise (paginating the
    /// `/relations` endpoint until exhausted).
    ///
    /// `filter` narrows the relations by type; `None` returns all of them.
    /// Both the target and the relations are returned decrypted, as raw
    /// event JSON, so a caller can read custom content the typed FFI
    /// `TimelineEvent` cannot represent.
    pub async fn load_or_fetch_event_with_relations(
        &self,
        event_id: String,
        filter: Option<RelationTypeFilter>,
    ) -> Result<EventWithRelations, ClientError> {
        let event_id = EventId::parse(event_id)?;
        let filter = filter.map(|f| vec![RelationType::from(f)]);

        let (event, relations) =
            self.inner.load_or_fetch_event_with_relations(&event_id, filter, None).await?;

        let event_json = event.into_raw().json().get().to_owned();
        let relations_json =
            relations.into_iter().map(|ev| ev.into_raw().json().get().to_owned()).collect();

        Ok(EventWithRelations { event_json, relations_json })
    }

    /// A live view of this room's event cache, for observing event types the
    /// timeline filter drops.
    ///
    /// Holding the returned observer — and the [`TaskHandle`] its `subscribe`
    /// hands back — keeps this room's events loaded in memory: the cache only
    /// shrinks itself when it has no subscribers. That is deliberate rather
    /// than incidental, because a re-decrypted event is only broadcast if it
    /// is still in memory. Subscribe when a room is opened, drop when it
    /// closes; do NOT hold one for every room at once.
    pub async fn spiral_event_observer(&self) -> Result<Arc<SpiralEventObserver>, ClientError> {
        let (cache, _drop_handles) = self.inner.event_cache().await?;
        Ok(Arc::new(SpiralEventObserver { cache }))
    }
}

/// Why a raw send (`send_raw_returning_event_id`) was refused.
///
/// A raw send bypasses the send queue, so a failure that the queue would
/// park as a wedged item — and the client would offer to resolve — arrived
/// here as an opaque `ClientError::Generic` whose only trace of the reason
/// was a `Debug` string. The three encryption refusals are now the same
/// classification the send queue makes (`QueueWedgeError::from(&Error)`),
/// so a client can resolve them the same way: ignore the unsigned devices,
/// or withdraw verification of a changed identity, then send again.
#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum RawSendError {
    /// A verified user — the sender included — has devices not signed by
    /// their identity. User ID → device IDs.
    #[error("a verified user has devices they did not sign")]
    InsecureDevices { user_device_map: HashMap<String, Vec<String>> },
    /// A user this device had verified has a new identity.
    #[error("a previously verified user has a new identity")]
    IdentityViolations { users: Vec<String> },
    /// This device is not cross-signed, or cross-signing is not set up.
    #[error("own verification is required")]
    CrossVerificationRequired,
    /// Anything else: the request, the network, the content.
    #[error("{msg}")]
    Generic { msg: String, details: Option<String> },
}

impl From<matrix_sdk::Error> for RawSendError {
    fn from(error: matrix_sdk::Error) -> Self {
        match QueueWedgeError::from(SdkQueueWedgeError::from(&error)) {
            QueueWedgeError::InsecureDevices { user_device_map } => {
                Self::InsecureDevices { user_device_map }
            }
            QueueWedgeError::IdentityViolations { users } => Self::IdentityViolations { users },
            QueueWedgeError::CrossVerificationRequired => Self::CrossVerificationRequired,
            QueueWedgeError::MissingMediaContent
            | QueueWedgeError::InvalidMimeType { .. }
            | QueueWedgeError::GenericApiError { .. } => {
                // Every other failure as the FFI would have reported it —
                // an API error keeps its errcode in the message.
                let client_error = ClientError::from(error);
                let details = match &client_error {
                    ClientError::Generic { details, .. }
                    | ClientError::MatrixApi { details, .. } => details.clone(),
                    ClientError::ContentScanner { .. } => None,
                };
                Self::Generic { msg: client_error.to_string(), details }
            }
        }
    }
}

#[matrix_sdk_ffi_macros::export]
impl Room {
    /// `send_raw`, returning the event id the server assigned, and saying
    /// WHY when encryption refused the recipients.
    ///
    /// A caller that wants to START a thread on what it just sent needs
    /// the id — the thread's root is named by it (Brief 0066 step 6: the
    /// request for an Interactive Post is the root, the drafts are the
    /// replies). `send_raw` throws the id away; this one keeps it.
    pub async fn send_raw_returning_event_id(
        &self,
        event_type: String,
        content: String,
    ) -> Result<String, RawSendError> {
        let content_json: serde_json::Value =
            serde_json::from_str(&content).map_err(|e| RawSendError::Generic {
                msg: format!("Failed to parse JSON: {e}"),
                details: Some(format!("{e:?}")),
            })?;

        let sent = self.inner.send_raw(&event_type, content_json).await?;

        Ok(sent.response.event_id.to_string())
    }

    /// Set the local trust of the given devices to `LocalTrust::Ignored`, so
    /// the next send shares its room key with them despite their owner not
    /// having signed them — the user's answer to
    /// [`RawSendError::InsecureDevices`].
    ///
    /// `ignore_device_trust_and_resend` without the resend: a raw send has no
    /// send-queue item to resend, and the caller sends again itself.
    pub async fn ignore_device_trust(
        &self,
        devices: HashMap<String, Vec<String>>,
    ) -> Result<(), ClientError> {
        let encryption = self.inner.client().encryption();

        for (user_id, device_ids) in devices {
            let user_id = UserId::parse(user_id)?;

            for device_id in device_ids {
                let device_id: OwnedDeviceId = device_id.into();

                if let Some(device) = encryption.get_device(&user_id, &device_id).await? {
                    device.set_local_trust(LocalTrust::Ignored).await?;
                }
            }
        }

        Ok(())
    }

    /// Withdraw the verification of the given users, so a changed identity
    /// no longer blocks sending — the user's answer to
    /// [`RawSendError::IdentityViolations`].
    ///
    /// `withdraw_verification_and_resend` without the resend, for the same
    /// reason as [`Self::ignore_device_trust`].
    pub async fn withdraw_verification(&self, user_ids: Vec<String>) -> Result<(), ClientError> {
        let encryption = self.inner.client().encryption();

        for user_id in user_ids {
            let user_id = UserId::parse(user_id)?;

            if let Some(identity) = encryption.get_user_identity(&user_id).await? {
                identity.withdraw_verification().await?;
            }
        }

        Ok(())
    }
}

/// Why an observed event was delivered.
#[derive(Clone, Copy, Debug, uniffi::Enum)]
pub enum SpiralEventOrigin {
    /// It arrived in a `/sync` response.
    Sync,
    /// It arrived through back-pagination.
    Pagination,
    /// The cache produced it rather than the network: the initial snapshot, or
    /// an event that has just been re-decrypted because its room key finally
    /// arrived. Either way the caller may already hold this `event_id` and
    /// must treat the delivery as an upsert, not as a new event.
    Cache,
}

impl From<EventsOrigin> for SpiralEventOrigin {
    fn from(origin: EventsOrigin) -> Self {
        match origin {
            EventsOrigin::Sync => Self::Sync,
            EventsOrigin::Pagination => Self::Pagination,
            EventsOrigin::Cache => Self::Cache,
        }
    }
}

/// One observed event, as raw (decrypted) JSON.
#[derive(Clone, Debug, uniffi::Record)]
pub struct SpiralObservedEvent {
    /// Stable key. The same event may be delivered more than once — on
    /// re-decryption, on a duplicate sync, after a lag reset — so callers
    /// upsert on this rather than appending.
    pub event_id: String,
    /// The `type` field, e.g. `com.spiral.app.action`.
    pub event_type: String,
    /// The event as it would appear in `/sync` (no `room_id`), decrypted
    /// unless `is_utd`.
    pub event_json: String,
    /// `origin_server_ts`. Deliveries are NOT ordered by it, so a caller that
    /// folds these must sort first.
    pub timestamp: Option<u64>,
    pub origin: SpiralEventOrigin,
    /// True while this event is still an undecryptable `m.room.encrypted`.
    /// The SDK will deliver it again, decrypted, if the key arrives — as long
    /// as the subscription is still alive.
    pub is_utd: bool,
}

/// Receives events of the requested types as they reach the room.
#[matrix_sdk_ffi_macros::export(callback_interface)]
pub trait SpiralEventListener: SendOutsideWasm + SyncOutsideWasm {
    /// Called with a batch of matching events. May repeat an `event_id`
    /// already delivered; see [`SpiralObservedEvent::event_id`].
    fn on_events(&self, events: Vec<SpiralObservedEvent>);
}

/// A type-filtered view over one room's event cache.
#[derive(uniffi::Object)]
pub struct SpiralEventObserver {
    cache: RoomEventCache,
}

#[matrix_sdk_ffi_macros::export]
impl SpiralEventObserver {
    /// Deliver every cached event whose `type` is in `event_types`, then every
    /// later one, until the returned handle is dropped.
    ///
    /// Matching is exact, not a prefix: pass every type you want.
    pub async fn subscribe(
        &self,
        event_types: Vec<String>,
        listener: Box<dyn SpiralEventListener>,
    ) -> Result<Arc<TaskHandle>, ClientError> {
        // `subscribe` hands back the snapshot and the stream under one lock,
        // so an event cannot slip between them.
        let (initial, mut subscriber) = self.cache.subscribe().await?;

        // The snapshot goes out BEFORE the task is spawned. Upstream learned
        // this the hard way in `Timeline::add_listener` (#3535): spawn first
        // and a listener can be handed an update for state it has not been
        // given yet.
        let initial = matching_events(&initial, &event_types, SpiralEventOrigin::Cache);
        if !initial.is_empty() {
            listener.on_events(initial);
        }

        let cache = self.cache.clone();

        Ok(Arc::new(TaskHandle::new(get_runtime_handle().spawn(async move {
            loop {
                match subscriber.recv().await {
                    Ok(RoomEventCacheUpdate::UpdateTimelineEvents(TimelineVectorDiffs {
                        diffs,
                        origin,
                    })) => {
                        let events = events_from_diffs(diffs);
                        let batch = matching_events(&events, &event_types, origin.into());
                        if !batch.is_empty() {
                            listener.on_events(batch);
                        }
                    }

                    // Read markers, membership and ephemeral events carry no
                    // timeline event for us to match on.
                    Ok(_) => {}

                    Err(RecvError::Lagged(skipped)) => {
                        // The channel holds 32 updates. Falling behind loses
                        // events outright, so re-read the cache rather than
                        // carry on with a hole.
                        warn!(skipped, "lagged behind the room event cache; re-reading it");

                        match cache.events().await {
                            Ok(events) => {
                                let batch = matching_events(
                                    &events,
                                    &event_types,
                                    SpiralEventOrigin::Cache,
                                );
                                if !batch.is_empty() {
                                    listener.on_events(batch);
                                }
                            }
                            Err(error) => {
                                error!(
                                    ?error,
                                    "could not re-read the room event cache after a lag"
                                );
                                break;
                            }
                        }
                    }

                    Err(RecvError::Closed) => break,
                }
            }
        }))))
    }
}

/// Pull every event a batch of diffs carries.
///
/// `Set` matters as much as the additions: it is how the cache announces that
/// an event it already held has changed — in practice, that a UTD has just
/// been decrypted. Dropping it would silently lose exactly the events this
/// subscription exists to recover. The removal-shaped diffs carry no event.
fn events_from_diffs(diffs: Vec<VectorDiff<CachedEvent>>) -> Vec<CachedEvent> {
    let mut events = Vec::new();

    for diff in diffs {
        match diff {
            VectorDiff::Append { values } | VectorDiff::Reset { values } => events.extend(values),
            VectorDiff::PushBack { value }
            | VectorDiff::PushFront { value }
            | VectorDiff::Insert { value, .. }
            | VectorDiff::Set { value, .. } => events.push(value),
            VectorDiff::Clear
            | VectorDiff::PopBack
            | VectorDiff::PopFront
            | VectorDiff::Remove { .. }
            | VectorDiff::Truncate { .. } => {}
        }
    }

    events
}

/// Keep the events whose type was asked for, as the FFI record.
///
/// An event with no id is dropped: the caller's only way to deduplicate a
/// re-delivery is the id, so one without it cannot be handled safely.
fn matching_events(
    events: &[CachedEvent],
    event_types: &[String],
    origin: SpiralEventOrigin,
) -> Vec<SpiralObservedEvent> {
    events
        .iter()
        .filter_map(|event| {
            let event_type = event.kind.event_type()?;
            if !event_types.iter().any(|wanted| wanted == &event_type) {
                return None;
            }

            Some(SpiralObservedEvent {
                event_id: event.event_id()?.to_string(),
                event_type,
                event_json: event.raw().json().get().to_owned(),
                timestamp: event.timestamp().map(|ts| ts.0.into()),
                origin,
                is_utd: event.kind.is_utd(),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use eyeball_im::VectorDiff;
    use matrix_sdk::ruma::{
        events::AnySyncTimelineEvent, room_version_rules::RoomVersionRules, serde::Raw,
    };
    use matrix_sdk_base::event_cache::Event as CachedEvent;

    use std::collections::BTreeMap;

    use matrix_sdk::{
        encryption::OlmError,
        ruma::{owned_device_id, owned_user_id},
    };
    use matrix_sdk_base::crypto::SessionRecipientCollectionError;

    use super::{
        APP_MSGTYPE, GENERATING_MSGTYPE, NOTICE_MSGTYPE, RawSendError, SpiralEventOrigin,
        events_from_diffs, matching_events, spiral_event_filter,
    };

    const ACTION_EVENT_TYPE: &str = "com.spiral.app.action";

    fn cached(event_type: &str, event_id: &str) -> CachedEvent {
        CachedEvent::from_plaintext(
            Raw::new(&serde_json::json!({
                "type": event_type,
                "event_id": event_id,
                "sender": "@alice:spiral.md",
                "origin_server_ts": 1_700_000_000_000u64,
                "content": { "seq": 1 }
            }))
            .expect("valid raw event")
            .cast_unchecked(),
        )
    }

    fn wanted() -> Vec<String> {
        vec![ACTION_EVENT_TYPE.to_owned()]
    }

    fn raw_event(event_type: &str) -> AnySyncTimelineEvent {
        serde_json::from_value::<Raw<AnySyncTimelineEvent>>(serde_json::json!({
            "type": event_type,
            "event_id": "$event_1",
            "sender": "@agent:spiral.md",
            "origin_server_ts": 1_700_000_000_000u64,
            "content": { "app_id": "poll", "sha256": "abc" }
        }))
        .expect("valid raw event")
        .deserialize()
        .expect("deserialises")
    }

    /// A post as it travels: a message, with a body every other client can
    /// render and the artifact's details beside it.
    fn raw_message(msgtype: &str) -> AnySyncTimelineEvent {
        serde_json::from_value::<Raw<AnySyncTimelineEvent>>(serde_json::json!({
            "type": "m.room.message",
            "event_id": "$event_1",
            "sender": "@agent:spiral.md",
            "origin_server_ts": 1_700_000_000_000u64,
            "content": {
                "msgtype": msgtype,
                "body": "Where to?",
                "app_id": "poll",
                "sha256": "abc"
            }
        }))
        .expect("valid raw event")
        .deserialize()
        .expect("deserialises")
    }

    #[test]
    fn filter_should_pass_app_post_msgtype() {
        let rules = RoomVersionRules::V11;
        assert!(spiral_event_filter(&raw_message(APP_MSGTYPE), &rules));
    }

    #[test]
    fn filter_should_pass_the_generating_marker() {
        let rules = RoomVersionRules::V11;
        assert!(spiral_event_filter(&raw_message(GENERATING_MSGTYPE), &rules));
    }

    #[test]
    fn filter_should_pass_a_notice() {
        let rules = RoomVersionRules::V11;
        assert!(spiral_event_filter(&raw_message(NOTICE_MSGTYPE), &rules));
    }

    #[test]
    fn filter_should_drop_other_unknown_msgtypes() {
        // The arm is for OUR msgtype, not for unknown ones in general:
        // otherwise anyone's private message shape would render as a post.
        let rules = RoomVersionRules::V11;
        assert!(!spiral_event_filter(&raw_message("com.example.unknown"), &rules));
    }

    #[test]
    fn filter_should_drop_a_post_shaped_custom_event() {
        // The old shape. A post is a message now; a custom event carrying the
        // same fields is not one, and must not sneak in through the old door.
        let rules = RoomVersionRules::V11;
        assert!(!spiral_event_filter(&raw_event("com.spiral.app"), &rules));
    }

    #[test]
    fn filter_should_drop_action_type() {
        let rules = RoomVersionRules::V11;
        assert!(!spiral_event_filter(&raw_event("com.spiral.app.action"), &rules));
    }

    #[test]
    fn filter_should_drop_other_unknown_types() {
        let rules = RoomVersionRules::V11;
        assert!(!spiral_event_filter(&raw_event("com.example.unknown"), &rules));
    }

    /// The agent-tools fallback (Brief 0066 § 5.89): without it a room with
    /// no grant shows the owner nothing when their agent's tools change.
    #[test]
    fn filter_should_pass_the_agent_tools_fallback() {
        let rules = RoomVersionRules::V11;
        assert!(spiral_event_filter(&raw_event("com.spiral.tools.changed"), &rules));
    }

    /// Exact type, not a family: the state row's own type as a message, or a
    /// longer name, is somebody else's event.
    #[test]
    fn filter_should_drop_types_next_to_the_fallback() {
        let rules = RoomVersionRules::V11;
        assert!(!spiral_event_filter(&raw_event("com.spiral.tools"), &rules));
        assert!(!spiral_event_filter(&raw_event("com.spiral.tools.changed.v2"), &rules));
    }

    #[test]
    fn filter_should_still_pass_ordinary_messages() {
        let rules = RoomVersionRules::V11;
        let event = serde_json::from_value::<Raw<AnySyncTimelineEvent>>(serde_json::json!({
            "type": "m.room.message",
            "event_id": "$event_2",
            "sender": "@alice:spiral.md",
            "origin_server_ts": 1_700_000_000_000u64,
            "content": { "msgtype": "m.text", "body": "hi" }
        }))
        .expect("valid raw event")
        .deserialize()
        .expect("deserialises");
        assert!(spiral_event_filter(&event, &rules));
    }

    #[test]
    fn observer_should_keep_only_the_requested_types() {
        let events = [cached(ACTION_EVENT_TYPE, "$a"), cached("m.room.message", "$b")];

        let matched = matching_events(&events, &wanted(), SpiralEventOrigin::Sync);

        assert_eq!(matched.len(), 1);
        assert_eq!(matched[0].event_id, "$a");
        assert_eq!(matched[0].event_type, ACTION_EVENT_TYPE);
        assert!(!matched[0].is_utd);
        assert_eq!(matched[0].timestamp, Some(1_700_000_000_000));
    }

    #[test]
    fn observer_should_match_every_requested_type() {
        let events = [
            cached(ACTION_EVENT_TYPE, "$a"),
            cached("com.spiral.other", "$b"),
            cached("m.room.message", "$c"),
        ];
        let types = vec![ACTION_EVENT_TYPE.to_owned(), "com.spiral.other".to_owned()];

        let matched = matching_events(&events, &types, SpiralEventOrigin::Sync);

        assert_eq!(matched.len(), 2);
    }

    #[test]
    fn observer_should_match_the_type_exactly_not_by_prefix() {
        // A shorter type must not swallow a longer one that starts with it.
        // Asking for `com.spiral.app` must never hand back an action.
        let events = [cached(ACTION_EVENT_TYPE, "$a")];

        let matched =
            matching_events(&events, &["com.spiral.app".to_owned()], SpiralEventOrigin::Sync);

        assert!(matched.is_empty());
    }

    #[test]
    fn observer_should_drop_an_event_without_an_id() {
        // Deduplicating a re-delivery is only possible by id, so an event
        // without one cannot be handed over safely.
        let no_id = CachedEvent::from_plaintext(
            Raw::new(&serde_json::json!({
                "type": ACTION_EVENT_TYPE,
                "sender": "@alice:spiral.md",
                "origin_server_ts": 1_700_000_000_000u64,
                "content": {}
            }))
            .expect("valid raw event")
            .cast_unchecked(),
        );

        assert!(matching_events(&[no_id], &wanted(), SpiralEventOrigin::Sync).is_empty());
    }

    #[test]
    fn diffs_should_carry_a_replaced_event() {
        // `Set` is how the cache announces that an event it already held has
        // changed — in practice that a UTD has just been decrypted. Losing it
        // would defeat the whole point of subscribing here.
        let diffs = vec![VectorDiff::Set { index: 0, value: cached(ACTION_EVENT_TYPE, "$a") }];

        let events = events_from_diffs(diffs);

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_id().expect("has an id"), "$a");
    }

    #[test]
    fn diffs_should_carry_appended_and_reset_events() {
        let diffs = vec![
            VectorDiff::Append { values: vec![cached(ACTION_EVENT_TYPE, "$a")].into() },
            VectorDiff::PushBack { value: cached(ACTION_EVENT_TYPE, "$b") },
            VectorDiff::PushFront { value: cached(ACTION_EVENT_TYPE, "$c") },
            VectorDiff::Insert { index: 0, value: cached(ACTION_EVENT_TYPE, "$d") },
            VectorDiff::Reset { values: vec![cached(ACTION_EVENT_TYPE, "$e")].into() },
        ];

        assert_eq!(events_from_diffs(diffs).len(), 5);
    }

    #[test]
    fn diffs_that_only_remove_should_carry_nothing() {
        let diffs = vec![
            VectorDiff::Clear,
            VectorDiff::PopBack,
            VectorDiff::PopFront,
            VectorDiff::Remove { index: 0 },
            VectorDiff::Truncate { length: 0 },
        ];

        assert!(events_from_diffs(diffs).is_empty());
    }

    fn refused(reason: SessionRecipientCollectionError) -> matrix_sdk::Error {
        matrix_sdk::Error::OlmError(Box::new(OlmError::SessionRecipientCollectionError(reason)))
    }

    /// The prod finding of 2026-10-02: an unsigned device of the sender
    /// themselves reached the client only as `Debug` text. It must arrive as
    /// the devices to resolve, keyed the way `ignore_device_trust` takes them.
    #[test]
    fn an_unsigned_device_of_a_verified_user_is_named() {
        let user = owned_user_id!("@emm2:spiral.md");
        let mut map = BTreeMap::new();
        map.insert(user, vec![owned_device_id!("VERIFY0266")]);

        let error = RawSendError::from(refused(
            SessionRecipientCollectionError::VerifiedUserHasUnsignedDevice(map),
        ));

        let RawSendError::InsecureDevices { user_device_map } = error else {
            panic!("expected InsecureDevices, got {error:?}");
        };
        assert_eq!(user_device_map.get("@emm2:spiral.md"), Some(&vec!["VERIFY0266".to_owned()]));
    }

    #[test]
    fn a_changed_identity_names_its_users() {
        let error = RawSendError::from(refused(
            SessionRecipientCollectionError::VerifiedUserChangedIdentity(vec![owned_user_id!(
                "@assistant-emm2:spiral.md"
            )]),
        ));

        let RawSendError::IdentityViolations { users } = error else {
            panic!("expected IdentityViolations, got {error:?}");
        };
        assert_eq!(users, vec!["@assistant-emm2:spiral.md".to_owned()]);
    }

    #[test]
    fn own_verification_is_one_reason_whichever_way_it_is_missing() {
        for reason in [
            SessionRecipientCollectionError::CrossSigningNotSetup,
            SessionRecipientCollectionError::SendingFromUnverifiedDevice,
        ] {
            let error = RawSendError::from(refused(reason));
            assert!(
                matches!(error, RawSendError::CrossVerificationRequired),
                "expected CrossVerificationRequired, got {error:?}"
            );
        }
    }

    /// Every other failure stays a failure, never a panic across the FFI.
    #[test]
    fn anything_else_is_generic() {
        let error =
            RawSendError::from(matrix_sdk::Error::UnknownError("the network went away".into()));

        let RawSendError::Generic { msg, .. } = error else {
            panic!("expected Generic, got {error:?}");
        };
        assert!(msg.contains("the network went away"), "{msg}");
    }
}

/// An encrypted upload: where the ciphertext lives and how to read it.
///
/// `file_json` is the `EncryptedFile` (v2) as the event will carry it —
/// `url`, `key`, `iv`, `hashes`, `v` — so a caller puts it into a message's
/// `file` field as is, and any client that can read an encrypted image can
/// read this.
#[derive(uniffi::Record)]
pub struct EncryptedUpload {
    pub mxc_uri: String,
    pub file_json: String,
}

#[matrix_sdk_ffi_macros::export]
impl Client {
    /// Encrypt `data` with a fresh attachment key, upload the ciphertext,
    /// and return what a message needs to reference it.
    ///
    /// The plain `upload_media` sends bytes as they are, which is wrong for
    /// an encrypted room: every media event there carries an `EncryptedFile`
    /// and the media itself is opaque to the server. The SDK does this for
    /// its own attachment sends; this exposes the same step on its own, for
    /// a caller that builds the message content itself — an Interactive
    /// Post's artifact (Brief 0066 step 6), which is not an image, a video
    /// or a file in the SDK's sense.
    pub async fn upload_encrypted_media(
        &self,
        data: Vec<u8>,
    ) -> Result<EncryptedUpload, ClientError> {
        let mut cursor = std::io::Cursor::new(data);
        let file = self.inner.upload_encrypted_file(&mut cursor).await?;
        let mxc_uri = file.url.to_string();
        let file_json = serde_json::to_string(&file)
            .map_err(|e| ClientError::Generic { msg: e.to_string(), details: None })?;
        Ok(EncryptedUpload { mxc_uri, file_json })
    }
}
