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

//! Membership state events for a MatrixRTC session.
//!
//! The wire shape implemented here is the one currently produced and consumed
//! by Element Call in its `Compatibility` mode: a per-device state event of
//! type [`CALL_MEMBER_EVENT_TYPE`] whose state key encodes the sending device.
//!
//! Reading and writing are deliberately asymmetric:
//!
//! * READING goes through ruma's `CallMemberEventContent` — the official
//!   MSC3401 model, which knows both the legacy `memberships` array and the
//!   one-event-per-device session shape, and which already drops expired
//!   memberships. A hand-rolled parser here was a second, divergent source of
//!   truth for "who is in the call".
//! * WRITING stays hand-serialised, because interoperability is defined by the
//!   bytes on the wire: ruma's serialisation omits `membershipID` and
//!   `m.call.intent`, both of which Element Call reads. The pin test in this
//!   file is what keeps that shape honest.

use std::time::Duration;

use ruma::{
    DeviceId, MilliSecondsSinceUnixEpoch, OwnedDeviceId, OwnedUserId, UInt, UserId,
    events::{call::member::CallMemberEventContent, rtc::notification::CallIntent},
};
use serde::{Deserialize, Serialize};

/// Event type of a MatrixRTC membership state event.
pub const CALL_MEMBER_EVENT_TYPE: &str = "org.matrix.msc3401.call.member";

/// The `application` a MatrixRTC session belongs to.
pub const CALL_APPLICATION: &str = "m.call";

/// The `scope` used for a room-wide call.
pub const ROOM_SCOPE: &str = "m.room";

/// Default membership lifetime, matching Element Call's default of four hours.
pub const DEFAULT_MEMBERSHIP_EXPIRY: Duration = Duration::from_secs(4 * 60 * 60);

/// Transport type identifier for a LiveKit SFU.
pub const LIVEKIT_TRANSPORT_TYPE: &str = "livekit";

/// A LiveKit transport as advertised inside `foci_preferred`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LiveKitFocus {
    /// Always `livekit`.
    #[serde(rename = "type")]
    pub focus_type: String,

    /// Base URL of the MatrixRTC authorisation service.
    pub livekit_service_url: String,

    /// The SFU room alias. Element Call and the Spiral agent runtime both use
    /// the Matrix room ID here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub livekit_alias: Option<String>,
}

impl LiveKitFocus {
    /// Build a LiveKit focus for the given service URL and alias.
    pub fn new(service_url: impl Into<String>, alias: Option<String>) -> Self {
        Self {
            focus_type: LIVEKIT_TRANSPORT_TYPE.to_owned(),
            livekit_service_url: service_url.into(),
            livekit_alias: alias,
        }
    }
}

/// How the active focus is selected among all advertised foci.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActiveFocus {
    /// Always `livekit` for the transports we support.
    #[serde(rename = "type")]
    pub focus_type: String,

    /// `oldest_membership` for the legacy single-SFU mode, `multi_sfu` when
    /// every member connects to their own transport.
    pub focus_selection: String,
}

impl ActiveFocus {
    /// The legacy selection, where everyone follows the oldest membership.
    pub fn oldest_membership() -> Self {
        Self {
            focus_type: LIVEKIT_TRANSPORT_TYPE.to_owned(),
            focus_selection: "oldest_membership".to_owned(),
        }
    }
}

/// The content of a MatrixRTC membership state event.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RtcMembershipContent {
    /// Always [`CALL_APPLICATION`] for calls.
    pub application: String,

    /// The call identifier. Empty for room-wide calls.
    pub call_id: String,

    /// Always [`ROOM_SCOPE`] for room-wide calls.
    pub scope: String,

    /// The device this membership belongs to.
    pub device_id: OwnedDeviceId,

    /// Stable identifier of this membership, `{user_id}:{device_id}`.
    #[serde(rename = "membershipID")]
    pub membership_id: String,

    /// Lifetime of the membership in milliseconds.
    pub expires: UInt,

    /// How the active focus is picked.
    pub focus_active: ActiveFocus,

    /// Transports this member is reachable on.
    pub foci_preferred: Vec<LiveKitFocus>,

    /// When the membership was first created. Only sent on refreshes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_ts: Option<MilliSecondsSinceUnixEpoch>,

    /// Soft indication of whether this participant joined for audio or video.
    #[serde(rename = "m.call.intent", default, skip_serializing_if = "Option::is_none")]
    pub call_intent: Option<CallIntent>,
}

impl RtcMembershipContent {
    /// Build the content Element Call would send for a joining member.
    pub fn new(
        user_id: &UserId,
        device_id: &DeviceId,
        focus_active: ActiveFocus,
        foci_preferred: Vec<LiveKitFocus>,
        expires: Duration,
        created_ts: Option<MilliSecondsSinceUnixEpoch>,
        call_intent: Option<CallIntent>,
    ) -> Self {
        Self {
            application: CALL_APPLICATION.to_owned(),
            call_id: String::new(),
            scope: ROOM_SCOPE.to_owned(),
            device_id: device_id.to_owned(),
            membership_id: membership_id(user_id, device_id),
            expires: duration_to_millis(expires),
            focus_active,
            foci_preferred,
            created_ts,
            call_intent,
        }
    }
}

/// The stable identifier of a membership, also used as the LiveKit identity on
/// the legacy authorisation path.
pub fn membership_id(user_id: &UserId, device_id: &DeviceId) -> String {
    format!("{user_id}:{device_id}")
}

/// The state key of a per-device membership event.
///
/// Element Call builds `_{user_id}_{device_id}_{application}`; the leading
/// underscore is only dropped on room versions that allow user-scoped state
/// keys with arbitrary suffixes (MSC3757), which we do not rely on.
pub fn membership_state_key(user_id: &UserId, device_id: &DeviceId) -> String {
    format!("_{user_id}_{device_id}_{CALL_APPLICATION}")
}

/// A membership of another (or our own) device in the session.
///
/// Parsed with ruma's official MSC3401 types (see [`parse_membership`]), so
/// this carries only what the session actually consumes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RtcMembership {
    /// The owner of the membership.
    pub user_id: OwnedUserId,

    /// The device that published the membership.
    pub device_id: OwnedDeviceId,

    /// When this membership was (re)published: `created_ts` from the content,
    /// or the event's `origin_server_ts` when absent — the same resolution
    /// peers use for the expiry. A renewal or a fast rejoin by the same
    /// device carries a NEW value while the identity stays the same, which is
    /// how the session tells "still here" from "here again".
    pub created_ts: MilliSecondsSinceUnixEpoch,
}

impl RtcMembership {
    /// The LiveKit participant identity of this member on the legacy
    /// authorisation path.
    pub fn livekit_identity(&self) -> String {
        membership_id(&self.user_id, &self.device_id)
    }
}

/// Parse the ACTIVE memberships out of a call member state event.
///
/// Reading is delegated to ruma's `CallMemberEventContent`: it models both
/// MSC3401 shapes (the legacy `memberships` array and the one-per-device
/// session content), and `active_memberships` already drops the expired ones
/// against `created_ts` — or `origin_server_ts` as the fallback the MSC
/// prescribes. Hand-rolling that parse meant carrying a second, divergent
/// definition of "who is in the call" next to the one `matrix-sdk-base` uses.
///
/// Writing stays ours ([`RtcMembershipContent`]): ruma's serialisation drops
/// `membershipID` and `m.call.intent`, which Element Call reads.
///
/// Returns nothing for leave events (empty content), for non-call
/// applications, and for events whose state key does not belong to the sender
/// — which is what stops a member publishing a membership on somebody else's
/// behalf.
pub fn parse_membership(
    state_key: &str,
    sender: &UserId,
    origin_server_ts: MilliSecondsSinceUnixEpoch,
    content: &serde_json::value::RawValue,
) -> Vec<RtcMembership> {
    let Ok(content) = serde_json::from_str::<CallMemberEventContent>(content.get()) else {
        return Vec::new();
    };

    content
        .active_memberships(Some(origin_server_ts))
        .into_iter()
        .filter(|membership| membership.is_room_call())
        .filter_map(|membership| {
            let device_id = membership.device_id().to_owned();
            // The state key is the sender's own `@user:host_DEVICE`; anything
            // else is a membership published on another device's behalf.
            if state_key != membership_state_key(sender, &device_id) {
                return None;
            }

            // A peer's advertised `foci_preferred` is deliberately not kept:
            // this client discovers its own transport (`.well-known`,
            // MSC4143) and never dials a peer's focus.
            Some(RtcMembership {
                user_id: sender.to_owned(),
                device_id,
                created_ts: membership.created_ts().unwrap_or(origin_server_ts),
            })
        })
        .collect()
}

fn duration_to_millis(duration: Duration) -> UInt {
    UInt::new_saturating(duration.as_millis().try_into().unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use ruma::{device_id, user_id};
    use serde_json::json;

    use super::*;

    #[test]
    fn membership_state_key_matches_element_call() {
        assert_eq!(
            membership_state_key(user_id!("@alice:example.org"), device_id!("DEVICEID")),
            "_@alice:example.org_DEVICEID_m.call"
        );
    }

    #[test]
    fn membership_content_serialises_to_the_element_call_shape() {
        let content = RtcMembershipContent::new(
            user_id!("@alice:example.org"),
            device_id!("DEVICEID"),
            ActiveFocus::oldest_membership(),
            vec![LiveKitFocus::new(
                "https://rtc.example.org/livekit/jwt",
                Some("!room:example.org".to_owned()),
            )],
            DEFAULT_MEMBERSHIP_EXPIRY,
            None,
            Some(CallIntent::Audio),
        );

        assert_eq!(
            serde_json::to_value(&content).unwrap(),
            json!({
                "application": "m.call",
                "call_id": "",
                "scope": "m.room",
                "device_id": "DEVICEID",
                "membershipID": "@alice:example.org:DEVICEID",
                "expires": 14_400_000,
                "m.call.intent": "audio",
                "focus_active": { "type": "livekit", "focus_selection": "oldest_membership" },
                "foci_preferred": [{
                    "type": "livekit",
                    "livekit_service_url": "https://rtc.example.org/livekit/jwt",
                    "livekit_alias": "!room:example.org",
                }],
            })
        );
    }

    #[test]
    fn parse_membership_rejects_a_spoofed_state_key() {
        let content = serde_json::value::to_raw_value(&json!({
            "application": "m.call",
            "call_id": "",
            "scope": "m.room",
            "device_id": "DEVICEID",
            "membershipID": "@alice:example.org:DEVICEID",
            "expires": 14_400_000,
            "focus_active": { "type": "livekit", "focus_selection": "oldest_membership" },
            "foci_preferred": [],
        }))
        .unwrap();

        // Bob replaying Alice's membership under his own state key is dropped.
        assert!(
            parse_membership(
                "_@alice:example.org_DEVICEID_m.call",
                user_id!("@bob:example.org"),
                MilliSecondsSinceUnixEpoch::now(),
                &content,
            )
            .is_empty()
        );

        let memberships = parse_membership(
            "_@alice:example.org_DEVICEID_m.call",
            user_id!("@alice:example.org"),
            MilliSecondsSinceUnixEpoch::now(),
            &content,
        );
        assert_eq!(memberships.len(), 1);
        assert_eq!(memberships[0].livekit_identity(), "@alice:example.org:DEVICEID");
    }

    #[test]
    fn parse_membership_ignores_leave_events() {
        let content = serde_json::value::to_raw_value(&json!({})).unwrap();
        assert!(
            parse_membership(
                "_@alice:example.org_DEVICEID_m.call",
                user_id!("@alice:example.org"),
                MilliSecondsSinceUnixEpoch::now(),
                &content,
            )
            .is_empty()
        );
    }

    /// Expiry is ruma's job now, so pin the BEHAVIOUR rather than our old
    /// arithmetic: a membership whose lifetime has elapsed must not be
    /// returned, or a client that died without leaving would hold a place in
    /// the call until somebody restarted the room.
    #[test]
    fn expired_memberships_are_not_returned() {
        fn membership_with(
            expires_ms: u64,
            created_ts: MilliSecondsSinceUnixEpoch,
        ) -> Vec<RtcMembership> {
            let content = serde_json::value::to_raw_value(&json!({
                "application": "m.call",
                "call_id": "",
                "scope": "m.room",
                "device_id": "DEVICEID",
                "membershipID": "@alice:example.org:DEVICEID",
                "expires": expires_ms,
                "created_ts": created_ts,
                "focus_active": { "type": "livekit", "focus_selection": "oldest_membership" },
                "foci_preferred": [],
            }))
            .unwrap();

            parse_membership(
                "_@alice:example.org_DEVICEID_m.call",
                user_id!("@alice:example.org"),
                MilliSecondsSinceUnixEpoch::now(),
                &content,
            )
        }

        let now: u64 = MilliSecondsSinceUnixEpoch::now().get().into();
        let long_ago = MilliSecondsSinceUnixEpoch(UInt::new_saturating(now.saturating_sub(60_000)));

        // Created a minute ago with a one-second lifetime: gone.
        assert!(membership_with(1_000, long_ago).is_empty());
        // Created a minute ago with the default four-hour lifetime: live.
        assert_eq!(membership_with(14_400_000, long_ago).len(), 1);
    }
}
