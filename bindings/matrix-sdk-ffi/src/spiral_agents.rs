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

// Spiral (Brief 0066 § 5.88): the room grants an agent needs, made by the
// person who adds it — from their own client, at the moment they add it.
//
// An agent joins a person's room at the default power level, so the two
// state types it maintains about itself — `com.spiral.tools` (the tools it
// runs here) and `io.element.functional_members` (that it is a service
// member, not a person) — are refused at `state_default`. The platform used
// to fix that by writing `m.room.power_levels` AS the room's admin, through
// an application-service namespace over every local user: a change in a
// person's room that the person never made. The grant belongs to the
// person's own client, on the person's own action.
//
// Kept beside `spiral_apps.rs` (Brief 0063 posture: what this fork adds
// lives in modules of its own, never in upstream's files).

use matrix_sdk::ruma::events::{
    AnySyncTimelineEvent, StateEventType, TimelineEventType,
    room::power_levels::{RoomPowerLevels, RoomPowerLevelsEventContent},
};

use crate::{error::ClientError, room::Room};

/// What the platform sends INSTEAD of an agent's `com.spiral.tools` state
/// row when the room refuses it — a room whose owner's client made no grant
/// (an older client, or a room the owner does not administer). The same
/// content plus `fallback: true`, as a message-like event, in the clear.
pub(crate) const TOOLS_FALLBACK_EVENT_TYPE: &str = "com.spiral.tools.changed";

/// Whether this is the agent-tools fallback.
///
/// `default_event_filter` drops a custom message-like type like any type it
/// does not know, and then a client has nothing to read the fallback from:
/// no row, and the tools banner — which folds the room's timeline items —
/// never hears of it. Let through, it arrives as an ordinary timeline item
/// (`MsgLikeKind::Other`) carrying its raw JSON, like the state row does.
/// A redacted one has nothing left to say and is left to upstream.
pub(crate) fn is_agent_tools_fallback(event: &AnySyncTimelineEvent) -> bool {
    let AnySyncTimelineEvent::MessageLike(message_like) = event else {
        return false;
    };

    message_like.original_content().is_some()
        && message_like.event_type().to_string() == TOOLS_FALLBACK_EVENT_TYPE
}

/// Lower each of `event_types` to `users_default`, leaving every other
/// level as it is. Returns the types it changed — empty when the room
/// already allowed them all.
fn allow_at_default(power_levels: &mut RoomPowerLevels, event_types: &[String]) -> Vec<String> {
    let default_level = power_levels.users_default;
    let mut changed = Vec::new();
    for event_type in event_types {
        let key = TimelineEventType::from(event_type.as_str());
        let required = power_levels.events.get(&key).copied().unwrap_or(power_levels.state_default);
        if required > default_level {
            power_levels.events.insert(key, default_level);
            changed.push(event_type.clone());
        }
    }
    changed
}

#[matrix_sdk_ffi_macros::export]
impl Room {
    /// Let members at the default power level send these state types.
    ///
    /// Returns `true` when the room now allows every one of them at
    /// `users_default` — whether it already did, or this call changed it —
    /// and `false` when it does not and this user may not change
    /// `m.room.power_levels`. Never raises anything, never touches `users`.
    /// One state event, or none when nothing needs to change.
    pub async fn allow_state_types_at_default_level(
        &self,
        event_types: Vec<String>,
    ) -> Result<bool, ClientError> {
        let mut power_levels = self.inner.power_levels().await.map_err(ClientError::from_err)?;
        let may_edit = power_levels
            .user_can_send_state(self.inner.own_user_id(), StateEventType::RoomPowerLevels);

        if allow_at_default(&mut power_levels, &event_types).is_empty() {
            return Ok(true);
        }
        if !may_edit {
            return Ok(false);
        }

        let content = RoomPowerLevelsEventContent::try_from(power_levels)
            .map_err(|e| ClientError::Generic { msg: e.to_string(), details: None })?;
        self.inner.send_state_event(content).await?;

        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use matrix_sdk::ruma::{
        events::{
            TimelineEventType,
            room::power_levels::{RoomPowerLevels, RoomPowerLevelsSource},
        },
        int, owned_user_id,
        room_version_rules::AuthorizationRules,
    };

    use super::allow_at_default;

    fn room() -> RoomPowerLevels {
        RoomPowerLevels::new(
            RoomPowerLevelsSource::None,
            &AuthorizationRules::V1,
            [owned_user_id!("@owner:spiral.md")],
        )
    }

    const TYPES: [&str; 2] = ["com.spiral.tools", "io.element.functional_members"];

    fn types() -> Vec<String> {
        TYPES.iter().map(|t| (*t).to_owned()).collect()
    }

    /// A person's room: state at 50, members at 0 — both types granted.
    #[test]
    fn a_room_at_the_defaults_grants_both_types() {
        let mut levels = room();
        let before_state_default = levels.state_default;

        let changed = allow_at_default(&mut levels, &types());

        assert_eq!(changed, types());
        for t in TYPES {
            assert_eq!(levels.events.get(&TimelineEventType::from(t)), Some(&int!(0)), "{t}");
        }
        assert_eq!(levels.state_default, before_state_default, "nothing else moves");
    }

    /// Run twice, the second run changes nothing — the client may call it
    /// on every invite.
    #[test]
    fn granting_is_idempotent() {
        let mut levels = room();
        allow_at_default(&mut levels, &types());

        assert!(allow_at_default(&mut levels, &types()).is_empty());
    }

    /// Never RAISES a level a room already set lower, and leaves a type it
    /// was not asked about alone.
    #[test]
    fn a_lower_level_stays_and_other_types_are_untouched() {
        let mut levels = room();
        levels.users_default = int!(10);
        levels.events.insert(TimelineEventType::from("com.spiral.tools"), int!(0));
        levels.events.insert(TimelineEventType::from("m.room.topic"), int!(50));

        let changed = allow_at_default(&mut levels, &types());

        assert_eq!(changed, vec!["io.element.functional_members".to_owned()]);
        assert_eq!(levels.events.get(&TimelineEventType::from("com.spiral.tools")), Some(&int!(0)));
        assert_eq!(levels.events.get(&TimelineEventType::from("m.room.topic")), Some(&int!(50)));
        assert_eq!(
            levels.events.get(&TimelineEventType::from("io.element.functional_members")),
            Some(&int!(10)),
            "granted at users_default, not below it"
        );
    }
}
