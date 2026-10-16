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

//! Spiral: which `m.rtc.notification` a call membership belongs to, and what
//! happened to the call each notification rang for. Every decision compares
//! server timestamps, never the device clock.

use std::sync::Arc;

use imbl::Vector;
use ruma::{MilliSecondsSinceUnixEpoch, OwnedEventId, OwnedUserId, UserId};

use super::super::{
    TimelineItem, TimelineItemContent, algorithms::rfind_event_item, event_item::RtcCallOutcome,
};

/// How far a call's membership may precede the ring it belongs to: the
/// caller publishes its membership, then sends the notification.
const RTC_MEMBERSHIP_LEAD_MS: u64 = 10_000;

/// The longest a ring lasts, whatever lifetime its notification states (or
/// when it states none): the cap the iOS app rings for.
const RTC_RING_LIFETIME_CAP_MS: u64 = 90_000;

/// A call membership change the timeline saw: an `m.call.member` state
/// event, by its server timestamp.
#[derive(Clone, Debug)]
pub(in crate::timeline) struct CallMemberChange {
    pub event_id: OwnedEventId,
    /// The event's state key: one membership per user and device.
    pub state_key: String,
    pub user_id: OwnedUserId,
    /// The event carries a membership (a join or a renewal), rather than
    /// none (a departure). Read from the content as sent, not from whether
    /// the membership has expired by now.
    pub is_join: bool,
    pub ts: MilliSecondsSinceUnixEpoch,
}

/// The end of a notification's ring window, in server milliseconds: its
/// origin timestamp plus its lifetime, capped at
/// [`RTC_RING_LIFETIME_CAP_MS`].
fn rtc_ring_window_end(ring_ts: MilliSecondsSinceUnixEpoch, lifetime_ms: Option<u64>) -> u64 {
    let lifetime_ms = lifetime_ms.unwrap_or(RTC_RING_LIFETIME_CAP_MS).min(RTC_RING_LIFETIME_CAP_MS);
    u64::from(ring_ts.0).saturating_add(lifetime_ms)
}

/// The event id of the newest `m.rtc.notification` item (none while it is a
/// local echo), if a call membership that became active at `activation_ts`
/// belongs to its ring: from shortly before the notification to the end of
/// its ring window. Both are server timestamps, so the device clock has no
/// say. Outside that span the notification belongs to an earlier call,
/// which is upstream's no-jump rule.
pub(in crate::timeline) fn rtc_notification_ringing_at(
    items: &Vector<Arc<TimelineItem>>,
    activation_ts: MilliSecondsSinceUnixEpoch,
) -> Option<OwnedEventId> {
    // The newest notification, sent or not: when it is my own unsent ring,
    // the call is its, and adopting the remote one before it would jump.
    let (_, item) = rfind_event_item(items, |it| {
        matches!(it.content(), TimelineItemContent::RtcNotification { .. })
    })?;
    let event_id = item.event_id()?;
    let TimelineItemContent::RtcNotification { lifetime_ms, .. } = item.content() else {
        return None;
    };
    let ring_ts = u64::from(item.timestamp().0);
    let activation_ts = u64::from(activation_ts.0);
    if activation_ts < ring_ts.saturating_sub(RTC_MEMBERSHIP_LEAD_MS) {
        return None;
    }
    if activation_ts > rtc_ring_window_end(item.timestamp(), *lifetime_ms) {
        return None;
    }
    Some(event_id.to_owned())
}

/// The outcome of every `m.rtc.notification` item, as `(item index, outcome)`.
///
/// A notification's call spans from its timestamp to the next notification's
/// (by timestamp, wherever the items sit): joins count from
/// [`RTC_MEMBERSHIP_LEAD_MS`] before each bound, departures from the bounds
/// themselves, so a caller's join just before its ring opens the new call
/// and a departure just before it closes the old one. The call also ends
/// once its sender and every membership seen joining it have left; changes
/// after that belong to a later call, even before that call's notification
/// is loaded. `changes` must be sorted by timestamp.
pub(in crate::timeline) fn rtc_call_outcomes(
    items: &Vector<Arc<TimelineItem>>,
    changes: &[CallMemberChange],
    own_user_id: &UserId,
) -> Vec<(usize, RtcCallOutcome)> {
    let mut rings: Vec<_> = items
        .iter()
        .enumerate()
        .filter_map(|(idx, item)| {
            let event = item.as_event()?;
            let TimelineItemContent::RtcNotification { lifetime_ms, .. } = event.content() else {
                return None;
            };
            Some((idx, event.timestamp(), event.sender().to_owned(), *lifetime_ms))
        })
        .collect();
    rings.sort_by_key(|(idx, ts, ..)| (*ts, *idx));

    let next_ring_ts: Vec<Option<u64>> = (0..rings.len())
        .map(|k| {
            rings[k + 1..]
                .iter()
                .map(|(_, ts, ..)| u64::from(ts.0))
                .find(|ts| *ts > u64::from(rings[k].1.0))
        })
        .collect();

    rings
        .iter()
        .zip(next_ring_ts)
        .map(|((idx, ring_at, sender, lifetime_ms), next_ts)| {
            let ring_ts = u64::from(ring_at.0);
            let join_from = ring_ts.saturating_sub(RTC_MEMBERSHIP_LEAD_MS);
            let join_until = next_ts.map(|ts| ts.saturating_sub(RTC_MEMBERSHIP_LEAD_MS));
            let answer_until = rtc_ring_window_end(*ring_at, *lifetime_ms);

            let mut outcome = RtcCallOutcome::default();
            // The sender is in the call they ring for, whether or not their
            // join is loaded.
            let mut sender_present = true;
            let mut present: Vec<&str> = Vec::new();
            for change in changes {
                let ts = u64::from(change.ts.0);
                if change.is_join {
                    if ts < join_from || join_until.is_some_and(|until| ts >= until) {
                        continue;
                    }
                    if !present.contains(&change.state_key.as_str()) {
                        present.push(&change.state_key);
                    }
                    if !outcome.joined_by.contains(&change.user_id) {
                        outcome.joined_by.push(change.user_id.clone());
                    }
                    if change.user_id == own_user_id {
                        outcome.own_user_joined = true;
                    }
                    if change.user_id == *sender {
                        continue;
                    }
                    outcome.first_joined_at.get_or_insert(change.ts);
                    if ts >= ring_ts
                        && ts <= answer_until
                        && !outcome.answered_by.contains(&change.user_id)
                    {
                        outcome.answered_by.push(change.user_id.clone());
                        outcome.answered_at.get_or_insert(change.ts);
                    }
                } else if ts >= ring_ts && next_ts.is_none_or(|next| ts < next) {
                    outcome.ended_at = Some(change.ts);
                    present.retain(|key| *key != change.state_key);
                    if change.user_id == *sender {
                        sender_present = false;
                    }
                    if !sender_present && present.is_empty() {
                        // Everyone seen in the call has left: it is over.
                        break;
                    }
                }
            }
            (*idx, outcome)
        })
        .collect()
}
