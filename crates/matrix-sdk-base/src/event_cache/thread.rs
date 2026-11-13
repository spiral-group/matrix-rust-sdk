// Copyright 2026 The Matrix.org Foundation C.I.C.
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

// Modified by the Spiral authors, 2026: ThreadInfo remembers the summary the
// server bundled with the thread root and whether the replies on this device
// are the whole thread; ThreadInfo::summary picks what a reader shows.

//! Threads types related to the Event Cache.

use matrix_sdk_common::{
    deserialized_responses::{ThreadSummary, TimelineEvent},
    serde_helpers::extract_bundled_thread,
};
use ruma::{EventId, MilliSecondsSinceUnixEpoch, OwnedEventId};
use serde::{Deserialize, Serialize};

use crate::read_receipts::ReadReceipts;

/// The summary the server bundled with a thread root, as this device keeps
/// it: a snapshot of the thread when the root was served, plus the replies
/// this device has seen since.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BundledThreadSummary {
    /// The number of replies the server counted.
    pub num_replies: u32,

    /// The latest reply the server knew of.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_reply: Option<OwnedEventId>,

    /// When that latest reply was sent. Replies on this device sent after it
    /// are not in [`Self::num_replies`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latest_reply_timestamp: Option<MilliSecondsSinceUnixEpoch>,

    /// The replies on this device sent after [`Self::latest_reply_timestamp`],
    /// as of the thread's last recount.
    #[serde(default)]
    pub newer_replies: u32,

    /// Whether the latest reply on this device, as of the thread's last
    /// recount, is [`Self::latest_reply`]. The thread's latest event is then
    /// the device's: the same reply, or a later edit of it.
    #[serde(default)]
    pub latest_reply_is_here: bool,
}

impl BundledThreadSummary {
    /// A summary just received from the server, with the time its latest
    /// reply was sent, if known.
    pub fn new(
        summary: ThreadSummary,
        latest_reply_timestamp: Option<MilliSecondsSinceUnixEpoch>,
    ) -> Self {
        Self {
            num_replies: summary.num_replies,
            latest_reply: summary.latest_reply,
            latest_reply_timestamp,
            newer_replies: 0,
            latest_reply_is_here: false,
        }
    }

    /// The summary the server bundled with `root`, if it is a thread root,
    /// with the time its latest reply was sent.
    ///
    /// That time is read from the bundled latest event itself, not from
    /// `TimelineEvent::bundled_latest_thread_event`, which caps it at the
    /// root's, earlier than every reply.
    pub fn from_root(root: &TimelineEvent) -> Option<Self> {
        let summary = root.thread_summary()?;
        let latest_reply_timestamp = extract_bundled_thread(root.raw()).and_then(|bundled| {
            bundled.latest_event.get_field::<MilliSecondsSinceUnixEpoch>("origin_server_ts").ok()?
        });
        Some(Self::new(summary, latest_reply_timestamp))
    }

    /// The size of the thread as far as this summary knows: the server's
    /// count and the replies this device has seen since.
    pub fn num_replies_now(&self) -> u32 {
        self.num_replies.saturating_add(self.newer_replies)
    }

    fn is_same_snapshot(&self, other: &Self) -> bool {
        self.num_replies == other.num_replies && self.latest_reply == other.latest_reply
    }
}

/// All the information about a thread.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThreadInfo {
    /// The number of events in the thread.
    ///
    /// This doesn't include:
    /// - the thread root event itself
    /// - events sent by ignored users
    /// - redacted events.
    ///
    /// Thus, it can be zero!
    ///
    /// It counts the replies on this device only, which can be part of the
    /// thread: see [`Self::all_replies_counted`] and [`Self::summary`].
    #[serde(default)] // For backwards compatibility.
    pub number_of_replies: u32,

    /// The ID of the latest event in the thread, if any.
    #[serde(default)] // For backwards compatibility.
    pub latest_event: Option<OwnedEventId>,

    /// Read receipts for the current thread.
    pub read_receipts: ReadReceipts,

    /// Whether [`Self::number_of_replies`] counts the whole thread: a
    /// back-pagination of the thread reached its root, and since then no
    /// sync has left a gap in the thread and no summary from the server has
    /// counted more replies, so every reply is on this device.
    ///
    /// Until then the count covers only the replies this device happens to
    /// have, which can be none (an old thread opened on a new device, or a
    /// threaded read receipt arriving first) or a few (the newest page of a
    /// long thread, a reply received over sync). Such a count is a lower
    /// bound, not the size of the thread.
    #[serde(default)]
    pub all_replies_counted: bool,

    /// The summary the server bundled with the thread root, the last time
    /// this device received the root with one.
    ///
    /// The event cache strips bundled relations from the events it stores,
    /// so this is where the server's count survives a restart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bundled_summary: Option<BundledThreadSummary>,
}

impl ThreadInfo {
    /// Create a new [`ThreadInfo`].
    pub fn new() -> Self {
        Self {
            number_of_replies: 0,
            latest_event: None,
            read_receipts: ReadReceipts::default(),
            all_replies_counted: false,
            bundled_summary: None,
        }
    }

    /// The summary to show for this thread.
    ///
    /// Once [`Self::all_replies_counted`], the count from this device. Before
    /// that, the count from this device is a lower bound: the larger count
    /// of [`Self::bundled_summary`] (the server's, plus the replies seen
    /// since) wins.
    ///
    /// The latest reply, before the whole thread is counted, is the bundled
    /// summary's when no reply on this device is newer than it (it carries a
    /// send time, and none of its newer replies is here) and it isn't the
    /// latest reply here either: this device may lack it, so the latest reply
    /// here is older. Otherwise it is the latest reply here, if any, which
    /// follows the edits of that reply.
    pub fn summary(&self) -> ThreadSummary {
        let counted = ThreadSummary::new(self.latest_event.clone(), self.number_of_replies);

        let Some(bundled) = self.bundled_summary.as_ref().filter(|_| !self.all_replies_counted)
        else {
            return counted;
        };

        let bundled_latest_is_newest = bundled.latest_reply_timestamp.is_some()
            && bundled.newer_replies == 0
            && bundled.latest_reply.is_some()
            && !bundled.latest_reply_is_here;
        let latest_reply = if bundled_latest_is_newest {
            bundled.latest_reply.clone()
        } else {
            counted.latest_reply.or_else(|| bundled.latest_reply.clone())
        };

        ThreadSummary {
            latest_reply,
            num_replies: counted.num_replies.max(bundled.num_replies_now()),
        }
    }

    /// Store a summary the server just bundled with the thread root.
    ///
    /// A summary counting more replies than a whole-thread count says
    /// replies came in that this device didn't see: the count is partial
    /// again (see [`Self::all_replies_counted`]).
    ///
    /// Returns whether it changed anything: the same snapshot received again
    /// keeps the replies counted since.
    pub fn remember_bundled_summary(&mut self, bundled: BundledThreadSummary) -> bool {
        if self.bundled_summary.as_ref().is_some_and(|stored| stored.is_same_snapshot(&bundled)) {
            return false;
        }
        if self.all_replies_counted && bundled.num_replies > self.number_of_replies {
            self.all_replies_counted = false;
        }
        self.bundled_summary = Some(bundled);
        true
    }

    /// Forget the bundled summary: the server served the root again without
    /// one, so the thread has no reply left.
    ///
    /// Returns whether there was one.
    pub fn forget_bundled_summary(&mut self) -> bool {
        self.bundled_summary.take().is_some()
    }

    /// Take a redacted reply out of the bundled summary, if it counted it:
    /// the reply was sent no later than the bundled latest reply (or the
    /// time of either is unknown). A newer reply is not in the server's
    /// count; the next recount takes it out of the newer replies.
    ///
    /// Returns whether the bundled summary changed.
    pub fn take_redacted_reply_out_of_bundled(
        &mut self,
        reply: &EventId,
        reply_sent_at: Option<MilliSecondsSinceUnixEpoch>,
    ) -> bool {
        let Some(bundled) = &mut self.bundled_summary else {
            return false;
        };

        let counted_by_bundle = match (reply_sent_at, bundled.latest_reply_timestamp) {
            (Some(sent_at), Some(latest_sent_at)) => sent_at <= latest_sent_at,
            _ => true,
        };
        if !counted_by_bundle {
            return false;
        }

        bundled.num_replies = bundled.num_replies.saturating_sub(1);
        if bundled.latest_reply.as_deref() == Some(reply) {
            bundled.latest_reply = None;
        }
        true
    }

    /// Weigh a summary bundled with a root a reader holds, that the event
    /// cache may not have stored (an event-focused or pinned cache), or that
    /// it stored and has since changed (a redacted reply, a forgotten
    /// summary): it is taken only when nothing is stored, or when its
    /// latest reply was sent after the stored one's, a later snapshot.
    pub fn consider_bundled_summary(&mut self, bundled: BundledThreadSummary) {
        let is_later = match &self.bundled_summary {
            None => true,
            Some(stored) => matches!(
                (bundled.latest_reply_timestamp, stored.latest_reply_timestamp),
                (Some(held), Some(stored)) if held > stored
            ),
        };
        if is_later {
            self.bundled_summary = Some(bundled);
        }
    }

    /// Compare the replies on this device with the bundled summary: count
    /// the ones sent after its latest reply, given when each was sent, and
    /// note whether `latest_reply_here` (the latest reply on this device,
    /// before any edit of it) is its latest reply.
    pub fn compare_local_replies_with_bundled(
        &mut self,
        reply_timestamps: &[MilliSecondsSinceUnixEpoch],
        latest_reply_here: Option<&EventId>,
    ) {
        if let Some(bundled) = &mut self.bundled_summary {
            bundled.newer_replies = bundled.latest_reply_timestamp.map_or(0, |since| {
                let newer = reply_timestamps.iter().filter(|sent| **sent > since).count();
                u32::try_from(newer).unwrap_or(u32::MAX)
            });
            bundled.latest_reply_is_here = latest_reply_here
                .is_some_and(|latest_here| bundled.latest_reply.as_deref() == Some(latest_here));
        }
    }

    /// Whether [`Self::summary`] says anything about the thread.
    ///
    /// Not when nothing is known yet: no reply on this device, no bundled
    /// summary, and the thread not paginated to its root. Such a zero must
    /// not replace a summary a reader got elsewhere.
    pub fn summary_is_known(&self) -> bool {
        self.all_replies_counted || self.summary().num_replies > 0
    }
}

impl Default for ThreadInfo {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use matrix_sdk_common::deserialized_responses::ThreadSummary;
    use ruma::{EventId, MilliSecondsSinceUnixEpoch, UInt, owned_event_id};
    use serde_json::json;

    use super::{BundledThreadSummary, ThreadInfo};

    fn at(millis: u32) -> MilliSecondsSinceUnixEpoch {
        MilliSecondsSinceUnixEpoch(UInt::from(millis))
    }

    /// Compare, with the thread's latest event as the latest reply here (no
    /// edit of it).
    fn compare_with_latest_here(
        info: &mut ThreadInfo,
        reply_timestamps: &[MilliSecondsSinceUnixEpoch],
    ) {
        let latest_here = info.latest_event.clone();
        info.compare_local_replies_with_bundled(reply_timestamps, latest_here.as_deref());
    }

    fn bundled(count: u32, latest: &str, sent_at: Option<u32>) -> BundledThreadSummary {
        BundledThreadSummary::new(
            ThreadSummary::new(Some(EventId::parse(latest).unwrap()), count),
            sent_at.map(at),
        )
    }

    fn thread_info(
        number_of_replies: u32,
        latest_event: Option<&str>,
        all_replies_counted: bool,
        bundled: Option<(u32, &str)>,
    ) -> ThreadInfo {
        ThreadInfo {
            number_of_replies,
            latest_event: latest_event.map(|id| EventId::parse(id).unwrap()),
            all_replies_counted,
            bundled_summary: bundled.map(|(count, latest)| self::bundled(count, latest, None)),
            ..ThreadInfo::default()
        }
    }

    #[test]
    fn nothing_on_the_device_shows_the_bundled_summary() {
        let info = thread_info(0, None, false, Some((42, "$41")));

        assert_eq!(info.summary(), ThreadSummary::new(Some(owned_event_id!("$41")), 42));
        assert!(info.summary_is_known());
    }

    #[test]
    fn a_partial_count_keeps_the_larger_bundled_count_with_the_newest_reply() {
        let info = thread_info(1, Some("$43"), false, Some((42, "$41")));

        assert_eq!(info.summary(), ThreadSummary::new(Some(owned_event_id!("$43")), 42));
    }

    #[test]
    fn a_larger_count_on_the_device_wins_over_an_older_bundled_one() {
        let info = thread_info(5, Some("$5"), false, Some((3, "$3")));

        assert_eq!(info.summary(), ThreadSummary::new(Some(owned_event_id!("$5")), 5));
    }

    #[test]
    fn a_whole_thread_count_wins_even_when_lower() {
        let info = thread_info(1, Some("$1"), true, Some((3, "$3")));

        assert_eq!(info.summary(), ThreadSummary::new(Some(owned_event_id!("$1")), 1));
    }

    #[test]
    fn a_whole_thread_count_of_zero_is_known() {
        let info = thread_info(0, None, true, Some((3, "$3")));

        assert_eq!(info.summary(), ThreadSummary::new(None, 0));
        assert!(info.summary_is_known());
    }

    #[test]
    fn nothing_known_is_not_a_summary() {
        let info = thread_info(0, None, false, None);

        assert_eq!(info.summary(), ThreadSummary::new(None, 0));
        assert!(!info.summary_is_known());
    }

    /// A `ThreadInfo` stored by an earlier build (no `all_replies_counted`,
    /// no `bundled_summary`, or the retired `summary_computed`) reads as a
    /// partial count with no bundled summary.
    #[test]
    fn an_earlier_stored_thread_info_reads_as_a_partial_count() {
        let mut stored = serde_json::to_value(thread_info(3, Some("$3"), false, None)).unwrap();
        let stored_object = stored.as_object_mut().unwrap();
        stored_object.remove("all_replies_counted").unwrap();
        stored_object.insert("summary_computed".to_owned(), json!(true));

        let thread_info: ThreadInfo = serde_json::from_value(stored).unwrap();

        assert!(!thread_info.all_replies_counted);
        assert_eq!(thread_info.bundled_summary, None);
        assert_eq!(thread_info.summary(), ThreadSummary::new(Some(owned_event_id!("$3")), 3));
    }

    #[test]
    fn the_bundled_summary_survives_a_round_trip() {
        let stored = thread_info(0, None, false, Some((42, "$41")));

        let thread_info: ThreadInfo =
            serde_json::from_value(serde_json::to_value(&stored).unwrap()).unwrap();

        assert_eq!(thread_info.bundled_summary, stored.bundled_summary);
        assert_eq!(thread_info.summary(), ThreadSummary::new(Some(owned_event_id!("$41")), 42));
    }

    #[test]
    fn replies_newer_than_the_bundled_one_add_to_its_count() {
        let mut info = thread_info(2, Some("$43"), false, None);
        info.bundled_summary = Some(bundled(42, "$42", Some(42)));

        compare_with_latest_here(&mut info, &[at(42), at(43)]);

        assert_eq!(info.summary(), ThreadSummary::new(Some(owned_event_id!("$43")), 43));
    }

    #[test]
    fn without_the_bundled_reply_time_no_reply_counts_as_newer() {
        let mut info = thread_info(1, Some("$43"), false, Some((42, "$42")));

        compare_with_latest_here(&mut info, &[at(43)]);

        assert_eq!(info.summary(), ThreadSummary::new(Some(owned_event_id!("$43")), 42));
    }

    #[test]
    fn the_same_bundled_summary_again_keeps_the_newer_replies() {
        let mut info = thread_info(2, Some("$43"), false, None);
        info.bundled_summary = Some(bundled(42, "$42", Some(42)));
        compare_with_latest_here(&mut info, &[at(42), at(43)]);

        assert!(!info.remember_bundled_summary(bundled(42, "$42", Some(42))));
        assert_eq!(info.summary().num_replies, 43);
    }

    #[test]
    fn a_new_bundled_summary_replaces_the_newer_replies() {
        let mut info = thread_info(2, Some("$43"), false, None);
        info.bundled_summary = Some(bundled(42, "$42", Some(42)));
        compare_with_latest_here(&mut info, &[at(42), at(43)]);

        assert!(info.remember_bundled_summary(bundled(43, "$43", Some(43))));
        assert_eq!(info.summary(), ThreadSummary::new(Some(owned_event_id!("$43")), 43));
    }

    #[test]
    fn a_redacted_reply_the_bundle_counted_leaves_it() {
        let mut info = thread_info(0, None, false, None);
        info.bundled_summary = Some(bundled(1, "$r1", Some(1)));

        assert!(info.take_redacted_reply_out_of_bundled(&owned_event_id!("$r1"), Some(at(1))));

        assert_eq!(info.summary(), ThreadSummary::new(None, 0));
    }

    #[test]
    fn a_redacted_reply_newer_than_the_bundle_leaves_it_alone() {
        let mut info = thread_info(0, None, false, None);
        info.bundled_summary = Some(bundled(3, "$3", Some(3)));

        assert!(!info.take_redacted_reply_out_of_bundled(&owned_event_id!("$4"), Some(at(4))));

        assert_eq!(info.summary().num_replies, 3);
    }

    #[test]
    fn a_bundled_count_does_not_go_below_zero() {
        let mut info = thread_info(0, None, false, None);
        info.bundled_summary = Some(bundled(0, "$1", None));

        info.take_redacted_reply_out_of_bundled(&owned_event_id!("$1"), None);

        assert_eq!(info.bundled_summary.unwrap().num_replies, 0);
    }

    #[test]
    fn a_larger_fresh_bundled_summary_makes_a_whole_count_partial() {
        let mut info = thread_info(2, Some("$2"), true, None);

        assert!(info.remember_bundled_summary(bundled(5, "$5", Some(5))));

        assert!(!info.all_replies_counted);
        assert_eq!(info.summary(), ThreadSummary::new(Some(owned_event_id!("$5")), 5));
    }

    #[test]
    fn a_fresh_bundled_summary_within_a_whole_count_keeps_it_whole() {
        let mut info = thread_info(3, Some("$3"), true, None);

        assert!(info.remember_bundled_summary(bundled(2, "$2", Some(2))));

        assert!(info.all_replies_counted);
        assert_eq!(info.summary(), ThreadSummary::new(Some(owned_event_id!("$3")), 3));
    }

    #[test]
    fn the_bundled_latest_reply_is_the_preview_when_none_here_is_newer() {
        // Replies 1 and 2 here, the server's summary counted them and its
        // latest, reply 3, which isn't here.
        let mut info = thread_info(2, Some("$2"), false, None);
        info.bundled_summary = Some(bundled(3, "$3", Some(3)));
        compare_with_latest_here(&mut info, &[at(1), at(2)]);

        assert_eq!(info.summary(), ThreadSummary::new(Some(owned_event_id!("$3")), 3));
    }

    #[test]
    fn the_bundled_latest_reply_stays_the_preview_on_a_tied_count() {
        let mut info = thread_info(2, Some("$2"), false, None);
        info.bundled_summary = Some(bundled(2, "$3", Some(3)));
        compare_with_latest_here(&mut info, &[at(2)]);

        assert_eq!(info.summary(), ThreadSummary::new(Some(owned_event_id!("$3")), 2));
    }

    #[test]
    fn a_redacted_bundled_latest_reply_leaves_the_preview() {
        let mut info = thread_info(2, Some("$2"), false, None);
        info.bundled_summary = Some(bundled(3, "$3", Some(3)));

        info.take_redacted_reply_out_of_bundled(&owned_event_id!("$3"), Some(at(3)));
        compare_with_latest_here(&mut info, &[at(1), at(2)]);

        assert_eq!(info.summary(), ThreadSummary::new(Some(owned_event_id!("$2")), 2));
    }

    #[test]
    fn an_edited_bundled_latest_reply_here_previews_the_edit() {
        // Reply 2, the server's latest, is here, and its latest edit is the
        // thread's latest event.
        let mut info = thread_info(2, Some("$edit"), false, None);
        info.bundled_summary = Some(bundled(2, "$2", Some(2)));

        info.compare_local_replies_with_bundled(&[at(1), at(2)], Some(&owned_event_id!("$2")));

        assert_eq!(info.summary(), ThreadSummary::new(Some(owned_event_id!("$edit")), 2));
    }

    #[test]
    fn a_held_bundled_summary_does_not_undo_a_stored_redaction() {
        // The stored summary lost reply 2 to a redaction; the root a reader
        // holds still carries the summary from before it.
        let mut info = thread_info(1, Some("$1"), false, None);
        let mut stored = bundled(2, "$2", Some(2));
        stored.num_replies = 1;
        stored.latest_reply = None;
        info.bundled_summary = Some(stored.clone());

        info.consider_bundled_summary(bundled(2, "$2", Some(2)));

        assert_eq!(info.bundled_summary, Some(stored));
        assert_eq!(info.summary(), ThreadSummary::new(Some(owned_event_id!("$1")), 1));
    }

    #[test]
    fn a_held_later_bundled_summary_is_taken() {
        let mut info = thread_info(1, Some("$1"), false, None);
        info.bundled_summary = Some(bundled(1, "$1", Some(1)));

        info.consider_bundled_summary(bundled(3, "$3", Some(3)));

        assert_eq!(info.summary(), ThreadSummary::new(Some(owned_event_id!("$3")), 3));
    }

    #[test]
    fn a_held_bundled_summary_is_taken_when_none_is_stored() {
        let mut info = thread_info(0, None, false, None);

        info.consider_bundled_summary(bundled(2, "$2", None));

        assert_eq!(info.summary(), ThreadSummary::new(Some(owned_event_id!("$2")), 2));
    }
}
