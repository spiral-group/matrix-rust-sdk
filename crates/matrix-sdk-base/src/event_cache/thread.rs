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

// Modified by the Spiral authors, 2026: ThreadInfo::summary_computed, whether
// the stored reply count is a real count.

//! Threads types related to the Event Cache.

use ruma::OwnedEventId;
use serde::{Deserialize, Serialize};

use crate::read_receipts::ReadReceipts;

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
    #[serde(default)] // For backwards compatibility.
    pub number_of_replies: u32,

    /// The ID of the latest event in the thread, if any.
    #[serde(default)] // For backwards compatibility.
    pub latest_event: Option<OwnedEventId>,

    /// Read receipts for the current thread.
    pub read_receipts: ReadReceipts,

    /// Whether [`Self::number_of_replies`] and [`Self::latest_event`] say
    /// something about the thread: they were computed from replies on this
    /// device, or from the whole thread once a back-pagination reached its
    /// root.
    ///
    /// A `ThreadInfo` is created with zero replies as soon as the thread is
    /// known (opened, or a threaded read receipt arrived), possibly before
    /// any of its replies is on this device. Until then its zero is not a
    /// count: readers fall back to the summary the server bundled with the
    /// thread root, if any.
    #[serde(default)]
    pub summary_computed: bool,
}

impl ThreadInfo {
    /// Create a new [`ThreadInfo`].
    pub fn new() -> Self {
        Self {
            number_of_replies: 0,
            latest_event: None,
            read_receipts: ReadReceipts::default(),
            summary_computed: false,
        }
    }
}

impl Default for ThreadInfo {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::ThreadInfo;

    /// A `ThreadInfo` stored before `summary_computed` existed reads as not
    /// computed: its zero is not taken for a count.
    #[test]
    fn a_stored_thread_info_without_the_flag_reads_as_not_computed() {
        let mut stored = serde_json::to_value(ThreadInfo {
            number_of_replies: 3,
            summary_computed: true,
            ..ThreadInfo::default()
        })
        .unwrap();
        stored.as_object_mut().unwrap().remove("summary_computed").unwrap();

        let thread_info: ThreadInfo = serde_json::from_value(stored).unwrap();
        assert!(!thread_info.summary_computed);
        assert_eq!(thread_info.number_of_replies, 3);

        let round_trip: ThreadInfo =
            serde_json::from_value(serde_json::to_value(&thread_info).unwrap()).unwrap();
        assert!(!round_trip.summary_computed);
        assert_eq!(round_trip.number_of_replies, 3);
    }
}
