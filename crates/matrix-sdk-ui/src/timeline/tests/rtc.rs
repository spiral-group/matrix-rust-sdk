// Modified by the Spiral authors, 2026: tests of call outcomes and of a ringing
// tile's adoption.

//! Tests for RTC notification timeline items.

use std::ops::Add;

use assert_matches::assert_matches;
use eyeball_im::VectorDiff;
use futures_util::{FutureExt, StreamExt};
use matrix_sdk_base::CallIntentConsensus;
use matrix_sdk_test::{ALICE, BOB, CAROL, User};
use ruma::{
    event_id,
    events::rtc::notification::{CallIntent, NotificationType, RtcNotificationEventContent},
};
use stream_assert::{assert_next_matches, assert_pending};

use super::*;
use crate::timeline::{RtcCallOutcome, TimelineItemContent, controller::ActiveCallInfo};

/// Test that `handle_rtc_members_update` correctly updates the active_members
/// of the RtcNotification timeline item.
#[tokio::test]
async fn test_rtc_members_update() {
    let timeline = TestTimeline::new().await;

    let mut stream = timeline.subscribe_events().await;

    let f = &timeline.factory;

    set_active_call_members(&timeline, &[User::Bob]).await;

    let notification_event = f
        .rtc_notification(NotificationType::Ring)
        .sender(User::Alice)
        .call_intent(CallIntent::Audio)
        .event_id(event_id!("$notification_event"));

    timeline.handle_live_event(notification_event).await;

    // This is two steps, first the item is added, then it is updated with the
    // active members
    assert_next_matches!(stream, VectorDiff::PushBack { value } => value);
    let notification_item =
        assert_next_matches!(stream, VectorDiff::Set { index: 0, value } => value);
    assert_rtc_notification_active_members(notification_item, &[User::Bob]);

    // Simulate two members joined the call
    set_active_call_members(&timeline, &[User::Bob, User::Carol]).await;

    let notification_item = assert_next_matches!(stream, VectorDiff::Set { value, .. } => value);
    assert_rtc_notification_active_members(notification_item, &[User::Bob, User::Carol]);

    // Simulate Carol left
    set_active_call_members(&timeline, &[User::Bob]).await;

    let notification_item = assert_next_matches!(stream, VectorDiff::Set { value, .. } => value);
    assert_rtc_notification_active_members(notification_item, &[User::Bob]);
}

/// Test that `handle_rtc_members_update` correctly updates the active_members
/// of the last notification event
#[tokio::test]
async fn test_rtc_members_update_last_only() {
    let timeline = TestTimeline::new().await;
    let mut stream = timeline.subscribe_events().await;

    let f = &timeline.factory;

    let some_timestamp = SystemTime::now();

    // ===========
    //
    // # ACT: simulate active members in the call
    set_active_call_members(&timeline, &[User::Bob]).await;

    // ===========
    //
    // # ACT: insert a first notification event
    timeline
        .handle_live_event(
            f.rtc_notification(NotificationType::Ring)
                .sender(User::Alice)
                .call_intent(CallIntent::Audio)
                .event_id(event_id!("$notification_event_old"))
                .server_ts(MilliSecondsSinceUnixEpoch::from_system_time(some_timestamp).unwrap()),
        )
        .await;

    assert_next_matches!(stream, VectorDiff::PushBack { .. });

    // ===========
    //
    // # ASSERT: the notification item should be updated with the active members

    let notification_item = assert_next_matches!(stream, VectorDiff::Set { value, .. } => value);
    let info = assert_rtc_notification_active_members(notification_item, &[User::Bob]);
    assert_eq!(
        info.call_started_ts_millis,
        Some(MilliSecondsSinceUnixEpoch::from_system_time(some_timestamp).unwrap())
    );

    // ===========
    //
    // # ACT: insert a new notification event with a newer timestamp
    let new_notification_ts = some_timestamp.add(Duration::from_secs(60));

    timeline
        .handle_live_event(
            f.rtc_notification(NotificationType::Ring)
                .sender(User::Alice)
                .call_intent(CallIntent::Audio)
                .event_id(event_id!("$notification_event_new"))
                .server_ts(
                    MilliSecondsSinceUnixEpoch::from_system_time(new_notification_ts).unwrap(),
                ),
        )
        .await;

    // ===========
    //
    // ASSERT: As per requirement only the latest notification item in the
    // timeline should have the active call info. So ensure the oldest
    // notification item is cleared and that the new one has the previously
    // known info
    //
    // ===========

    // The notification is first added
    assert_next_matches!(
        stream,
        VectorDiff::PushBack { value } => value
    );

    // The old one is cleared
    let old_notif_updated = assert_next_matches!(
        stream,
        VectorDiff::Set { index: 0, value } => value
    );
    assert_eq!(
        old_notif_updated.event_id().expect("eventId should be set"),
        event_id!("$notification_event_old")
    );
    assert_matches!(old_notif_updated.content, TimelineItemContent::RtcNotification { active_call_info: None, .. } => {});

    // The new one is updated with the previously known info
    let item = assert_next_matches!(
        stream,
        VectorDiff::Set { index: 1, value } => value
    );
    let info = assert_rtc_notification_active_members(item, &[User::Bob]);
    assert_eq!(
        info.call_started_ts_millis,
        Some(MilliSecondsSinceUnixEpoch::from_system_time(new_notification_ts).unwrap())
    );

    // ===========
    //
    // # ACT: simulate new active members in the call

    set_active_call_members(&timeline, &[User::Bob, User::Carol]).await;

    // ===========
    //
    // # ASSERT: should only update the latest notification item

    let notification_item = assert_next_matches!(stream, VectorDiff::Set { value, .. } => value);
    assert_eq!(
        notification_item.event_id().expect("eventId should be set"),
        event_id!("$notification_event_new")
    );
    assert_rtc_notification_active_members(notification_item, &[User::Bob, User::Carol]);

    // Simulate back paginated rtc notification

    let old_notification_ts = some_timestamp.sub(Duration::from_secs(60));

    timeline
        .controller
        .handle_remote_events_with_diffs(
            vec![VectorDiff::PushFront {
                value: f
                    .rtc_notification(NotificationType::Ring)
                    .sender(User::Alice)
                    .call_intent(CallIntent::Audio)
                    .event_id(event_id!("$notification_event_old"))
                    .server_ts(
                        MilliSecondsSinceUnixEpoch::from_system_time(old_notification_ts).unwrap(),
                    )
                    .into_event(),
            }],
            RemoteEventOrigin::Pagination,
        )
        .await;

    // Should only add this notification, and not modify the most recent one
    let notification_item =
        assert_next_matches!(stream, VectorDiff::PushFront { value, .. } => value);
    assert_eq!(
        notification_item.event_id().expect("eventId should be set"),
        event_id!("$notification_event_old")
    );

    assert_pending!(stream);
}

#[track_caller]
fn assert_rtc_notification_active_members(
    item: EventTimelineItem,
    members: &[User],
) -> ActiveCallInfo {
    assert_matches!(item.content, TimelineItemContent::RtcNotification { active_call_info: Some(active_call_info), .. } => {
        let active_members = &active_call_info.active_members;
        assert_eq!(active_members.len(), members.len());
        for member in members {
            assert!(active_members.contains(member));
        }
        active_call_info
    })
}

/// Spiral 2026-09-06: a room opened while its call is already live. The
/// newest notification is on the timeline before the active-call info
/// arrives. Then a back-paginated OLD notification must not restamp the
/// call: the tile once read a two-day timer on a call that had just begun,
/// on every cold open. The notification's own timestamp is the call's
/// start.
///
/// Since 2026-10-15 the active-call update itself adopts the newest
/// notification when a handled membership belongs to its ring, so the
/// attach happens there; the back-paginated event leaves it alone.
#[tokio::test]
async fn test_a_back_paginated_old_notification_does_not_restamp_the_live_call() {
    let timeline = TestTimeline::new().await;
    let f = &timeline.factory;

    // The live call's notification and its caller's membership are on the
    // timeline; the call is not yet known to be active.
    let new_ts = SystemTime::now();
    timeline
        .handle_live_event(
            f.rtc_notification(NotificationType::Ring)
                .sender(*ALICE)
                .call_intent(CallIntent::Audio)
                .event_id(event_id!("$new_notification"))
                .server_ts(ts(new_ts)),
        )
        .await;
    handle_call_member(&timeline, *ALICE, before(new_ts, 300)).await;
    assert!(newest_notification_call(&timeline).await.is_none());

    // The call becomes active: the membership belongs to the newest
    // notification's ring, so it adopts the call, stamped with its own time.
    set_active_call_members(&timeline, &[User::Alice]).await;
    let info = newest_notification_call(&timeline).await.expect("the update adopted the tile");
    assert_eq!(info.call_started_ts_millis, Some(ts(new_ts)));

    // Initial back-pagination brings a notification from two days ago.
    let old_ts = new_ts.checked_sub(Duration::from_secs(2 * 24 * 60 * 60)).unwrap();
    timeline
        .handle_back_paginated_event(
            f.rtc_notification(NotificationType::Ring)
                .sender(*CAROL)
                .call_intent(CallIntent::Audio)
                .room(room_id!("!r0"))
                .event_id(event_id!("$old_notification"))
                .server_ts(ts(old_ts))
                .into_raw_timeline(),
        )
        .await;

    // The live call stays on the NEWEST notification, with ITS time.
    let info = newest_notification_call(&timeline)
        .await
        .expect("the live call attached to the newest notification");
    assert_eq!(
        info.call_started_ts_millis,
        Some(ts(new_ts)),
        "the tile's timer is the notification's own time, not the back-paginated event's"
    );
}

/// Spiral 2026-10-15 (F7): the `m.rtc.notification` reaches the timeline
/// before the room info carries the `m.call.member` change. The notification
/// is handled with no active call, so nothing attaches; the active-call
/// update that follows used to update a tile only through an id that was
/// never set, and the tile stayed without its Join for the whole call. The
/// update now adopts the newest notification when a membership the timeline
/// handled belongs to its ring.
#[tokio::test]
async fn test_a_notification_before_the_membership_adopts_the_live_call() {
    let timeline = TestTimeline::new().await;
    let f = &timeline.factory;

    let ring_ts = SystemTime::now();
    timeline
        .handle_live_event(
            f.rtc_notification(NotificationType::Ring)
                .sender(*ALICE)
                .call_intent(CallIntent::Audio)
                .event_id(event_id!("$ring"))
                .server_ts(ts(ring_ts)),
        )
        .await;
    // The caller's membership, published just before the ring, reaches the
    // timeline after it.
    handle_call_member(&timeline, *ALICE, before(ring_ts, 300)).await;
    assert!(newest_notification_call(&timeline).await.is_none());

    // The membership lands in the room info only now.
    set_active_call_members(&timeline, &[User::Alice]).await;
    let info = newest_notification_call(&timeline).await.expect("the update adopted the tile");
    assert!(info.active_members.contains(&ALICE.to_owned()));
    assert_eq!(info.call_started_ts_millis, Some(ts(ring_ts)));

    // The tile is now the call's: later changes reach it.
    set_active_call_members(&timeline, &[User::Alice, User::Bob]).await;
    let info = newest_notification_call(&timeline).await.expect("still the call's tile");
    assert_eq!(info.active_members.len(), 2);

    // And the call ending clears it.
    timeline.controller.handle_active_call_update(None).await;
    assert!(newest_notification_call(&timeline).await.is_none());
}

/// The other order of the same race: the room info update comes before the
/// timeline handles the membership. The update has nothing to go on; the
/// membership event, when it is handled, adopts the tile.
#[tokio::test]
async fn test_a_membership_handled_after_the_call_update_adopts_the_tile() {
    let timeline = TestTimeline::new().await;
    let f = &timeline.factory;

    let ring_ts = SystemTime::now();
    timeline
        .handle_live_event(
            f.rtc_notification(NotificationType::Ring)
                .sender(*ALICE)
                .call_intent(CallIntent::Audio)
                .event_id(event_id!("$ring"))
                .server_ts(ts(ring_ts)),
        )
        .await;
    set_active_call_members(&timeline, &[User::Alice, User::Bob]).await;
    assert!(newest_notification_call(&timeline).await.is_none());

    // Bob's answer, inside the ring.
    handle_call_member(&timeline, *BOB, after(ring_ts, 4_000)).await;
    let info = newest_notification_call(&timeline).await.expect("the membership adopted the tile");
    assert_eq!(info.active_members.len(), 2);
    assert_eq!(info.call_started_ts_millis, Some(ts(ring_ts)));
}

/// The decision compares two server timestamps, the membership's and the
/// notification's; the device clock has no say. Here the device runs ten
/// minutes ahead of the homeserver: by its own clock every ring is long
/// over, and a rule reading it would never adopt.
#[tokio::test]
async fn test_adoption_reads_server_timestamps_not_the_device_clock() {
    let timeline = TestTimeline::new().await;
    let f = &timeline.factory;

    let ring_ts = before(SystemTime::now(), 10 * 60 * 1000);
    timeline
        .handle_live_event(
            f.rtc_notification(NotificationType::Ring)
                .sender(*ALICE)
                .call_intent(CallIntent::Audio)
                .event_id(event_id!("$ring"))
                .server_ts(ts(ring_ts)),
        )
        .await;
    handle_call_member(&timeline, *ALICE, before(ring_ts, 300)).await;

    set_active_call_members(&timeline, &[User::Alice]).await;
    let info = newest_notification_call(&timeline).await.expect("the update adopted the tile");
    assert_eq!(info.call_started_ts_millis, Some(ts(ring_ts)));
}

/// A notification whose ring was over when the membership became active
/// belongs to an earlier call, so a call becoming active after it attaches
/// nothing. The next handled notification event still attaches the newest
/// one, with its own time.
#[tokio::test]
async fn test_a_notification_past_its_ring_window_is_not_adopted() {
    let timeline = TestTimeline::new().await;
    let f = &timeline.factory;

    // The factory's notification rings for 30 s; this one rang 2 min before
    // the membership.
    let now = SystemTime::now();
    let ring_ts = before(now, 120_000);
    timeline
        .handle_live_event(
            f.rtc_notification(NotificationType::Ring)
                .sender(*ALICE)
                .call_intent(CallIntent::Audio)
                .event_id(event_id!("$earlier_ring"))
                .server_ts(ts(ring_ts)),
        )
        .await;
    handle_call_member(&timeline, *BOB, now).await;

    set_active_call_members(&timeline, &[User::Bob]).await;
    assert!(newest_notification_call(&timeline).await.is_none());

    let old_ts = ring_ts.checked_sub(Duration::from_secs(24 * 60 * 60)).unwrap();
    timeline
        .handle_back_paginated_event(
            f.rtc_notification(NotificationType::Ring)
                .sender(*CAROL)
                .call_intent(CallIntent::Audio)
                .room(room_id!("!r0"))
                .event_id(event_id!("$older_ring"))
                .server_ts(ts(old_ts))
                .into_raw_timeline(),
        )
        .await;

    let info =
        newest_notification_call(&timeline).await.expect("the next notification event attached");
    assert_eq!(info.call_started_ts_millis, Some(ts(ring_ts)));
}

/// Upstream's no-jump rule (`test_only_update_notification_after_it_has_been_
/// marked_as_last`) with realistic timestamps — upstream's passes because its
/// notifications are from 1970. A new call publishes its membership before
/// its own ring; the previous ring is minutes old. The update must not
/// attach the call to the old tile, or the tile jumps when the new
/// notification lands.
#[tokio::test]
async fn test_a_new_call_after_an_earlier_ring_does_not_jump() {
    let timeline = TestTimeline::new().await;
    let mut stream = timeline.subscribe_events().await;
    let f = &timeline.factory;

    // The earlier call: Alice rang, nobody answered, she left.
    let now = SystemTime::now();
    let earlier = before(now, 5 * 60 * 1000);
    handle_call_member(&timeline, *ALICE, before(earlier, 300)).await;
    timeline
        .handle_live_event(
            f.rtc_notification(NotificationType::Ring)
                .sender(*ALICE)
                .event_id(event_id!("$earlier_call"))
                .server_ts(ts(earlier)),
        )
        .await;
    timeline
        .handle_live_event(
            f.call_membership_state(ALICE.to_owned(), "DEVICE".to_owned())
                .leave()
                .server_ts(ts(after(earlier, 32_000))),
        )
        .await;
    // The two membership items, the ring, and its outcome updates.
    while stream.next().now_or_never().flatten().is_some() {}

    handle_call_member(&timeline, *BOB, before(now, 300)).await;
    set_active_call_members(&timeline, &[User::Bob]).await;
    // Only the membership's own state item; the old tile is untouched.
    assert_next_matches!(stream, VectorDiff::PushBack { .. });
    assert_pending!(stream);

    timeline
        .handle_live_event(
            f.rtc_notification(NotificationType::Ring)
                .sender(*BOB)
                .event_id(event_id!("$new_call"))
                .server_ts(ts(now)),
        )
        .await;
    // The new ring arrives and takes the call (its outcome is filled in
    // too); the old tile is never touched.
    assert_next_matches!(stream, VectorDiff::PushBack { .. });
    while let Some(diff) = stream.next().now_or_never().flatten() {
        let VectorDiff::Set { value, .. } = diff else { panic!("unexpected diff {diff:?}") };
        assert_eq!(value.event_id(), Some(event_id!("$new_call")), "the old tile jumped");
    }
    let info = notification_call(&timeline, event_id!("$new_call")).await.expect("the call");
    assert!(info.active_members.contains(&BOB.to_owned()));
}

/// The residual of the adoption, pinned: a redial inside the previous ring's
/// window (here 20 s after it, against a 30 s lifetime) is indistinguishable
/// from an answer to it, so the update attaches the call to the old tile;
/// the new notification then takes it over (one jump).
#[tokio::test]
async fn test_a_redial_inside_the_previous_ring_window_attaches_there_first() {
    let timeline = TestTimeline::new().await;
    let f = &timeline.factory;

    let now = SystemTime::now();
    timeline
        .handle_live_event(
            f.rtc_notification(NotificationType::Ring)
                .sender(*ALICE)
                .event_id(event_id!("$first_ring"))
                .server_ts(ts(before(now, 20_000))),
        )
        .await;
    handle_call_member(&timeline, *ALICE, before(now, 300)).await;
    set_active_call_members(&timeline, &[User::Alice]).await;
    let first = notification_call(&timeline, event_id!("$first_ring")).await;
    assert!(first.is_some(), "the residual: the redial attaches to the previous tile first");

    timeline
        .handle_live_event(
            f.rtc_notification(NotificationType::Ring)
                .sender(*ALICE)
                .event_id(event_id!("$redial"))
                .server_ts(ts(now)),
        )
        .await;
    assert!(notification_call(&timeline, event_id!("$first_ring")).await.is_none());
    assert!(notification_call(&timeline, event_id!("$redial")).await.is_some());
}

/// Spiral 2026-10-15 (F7, second cause): a gappy sync clears the timeline
/// mid-call (the app resumed from the background) and the same notification
/// comes back. The metadata kept the old tile's id, so the re-added
/// notification looked already attached and the tile had no Join until the
/// room was reopened.
#[tokio::test]
async fn test_a_cleared_timeline_reattaches_the_live_call() {
    let timeline = TestTimeline::new().await;
    let f = &timeline.factory;

    set_active_call_members(&timeline, &[User::Bob]).await;
    let ring = || {
        f.rtc_notification(NotificationType::Ring)
            .sender(*ALICE)
            .call_intent(CallIntent::Audio)
            .event_id(event_id!("$ring"))
            .server_ts(MilliSecondsSinceUnixEpoch::now())
            .into_event()
    };
    timeline
        .controller
        .handle_remote_events_with_diffs(
            vec![VectorDiff::PushBack { value: ring() }],
            RemoteEventOrigin::Sync,
        )
        .await;
    assert!(newest_notification_call(&timeline).await.is_some(), "attached before the clear");

    timeline
        .controller
        .handle_remote_events_with_diffs(
            vec![VectorDiff::Clear, VectorDiff::PushBack { value: ring() }],
            RemoteEventOrigin::Sync,
        )
        .await;

    let info = newest_notification_call(&timeline)
        .await
        .expect("the re-added notification carries the live call");
    assert!(info.active_members.contains(&BOB.to_owned()));
}

/// A ring lasts at most 90 s, whatever lifetime its notification states:
/// the cap the iOS app rings for. A membership two minutes after a ring
/// claiming five is neither an answer to it nor its call.
#[tokio::test]
async fn test_the_ring_window_is_capped_at_90_seconds() {
    let timeline = TestTimeline::new().await;
    let f = &timeline.factory;

    let ring_ts = before(SystemTime::now(), 120_000);
    timeline
        .handle_live_event(
            f.event(RtcNotificationEventContent::new(
                ts(ring_ts),
                Duration::from_secs(300),
                NotificationType::Ring,
            ))
            .sender(*ALICE)
            .event_id(event_id!("$long_ring"))
            .server_ts(ts(ring_ts)),
        )
        .await;
    handle_call_member(&timeline, *BOB, after(ring_ts, 120_000)).await;

    let items = timeline.controller.items().await;
    let ring = items
        .iter()
        .filter_map(|item| item.as_event())
        .find(|event| event.event_id() == Some(event_id!("$long_ring")))
        .unwrap();
    assert_matches!(ring.content(), TimelineItemContent::RtcNotification { outcome, .. } => {
        assert!(outcome.answered_by.is_empty(), "two minutes in is past the 90 s ring");
    });

    set_active_call_members(&timeline, &[User::Bob]).await;
    assert!(newest_notification_call(&timeline).await.is_none(), "not this ring's call");
}

/// The newest notification is my own redial, still a local echo with no
/// event id. The call belongs to it, not to the remote ring before it:
/// adopting that one makes the tile jump when the echo lands.
#[tokio::test]
async fn test_my_unsent_redial_keeps_the_call_off_the_previous_ring() {
    let timeline = TestTimeline::new().await;
    let f = &timeline.factory;

    let now = SystemTime::now();
    timeline
        .handle_live_event(
            f.rtc_notification(NotificationType::Ring)
                .sender(*BOB)
                .event_id(event_id!("$bob_ring"))
                .server_ts(ts(before(now, 5_000))),
        )
        .await;
    // I call back: my membership, then my ring, not yet sent.
    handle_call_member(&timeline, *ALICE, before(now, 300)).await;
    timeline
        .handle_local_event(AnyMessageLikeEventContent::RtcNotification(
            RtcNotificationEventContent::new(
                ts(now),
                Duration::from_secs(30),
                NotificationType::Ring,
            ),
        ))
        .await;

    set_active_call_members(&timeline, &[User::Alice]).await;
    assert!(
        notification_call(&timeline, event_id!("$bob_ring")).await.is_none(),
        "the call is my redial's, not Bob's earlier ring"
    );
}

// --- Call outcomes do not depend on load order (decision 172, cause A) ---
//
// Calls as the timeline sees them after a restart: the last chunk first,
// older history paginated in later. T0 is an hour ago.

/// Back-pagination hands the events newest-first: a call's departures, its
/// answer, its ring, its caller's join. A newer call's ring is already
/// loaded. The old call is still answered, and ends at its last departure.
#[tokio::test]
async fn test_a_back_paginated_call_keeps_its_answer() {
    let timeline = TestTimeline::new().await;
    let t0 = before(SystemTime::now(), 3_600_000);
    load_newer_call(&timeline, t0).await;

    let f = &timeline.factory;
    push_front(
        &timeline,
        vec![
            leave_ev(f, *ALICE, after(t0, 151_000)),
            leave_ev(f, *BOB, after(t0, 150_000)),
            join_ev(f, *BOB, after(t0, 16_000)),
            ring_ev(f, event_id!("$old_call"), *ALICE, t0),
            join_ev(f, *ALICE, before(t0, 300)),
        ],
    )
    .await;

    assert_eq!(ring_answered_by(&timeline, event_id!("$old_call")).await, vec![BOB.to_owned()]);
    assert_eq!(
        ring_ended_at(&timeline, event_id!("$old_call")).await,
        Some(ts(after(t0, 151_000)))
    );
}

/// An older chunk inserted in front, in order, while a newer ring is loaded:
/// the old call's memberships belong to the old ring, not to the newest one.
#[tokio::test]
async fn test_a_newer_ring_already_loaded_does_not_swallow_older_answers() {
    let timeline = TestTimeline::new().await;
    let t0 = before(SystemTime::now(), 3_600_000);
    load_newer_call(&timeline, t0).await;

    let f = &timeline.factory;
    let older = vec![
        join_ev(f, *ALICE, before(t0, 300)),
        ring_ev(f, event_id!("$old_call"), *ALICE, t0),
        join_ev(f, *BOB, after(t0, 16_000)),
        leave_ev(f, *BOB, after(t0, 150_000)),
        leave_ev(f, *ALICE, after(t0, 151_000)),
    ];
    timeline
        .controller
        .handle_remote_events_with_diffs(
            older
                .into_iter()
                .enumerate()
                .map(|(index, value)| VectorDiff::Insert { index, value })
                .collect(),
            RemoteEventOrigin::Pagination,
        )
        .await;

    assert_eq!(ring_answered_by(&timeline, event_id!("$old_call")).await, vec![BOB.to_owned()]);
    assert_eq!(
        ring_ended_at(&timeline, event_id!("$old_call")).await,
        Some(ts(after(t0, 151_000)))
    );
    assert_eq!(ring_answered_by(&timeline, event_id!("$new_call")).await, vec![BOB.to_owned()]);
    assert_eq!(ring_ended_at(&timeline, event_id!("$new_call")).await, None, "still running");
}

/// The chunk boundary falls between a ring and its members: the answer and
/// the departures load first, the ring arrives with the next page.
#[tokio::test]
async fn test_a_ring_split_from_its_members_across_chunks() {
    let timeline = TestTimeline::new().await;
    let t0 = before(SystemTime::now(), 3_600_000);
    let f = &timeline.factory;

    push_back(
        &timeline,
        vec![
            join_ev(f, *BOB, after(t0, 16_000)),
            leave_ev(f, *BOB, after(t0, 150_000)),
            leave_ev(f, *ALICE, after(t0, 151_000)),
        ],
    )
    .await;
    push_front(
        &timeline,
        vec![ring_ev(f, event_id!("$call"), *ALICE, t0), join_ev(f, *ALICE, before(t0, 300))],
    )
    .await;

    assert_eq!(ring_answered_by(&timeline, event_id!("$call")).await, vec![BOB.to_owned()]);
    assert_eq!(ring_ended_at(&timeline, event_id!("$call")).await, Some(ts(after(t0, 151_000))));
}

/// A ring rebuilt in place (`UpdateAt`, e.g. decrypted late) keeps the
/// outcome its memberships give it.
#[tokio::test]
async fn test_a_ring_rebuilt_in_place_keeps_its_outcome() {
    let timeline = TestTimeline::new().await;
    let t0 = before(SystemTime::now(), 3_600_000);
    let f = &timeline.factory;

    let ring = ring_ev(f, event_id!("$call"), *ALICE, t0);
    push_back(
        &timeline,
        vec![
            ring.clone(),
            join_ev(f, *BOB, after(t0, 16_000)),
            leave_ev(f, *BOB, after(t0, 150_000)),
            leave_ev(f, *ALICE, after(t0, 151_000)),
        ],
    )
    .await;
    assert_eq!(ring_answered_by(&timeline, event_id!("$call")).await, vec![BOB.to_owned()]);

    timeline
        .controller
        .handle_remote_events_with_diffs(
            vec![VectorDiff::Set { index: 0, value: ring }],
            RemoteEventOrigin::Sync,
        )
        .await;

    assert_eq!(ring_answered_by(&timeline, event_id!("$call")).await, vec![BOB.to_owned()]);
    assert_eq!(ring_ended_at(&timeline, event_id!("$call")).await, Some(ts(after(t0, 151_000))));
}

/// A membership that has since expired was still a join when it was sent.
/// Reading it through "is it active now" turned yesterday's answers into
/// departures, so every answered call older than a membership's expiry
/// read as missed after a restart.
#[tokio::test]
async fn test_an_expired_membership_still_answered_its_ring() {
    let timeline = TestTimeline::new().await;
    let t0 = before(SystemTime::now(), 6 * 3_600_000);
    let f = &timeline.factory;

    push_back(
        &timeline,
        vec![
            ring_ev(f, event_id!("$call"), *ALICE, t0),
            f.call_membership_state(BOB.to_owned(), "DEVICE".to_owned())
                .created_ts(ts(after(t0, 16_000)))
                .server_ts(ts(after(t0, 16_000)))
                .into_event(),
        ],
    )
    .await;

    assert_eq!(ring_answered_by(&timeline, event_id!("$call")).await, vec![BOB.to_owned()]);
}

/// The tile carrying the running call is rebuilt in place (a late
/// decryption): it keeps the call. The rebuilt item came without it and the
/// id already pointed at it, so nothing put it back.
#[tokio::test]
async fn test_a_ringing_tile_rebuilt_in_place_keeps_the_call() {
    let timeline = TestTimeline::new().await;
    let f = &timeline.factory;
    set_active_call_members(&timeline, &[User::Alice]).await;

    let ring = ring_ev(f, event_id!("$call"), *ALICE, SystemTime::now());
    push_back(&timeline, vec![ring.clone()]).await;
    assert!(notification_call(&timeline, event_id!("$call")).await.is_some());

    timeline
        .controller
        .handle_remote_events_with_diffs(
            vec![VectorDiff::Set { index: 0, value: ring }],
            RemoteEventOrigin::Sync,
        )
        .await;
    assert!(notification_call(&timeline, event_id!("$call")).await.is_some());
}

/// D: the outcome says when the call was answered, so the app can measure
/// the call from the answer rather than from the ring.
#[tokio::test]
async fn test_the_outcome_records_when_the_call_was_answered() {
    let timeline = TestTimeline::new().await;
    let t0 = before(SystemTime::now(), 3_600_000);
    let f = &timeline.factory;

    push_back(
        &timeline,
        vec![
            join_ev(f, *ALICE, before(t0, 300)),
            ring_ev(f, event_id!("$call"), *ALICE, t0),
            join_ev(f, *BOB, after(t0, 16_000)),
            // Bob's membership renewed mid-call: not a second answer.
            join_ev(f, *BOB, after(t0, 60_000)),
            leave_ev(f, *BOB, after(t0, 150_000)),
            leave_ev(f, *ALICE, after(t0, 151_000)),
        ],
    )
    .await;

    let outcome = ring_outcome(&timeline, event_id!("$call")).await;
    assert_eq!(outcome.answered_by, vec![BOB.to_owned()]);
    assert_eq!(outcome.answered_at, Some(ts(after(t0, 16_000))));
    assert_eq!(outcome.first_joined_at, Some(ts(after(t0, 16_000))));
    assert_eq!(outcome.joined_by, vec![ALICE.to_owned(), BOB.to_owned()]);
    assert!(outcome.own_user_joined);
    assert_eq!(outcome.ended_at, Some(ts(after(t0, 151_000))));
}

/// E: a group call joined after the ring window is still a call someone
/// joined. The outcome carries who joined and when, with no ring window;
/// only `answered_by` keeps the window.
#[tokio::test]
async fn test_a_late_join_counts_without_a_ring_window() {
    let timeline = TestTimeline::new().await;
    let t0 = before(SystemTime::now(), 3_600_000);
    let f = &timeline.factory;

    push_back(
        &timeline,
        vec![
            join_ev(f, *ALICE, before(t0, 300)),
            ring_ev(f, event_id!("$call"), *ALICE, t0),
            join_ev(f, *BOB, after(t0, 73_000)),
            leave_ev(f, *BOB, after(t0, 100_000)),
            leave_ev(f, *ALICE, after(t0, 100_800)),
        ],
    )
    .await;

    let outcome = ring_outcome(&timeline, event_id!("$call")).await;
    assert!(outcome.answered_by.is_empty(), "73 s is past the 30 s ring");
    assert_eq!(outcome.answered_at, None);
    assert_eq!(outcome.joined_by, vec![ALICE.to_owned(), BOB.to_owned()]);
    assert_eq!(outcome.first_joined_at, Some(ts(after(t0, 73_000))));
    assert_eq!(outcome.ended_at, Some(ts(after(t0, 100_800))));
}

/// E: a member who never joined is told so; `answered_by` holds anyone who
/// answered, not "me".
#[tokio::test]
async fn test_the_outcome_says_whether_i_joined() {
    let timeline = TestTimeline::new().await;
    let t0 = before(SystemTime::now(), 3_600_000);
    let f = &timeline.factory;

    push_back(
        &timeline,
        vec![
            join_ev(f, *BOB, before(t0, 300)),
            ring_ev(f, event_id!("$call"), *BOB, t0),
            join_ev(f, *CAROL, after(t0, 5_000)),
            leave_ev(f, *CAROL, after(t0, 40_000)),
            leave_ev(f, *BOB, after(t0, 41_000)),
        ],
    )
    .await;

    let outcome = ring_outcome(&timeline, event_id!("$call")).await;
    assert_eq!(outcome.answered_by, vec![CAROL.to_owned()]);
    assert!(!outcome.own_user_joined, "the timeline's user, Alice, never joined");
}

/// A newer call, live: Alice rings at T0 + 10 min, Bob answers.
async fn load_newer_call(timeline: &TestTimeline, t0: SystemTime) {
    let f = &timeline.factory;
    let ring_ts = after(t0, 600_000);
    push_back(
        timeline,
        vec![
            join_ev(f, *ALICE, before(ring_ts, 300)),
            ring_ev(f, event_id!("$new_call"), *ALICE, ring_ts),
            join_ev(f, *BOB, after(ring_ts, 10_000)),
        ],
    )
    .await;
}

fn ring_ev(f: &EventFactory, id: &EventId, sender: &UserId, at: SystemTime) -> TimelineEvent {
    f.rtc_notification(NotificationType::Ring)
        .sender(sender)
        .event_id(id)
        .server_ts(ts(at))
        .into_event()
}

fn join_ev(f: &EventFactory, user: &UserId, at: SystemTime) -> TimelineEvent {
    f.call_membership_state(user.to_owned(), "DEVICE".to_owned()).server_ts(ts(at)).into_event()
}

fn leave_ev(f: &EventFactory, user: &UserId, at: SystemTime) -> TimelineEvent {
    f.call_membership_state(user.to_owned(), "DEVICE".to_owned())
        .leave()
        .server_ts(ts(at))
        .into_event()
}

async fn push_back(timeline: &TestTimeline, events: Vec<TimelineEvent>) {
    timeline
        .controller
        .handle_remote_events_with_diffs(
            events.into_iter().map(|value| VectorDiff::PushBack { value }).collect(),
            RemoteEventOrigin::Sync,
        )
        .await;
}

async fn push_front(timeline: &TestTimeline, events: Vec<TimelineEvent>) {
    timeline
        .controller
        .handle_remote_events_with_diffs(
            events.into_iter().map(|value| VectorDiff::PushFront { value }).collect(),
            RemoteEventOrigin::Pagination,
        )
        .await;
}

async fn ring_content(timeline: &TestTimeline, event_id: &EventId) -> TimelineItemContent {
    let items = timeline.controller.items().await;
    items
        .iter()
        .filter_map(|item| item.as_event())
        .find(|event| event.event_id() == Some(event_id))
        .expect("the ring is on the timeline")
        .content()
        .clone()
}

async fn ring_outcome(timeline: &TestTimeline, event_id: &EventId) -> RtcCallOutcome {
    let TimelineItemContent::RtcNotification { outcome, .. } =
        ring_content(timeline, event_id).await
    else {
        panic!("not a ring");
    };
    outcome
}

async fn ring_answered_by(timeline: &TestTimeline, event_id: &EventId) -> Vec<OwnedUserId> {
    ring_outcome(timeline, event_id).await.answered_by
}

async fn ring_ended_at(
    timeline: &TestTimeline,
    event_id: &EventId,
) -> Option<MilliSecondsSinceUnixEpoch> {
    ring_outcome(timeline, event_id).await.ended_at
}

/// The active call on the newest notification item, if any.
async fn newest_notification_call(timeline: &TestTimeline) -> Option<ActiveCallInfo> {
    let items = timeline.controller.items().await;
    let newest = items
        .iter()
        .rev()
        .filter_map(|item| item.as_event())
        .find(|event| matches!(event.content(), TimelineItemContent::RtcNotification { .. }))?;
    let TimelineItemContent::RtcNotification { active_call_info, .. } = newest.content() else {
        return None;
    };
    active_call_info.clone()
}

/// The active call on the notification item with this event id, if any.
async fn notification_call(timeline: &TestTimeline, event_id: &EventId) -> Option<ActiveCallInfo> {
    let items = timeline.controller.items().await;
    let item = items
        .iter()
        .filter_map(|item| item.as_event())
        .find(|event| event.event_id() == Some(event_id))?;
    let TimelineItemContent::RtcNotification { active_call_info, .. } = item.content() else {
        return None;
    };
    active_call_info.clone()
}

/// The timeline handles `user`'s call membership becoming active, with this
/// server timestamp.
async fn handle_call_member(timeline: &TestTimeline, user: &UserId, at: SystemTime) {
    timeline
        .handle_live_event(
            timeline
                .factory
                .call_membership_state(user.to_owned(), "DEVICE".to_owned())
                .server_ts(ts(at)),
        )
        .await;
}

fn ts(at: SystemTime) -> MilliSecondsSinceUnixEpoch {
    MilliSecondsSinceUnixEpoch::from_system_time(at).unwrap()
}

fn before(at: SystemTime, ms: u64) -> SystemTime {
    at.checked_sub(Duration::from_millis(ms)).unwrap()
}

fn after(at: SystemTime, ms: u64) -> SystemTime {
    at.checked_add(Duration::from_millis(ms)).unwrap()
}

async fn set_active_call_members(timeline: &TestTimeline, members: &[User]) {
    let active_call = ActiveCallInfo {
        active_members: members.iter().copied().collect(),
        call_intent: CallIntentConsensus::None,
        is_joined: false,
        call_started_ts_millis: MilliSecondsSinceUnixEpoch::now().into(),
    };
    timeline.controller.handle_active_call_update(Some(active_call)).await;
}
