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

//! Distribution of the per-participant media keys of a MatrixRTC session.
//!
//! Keys travel as Olm-encrypted to-device events of type
//! [`ENCRYPTION_KEYS_EVENT_TYPE`], the transport Element Call and the Spiral
//! voice agent already speak. The key bytes never leave this module in
//! plaintext form except through the typed [`RtcEncryptionKey`], which is
//! zeroized on drop and redacted in `Debug` output.

use std::fmt;

use ruma::{
    DeviceId, MilliSecondsSinceUnixEpoch, OwnedDeviceId, OwnedUserId, RoomId, UInt, UserId,
    serde::Base64,
};
use serde::{Deserialize, Serialize};
use tracing::warn;
use zeroize::{Zeroize, ZeroizeOnDrop};

use super::membership::{CALL_APPLICATION, ROOM_SCOPE, membership_id};

/// To-device event type carrying MatrixRTC media keys.
pub const ENCRYPTION_KEYS_EVENT_TYPE: &str = "io.element.call.encryption_keys";

/// Length in bytes of a media key. LiveKit derives AES-128-GCM from it.
pub const MEDIA_KEY_LENGTH: usize = 16;

/// A media key together with the index it occupies in the key ring.
#[derive(Clone, PartialEq, Eq, Zeroize, ZeroizeOnDrop)]
pub struct RtcEncryptionKey {
    /// Position of the key in the ring, `0..=255`.
    #[zeroize(skip)]
    pub index: u8,

    /// The raw key material.
    pub key: Vec<u8>,
}

impl RtcEncryptionKey {
    /// Generate a fresh random key for the given index.
    pub fn generate(index: u8) -> Self {
        use rand::Rng as _;

        let mut key = vec![0u8; MEDIA_KEY_LENGTH];
        rand::rng().fill_bytes(&mut key);

        Self { index, key }
    }
}

impl fmt::Debug for RtcEncryptionKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RtcEncryptionKey")
            .field("index", &self.index)
            .finish_non_exhaustive()
    }
}

/// A media key received from another participant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceivedRtcEncryptionKey {
    /// The Matrix user that sent the key.
    pub user_id: OwnedUserId,

    /// The device that sent the key.
    pub device_id: OwnedDeviceId,

    /// The participant identity the key must be applied to.
    pub participant_identity: String,

    /// The key and its index.
    pub key: RtcEncryptionKey,

    /// When the sender claims to have sent the key. Used to drop keys that
    /// arrive out of order.
    pub sent_ts: Option<MilliSecondsSinceUnixEpoch>,
}

/// Wire content of an [`ENCRYPTION_KEYS_EVENT_TYPE`] event.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct EncryptionKeysContent {
    pub keys: KeyEntry,
    pub member: MemberEntry,
    pub room_id: String,
    pub session: SessionEntry,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sent_ts: Option<MilliSecondsSinceUnixEpoch>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct KeyEntry {
    pub index: UInt,
    /// The media key, serialized as **padded** standard base64.
    ///
    /// This event belongs to Element Call, and its reference implementation
    /// (matrix-js-sdk's `encodeBase64`) pads. Ruma's [`Base64`] always encodes
    /// unpadded, following the Matrix spec convention, so serializing it
    /// directly puts a spelling on the wire that a strict consumer rejects —
    /// and a dropped media key is not a loud failure: the receiver's
    /// `FrameCryptor` reports a missing key and every frame we send decodes to
    /// silence. Deserialization needs no such care because ruma decodes with
    /// `DecodePaddingMode::Indifferent` and accepts either spelling.
    #[serde(serialize_with = "serialize_padded_base64")]
    pub key: Base64,
}

/// Serialize a [`Base64`] with the trailing `=` padding that Element Call and
/// the Spiral voice agent expect. See [`KeyEntry::key`].
fn serialize_padded_base64<S>(key: &Base64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let mut encoded = key.encode();
    // Standard base64 encodes to a multiple of four characters.
    while encoded.len() % 4 != 0 {
        encoded.push('=');
    }

    serializer.serialize_str(&encoded)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct MemberEntry {
    pub claimed_device_id: OwnedDeviceId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct SessionEntry {
    pub application: String,
    pub call_id: String,
    pub scope: String,
}

impl EncryptionKeysContent {
    pub(super) fn new(
        user_id: &UserId,
        device_id: &DeviceId,
        room_id: &RoomId,
        key: &RtcEncryptionKey,
    ) -> Self {
        Self {
            keys: KeyEntry { index: key.index.into(), key: Base64::new(key.key.clone()) },
            member: MemberEntry {
                claimed_device_id: device_id.to_owned(),
                id: Some(membership_id(user_id, device_id)),
            },
            room_id: room_id.to_string(),
            session: SessionEntry {
                application: CALL_APPLICATION.to_owned(),
                call_id: String::new(),
                scope: ROOM_SCOPE.to_owned(),
            },
            sent_ts: Some(MilliSecondsSinceUnixEpoch::now()),
        }
    }
}

/// Parse a received key, rejecting anything that does not belong to this room,
/// does not carry usable key material, or claims a device the Olm session does
/// not back.
///
/// `sending_device_id` is the device the crypto layer authenticated the
/// to-device message as coming from. It is REQUIRED to match the device the
/// payload claims: `member.device_id` is self-asserted, so without this check
/// any device of a participating user could publish a key under a SIBLING
/// device's identity — the receiver would install it against that identity in
/// the frame cryptor and stop being able to decrypt the real device.
pub(super) fn parse_received_key(
    sender: &UserId,
    sending_device_id: Option<&DeviceId>,
    room_id: &RoomId,
    content: EncryptionKeysContent,
) -> Option<ReceivedRtcEncryptionKey> {
    if content.room_id != room_id.as_str() {
        return None;
    }

    if let Some(sending_device_id) = sending_device_id
        && content.member.claimed_device_id != sending_device_id
    {
        warn!(
            %sender,
            claimed = %content.member.claimed_device_id,
            "Dropping a MatrixRTC media key whose claimed device is not the sending device"
        );
        return None;
    }

    if content.session.application != CALL_APPLICATION {
        return None;
    }

    let index: u64 = content.keys.index.into();
    let index = u8::try_from(index).ok()?;

    let key = content.keys.key.into_inner();
    if key.len() != MEDIA_KEY_LENGTH {
        return None;
    }

    // Both halves of the identity are now authenticated: the user id by Olm,
    // the device id by the check above.
    let device_id = content.member.claimed_device_id;
    let participant_identity = membership_id(sender, &device_id);

    Some(ReceivedRtcEncryptionKey {
        user_id: sender.to_owned(),
        device_id,
        participant_identity,
        key: RtcEncryptionKey { index, key },
        sent_ts: content.sent_ts,
    })
}

#[cfg(test)]
mod tests {
    use ruma::{device_id, room_id, user_id};
    use serde_json::json;

    use super::*;

    #[test]
    fn serialized_key_is_padded_like_element_call() {
        let content = EncryptionKeysContent::new(
            user_id!("@alice:localhost"),
            device_id!("DEVICE"),
            room_id!("!room:localhost"),
            &RtcEncryptionKey { index: 0, key: vec![7u8; MEDIA_KEY_LENGTH] },
        );

        let serialized = serde_json::to_value(&content).expect("serializes");
        let encoded = serialized["keys"]["key"].as_str().expect("key is a string");

        // 16 bytes encode to 22 significant characters plus "==". Without the
        // padding a strict decoder drops the key and the call goes silent.
        assert!(encoded.ends_with("=="), "expected padded base64, got {encoded}");
        assert_eq!(encoded.len() % 4, 0);
    }

    #[test]
    fn received_key_is_accepted_with_or_without_padding() {
        let key = vec![7u8; MEDIA_KEY_LENGTH];
        let sent = EncryptionKeysContent::new(
            user_id!("@alice:localhost"),
            device_id!("DEVICE"),
            room_id!("!room:localhost"),
            &RtcEncryptionKey { index: 0, key: key.clone() },
        );
        let padded = serde_json::to_value(&sent).expect("serializes")["keys"]["key"]
            .as_str()
            .expect("key is a string")
            .to_owned();
        let unpadded = padded.trim_end_matches('=').to_owned();
        assert_ne!(padded, unpadded);

        for spelling in [padded, unpadded] {
            let content: EncryptionKeysContent = serde_json::from_value(json!({
                "keys": { "index": 0, "key": spelling },
                "member": { "claimed_device_id": "DEVICE" },
                "room_id": "!room:localhost",
                "session": { "application": "m.call", "call_id": "", "scope": "m.room" },
            }))
            .expect("deserializes");

            let parsed = parse_received_key(
                user_id!("@bob:localhost"),
                Some(device_id!("DEVICE")),
                room_id!("!room:localhost"),
                content,
            )
            .expect("parses");
            assert_eq!(parsed.key.key, key);
        }
    }

    #[test]
    fn key_debug_output_is_redacted() {
        let key = RtcEncryptionKey { index: 3, key: vec![7u8; MEDIA_KEY_LENGTH] };
        let debug = format!("{key:?}");

        assert_eq!(debug, "RtcEncryptionKey { index: 3, .. }");
        assert!(!debug.contains('7'));
    }

    #[test]
    fn content_serialises_to_the_element_call_shape() {
        let key = RtcEncryptionKey { index: 0, key: vec![0u8; MEDIA_KEY_LENGTH] };
        let mut content = EncryptionKeysContent::new(
            user_id!("@alice:example.org"),
            device_id!("DEVICEID"),
            room_id!("!room:example.org"),
            &key,
        );
        content.sent_ts = Some(MilliSecondsSinceUnixEpoch(UInt::from(
            1_700_000_000_000u64.try_into().unwrap_or(u32::MAX),
        )));

        let value = serde_json::to_value(&content).unwrap();
        assert_eq!(value["keys"]["index"], json!(0));
        // Padded, like matrix-js-sdk's `encodeBase64`. This assertion used to
        // expect ruma's unpadded spelling, which is what made the native
        // client inaudible to strict consumers such as the voice agent.
        assert_eq!(value["keys"]["key"], json!("AAAAAAAAAAAAAAAAAAAAAA=="));
        assert_eq!(value["member"]["claimed_device_id"], json!("DEVICEID"));
        assert_eq!(value["member"]["id"], json!("@alice:example.org:DEVICEID"));
        assert_eq!(value["room_id"], json!("!room:example.org"));
        assert_eq!(
            value["session"],
            json!({ "application": "m.call", "call_id": "", "scope": "m.room" })
        );
    }

    #[test]
    fn received_identity_comes_from_the_authenticated_sender() {
        let content: EncryptionKeysContent = serde_json::from_value(json!({
            "keys": { "index": 1, "key": "AAAAAAAAAAAAAAAAAAAAAA" },
            // A malicious sender claims to be somebody else.
            "member": { "claimed_device_id": "DEVICEID", "id": "@mallory:example.org:OTHER" },
            "room_id": "!room:example.org",
            "session": { "application": "m.call", "call_id": "", "scope": "m.room" },
        }))
        .unwrap();

        let received = parse_received_key(
            user_id!("@bob:example.org"),
            Some(device_id!("DEVICEID")),
            room_id!("!room:example.org"),
            content,
        )
        .unwrap();

        assert_eq!(received.participant_identity, "@bob:example.org:DEVICEID");
        assert_eq!(received.key.index, 1);
    }

    #[test]
    fn keys_for_another_room_are_rejected() {
        let content: EncryptionKeysContent = serde_json::from_value(json!({
            "keys": { "index": 0, "key": "AAAAAAAAAAAAAAAAAAAAAA" },
            "member": { "claimed_device_id": "DEVICEID" },
            "room_id": "!other:example.org",
            "session": { "application": "m.call", "call_id": "", "scope": "m.room" },
        }))
        .unwrap();

        assert!(
            parse_received_key(
                user_id!("@bob:example.org"),
                Some(device_id!("DEVICEID")),
                room_id!("!room:example.org"),
                content
            )
            .is_none()
        );
    }

    #[test]
    fn short_keys_are_rejected() {
        let content: EncryptionKeysContent = serde_json::from_value(json!({
            "keys": { "index": 0, "key": "AAAA" },
            "member": { "claimed_device_id": "DEVICEID" },
            "room_id": "!room:example.org",
            "session": { "application": "m.call", "call_id": "", "scope": "m.room" },
        }))
        .unwrap();

        assert!(
            parse_received_key(
                user_id!("@bob:example.org"),
                Some(device_id!("DEVICEID")),
                room_id!("!room:example.org"),
                content
            )
            .is_none()
        );
    }

    /// The device half of the identity is self-asserted in the payload. If it
    /// is not checked against the Olm-authenticated sending device, any device
    /// of a participating user can publish a key under a SIBLING device's
    /// identity: the receiver installs it for that identity and can no longer
    /// decrypt the real device (and would decrypt the attacker's media as if
    /// it came from the victim).
    #[test]
    fn a_key_claiming_another_device_of_the_same_user_is_rejected() {
        let content: EncryptionKeysContent = serde_json::from_value(json!({
            "keys": { "index": 0, "key": "AAAAAAAAAAAAAAAAAAAAAA" },
            "member": { "claimed_device_id": "VICTIMDEVICE" },
            "room_id": "!room:example.org",
            "session": { "application": "m.call", "call_id": "", "scope": "m.room" },
        }))
        .unwrap();

        assert!(
            parse_received_key(
                user_id!("@bob:example.org"),
                // Olm says the message came from a DIFFERENT device.
                Some(device_id!("ATTACKERDEVICE")),
                room_id!("!room:example.org"),
                content
            )
            .is_none()
        );
    }

    /// An unknown sending device (the crypto layer could not attribute one)
    /// leaves the claim unverifiable; the key still parses, because dropping
    /// it would silence the call, and the `EncryptionInfo` null-check upstream
    /// of this already rejects unencrypted keys.
    #[test]
    fn an_unattributed_sending_device_still_parses() {
        let content: EncryptionKeysContent = serde_json::from_value(json!({
            "keys": { "index": 0, "key": "AAAAAAAAAAAAAAAAAAAAAA" },
            "member": { "claimed_device_id": "DEVICEID" },
            "room_id": "!room:example.org",
            "session": { "application": "m.call", "call_id": "", "scope": "m.room" },
        }))
        .unwrap();

        let received = parse_received_key(
            user_id!("@bob:example.org"),
            None,
            room_id!("!room:example.org"),
            content,
        )
        .expect("parses");
        assert_eq!(received.participant_identity, "@bob:example.org:DEVICEID");
    }
}
