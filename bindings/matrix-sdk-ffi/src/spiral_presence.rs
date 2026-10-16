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

// Spiral (Brief 0072, ADR-0083): reading another user's presence.
//
// The FFI had `Client::set_presence` and nothing to READ presence with.
// Sliding sync (MSC4186) carries `set_presence` for one's own state but
// has no presence extension yet — it is listed in the MSC as "to be
// specified in a future MSC" — so a sliding sync client never receives
// `m.presence` for others. Until that extension exists, the only
// spec-stable way to learn who is online is `GET
// /presence/{userId}/status`, asked for the users the client is showing.
// This file exports that one call; the polling policy (who, how often,
// not in the background) is the client's. When the extension lands, the
// client swaps this source for the stream and keeps its UI.
//
// Privacy (ADR-0084): Spiral's Synapse answers a pair where either side has
// turned sharing off with only `presence: offline` and a coarse
// `com.spiral.last_seen` bucket, as Telegram does. The stock ruma response
// type drops unknown fields, so the request is declared here with that one
// field added.
//
// Kept beside `spiral_apps.rs` / `spiral_agents.rs` (Brief 0063 posture:
// what this fork adds lives in modules of its own, never in upstream's
// files).

use matrix_sdk::ruma::UserId;

use crate::{client::Client, error::ClientError, ruma::PresenceState};

/// `GET /presence/{userId}/status`, as ruma's `get_presence::v3` plus the
/// `com.spiral.last_seen` field. Written by hand: ruma's `#[request]` /
/// `#[response]` macros need `client`/`server` features in the calling
/// crate, which this one does not define.
mod spiral_get_presence {
    use std::time::Duration;

    use matrix_sdk::ruma::{
        OwnedUserId,
        api::{
            EmptyBody, EndpointError, IncomingResponse, Metadata, OutgoingRequest,
            auth_scheme::AccessToken,
            error::{Error as ClientApiError, FromHttpResponseError, IntoHttpError},
            path_builder::PathBuilder,
        },
        exports::{
            http::{Request, Response},
            serde_json,
        },
        metadata,
        presence::PresenceState,
    };
    use serde::Deserialize;

    metadata! {
        @for PresenceRequest,
        method: GET,
        rate_limited: false,
        authentication: AccessToken,
        history: {
            1.0 => "/_matrix/client/r0/presence/{user_id}/status",
            1.1 => "/_matrix/client/v3/presence/{user_id}/status",
        }
    }

    #[derive(Debug, Clone)]
    pub(super) struct PresenceRequest {
        pub(super) user_id: OwnedUserId,
    }

    impl OutgoingRequest for PresenceRequest {
        type Body = EmptyBody;
        type EndpointError = ClientApiError;
        type IncomingResponse = PresenceResponse;

        fn try_into_http_request_inner(
            self,
            base_url: &str,
            path_builder_input: <Self::PathBuilder as PathBuilder>::Input<'_>,
        ) -> Result<Request<Self::Body>, IntoHttpError> {
            let url = Self::make_endpoint_url(path_builder_input, base_url, &[&self.user_id], "")?;
            Ok(Request::builder().method(Self::METHOD).uri(url).body(EmptyBody)?)
        }
    }

    #[derive(Debug, Deserialize)]
    pub(super) struct PresenceResponse {
        #[serde(default)]
        pub(super) status_msg: Option<String>,
        #[serde(default)]
        pub(super) currently_active: Option<bool>,
        #[serde(default, with = "matrix_sdk::ruma::serde::duration::opt_ms")]
        pub(super) last_active_ago: Option<Duration>,
        pub(super) presence: PresenceState,
        #[serde(default, rename = "com.spiral.last_seen")]
        pub(super) last_seen: Option<String>,
    }

    impl IncomingResponse for PresenceResponse {
        type EndpointError = ClientApiError;

        fn try_from_http_response<T: AsRef<[u8]>>(
            response: Response<T>,
        ) -> Result<Self, FromHttpResponseError<Self::EndpointError>> {
            // As ruma's generated responses do: an error status is the
            // server's error, parsed as a client API error (403 stays the
            // typed `M_FORBIDDEN`).
            if response.status().as_u16() >= 400 {
                return Err(FromHttpResponseError::Server(ClientApiError::from_http_response(
                    response,
                )));
            }
            Ok(serde_json::from_slice(response.body().as_ref())?)
        }
    }
}

/// When a user was last seen, as coarsely as the server will say it to
/// someone who may not know exactly (ADR-0084): Telegram's buckets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, uniffi::Enum)]
pub enum PresenceLastSeen {
    /// Up to about three days ago, or online now.
    Recently,
    WithinWeek,
    WithinMonth,
    LongAgo,
}

impl PresenceLastSeen {
    fn from_wire(value: &str) -> Option<Self> {
        match value {
            "recently" => Some(Self::Recently),
            "within_week" => Some(Self::WithinWeek),
            "within_month" => Some(Self::WithinMonth),
            "long_ago" => Some(Self::LongAgo),
            _ => None,
        }
    }
}

/// One user's presence as the homeserver reports it.
#[derive(Debug, Clone, uniffi::Record)]
pub struct UserPresence {
    pub user_id: String,
    pub presence: PresenceState,
    pub status_msg: Option<String>,
    /// Milliseconds since the user's last "pro-active" action as the server
    /// counts them (a message, an explicit presence change, a sync that
    /// marks them online). Measured when the server answered: a caller that
    /// shows "N minutes ago" later must add the time since the answer.
    pub last_active_ago_ms: Option<u64>,
    /// Whether the user has been active within the server's
    /// `last_active_granularity` (Synapse: 60 s). The closest thing Matrix
    /// has to a messenger's green dot.
    pub currently_active: Option<bool>,
    /// Set when either side hides their presence (ADR-0084): then the server
    /// gives no `last_active_ago`, no `currently_active`, presence `offline`,
    /// and this is all a caller may show. `None` for an exact answer — and
    /// for a bucket these bindings do not know.
    pub last_seen: Option<PresenceLastSeen>,
}

#[matrix_sdk_ffi_macros::export]
impl Client {
    /// `GET /_matrix/client/v3/presence/{userId}/status`.
    ///
    /// Synapse answers `403 M_FORBIDDEN` ("You are not allowed to see their
    /// presence") for a user with whom the caller shares no room — and the
    /// same for an unknown user. A caller should treat it as "not now",
    /// not as "never": a shared room can appear later. Returned as
    /// [`ClientError::MatrixApi`] with `ErrorKind::Forbidden`.
    pub async fn get_user_presence(&self, user_id: String) -> Result<UserPresence, ClientError> {
        let user_id = UserId::parse(&user_id)?;
        // Through `matrix_sdk::Error`, not `HttpError`: only that conversion
        // keeps the server's errcode as `ClientError::MatrixApi`.
        let response = self
            .inner
            .send(spiral_get_presence::PresenceRequest { user_id: user_id.to_owned() })
            .await
            .map_err(matrix_sdk::Error::from)?;

        Ok(UserPresence {
            user_id: user_id.to_string(),
            presence: response.presence.into(),
            status_msg: response.status_msg,
            last_active_ago_ms: response
                .last_active_ago
                .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
            currently_active: response.currently_active,
            last_seen: response.last_seen.as_deref().and_then(PresenceLastSeen::from_wire),
        })
    }
}

#[cfg(test)]
mod tests {
    use matrix_sdk::test_utils::mocks::MatrixMockServer;
    use matrix_sdk_common::cross_process_lock::CrossProcessLockConfig;
    use serde_json::json;
    use wiremock::{
        Mock, ResponseTemplate,
        matchers::{method, path},
    };

    use super::*;
    use crate::error::ErrorKind;

    async fn client_for(server: &MatrixMockServer) -> Client {
        // The FFI `Client` wants session delegates under the multi-process
        // lock the mock builder defaults to; a single-process store needs none.
        let sdk = server
            .client_builder()
            .on_builder(|b| b.cross_process_store_config(CrossProcessLockConfig::SingleProcess))
            .build()
            .await;
        Client::new(sdk, None, None).await.unwrap()
    }

    #[tokio::test]
    async fn an_online_user_is_read_with_every_field() {
        let server = MatrixMockServer::new().await;
        Mock::given(method("GET"))
            .and(path("/_matrix/client/v3/presence/@alice:example.org/status"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "presence": "online",
                "last_active_ago": 2500,
                "currently_active": true,
                "status_msg": "in a call"
            })))
            .mount(server.server())
            .await;
        let client = client_for(&server).await;

        let presence = client.get_user_presence("@alice:example.org".to_owned()).await.unwrap();

        assert_eq!(presence.user_id, "@alice:example.org");
        assert_eq!(presence.presence, PresenceState::Online);
        assert_eq!(presence.last_active_ago_ms, Some(2500));
        assert_eq!(presence.currently_active, Some(true));
        assert_eq!(presence.status_msg.as_deref(), Some("in a call"));
        assert_eq!(presence.last_seen, None, "an exact answer has no bucket");
    }

    /// Synapse's offline answer has no `currently_active`; the fields that are
    /// not there stay `None` rather than becoming a guess.
    #[tokio::test]
    async fn an_offline_user_keeps_the_absent_fields_absent() {
        let server = MatrixMockServer::new().await;
        Mock::given(method("GET"))
            .and(path("/_matrix/client/v3/presence/@bob:example.org/status"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "presence": "offline",
                "last_active_ago": 4446167
            })))
            .mount(server.server())
            .await;
        let client = client_for(&server).await;

        let presence = client.get_user_presence("@bob:example.org".to_owned()).await.unwrap();

        assert_eq!(presence.presence, PresenceState::Offline);
        assert_eq!(presence.last_active_ago_ms, Some(4446167));
        assert_eq!(presence.currently_active, None);
        assert_eq!(presence.status_msg, None);
    }

    /// No shared room (or no such user): the refusal reaches the caller as
    /// the typed `Forbidden`, so it can back off instead of retrying blindly.
    #[tokio::test]
    async fn a_forbidden_answer_is_the_typed_error() {
        let server = MatrixMockServer::new().await;
        Mock::given(method("GET"))
            .and(path("/_matrix/client/v3/presence/@stranger:example.org/status"))
            .respond_with(ResponseTemplate::new(403).set_body_json(json!({
                "errcode": "M_FORBIDDEN",
                "error": "You are not allowed to see their presence."
            })))
            .mount(server.server())
            .await;
        let client = client_for(&server).await;

        let error = client.get_user_presence("@stranger:example.org".to_owned()).await.unwrap_err();

        match error {
            ClientError::MatrixApi { kind, code, .. } => {
                assert_eq!(kind, ErrorKind::Forbidden);
                assert_eq!(code, "M_FORBIDDEN");
            }
            other => panic!("expected MatrixApi, got {other:?}"),
        }
    }

    /// A state these bindings do not model (MSC3026 `busy`) reads as the
    /// bindings' default, `Unavailable` — never as `Online`.
    #[tokio::test]
    async fn an_unknown_state_is_not_online() {
        let server = MatrixMockServer::new().await;
        Mock::given(method("GET"))
            .and(path("/_matrix/client/v3/presence/@carol:example.org/status"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "presence": "busy" })))
            .mount(server.server())
            .await;
        let client = client_for(&server).await;

        let presence = client.get_user_presence("@carol:example.org".to_owned()).await.unwrap();

        assert_eq!(presence.presence, PresenceState::Unavailable);
    }

    /// A hidden pair (ADR-0084): offline and a bucket, nothing exact.
    #[tokio::test]
    async fn a_hidden_user_reads_as_the_bucket_alone() {
        let server = MatrixMockServer::new().await;
        Mock::given(method("GET"))
            .and(path("/_matrix/client/v3/presence/@dave:example.org/status"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "presence": "offline",
                "com.spiral.last_seen": "within_week"
            })))
            .mount(server.server())
            .await;
        let client = client_for(&server).await;

        let presence = client.get_user_presence("@dave:example.org".to_owned()).await.unwrap();

        assert_eq!(presence.presence, PresenceState::Offline);
        assert_eq!(presence.last_seen, Some(PresenceLastSeen::WithinWeek));
        assert_eq!(presence.last_active_ago_ms, None);
        assert_eq!(presence.currently_active, None);
    }

    /// Every bucket the server sends maps, and one it might send tomorrow
    /// does not become a guess.
    #[test]
    fn the_buckets_map_and_an_unknown_one_does_not() {
        assert_eq!(PresenceLastSeen::from_wire("recently"), Some(PresenceLastSeen::Recently));
        assert_eq!(PresenceLastSeen::from_wire("within_week"), Some(PresenceLastSeen::WithinWeek));
        assert_eq!(
            PresenceLastSeen::from_wire("within_month"),
            Some(PresenceLastSeen::WithinMonth)
        );
        assert_eq!(PresenceLastSeen::from_wire("long_ago"), Some(PresenceLastSeen::LongAgo));
        assert_eq!(PresenceLastSeen::from_wire("someday"), None);
    }

    #[tokio::test]
    async fn a_malformed_user_id_fails_before_any_request() {
        let server = MatrixMockServer::new().await;
        let client = client_for(&server).await;

        assert!(client.get_user_presence("not a user id".to_owned()).await.is_err());
    }
}
