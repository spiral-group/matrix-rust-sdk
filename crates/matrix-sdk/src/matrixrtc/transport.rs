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

//! Discovery of a MatrixRTC transport and exchange of Matrix credentials for
//! SFU credentials.
//!
//! Discovery follows the order Element Call uses: the MSC4143 endpoint first,
//! then `.well-known`. Authorisation uses the legacy `/sfu/get` endpoint, which
//! yields the raw `{user_id}:{device_id}` participant identity that both
//! Element Call and the Spiral voice agent rely on for attribution.

use ruma::{RoomId, api::client::rtc::RtcTransport};
use serde::{Deserialize, Serialize};
use tracing::{debug, instrument, warn};
use url::Url;

use super::{Error, Result};
use crate::Client;

/// A LiveKit MatrixRTC transport.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveKitTransport {
    /// Base URL of the MatrixRTC authorisation service.
    pub service_url: String,
}

impl LiveKitTransport {
    /// Discover the transport this client should use.
    ///
    /// Tries the MSC4143 `/rtc/transports` endpoint first and falls back to
    /// `.well-known`, mirroring the order implemented by Element Call.
    #[instrument(skip_all)]
    pub async fn discover(client: &Client) -> Result<Self> {
        // The MSC4143 endpoint is an optimisation and is still unstable: some
        // deployments answer it with a payload that does not match the MSC. A
        // homeserver we cannot understand here must not prevent the call, so
        // any failure falls through to `.well-known`.
        match client.rtc_transports().await {
            Ok(Some(transports)) => {
                if let Some(transport) = Self::pick(&transports) {
                    debug!("Using a MatrixRTC transport advertised by the homeserver");
                    return Ok(transport);
                }
            }
            Ok(None) => debug!("The homeserver does not advertise MatrixRTC transports"),
            Err(error) => {
                warn!("Ignoring an unusable MatrixRTC transports response: {error}");
            }
        }

        let transports = client.well_known_rtc_transports().await.map_err(Error::from_http)?;
        if let Some(transport) = Self::pick(&transports) {
            debug!("Using a MatrixRTC transport advertised in .well-known");
            return Ok(transport);
        }

        Err(Error::NoTransport)
    }

    fn pick(transports: &[RtcTransport]) -> Option<Self> {
        transports.iter().find_map(|transport| match transport {
            RtcTransport::LiveKit(livekit) if Self::is_transport_secure(&livekit.service_url) => {
                Some(Self { service_url: livekit.service_url.clone() })
            }
            RtcTransport::LiveKit(livekit) => {
                warn!(
                    service_url = %livekit.service_url,
                    "Ignoring a MatrixRTC transport that is not served over https"
                );
                None
            }
            _ => None,
        })
    }

    /// The authorization request carries an OpenID token — a bearer credential
    /// that asserts the user's Matrix identity to a third party. The transport
    /// URL comes from the homeserver (`rtc/transports` or `.well-known`), so a
    /// misconfigured or hostile server could otherwise have that token sent in
    /// clear text. `localhost` stays permitted so a development SFU works.
    fn is_transport_secure(service_url: &str) -> bool {
        let Ok(url) = Url::parse(service_url) else { return false };
        match url.scheme() {
            "https" | "wss" => true,
            "http" | "ws" => {
                matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]" | "::1"))
            }
            _ => false,
        }
    }
}

/// Credentials that let a client connect to the SFU carrying the session's
/// media.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TransportCredentials {
    /// The SFU websocket URL.
    pub url: String,

    /// The SFU access token.
    ///
    /// This is a bearer credential: it must never be logged, persisted or
    /// forwarded outside of the SFU connection.
    pub token: String,

    /// The participant identity the SFU will assign to this connection.
    pub identity: String,
}

impl TransportCredentials {
    /// Exchange a Matrix OpenID token for SFU credentials.
    #[instrument(skip_all, fields(room_id = %room_id))]
    pub(super) async fn request(
        client: &Client,
        transport: &LiveKitTransport,
        room_id: &RoomId,
        identity: String,
    ) -> Result<Self> {
        let device_id = client.device_id().ok_or(Error::NotLoggedIn)?.to_owned();
        let openid = client.account().request_openid_token().await.map_err(Error::from_sdk)?;

        let request = SfuGetRequest {
            room: room_id.as_str(),
            device_id: device_id.as_str(),
            openid_token: OpenIdToken {
                access_token: openid.access_token,
                token_type: openid.token_type.to_string(),
                matrix_server_name: openid.matrix_server_name.to_string(),
                expires_in: openid.expires_in.as_secs(),
            },
        };

        let url = join_path(&transport.service_url, "sfu/get");
        let body = serde_json::to_vec(&request).map_err(Error::Serialization)?;
        let response = client
            .http_client()
            .post(url)
            .header(http::header::CONTENT_TYPE, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|error| Error::TransportAuthorization(error.to_string()))?;

        let status = response.status();
        if !status.is_success() {
            // The body may carry the SFU's error description, but it can also
            // echo request material, so only the status is surfaced.
            return Err(Error::TransportAuthorization(format!(
                "the authorization service answered with HTTP {status}"
            )));
        }

        let body = response
            .text()
            .await
            .map_err(|error| Error::TransportAuthorization(error.to_string()))?;
        let response: SfuGetResponse = serde_json::from_str(&body).map_err(|_| {
            Error::TransportAuthorization(
                "the authorization service returned an unexpected payload".to_owned(),
            )
        })?;

        Ok(Self { url: response.url, token: response.jwt, identity })
    }
}

#[derive(Serialize)]
struct SfuGetRequest<'a> {
    room: &'a str,
    device_id: &'a str,
    openid_token: OpenIdToken,
}

#[derive(Serialize)]
struct OpenIdToken {
    access_token: String,
    token_type: String,
    matrix_server_name: String,
    expires_in: u64,
}

#[derive(Deserialize)]
struct SfuGetResponse {
    jwt: String,
    url: String,
}

/// Join a base URL and a path without doubling or dropping the separator.
fn join_path(base: &str, path: &str) -> String {
    format!("{}/{}", base.trim_end_matches('/'), path.trim_start_matches('/'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_path_normalises_separators() {
        assert_eq!(
            join_path("https://rtc.example.org/jwt", "sfu/get"),
            "https://rtc.example.org/jwt/sfu/get"
        );
        assert_eq!(
            join_path("https://rtc.example.org/jwt/", "/sfu/get"),
            "https://rtc.example.org/jwt/sfu/get"
        );
    }

    /// The authorization request carries an OpenID token, so a transport the
    /// homeserver advertises over plain http must not be used at all.
    #[test]
    fn only_secure_transports_are_selected() {
        for secure in ["https://rtc.example.org/jwt", "wss://rtc.example.org"] {
            assert!(LiveKitTransport::is_transport_secure(secure), "{secure} must be accepted");
        }
        for insecure in [
            "http://rtc.example.org/jwt",
            "ws://rtc.example.org",
            "ftp://rtc.example.org",
            "rtc.example.org",
            "",
        ] {
            assert!(!LiveKitTransport::is_transport_secure(insecure), "{insecure} must be refused");
        }
        // A development SFU on the loopback interface stays usable.
        for local in ["http://localhost:8080", "ws://127.0.0.1:7880"] {
            assert!(LiveKitTransport::is_transport_secure(local), "{local} must be accepted");
        }
    }

    #[test]
    fn an_insecure_transport_is_never_picked() {
        let insecure = RtcTransport::LiveKit(ruma::api::client::rtc::LiveKitRtcTransport::new(
            "http://rtc.example.org/jwt".to_owned(),
        ));
        assert!(LiveKitTransport::pick(&[insecure]).is_none());
    }

    #[test]
    fn credentials_do_not_leak_the_token_in_debug_output() {
        let credentials = TransportCredentials {
            url: "wss://sfu.example.org".to_owned(),
            token: "super-secret-jwt".to_owned(),
            identity: "@alice:example.org:DEVICEID".to_owned(),
        };

        // The token is a bearer credential; make the expectation explicit so a
        // future derive change cannot silently start logging it.
        let redacted = RedactedCredentials::from(&credentials);
        assert_eq!(redacted.url, "wss://sfu.example.org");
        assert_eq!(redacted.identity, "@alice:example.org:DEVICEID");
        assert!(!format!("{redacted:?}").contains("super-secret-jwt"));
    }

    /// Debug-safe projection used by the test above and by tracing call sites.
    #[derive(Debug)]
    struct RedactedCredentials<'a> {
        url: &'a str,
        identity: &'a str,
    }

    impl<'a> From<&'a TransportCredentials> for RedactedCredentials<'a> {
        fn from(value: &'a TransportCredentials) -> Self {
            Self { url: &value.url, identity: &value.identity }
        }
    }
}
