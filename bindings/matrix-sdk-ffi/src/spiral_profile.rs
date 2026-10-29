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

// Spiral (Brief 0082, decision 284): the own user's custom profile fields.
//
// The FFI had `Client::extended_profile_fields` and nothing to write a field
// with; `matrix_sdk::Account` has `set_profile_field`,
// `delete_profile_field` and `fetch_profile_field_of` (MSC4133). Spiral's
// feed shows an author's bio from the string field `io.spiral.bio`, so this
// file exports the string-valued subset of those three for the own user,
// plus the capability check the app gates its editor on.
//
// ruma sends a custom field to `/_matrix/client/v3/profile/...` when the
// server advertises Matrix 1.16 or `uk.tcpip.msc4133.stable` (Synapse 1.158
// does the latter), and to the `uk.tcpip.msc4133` unstable prefix otherwise.
//
// Kept beside `spiral_presence.rs` (Brief 0063 posture: what this fork adds
// lives in modules of its own, never in upstream's files).

use matrix_sdk::ruma::{
    api::error::ErrorKind,
    profile::{ProfileFieldName, ProfileFieldValue},
};
use serde_json::Value as JsonValue;

use crate::{client::Client, error::ClientError};

#[matrix_sdk_ffi_macros::export]
impl Client {
    /// Whether the homeserver lets this user set the profile field `field`
    /// (the `m.profile_fields` capability: `enabled`, then `allowed`, then
    /// `disallowed`).
    ///
    /// Unlike `HomeserverCapabilities::extended_profile_fields`, this keeps the
    /// difference between an absent `allowed` list (any field) and an empty
    /// one (none).
    pub async fn can_set_profile_field(&self, field: String) -> Result<bool, ClientError> {
        let capability = self.inner.homeserver_capabilities().extended_profile_fields().await?;
        Ok(capability.can_set_field(&ProfileFieldName::from(field.as_str())))
    }

    /// `PUT /_matrix/client/v3/profile/{ownUserId}/{field}` with a string
    /// value.
    ///
    /// Only for custom (namespaced) fields; the fields with typed values
    /// (`displayname`, `avatar_url`, `m.tz`) have their own setters and are
    /// refused here.
    pub async fn set_own_profile_field(
        &self,
        field: String,
        value: String,
    ) -> Result<(), ClientError> {
        let value = custom_string_value(&field, value)?;
        self.inner.account().set_profile_field(value).await?;
        Ok(())
    }

    /// `DELETE /_matrix/client/v3/profile/{ownUserId}/{field}`.
    pub async fn delete_own_profile_field(&self, field: String) -> Result<(), ClientError> {
        self.inner.account().delete_profile_field(ProfileFieldName::from(field.as_str())).await?;
        Ok(())
    }

    /// `GET /_matrix/client/v3/profile/{ownUserId}/{field}`, as a string.
    ///
    /// `None` only when the field is not set: the server's `404 M_NOT_FOUND`
    /// (for the own user it can only mean the field) or an absent or `null`
    /// value. Every other failure is an error, and so is a value that is not
    /// a string — a caller must not mistake either for "no value" and
    /// overwrite it.
    pub async fn own_profile_field(&self, field: String) -> Result<Option<String>, ClientError> {
        let user_id = self
            .inner
            .user_id()
            .ok_or_else(|| ClientError::from_str("the client is not logged in", None))?;
        let name = ProfileFieldName::from(field.as_str());

        match self.inner.account().fetch_profile_field_of(user_id.to_owned(), name).await {
            Ok(value) => string_value(&field, value),
            Err(error) => match error.client_api_error_kind() {
                Some(ErrorKind::NotFound) => Ok(None),
                _ => Err(error.into()),
            },
        }
    }
}

const TYPED_FIELDS: [&str; 3] = ["displayname", "avatar_url", "m.tz"];

fn custom_string_value(field: &str, value: String) -> Result<ProfileFieldValue, ClientError> {
    if TYPED_FIELDS.contains(&field) {
        return Err(ClientError::from_str(
            format!("`{field}` has a typed setter; this one is for custom fields"),
            None,
        ));
    }
    Ok(ProfileFieldValue::new(field, JsonValue::String(value))?)
}

fn string_value(
    field: &str,
    value: Option<ProfileFieldValue>,
) -> Result<Option<String>, ClientError> {
    let Some(value) = value else { return Ok(None) };
    match value.value().into_owned() {
        JsonValue::Null => Ok(None),
        JsonValue::String(string) => Ok(Some(string)),
        _ => Err(ClientError::from_str(format!("profile field `{field}` is not a string"), None)),
    }
}

#[cfg(test)]
mod tests {
    use matrix_sdk::{ruma::api::MatrixVersion, test_utils::mocks::MatrixMockServer};
    use matrix_sdk_common::cross_process_lock::CrossProcessLockConfig;
    use serde_json::json;
    use wiremock::{
        Mock, ResponseTemplate,
        matchers::{body_json, method, path},
    };

    use super::*;

    const FIELD: &str = "io.spiral.bio";
    const OWN_FIELD_PATH: &str = "/_matrix/client/v3/profile/@example:localhost/io.spiral.bio";

    async fn client_for(server: &MatrixMockServer) -> Client {
        // A custom field takes the stable path only from Matrix 1.16 (or with
        // `uk.tcpip.msc4133.stable`, as Spiral's Synapse advertises); below
        // that ruma falls back to the unstable prefix.
        //
        // The FFI `Client` wants session delegates under the multi-process
        // lock the mock builder defaults to; a single-process store needs none.
        let sdk = server
            .client_builder()
            .server_versions(vec![MatrixVersion::V1_16])
            .on_builder(|b| b.cross_process_store_config(CrossProcessLockConfig::SingleProcess))
            .build()
            .await;
        Client::new(sdk, None, None).await.unwrap()
    }

    async fn mount_capabilities(server: &MatrixMockServer, profile_fields: serde_json::Value) {
        Mock::given(method("GET"))
            .and(path("/_matrix/client/v3/capabilities"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "capabilities": { "m.profile_fields": profile_fields }
            })))
            .mount(server.server())
            .await;
    }

    #[tokio::test]
    async fn a_set_field_is_sent_as_its_own_string() {
        let server = MatrixMockServer::new().await;
        Mock::given(method("PUT"))
            .and(path(OWN_FIELD_PATH))
            .and(body_json(json!({ "io.spiral.bio": "Hello there" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .expect(1)
            .mount(server.server())
            .await;
        let client = client_for(&server).await;

        client.set_own_profile_field(FIELD.to_owned(), "Hello there".to_owned()).await.unwrap();
    }

    #[tokio::test]
    async fn a_typed_field_is_refused_before_any_request() {
        let server = MatrixMockServer::new().await;
        let client = client_for(&server).await;

        let result = client.set_own_profile_field("displayname".to_owned(), "x".to_owned()).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn a_deleted_field_is_a_delete_request() {
        let server = MatrixMockServer::new().await;
        Mock::given(method("DELETE"))
            .and(path(OWN_FIELD_PATH))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({})))
            .expect(1)
            .mount(server.server())
            .await;
        let client = client_for(&server).await;

        client.delete_own_profile_field(FIELD.to_owned()).await.unwrap();
    }

    #[tokio::test]
    async fn a_set_field_reads_back_as_its_string() {
        let server = MatrixMockServer::new().await;
        Mock::given(method("GET"))
            .and(path(OWN_FIELD_PATH))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "io.spiral.bio": "Hi" })),
            )
            .mount(server.server())
            .await;
        let client = client_for(&server).await;

        let value = client.own_profile_field(FIELD.to_owned()).await.unwrap();

        assert_eq!(value.as_deref(), Some("Hi"));
    }

    #[tokio::test]
    async fn an_unset_field_reads_as_none() {
        let server = MatrixMockServer::new().await;
        Mock::given(method("GET"))
            .and(path(OWN_FIELD_PATH))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "errcode": "M_NOT_FOUND",
                "error": "The requested profile key does not exist."
            })))
            .mount(server.server())
            .await;
        let client = client_for(&server).await;

        assert_eq!(client.own_profile_field(FIELD.to_owned()).await.unwrap(), None);
    }

    /// A failed read is never "no value": the caller would offer to
    /// overwrite the bio with nothing.
    #[tokio::test]
    async fn a_server_error_is_an_error_not_none() {
        let server = MatrixMockServer::new().await;
        Mock::given(method("GET"))
            .and(path(OWN_FIELD_PATH))
            .respond_with(ResponseTemplate::new(500).set_body_json(json!({
                "errcode": "M_UNKNOWN",
                "error": "boom"
            })))
            .expect(1..)
            .mount(server.server())
            .await;
        let client = client_for(&server).await;

        assert!(client.own_profile_field(FIELD.to_owned()).await.is_err());
    }

    /// A 404 that is not `M_NOT_FOUND` (a server without the endpoint
    /// answers `M_UNRECOGNIZED`) says nothing about the field.
    #[tokio::test]
    async fn a_404_without_not_found_is_an_error() {
        let server = MatrixMockServer::new().await;
        Mock::given(method("GET"))
            .and(path(OWN_FIELD_PATH))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({
                "errcode": "M_UNRECOGNIZED",
                "error": "Unrecognized request"
            })))
            .expect(1)
            .mount(server.server())
            .await;
        let client = client_for(&server).await;

        assert!(client.own_profile_field(FIELD.to_owned()).await.is_err());
    }

    /// A value a later version might write (an object) is an error, not a
    /// string guess and not "unset".
    #[tokio::test]
    async fn a_value_that_is_not_a_string_is_an_error() {
        let server = MatrixMockServer::new().await;
        Mock::given(method("GET"))
            .and(path(OWN_FIELD_PATH))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({ "io.spiral.bio": { "text": "Hi" } })),
            )
            .mount(server.server())
            .await;
        let client = client_for(&server).await;

        assert!(client.own_profile_field(FIELD.to_owned()).await.is_err());
    }

    #[tokio::test]
    async fn a_field_outside_an_allowed_list_cannot_be_set() {
        let server = MatrixMockServer::new().await;
        mount_capabilities(&server, json!({ "enabled": true, "allowed": ["m.tz"] })).await;
        let client = client_for(&server).await;

        assert!(!client.can_set_profile_field(FIELD.to_owned()).await.unwrap());
        assert!(client.can_set_profile_field("m.tz".to_owned()).await.unwrap());
    }

    /// Synapse's default: enabled, no list — any field.
    #[tokio::test]
    async fn an_enabled_capability_without_lists_allows_the_field() {
        let server = MatrixMockServer::new().await;
        mount_capabilities(&server, json!({ "enabled": true })).await;
        let client = client_for(&server).await;

        assert!(client.can_set_profile_field(FIELD.to_owned()).await.unwrap());
    }

    #[tokio::test]
    async fn a_disallowed_or_disabled_field_cannot_be_set() {
        let server = MatrixMockServer::new().await;
        mount_capabilities(&server, json!({ "enabled": true, "disallowed": [FIELD] })).await;
        let client = client_for(&server).await;
        assert!(!client.can_set_profile_field(FIELD.to_owned()).await.unwrap());

        let server = MatrixMockServer::new().await;
        mount_capabilities(&server, json!({ "enabled": false })).await;
        let client = client_for(&server).await;
        assert!(!client.can_set_profile_field(FIELD.to_owned()).await.unwrap());
    }
}
