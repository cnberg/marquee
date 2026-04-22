use axum::{
    extract::FromRef,
    extract::FromRequestParts,
    http::{header::AUTHORIZATION, request::Parts, StatusCode},
};

use crate::api::AppState;

use super::jwt;

#[derive(Debug, Clone)]
pub struct AuthUser {
    pub id: i64,
    pub username: String,
}

/// Optional auth: returns Some(AuthUser) if valid token, None otherwise. Never rejects.
pub struct OptionalUser(pub Option<AuthUser>);

/// Required auth: returns AuthUser or 401.
pub struct RequireUser(pub AuthUser);

fn extract_token(parts: &Parts) -> Option<String> {
    let header_value = parts.headers.get(AUTHORIZATION)?.to_str().ok()?;
    let token = header_value.strip_prefix("Bearer ")?;
    Some(token.to_string())
}

impl<S> FromRequestParts<S> for OptionalUser
where
    AppState: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = StatusCode;

    fn from_request_parts(
        parts: &mut Parts,
        state: &S,
    ) -> impl futures::Future<Output = Result<Self, Self::Rejection>> + Send {
        let app_state = AppState::from_ref(state);
        let secret = app_state.config.auth.jwt_secret.clone();
        let token = extract_token(parts);

        async move {
            if let Some(token) = token {
                if let Ok(claims) = jwt::verify_token(&token, &secret) {
                    return Ok(OptionalUser(Some(AuthUser {
                        id: claims.sub,
                        username: claims.username,
                    })));
                }
            }

            Ok(OptionalUser(None))
        }
    }
}

impl<S> FromRequestParts<S> for RequireUser
where
    AppState: FromRef<S>,
    S: Send + Sync,
{
    type Rejection = StatusCode;

    fn from_request_parts(
        parts: &mut Parts,
        state: &S,
    ) -> impl futures::Future<Output = Result<Self, Self::Rejection>> + Send {
        let app_state = AppState::from_ref(state);
        let secret = app_state.config.auth.jwt_secret.clone();
        let token = extract_token(parts);

        async move {
            if let Some(token) = token {
                if let Ok(claims) = jwt::verify_token(&token, &secret) {
                    return Ok(RequireUser(AuthUser {
                        id: claims.sub,
                        username: claims.username,
                    }));
                }
            }

            Err(StatusCode::UNAUTHORIZED)
        }
    }
}
