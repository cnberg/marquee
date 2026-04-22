use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub sub: i64,
    pub username: String,
    pub exp: usize,
}

pub fn create_token(
    user_id: i64,
    username: &str,
    secret: &str,
    expiry_days: u64,
) -> Result<String, jsonwebtoken::errors::Error> {
    let exp = chrono::Utc::now()
        .checked_add_signed(chrono::Duration::days(expiry_days as i64))
        .expect("valid timestamp")
        .timestamp() as usize;

    let claims = Claims {
        sub: user_id,
        username: username.to_string(),
        exp,
    };

    encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(secret.as_bytes()),
    )
}

pub fn verify_token(token: &str, secret: &str) -> Result<Claims, jsonwebtoken::errors::Error> {
    let token_data = decode::<Claims>(
        token,
        &DecodingKey::from_secret(secret.as_bytes()),
        &Validation::default(),
    )?;
    Ok(token_data.claims)
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::errors::ErrorKind;

    const SECRET: &str = "test-secret-ABCDEFG";

    #[test]
    fn sign_and_verify_roundtrip() {
        let token = create_token(42, "alice", SECRET, 7).unwrap();
        let claims = verify_token(&token, SECRET).unwrap();
        assert_eq!(claims.sub, 42);
        assert_eq!(claims.username, "alice");
        // exp should be in the future
        let now = chrono::Utc::now().timestamp() as usize;
        assert!(claims.exp > now);
    }

    #[test]
    fn wrong_secret_fails_verification() {
        let token = create_token(1, "bob", SECRET, 1).unwrap();
        let err = verify_token(&token, "other-secret").unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::InvalidSignature));
    }

    #[test]
    fn tampered_payload_fails_verification() {
        let token = create_token(1, "carol", SECRET, 1).unwrap();
        // Flip a character in the payload segment (middle of the JWT).
        let mut parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3);
        let tampered_payload = format!("{}X", parts[1]);
        parts[1] = &tampered_payload;
        let tampered = parts.join(".");
        assert!(verify_token(&tampered, SECRET).is_err());
    }

    #[test]
    fn expired_token_is_rejected() {
        // Craft a token whose exp is in the past by bypassing create_token.
        use jsonwebtoken::{encode, EncodingKey, Header};
        // Must exceed jsonwebtoken's default 60s leeway.
        let past = chrono::Utc::now().timestamp() as usize - 3600;
        let claims = Claims {
            sub: 7,
            username: "dave".to_string(),
            exp: past,
        };
        let token = encode(
            &Header::default(),
            &claims,
            &EncodingKey::from_secret(SECRET.as_bytes()),
        )
        .unwrap();
        let err = verify_token(&token, SECRET).unwrap_err();
        assert!(matches!(err.kind(), ErrorKind::ExpiredSignature));
    }

    #[test]
    fn garbage_token_is_rejected() {
        assert!(verify_token("not.a.jwt", SECRET).is_err());
        assert!(verify_token("", SECRET).is_err());
    }

    #[test]
    fn expiry_days_is_respected() {
        let token = create_token(1, "eve", SECRET, 3).unwrap();
        let claims = verify_token(&token, SECRET).unwrap();
        let now = chrono::Utc::now().timestamp() as usize;
        let expected = now + 3 * 24 * 60 * 60;
        // Allow a few seconds of drift between create and assertion.
        let diff = (claims.exp as i64 - expected as i64).abs();
        assert!(diff < 10, "exp drift {} too large", diff);
    }
}
