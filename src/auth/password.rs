use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};

pub fn hash_password(password: &str) -> Result<String, argon2::password_hash::Error> {
    let salt = SaltString::generate(&mut OsRng);
    let argon2 = Argon2::default();
    let hash = argon2.hash_password(password.as_bytes(), &salt)?;
    Ok(hash.to_string())
}

pub fn verify_password(password: &str, hash: &str) -> Result<bool, argon2::password_hash::Error> {
    let parsed_hash = PasswordHash::new(hash)?;
    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed_hash)
        .is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_and_verify_roundtrip() {
        let hash = hash_password("correct horse battery staple").unwrap();
        assert!(verify_password("correct horse battery staple", &hash).unwrap());
    }

    #[test]
    fn wrong_password_is_rejected() {
        let hash = hash_password("s3cret!").unwrap();
        assert!(!verify_password("s3cret", &hash).unwrap());
        assert!(!verify_password("S3cret!", &hash).unwrap());
        assert!(!verify_password("", &hash).unwrap());
    }

    #[test]
    fn each_hash_is_unique_due_to_salt() {
        let a = hash_password("same-password").unwrap();
        let b = hash_password("same-password").unwrap();
        assert_ne!(a, b, "salts should differ between calls");
        assert!(verify_password("same-password", &a).unwrap());
        assert!(verify_password("same-password", &b).unwrap());
    }

    #[test]
    fn malformed_hash_returns_error() {
        let err = verify_password("any", "not-a-real-phc-hash");
        assert!(err.is_err());
    }

    #[test]
    fn empty_password_can_be_hashed_and_verified() {
        // Not recommended, but the primitives should be consistent.
        let hash = hash_password("").unwrap();
        assert!(verify_password("", &hash).unwrap());
        assert!(!verify_password("x", &hash).unwrap());
    }

    #[test]
    fn unicode_password_roundtrip() {
        let pwd = "密码🔐測試";
        let hash = hash_password(pwd).unwrap();
        assert!(verify_password(pwd, &hash).unwrap());
        assert!(!verify_password("密码测试", &hash).unwrap());
    }
}
