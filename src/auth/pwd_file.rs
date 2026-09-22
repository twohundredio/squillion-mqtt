use async_trait::async_trait;
use std::collections::HashMap;
use std::fs;

use base64::{engine::general_purpose, Engine as _};
use sha1::{Digest, Sha1};

use crate::auth::AuthProvider;
use crate::config;
use crate::messages::ReturnCode;

use super::AuthResponse;

fn check_sha_password(password: &str, hash: &str) -> bool {
    let digest = Sha1::digest(password.as_bytes());
    general_purpose::STANDARD.encode(digest).eq(hash)
}

pub struct PasswordAuth {
    users: HashMap<String, String>,
}

impl PasswordAuth {
    pub fn new() -> Self {
        PasswordAuth {
            users: HashMap::new(),
        }
    }

    pub fn load_password_file(&mut self, filename: &str) {
        let contents = fs::read_to_string(filename);
        match contents {
            Ok(content) => {
                let users = content.lines();
                for user in users {
                    let t: Vec<_> = user.split(':').collect();
                    if t.len() != 2 {
                        println!("Error parsing: {}", user);
                    } else {
                        self.users.insert(t[0].to_string(), t[1].to_string());
                    }
                }
            }
            Err(e) => {
                println!("Unable to open file: {}", e);
            }
        }
    }

    pub fn check_password_internal(&self, user: &str, password: &str) -> ReturnCode {
        match self.users.get(user) {
            Some(hash) => {
                if let Some(h) = hash.strip_prefix("{SHA}") {
                    if check_sha_password(password, h) {
                        ReturnCode::Accepted
                    } else {
                        ReturnCode::BadUsernameOrPassword
                    }
                } else {
                    ReturnCode::BadUsernameOrPassword
                }
            }
            None => ReturnCode::BadUsernameOrPassword,
        }
    }
}

#[async_trait]
impl AuthProvider for PasswordAuth {
    fn initialize(&mut self) -> Result<(), ()> {
        let filename = config::get_string("password_file").unwrap();
        self.load_password_file(&filename);
        Ok(())
    }

    async fn check_password(&self, user: &str, password: &str) -> AuthResponse {
        let return_code = self.check_password_internal(user, password);
        AuthResponse {
            return_code,
            tenant: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha_password() {
        assert!(check_sha_password(
            "password",
            "W6ph5Mm5Pz8GgiULbPgzG37mj9g="
        ));
        assert!(!check_sha_password(
            "notpassword",
            "W6ph5Mm5Pz8GgiULbPgzG37mj9g="
        ));
    }

    #[test]
    fn test_password_file() {
        let mut pa = PasswordAuth::new();
        pa.load_password_file("tests/users.txt");

        assert_eq!(
            pa.check_password_internal("user1", "password1"),
            ReturnCode::Accepted
        );
        assert_eq!(
            pa.check_password_internal("user1", "password2"),
            ReturnCode::BadUsernameOrPassword
        );
        assert_eq!(
            pa.check_password_internal("user5", "password5"),
            ReturnCode::Accepted
        );
    }
}
