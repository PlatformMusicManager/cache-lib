use domain::cache::await_verification::UserAwaitVerification;
use domain::cache::user_verify_result::UserVerifyResult;
use domain::db::user::UserWithPlaylists;
use redis::{ AsyncTypedCommands, JsonAsyncCommands, RedisResult, TypedCommands};
use uuid::Uuid;

use crate::errors::session_errors::SessionError;
use crate::errors::user_errors::UserError;
use crate::errors::verify_user_errors::UserVerifyError;

pub mod errors;

#[derive(Clone)]
pub struct RedisClient {
    client: redis::Client,
    session_ttl_s: u64,
    verify_ttl_s: u64,
    user_ttl_s: i64,
    verify_attempts: u8,
}

impl RedisClient {
    pub fn new(connection_string: String, session_ttl_s: u64, user_ttl_s: i64, verify_ttl_s: u64, verify_attempts: u8) -> Self {
        Self {
            client: redis::Client::open(connection_string).unwrap(),
            session_ttl_s,
            verify_ttl_s,
            user_ttl_s,
            verify_attempts
        }
    }

    pub async fn create_session(&self, session_id: Uuid, sn: Uuid) -> RedisResult<()> {
        let mut conn = self.client
            .get_multiplexed_async_connection()
            .await?;

        let key = format!("session:{}", session_id);

        conn.set_ex(&key, sn.to_string(), self.session_ttl_s).await?;

        Ok(())
    }

    pub async fn proof_session(&self, session_id: Uuid, sn: Uuid) -> RedisResult<Option<SessionError>> {
        let mut conn = self.client
            .get_multiplexed_async_connection()
            .await?;

        let sn_r: Option<String> = conn
        .get(format!("session:{}", session_id))
        .await?;

        match sn_r {
            Some(sn_r) => {
                if sn.to_string() != sn_r
                {
                    return Ok(Some(SessionError::SessionWasUpdated))
                }

                Ok(None)
            },
            None => Ok(Some(SessionError::SessionNotFound))
        }
    }

    pub async fn remove_session(&self, session_id: Uuid, sn: Uuid) -> RedisResult<Option<SessionError>> {
        let mut conn = self.client
            .get_multiplexed_async_connection()
            .await?;

        let key = format!("session:{}", session_id);

        let sn_r: Option<String> = conn
        .get(&key)
        .await?;

        match sn_r {
            Some(sn_r) => {
                if sn.to_string() != sn_r
                {
                    return Ok(Some(SessionError::SessionWasUpdated))
                }

                conn.del(key).await?;

                Ok(None)
            },
            None => Ok(Some(SessionError::SessionNotFound))
        }
    }

    pub async fn create_user(&self, user: UserWithPlaylists) -> RedisResult<()> {
        let mut conn = self.client
            .get_multiplexed_async_connection()
            .await?;

        let key = format!("user:{}", &user.id);

        let mut pipe = redis::pipe();
        pipe.atomic() // Ensures the commands are executed atomically (like a transaction)
            .json_set(&key, "$", &user)?
            .expire(&key, self.user_ttl_s);

        pipe.query_async::<()>(&mut conn).await?;

        Ok(())
    }

    pub async fn get_user(&self, user_id: i64) -> RedisResult<Result<UserWithPlaylists, UserError>> {
        let mut conn = self.client
            .get_multiplexed_async_connection()
            .await?;

        let key = format!("user:{}", &user_id);

        let Some(res) = conn.json_get::<_, _, Option<String>>(key, "$").await? else {
            return Ok(Err(UserError::UserNotFound))
        };

        let Ok(res) = serde_json::from_str::<UserWithPlaylists>(&res) else {
            return Ok(Err(UserError::ParseError))
        };

        Ok(Ok(res))
    }

    pub async fn extend_user_ttl(&self, user_id: i64) -> RedisResult<bool> {
        let mut conn = self.client
            .get_multiplexed_async_connection()
            .await?;

        let key = format!("user:{}", &user_id);

        let res = conn.expire(&key, self.user_ttl_s).await?;

        Ok(res)
    }

    pub async fn remove_user(&self, user_id: i64) -> RedisResult<()> {
        let mut conn = self.client
            .get_multiplexed_async_connection()
            .await?;

        let key = format!("user:{}", &user_id);

        conn.del(key).await?;

        Ok(())
    }

    pub async fn add_user_verify(&self, sn: Uuid, user: UserAwaitVerification) -> RedisResult<()> {
        let mut conn = self.client
            .get_multiplexed_async_connection()
            .await?;

        let key = format!("verify-user:{}", &sn);

        let mut pipe = redis::pipe();
        pipe.atomic() // Ensures the commands are executed atomically (like a transaction)
            .hset_multiple(&key, &user.to_hash_array())
            .expire(&key, self.user_ttl_s);

        pipe.query_async::<()>(&mut conn).await?;

        Ok(())
    }

    pub async fn verify_user(&self, sn: Uuid, code: String) -> RedisResult<Result<UserVerifyResult, UserVerifyError>> {
        let mut conn = self.client
            .get_multiplexed_async_connection()
            .await?;

        let key = format!("verify-user:{}", &sn);

        let user = conn.hgetall(&key).await?;

        let (Some(attempts), Some(code_r)) = (user.get("attempts"), user.get("code")) else {
            return Ok(Err(UserVerifyError::UserNotFound))
        };

        if (code != *code_r) {
            let attempts = match attempts.parse::<u8>() {
                Ok(num) => num + 1, // ADDING ONE ATTEMPT HERE
                Err(_) => return Ok(Err(UserVerifyError::ParseError)),
            };

            if attempts >= self.verify_attempts {
                conn.del(&key).await?;
                Ok(Err(UserVerifyError::ExceededAttempts))
            } else {
                conn.hset(&key, "attempts", attempts.to_string()).await?;
                Ok(Err(UserVerifyError::WrongCode))
            }
        } else {
            conn.del(&key).await?;

            let (
                Some(email),
                Some(username),
                Some(password_hash)
            ) = (
                user.get("email"),
                user.get("username"),
                user.get("password_hash")
                )
            else {
                return Ok(Err(UserVerifyError::ParseError));
            };

            Ok(Ok(UserVerifyResult {
                email: email.to_string(),
                username: username.to_string(),
                password_hash: password_hash.to_string(),
            }))
        }
    }
}
