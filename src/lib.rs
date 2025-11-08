use chrono::{DateTime, Duration, Utc};
use domain::errors::cache::session_errors::SessionError;
use domain::errors::cache::user_errors::UserError;
use domain::errors::cache::verify_user_errors::UserVerifyError;
use domain::models::cache::await_verification::UserAwaitVerification;
use domain::models::cache::user_verify_result::UserVerifyResult;
use domain::models::db::user::UserWithPlaylists;
use redis::{AsyncTypedCommands, JsonAsyncCommands, RedisResult};
use uuid::Uuid;

#[derive(Clone)]
pub struct RedisClient {
    client: redis::Client,
    session_ttl_s: u64,
    verify_ttl_s: i64,
    verify_duration: Duration,
    user_ttl_s: i64,
    verify_attempts: u8,
}

impl RedisClient {
    pub fn new(
        connection_string: String,
        session_ttl_s: u64,
        user_ttl_s: i64,
        verify_ttl_s: u64,
        verify_attempts: u8
    ) -> Self {
        Self {
            client: redis::Client::open(connection_string).unwrap(),
            session_ttl_s,
            verify_ttl_s: verify_ttl_s as i64,
            user_ttl_s,
            verify_duration: Duration::seconds(verify_ttl_s as i64),
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
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = format!("user:{}", &user_id);

        let res: Option<String> = conn.json_get(key, "$").await?;

        match res {
            Some(res) => match serde_json::from_str::<UserWithPlaylists>(&res) {
                Ok(user) => Ok(Ok(user)),
                Err(_) => Ok(Err(UserError::ParseError)),
            },
            None => Ok(Err(UserError::UserNotFound)),
        }
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
            .expire(&key, self.verify_ttl_s);

        pipe.query_async::<()>(&mut conn).await?;

        Ok(())
    }

    pub async fn verify_user(&self, sn: Uuid, code: String)
        -> RedisResult<Result<UserVerifyResult, UserVerifyError>> {
        let mut conn = self.client
            .get_multiplexed_async_connection()
            .await?;

        let key = format!("verify-user:{}", &sn);

        let user = conn.hgetall(&key).await?;

        // CHECK IS STILL VALID
        let Some(created_at) = user.get("created_at") else {
            return Ok(Err(UserVerifyError::UserNotFound))
        };

        match DateTime::parse_from_rfc3339(&created_at) {
            Ok(created_at) => {
                if created_at + self.verify_duration > Utc::now() {
                    return Ok(Err(UserVerifyError::Expired))
                }
            },
            Err(_) => return Ok(Err(UserVerifyError::ParseError))
        };

        // CHECK CODE
        let (Some(attempts), Some(code_r)) =
            (user.get("attempts"), user.get("code")) else {
            return Ok(Err(UserVerifyError::UserNotFound))
        };

        if code != *code_r {
            // ADD ATTEMPTS
            let attempts = match attempts.parse::<u8>() {
                Ok(num) => num + 1, // ADDING ONE ATTEMPT HERE
                Err(_) => return Ok(Err(UserVerifyError::ParseError)),
            };

            // IF ExceededAttempts
            if attempts >= self.verify_attempts {
                conn.del(&key).await?;
                Ok(Err(UserVerifyError::ExceededAttempts))
            } else {
                // Add to attempts
                conn.hset(&key, "attempts", attempts.to_string()).await?;
                Ok(Err(UserVerifyError::WrongCode))
            }
        } else {
            // VERIFIED
            conn.del(&key).await?;

            let (
                Some(email), Some(username), Some(password_hash)
            ) = (
                user.get("email"), user.get("username"), user.get("password_hash")
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
