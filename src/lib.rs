use domain::db::user::UserWithPlaylists;
use redis::{AsyncTypedCommands, JsonAsyncCommands, RedisResult, TypedCommands};
use uuid::Uuid;

use crate::errors::session_errors::SessionError;
use crate::errors::user_errors::UserError;

pub mod errors;

struct RedisClient {
    client: redis::Client,
    session_ttl_s: u64,
    user_ttl_s: i64
}

impl RedisClient {
    pub fn new(connection_string: String, session_ttl_s: u64, user_ttl_s: i64) -> Self {
        Self {
            client: redis::Client::open(connection_string).unwrap(),
            session_ttl_s,
            user_ttl_s
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
}
