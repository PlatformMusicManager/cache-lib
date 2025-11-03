use redis::{AsyncTypedCommands, RedisResult, TypedCommands};
use uuid::Uuid;

use crate::errors::session_errors::SessionError;

pub mod models;
pub mod errors;

struct RedisClient {
    client: redis::Client,
    session_ttl_s: u64
}

impl RedisClient {
    pub fn new(connection_string: String, session_ttl_s: u64) -> Self {
        Self {
            client: redis::Client::open(connection_string).unwrap(),
            session_ttl_s
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

    
}
