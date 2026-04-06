use sqlx::PgPool;
use std::sync::Arc;

use crate::idl::schema::IdlSchema;

#[derive(Clone)]
pub struct AppState {
    pub pool: Arc<PgPool>,
    pub schema: Arc<IdlSchema>,
    pub program_id: String,
}
