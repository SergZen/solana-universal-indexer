pub mod queries;

use anyhow::Result;
use sqlx::PgPool;
use tracing::info;

use crate::idl::schema::IdlSchema;

pub async fn connect(database_url: &str, schema: &IdlSchema) -> Result<PgPool> {
    info!("Connecting to database…");
    let pool = PgPool::connect(database_url).await?;

    info!("Running base migrations…");
    sqlx::migrate!("./migrations").run(&pool).await?;

    // Generate and execute DDL from IDL — idempotent (CREATE TABLE IF NOT EXISTS)
    info!("Generating dynamic schema from IDL…");
    let statements = schema.generate_ddl();
    for sql in &statements {
        sqlx::query(sql).execute(&pool).await?;
    }
    info!(tables = statements.len(), "Dynamic schema applied");

    Ok(pool)
}
