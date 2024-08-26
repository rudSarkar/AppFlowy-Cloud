use futures_util::stream::BoxStream;
use sqlx::PgPool;

use crate::pg_row::AFPolicyRow;

pub fn select_policy_stream(pg_pool: &PgPool) -> BoxStream<'_, sqlx::Result<AFPolicyRow>> {
  sqlx::query_as!(AFPolicyRow, "SELECT subject, object, action FROM af_policy").fetch(pg_pool)
}
