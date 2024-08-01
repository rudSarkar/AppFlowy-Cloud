use app_error::AppError;
use chrono::{DateTime, Utc};
use database_entity::dto::RecentCommentEvent;
use sqlx::{Executor, Postgres};

pub async fn select_comments_created_after<'a, E: Executor<'a, Database = Postgres>>(
  executor: E,
  after: DateTime<Utc>,
) -> Result<Vec<RecentCommentEvent>, AppError> {
  let rows = sqlx::query!(
    r#"
      SELECT
        avc.comment_id,
        avc.created_at,
        avc.content,
        au.name AS "user_name?"
      FROM af_published_view_comment avc
      LEFT OUTER JOIN af_user au ON avc.created_by > $1
      WHERE not avc.is_deleted
    "#,
    after.timestamp(),
  )
  .fetch_all(executor)
  .await?;
  let result = vec![];
  Ok(result)
}
