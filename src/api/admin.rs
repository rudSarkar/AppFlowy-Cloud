use access_control::access::AccessControl;
use actix_web::{
  web::{self, Json},
  Scope,
};
use app_error::AppError;
use shared_entity::response::{AppResponse, JsonAppResponse};

use crate::biz::admin::access_control::MiddlewareAdminAccessControlTransform;

pub fn admin_scope(access_control: AccessControl) -> Scope {
  let middleware = MiddlewareAdminAccessControlTransform::new(access_control);
  web::scope("/api/admin").service(
    web::resource("/policy")
      .wrap(middleware)
      .route(web::put().to(put_policy_handler))
      .route(web::delete().to(delete_policy_handler)),
  )
}

async fn put_policy_handler() -> Result<JsonAppResponse<()>, AppError> {
  Ok(Json(AppResponse::Ok()))
}

async fn delete_policy_handler() -> Result<JsonAppResponse<()>, AppError> {
  Ok(Json(AppResponse::Ok()))
}
