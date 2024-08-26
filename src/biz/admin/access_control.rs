use std::{
  future::{ready, Future, Ready},
  pin::Pin,
};

use access_control::{
  access::{AccessControl, ObjectType},
  act::ActionVariant,
  request::PolicyRequest,
};
use actix_service::{forward_ready, Service, Transform};
use actix_web::{
  dev::{ServiceRequest, ServiceResponse},
  web::Data,
  Error,
};
use app_error::AppError;
use authentication::jwt::UserUuid;
use database_entity::dto::AFRole;

use crate::state::AppState;

pub struct MiddlewareAdminAccessControlTransform {
  access_control: AccessControl,
}

impl MiddlewareAdminAccessControlTransform {
  pub fn new(access_control: AccessControl) -> Self {
    Self { access_control }
  }
}

impl<S, B> Transform<S, ServiceRequest> for MiddlewareAdminAccessControlTransform
where
  S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
  S::Future: 'static,
  B: 'static,
{
  type Response = ServiceResponse<B>;
  type Error = Error;
  type Transform = MiddlewareAdminAccessControl<S>;
  type InitError = ();
  type Future = Ready<Result<Self::Transform, Self::InitError>>;

  fn new_transform(&self, service: S) -> Self::Future {
    ready(Ok(MiddlewareAdminAccessControl {
      service,
      access_control: self.access_control.clone(),
    }))
  }
}

pub struct MiddlewareAdminAccessControl<S> {
  service: S,
  access_control: AccessControl,
}

impl<S, B> Service<ServiceRequest> for MiddlewareAdminAccessControl<S>
where
  S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error>,
  S::Future: 'static,
  B: 'static,
{
  type Response = ServiceResponse<B>;
  type Error = Error;
  type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>>>>;

  forward_ready!(service);

  fn call(&self, mut req: ServiceRequest) -> Self::Future {
    let user_uuid = req.extract::<UserUuid>();
    let user_cache = req
      .app_data::<Data<AppState>>()
      .map(|state| state.user_cache.clone());
    let access_control = self.access_control.clone();
    let fut = self.service.call(req);
    Box::pin(async move {
      let user_uuid = user_uuid.await.map_err(|err| {
        AppError::Internal(anyhow::anyhow!(
          "Can't find the user uuid from the request: {}",
          err
        ))
      })?;
      let uid = user_cache
        .ok_or_else(|| AppError::Internal(anyhow::anyhow!("AppState is not found in the request")))?
        .get_user_uid(&user_uuid)
        .await?;
      let action = ActionVariant::FromRole(&AFRole::Owner);
      let has_permission = access_control
        .enforce_policy(PolicyRequest {
          uid,
          object_type: &ObjectType::Admin,
          action: &action,
        })
        .await?;
      if !has_permission {
        return Err(Error::from(AppError::NotEnoughPermissions {
          user: uid.to_string(),
          action: action.to_enforce_act().to_string(),
        }));
      }
      fut.await
    })
  }
}
