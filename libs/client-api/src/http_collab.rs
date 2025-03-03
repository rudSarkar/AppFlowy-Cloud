use crate::http::log_request_id;
use crate::{blocking_brotli_compress, Client};
use app_error::AppError;
use client_api_entity::workspace_dto::AFDatabase;
use client_api_entity::{
  BatchQueryCollabParams, BatchQueryCollabResult, CreateCollabParams, DeleteCollabParams,
  QueryCollab, UpdateCollabWebParams,
};
use reqwest::Method;
use shared_entity::response::{AppResponse, AppResponseError};
use tracing::instrument;

impl Client {
  #[instrument(level = "info", skip_all, err)]
  pub async fn create_collab(&self, params: CreateCollabParams) -> Result<(), AppResponseError> {
    let url = format!(
      "{}/api/workspace/{}/collab/{}",
      self.base_url, params.workspace_id, &params.object_id
    );
    let bytes = params
      .to_bytes()
      .map_err(|err| AppError::Internal(err.into()))?;

    let compress_bytes = blocking_brotli_compress(
      bytes,
      self.config.compression_quality,
      self.config.compression_buffer_size,
    )
    .await?;

    #[allow(unused_mut)]
    let mut builder = self
      .http_client_with_auth_compress(Method::POST, &url)
      .await?;

    #[cfg(not(target_arch = "wasm32"))]
    {
      builder = builder.timeout(std::time::Duration::from_secs(60));
    }

    let resp = builder.body(compress_bytes).send().await?;
    log_request_id(&resp);
    AppResponse::<()>::from_response(resp).await?.into_error()
  }

  #[instrument(level = "info", skip_all, err)]
  pub async fn update_collab(&self, params: CreateCollabParams) -> Result<(), AppResponseError> {
    let url = format!(
      "{}/api/workspace/{}/collab/{}",
      self.base_url, &params.workspace_id, &params.object_id
    );
    let resp = self
      .http_client_with_auth(Method::PUT, &url)
      .await?
      .json(&params)
      .send()
      .await?;
    log_request_id(&resp);
    AppResponse::<()>::from_response(resp).await?.into_error()
  }

  pub async fn update_web_collab(
    &self,
    workspace_id: &str,
    object_id: &str,
    params: UpdateCollabWebParams,
  ) -> Result<(), AppResponseError> {
    let url = format!(
      "{}/api/workspace/v1/{}/collab/{}/web-update",
      self.base_url, workspace_id, object_id
    );
    let resp = self
      .http_client_with_auth(Method::POST, &url)
      .await?
      .json(&params)
      .send()
      .await?;
    log_request_id(&resp);
    AppResponse::<()>::from_response(resp).await?.into_error()
  }

  // The browser will call this API to get the collab list, because the URL length limit and browser can't send the body in GET request
  #[instrument(level = "info", skip_all, err)]
  pub async fn batch_post_collab(
    &self,
    workspace_id: &str,
    params: Vec<QueryCollab>,
  ) -> Result<BatchQueryCollabResult, AppResponseError> {
    self
      .send_batch_collab_request(Method::POST, workspace_id, params)
      .await
  }

  #[instrument(level = "info", skip_all, err)]
  pub async fn batch_get_collab(
    &self,
    workspace_id: &str,
    params: Vec<QueryCollab>,
  ) -> Result<BatchQueryCollabResult, AppResponseError> {
    self
      .send_batch_collab_request(Method::GET, workspace_id, params)
      .await
  }

  async fn send_batch_collab_request(
    &self,
    method: Method,
    workspace_id: &str,
    params: Vec<QueryCollab>,
  ) -> Result<BatchQueryCollabResult, AppResponseError> {
    let url = format!(
      "{}/api/workspace/{}/collab_list",
      self.base_url, workspace_id
    );
    let params = BatchQueryCollabParams(params);
    let resp = self
      .http_client_with_auth(method, &url)
      .await?
      .json(&params)
      .send()
      .await?;
    log_request_id(&resp);
    AppResponse::<BatchQueryCollabResult>::from_response(resp)
      .await?
      .into_data()
  }

  #[instrument(level = "info", skip_all, err)]
  pub async fn delete_collab(&self, params: DeleteCollabParams) -> Result<(), AppResponseError> {
    let url = format!(
      "{}/api/workspace/{}/collab/{}",
      self.base_url, &params.workspace_id, &params.object_id
    );
    let resp = self
      .http_client_with_auth(Method::DELETE, &url)
      .await?
      .json(&params)
      .send()
      .await?;
    log_request_id(&resp);
    AppResponse::<()>::from_response(resp).await?.into_error()
  }

  #[instrument(level = "info", skip_all, err)]
  pub async fn list_databases(
    &self,
    workspace_id: &str,
  ) -> Result<Vec<AFDatabase>, AppResponseError> {
    let url = format!("{}/api/workspace/{}/database", self.base_url, workspace_id);
    let resp = self
      .http_client_with_auth(Method::GET, &url)
      .await?
      .send()
      .await?;
    log_request_id(&resp);
    AppResponse::from_response(resp).await?.into_data()
  }
}
