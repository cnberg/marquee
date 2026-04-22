use axum::{
    body::Body,
    http::{header, StatusCode, Uri},
    response::{IntoResponse, Response},
};
use rust_embed::Embed;

#[derive(Embed)]
#[folder = "web/dist"]
struct StaticAssets;

/// Serve embedded static assets, falling back to `index.html` for SPA routes.
pub async fn static_handler(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches('/');

    let (asset_path, asset) = if path.is_empty() {
        ("index.html", StaticAssets::get("index.html"))
    } else if let Some(content) = StaticAssets::get(path) {
        (path, Some(content))
    } else {
        ("index.html", StaticAssets::get("index.html"))
    };

    match asset {
        Some(content) => {
            let body = Body::from(content.data.into_owned());
            let mime = mime_guess::from_path(asset_path).first_or_octet_stream();

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, mime.as_ref())
                .body(body)
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
        None => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
