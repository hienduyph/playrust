use axum::{body::Body, http::Response, response::IntoResponse};

pub struct Error {}

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        Response::builder().status(500).body(Body::empty()).unwrap()
    }
}
