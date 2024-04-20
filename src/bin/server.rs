use std::{sync::Arc, time::Duration};

use axum::{
    extract::{FromRef, Path, State},
    response::Result,
    routing::{get, post},
    Json, Router,
};
use rdkafka::{
    producer::{FutureProducer, FutureRecord},
    ClientConfig,
};
use serde::{Deserialize, Serialize};

fn create_producer() -> FutureProducer {
    let brokers: String = "127.0.0.1:19092".into();
    log::info!("connect to kafka {}", brokers);

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("message.timeout.ms", "5000")
        .create()
        .expect("producer creation error");

    producer
}

#[derive(Serialize, Deserialize)]
struct Body {
    key: String,
    data: Vec<u8>,
}

async fn send(producer: Arc<FutureProducer>, topic: &str, body: Body) {
    let record = FutureRecord::to(topic).key(&body.key).payload(&body.data);
    let fut = producer.send(record, Duration::from_secs(0));
    match fut.await {
        Ok(delivery) => println!("Send: {:?}", delivery),
        Err((e, _)) => println!("send failed: {:?}", e),
    }
}

#[derive(Serialize, Deserialize)]
struct Event {
    schema: String,
    data: Vec<EventData>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EventData {
    pub e: String,
    pub eid: String,
    pub tv: String,
    pub tna: String,
    pub aid: String,
    pub p: String,
    pub cookie: String,
    pub cs: Option<String>,
    pub cx: Option<String>,
    pub lang: String,
    pub res: String,
    pub cd: String,
    pub tz: String,
    pub dtm: String,
    pub vp: String,
    pub ds: String,
    pub vid: String,
    pub sid: String,
    pub duid: String,
    pub refr: Option<String>,
    pub url: String,
    pub ue_pr: Option<String>,
    pub ue_px: Option<String>,
    pub co: Option<String>,
    pub stm: String,
}

async fn hello() -> Result<String, playrust::core::error::Error> {
    Ok("ok".to_string())
}

async fn received_events(
    State(producer): State<Arc<FutureProducer>>,
    Path((provider, version)): Path<(String, String)>,
    Json(body): Json<Event>,
) -> Result<String, playrust::core::error::Error> {
    let topic = "hihi";
    log::info!(
        "got info {}/{}, items {}",
        provider,
        version,
        body.data.len()
    );

    for item in body.data.iter() {
        let buf = serde_json::to_vec(&item).unwrap();
        send(
            producer.clone(),
            topic,
            Body {
                key: item.sid.clone(),
                data: buf,
            },
        )
        .await;
    }
    Ok("ok".to_string())
}

async fn capture_state(
    State(_producer): State<AppState>,
) -> Result<String, playrust::core::error::Error> {
    Ok("ok".to_string())
}

#[derive(Clone)]
struct AppState {
    producer: Arc<FutureProducer>,
}

// support converting an `AppState` in an `ApiState`
impl FromRef<AppState> for Arc<FutureProducer> {
    fn from_ref(app_state: &AppState) -> Arc<FutureProducer> {
        app_state.producer.clone()
    }
}

async fn serve() -> std::io::Result<()> {
    let conn = create_producer();
    let state = AppState {
        producer: Arc::new(conn),
    };

    let app = Router::new()
        .route("/:provider/:version", post(received_events))
        .route("/hello", get(capture_state))
        .route("/", get(hello))
        .with_state(state);

    let addr = "0.0.0.0:8888";
    log::info!("listening on address: {}", addr);
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
    Ok(())
}

#[tokio::main]
async fn main() {
    // Initialize the logger.
    structured_logger::Builder::with_level("info")
        .with_target_writer(
            "*",
            structured_logger::async_json::new_writer(tokio::io::stdout()),
        )
        .init();
    let _ = serve().await;
}
