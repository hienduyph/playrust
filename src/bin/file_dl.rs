use playrust::workers::kafka_processing;

#[tokio::main]
async fn main() {
    kafka_processing::start().await;
}
