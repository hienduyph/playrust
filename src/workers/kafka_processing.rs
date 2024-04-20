use rdkafka::{
    consumer::{Consumer, ConsumerContext, StreamConsumer},
    message::BorrowedMessage,
    ClientConfig, ClientContext, Message,
};
use serde::{Deserialize, Serialize};
use tokio::{fs::File, io::AsyncWriteExt};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

async fn fetch_url(url: String, file_name: String) -> Result<()> {
    let mut response = reqwest::get(url).await?;
    if response.status().is_success() {
        let mut file = File::create(file_name).await?;
        while let Some(chunk) = response.chunk().await? {
            println!("write chunk");
            file.write_all(&chunk).await?;
        }
    }
    Ok(())
}

struct ConsumerCtx {}
impl ClientContext for ConsumerCtx {}
impl ConsumerContext for ConsumerCtx {
    fn commit_callback(
        &self,
        result: rdkafka::error::KafkaResult<()>,
        offsets: &rdkafka::TopicPartitionList,
    ) {
        match result {
            Ok(_) => log::info!("Offsets committed successfully: {:?}", offsets),
            Err(e) => log::warn!("Error while committing offetts: {}", e),
        }
    }
}

fn create_consumer(brokers: &str, group_id: &str, topic: &str) -> StreamConsumer<ConsumerCtx> {
    let ctx = ConsumerCtx {};
    let consumer: StreamConsumer<ConsumerCtx> = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        // Commit automatically every 5 seconds.
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "5000")
        // but only commit the offsets explicitly stored via `consumer.store_offset`.
        .set("enable.auto.offset.store", "false")
        .set_log_level(rdkafka::config::RDKafkaLogLevel::Debug)
        .create_with_context(ctx)
        .expect("created consumer failed");

    consumer
        .subscribe(&[topic])
        .expect("can't subscribe to specificed topic");
    consumer
}

#[derive(Serialize, Deserialize)]
struct KafkaMessage {
    url: String,
    fname: String,
}

async fn process_message(message: &BorrowedMessage<'_>) {
    if let Some(p) = message.payload() {
        let msg: KafkaMessage = serde_json::from_slice(p).unwrap();
        fetch_url(msg.url, msg.fname).await.unwrap();
    }
}

pub async fn start() {
    let consumer = create_consumer("localhost:19092", "hienphdv", "sstreamstreamtream");
    loop {
        match consumer.recv().await {
            Err(e) => log::warn!("Kafka Error {}", e),
            Ok(m) => process_message(&m).await,
        }
    }
}
