use futures::StreamExt;
use log::{info, trace};
use protobuf::Message;
use rabbitmq_stream_client::{
    error::StreamCreateError, types::{ByteCapacity, OffsetSpecification, ResponseCode}, Consumer, ConsumerHandle, Environment
};
use tokio::task;

include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));

struct CacheEntry {
    id: String,

}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace"))
        .format_timestamp_secs()
        .init();

    info!("Starting application...");

    let environment = Environment::builder()
        .username("user")
        .password("password")
        .build()
        .await?;

    info!("Connected to RabbitMQ Stream API");

    let stream = "provider-stream";
    let create_response = environment
        .stream_creator()
        .max_length(ByteCapacity::GB(5))
        .create(stream)
        .await;

    if let Err(e) = create_response {
        if let StreamCreateError::Create { stream, status } = e {
            match status {
                // we can ignore this error because the stream already exists
                ResponseCode::StreamAlreadyExists => {}
                err => {
                    println!("Error creating stream: {:?} {:?}", stream, err);
                }
            }
        }
    }

    let consumer = environment
        .consumer()
        .offset(OffsetSpecification::First)
        .build(stream)
        .await
        .unwrap();

    info!("Consumer created for stream: {}", stream);

    let handle = consumer.handle();
    handle_provider_message(consumer).await;

    Ok(())
}

async fn handle_provider_message(mut consumer: Consumer)
{
    while let Some(delivery) = consumer.next().await {
        let d = delivery.unwrap();
        trace!("Got message: {:#?} with offset: {}",
                 d.message().data().map(|data| String::from_utf8(data.to_vec()).unwrap()),
                 d.offset(),);
        if let Some(data) = d.message().data() {
            match deco::CacheEntry::parse_from_bytes(data){
                Ok(cache_entry) => {
                    trace!("Parsed CacheEntry: id={}", cache_entry.id.unwrap());
                    // Process the cache entry here
                }
                Err(err) => {
                    log::error!("Failed to parse CacheEntry: {}", err);
                }
            }
        } else {
            trace!("Message has no data");
        }
    }
}

fn schedule_rr() {
    // Schedule wasi binary
}

#[test]
fn schedule_wasi_binary() {
    // This function is a placeholder for scheduling the WASI binary.
    // You can implement the logic to run the WASI binary here.
    info!("Scheduling WASI binary execution...");
    // Example: task::spawn(async { run_wasi_binary().await; });
}