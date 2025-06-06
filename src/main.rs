use futures::StreamExt;
use log::{info, trace};
use protobuf::Message;

mod scheduler;

mod protobuf_gen {
    include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProviderCacheEntry {
    id: String,
}

#[derive(Debug, Clone)]
struct Task {}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("trace"))
        .format_timestamp_secs()
        .init();

    trace!("Starting application...");

    let client = async_nats::connect("nats").await?;
    let mut subscriber = client.subscribe("providers").await?;

    trace!("Connected to NATS server and subscribed to 'providers' channel.");

    // Receive and process messages
    while let Some(message) = subscriber.next().await {
        handle_provider_message(message).await;
    }

    Ok(())
}

async fn handle_provider_message(message: async_nats::Message) {
    trace!("Got message: {:#?}", message);
    // Deserialize the message into a ProviderCacheEntry

    match protobuf_gen::deco::CacheEntry::parse_from_bytes(&message.payload) {
        Ok(cache_entry) => {
            trace!("Parsed CacheEntry: {:?}", cache_entry);
            // Process the cache entry here
        }
        Err(err) => {
            log::error!("Failed to parse CacheEntry: {}", err);
        }
    }
}

#[test]
fn schedule_wasi_binary() {
    // This function is a placeholder for scheduling the WASI binary.
    // You can implement the logic to run the WASI binary here.
    info!("Scheduling WASI binary execution...");
    // Example: task::spawn(async { run_wasi_binary().await; });
}
