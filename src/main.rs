use futures::StreamExt;
use log::{info, trace};
use rabbitmq_stream_client::{
    error::StreamCreateError, types::{ByteCapacity, Message, OffsetSpecification, ResponseCode}, Consumer, ConsumerHandle, Environment
};
use tokio::task;

struct CacheEntry {
    id: String,

}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let environment = Environment::builder()
        .host("rabbitmq")
        .port(5552)
        .username("user")
        .password("password")
        .build()
        .await?;

    info!("Connected to RabbitMQ Stream");

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
    task::spawn(async move {
        handle_provider_message(consumer).await;
    });

    Ok(())
}

async fn handle_provider_message(mut consumer: Consumer)
{
    while let Some(delivery) = consumer.next().await {
        let d = delivery.unwrap();
        trace!("Got message: {:#?} with offset: {}",
                 d.message().data().map(|data| String::from_utf8(data.to_vec()).unwrap()),
                 d.offset(),);
        
    }
}