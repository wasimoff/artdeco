use rabbitmq_stream_client::{
    Environment,
    error::StreamCreateError,
    types::{ByteCapacity, Message, ResponseCode},
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Connecting to RabbitMQ server");

    let environment = Environment::builder()
        .host("rabbitmq")
        .port(5552)
        .username("user")
        .password("password")
        .build()
        .await?;

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

    let producer = environment.producer().build(stream).await?;
    producer
        .send_with_confirm(Message::builder().body("Hello, World!").build())
        .await?;

    println!("Sent message to stream: {}", stream);
    producer.close().await?;

    Ok(())
}
