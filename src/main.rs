use rabbitmq_stream_client::{
    Environment, RabbitMQStreamResult,
    error::StreamCreateError,
    types::{ByteCapacity, ResponseCode},
};

#[tokio::main]
async fn main() -> RabbitMQStreamResult<()> {
    println!("Connecting to RabbitMQ server");

    let environment = Environment::builder().host("172.17.0.1").username("user").password("password").build().await?;
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
    Ok(())
}
