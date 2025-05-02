use redis::{Client,AsyncCommands,RedisError};
use std::env;
use std::thread;
use std::time::Duration;
use bb8_redis::RedisConnectionManager;
use tokio;

use tokio::sync::broadcast;


pub async fn create_pool(redis_url: &str) -> Result<bb8::Pool<RedisConnectionManager>, redis::RedisError> {
    let manager = RedisConnectionManager::new(redis_url)?;
    let pool = bb8::Pool::builder()
        .connection_timeout(Duration::from_secs(60))
        .max_size(15)
        .build(manager)
        .await
        .map_err(|e| RedisError::from((redis::ErrorKind::IoError, "Failed to build pool", e.to_string())))?;
    Ok(pool)
}

async fn publish_message(redis_url: String) -> redis::RedisResult<()> {
    println!("redis_url: {}", redis_url);

    // create connection pool
    let pool = match create_pool(&redis_url).await {
        Ok(pool) => pool,
        Err(e) => {
            eprintln!("Failed to create pool: {}", e);
            return Err(e);
        }
    };
    let mut con  = pool.get().await.unwrap();

    let _: () = con.publish("my_channel", "Hello, world!").await?;
    Ok(())
}

fn connect_with_retry(client: &redis::Client, max_retries: u32) -> redis::RedisResult<redis::Connection> {
    for attempt in 1..=max_retries {
        match client.get_connection_with_timeout(Duration::from_secs(10)) {
            Ok(con) => return Ok(con),
            Err(e) => {
                eprintln!("attempt {} faile: {}", attempt, e);
                if attempt < max_retries {
                    thread::sleep(Duration::from_secs(2));
                }
            }
        }
    }
    Err(redis::RedisError::from((redis::ErrorKind::IoError, "Over max retry count for redis connection")))
}

#[tokio::main]
async fn main() -> redis::RedisResult<()> {
    dotenv::dotenv().ok();

    let (tx, mut rx1) = broadcast::channel(16);
    let mut rx2: broadcast::Receiver<String> = tx.subscribe(); // 新しい受信機を作成

    let redis_url = env::var("REDIS_URL") .unwrap_or("redis://127.0.0.1:6379/".to_string());
    let redis_url_clone = redis_url.clone();
 
    let client = Client::open(redis_url)?;
    let mut con = connect_with_retry(&client, 10)?;
    con.as_pubsub().subscribe("my_channel")?;

    let mut pubsub_task = con; // Make pubsub_task mutable

    //
    // spawn task to subscribe message
    //
    let _handle1 = tokio::spawn(async move {
        let mut pubsub = pubsub_task.as_pubsub(); // Use pubsub_task as_pubsub directly
        loop {
            let msg = match pubsub.get_message() {
                Ok(message) => {
                    let payload = message.get_payload().unwrap_or_else(|_| {
                        println!("Failed to get payload");
                        String::new()
                    });
                    // send to broadcast channel
                    if let Err(e) = tx.send(payload.clone()) {
                        println!("Failed to send message to broadcast channel: {}", e);
                    }

                    Ok((message, payload))
                }
                Err(e) => {
                    println!("Failed to get message: {}", e);
                    Err(e)
                }
            };

            match msg {
                Ok((msg, payload)) => println!("channel '{}': {}", msg.get_channel_name(), payload),
                Err(e) => println!("Failed to get message: {}", e),
            }
        }
    });


    let _handle2 = tokio::spawn(async move {
        if let Err(e) = publish_message(redis_url_clone).await {
            println!("{:?}", e);
        };
    });



    // channelからreeive 1
    let receiver1 = tokio::spawn(async move {
        while let Ok(msg) = rx1.recv().await {
            println!("受信機1: {}", msg);
        }
    });

    // channelからreeive 2
    let receiver2 = tokio::spawn(async move {
        while let Ok(msg) = rx2.recv().await {
            println!("受信機2: {}", msg);
        }
    });



    // wait for the task to finish
    let _ = tokio::join!(_handle1, _handle2);

    Ok(())
}
