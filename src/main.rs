use redis::{Client,AsyncCommands,RedisError};
use std::env;
use std::thread;
use std::time::Duration;
use bb8_redis::RedisConnectionManager;
use tokio;

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

    let redis_url = env::var("REDIS_URL") .unwrap_or("redis://127.0.0.1:6379/".to_string());
    let redis_url_clone = redis_url.clone();
 
    let client = Client::open(redis_url)?;
    let mut con = connect_with_retry(&client, 10)?;
    let mut pubsub = con.as_pubsub();
    pubsub.subscribe("my_channel")?;

    let _handle = tokio::spawn(async move {
        if let Err(e) = publish_message(redis_url_clone).await{
            println!("{:?}", e);
        };
    });

    //
    // receive message
    //
    loop {
        let msg = pubsub.get_message()?;
        let payload : String = msg.get_payload()?;
        println!("channel '{}': {}", msg.get_channel_name(), payload);
        
       // break; // for test
    };

    Ok(())
}
