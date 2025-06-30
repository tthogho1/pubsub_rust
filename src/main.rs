use redis::{Client, AsyncCommands, RedisError};
use redis::PubSubCommands;
use std::env;
use std::thread;
use std::time::Duration;
use bb8_redis::RedisConnectionManager;
use tokio;
use tokio::time::timeout;
use futures_util::stream::StreamExt; // Import StreamExt for .next() on streams

use tokio::sync::broadcast;
use dotenv::dotenv;

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
    println!("create pool with redis_url: {}", redis_url);

    // create connection pool
    let pool = match create_pool(&redis_url).await {
        Ok(pool) => pool,
        Err(e) => {
            eprintln!("Failed to create pool: {}", e);
            return Err(e);
        }
    };
    println!("pool created successfully");

    let con_result = timeout(Duration::from_secs(5), pool.get()).await;
    let mut con = match con_result {
        Ok(Ok(con)) => {
            // コネクション取得成功
            println!("Connection obtained from pool successfully");
            con
        }
        Ok(Err(e)) => {
            // プール側のエラー
            eprintln!("Pool error: {}", e);
            return Err(redis::RedisError::from((
                redis::ErrorKind::IoError,
                "Pool error",
                e.to_string(),
            )));
        }
        Err(_) => {
            // タイムアウト
            eprintln!("Timeout while getting connection from pool");
            return Err(redis::RedisError::from((
                redis::ErrorKind::IoError,
                "Timeout while getting connection from pool",
            )));
        }
    };

    //let mut con  = pool.get().await.unwrap();
    println!("publish to Redis : message Hello, world!");
    let _: () = con.publish("my_channel", "Hello, world!").await?;
    println!("message published successfully");
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
    if let Err(e) = dotenv() {
        eprintln!("❌ Error loading .env file: {}", e);
    } else {
        println!("✅ .env file loaded successfully");
    }

    let (tx, mut rx1) = broadcast::channel(16);
    let mut rx2: broadcast::Receiver<String> = tx.subscribe(); // 新しい受信機を作成

    let redis_url = env::var("REDIS_URL") .unwrap_or("redis://127.0.0.1:6379/".to_string());
    let redis_url_clone = redis_url.clone();
    println!("get redis_url: {}", redis_url);

    let client = Client::open(redis_url)?;
    
    //
    // spawn task to subscribe message
    //
    let _handle1 = tokio::spawn(async move {

        let mut con = match client.get_async_connection().await {
            Ok(con) => con,
            Err(e) => {
                eprintln!("Failed to get async connection: {}", e);
                std::process::exit(1);
            }
        };

        let mut pubsub = con.into_pubsub();
        if let Err(e) = pubsub.subscribe("my_channel").await {
            eprintln!("Failed to subscribe to channel: {}", e);
            return;
        }

        loop {
            println!("get message from Redis pubsub");
            match pubsub.on_message().next().await {
                Some(msg) => {
                    let payload: String = match msg.get_payload() {
                        Ok(p) => p,
                        Err(e) => {
                            println!("Failed to get payload: {}", e);
                            continue;
                        }
                    };
                    println!("Received message: {}", payload);

                    // send to broadcast channel
                    if let Err(e) = tx.send(payload.clone()) {
                        println!("Failed to send message to broadcast channel: {}", e);
                    }
                }
                None => {
                    println!("No more messages from pubsub");
                    break;
                }
            }
        }
    });

    // publish message to Redis pubsub
    let _handle2 = tokio::spawn(async move {
        println!("publish message to Redis pubsub");
        if let Err(e) = publish_message(redis_url_clone).await {
            println!("{:?}", e);
        };
    });

    // channelからreeive 1
    let receiver1 = tokio::spawn(async move {
        println!("receive 1: start");
        while let Ok(msg) = rx1.recv().await {
            println!("recive 1: {}", msg);
        }
    });

    // channelからreeive 2
    let receiver2 = tokio::spawn(async move {
        println!("receive 2: start");
        while let Ok(msg) = rx2.recv().await {
            println!("recieve 2: {}", msg);
        }
    });



    // wait for the task to finish
    let _ = tokio::join!(_handle1, _handle2);

    Ok(())
}
