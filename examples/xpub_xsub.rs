use futures::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::time::sleep;
use tmq::{xpub, xsub, Context, Result};

const ENDPOINT: &str = "tcp://127.0.0.1:5555";

/// This example demonstrates the use of XPUB/XSUB sockets.
/// XPUB/XSUB sockets extend the PUB/SUB pattern with the ability
/// to track subscriptions.
#[tokio::main]
async fn main() -> Result<()> {
    let ctx = Context::new();

    // Create an XPUB socket which can send messages
    let mut publisher = xpub(&ctx).bind(ENDPOINT)?;

    // Enable verbose mode to receive all subscription messages
    publisher.set_xpub_verbose(true)?;
    println!("Publisher bound to {}", ENDPOINT);

    // Set a welcome message that will be sent to new subscribers
    publisher.set_xpub_welcome_msg(Some("Welcome to XPUB example"))?;

    // Create a separate task for the subscriber
    let subscriber_task = tokio::spawn(async move {
        // Give the publisher time to initialize
        sleep(Duration::from_millis(100)).await;

        // Create an XSUB socket which can send custom subscriptions
        let mut subscriber = xsub(&ctx).connect(ENDPOINT)?;
        println!("Subscriber connected to {}", ENDPOINT);

        // Subscribe to a topic
        let topic = b"weather";
        println!("Subscribing to 'weather' topic");
        subscriber.subscribe(topic).await?;

        // Wait for and print 3 messages
        for i in 0..3 {
            match subscriber.next().await {
                Some(Ok(msg)) => {
                    println!(
                        "Subscriber received message #{}: {:?}",
                        i + 1,
                        msg.iter()
                            .map(|item| String::from_utf8_lossy(item.as_ref()).to_string())
                            .collect::<Vec<String>>()
                    );
                }
                Some(Err(e)) => eprintln!("Error receiving message: {}", e),
                None => break,
            }
        }

        // Unsubscribe from the topic
        println!("Unsubscribing from 'weather' topic");
        subscriber.unsubscribe(topic).await?;

        // Give the publisher time to process the unsubscription
        sleep(Duration::from_millis(100)).await;

        Ok::<(), tmq::TmqError>(())
    });

    // In the main task, handle both publishing and subscription monitoring
    let mut subscription_count = 0;

    // Send messages while also monitoring for subscription events
    for i in 1..=5 {
        // Send a message
        println!("Publishing weather update #{}", i);
        publisher
            .send(vec![
                b"weather".to_vec(),
                format!("Temperature: {} degrees", 20 + i).into_bytes(),
            ])
            .await?;

        // Check for any subscription messages (with a short timeout)
        // that might have arrived since the last message
        match tokio::time::timeout(Duration::from_millis(100), publisher.next()).await {
            Ok(Some(Ok(msg))) => {
                if !msg.is_empty() && !msg[0].is_empty() {
                    let data = msg[0].as_ref();
                    let command = data[0];
                    let topic = &data[1..];

                    if command == 1 {
                        subscription_count += 1;
                        println!(
                            "Publisher received SUBSCRIBE to topic: {}",
                            String::from_utf8_lossy(topic)
                        );
                        println!("Current subscription count: {}", subscription_count);
                    } else if command == 0 {
                        subscription_count -= 1;
                        println!(
                            "Publisher received UNSUBSCRIBE from topic: {}",
                            String::from_utf8_lossy(topic)
                        );
                        println!("Current subscription count: {}", subscription_count);
                    }
                }
            }
            Ok(Some(Err(e))) => eprintln!("Error receiving subscription: {}", e),
            _ => {} // Timeout or None, just continue
        }

        sleep(Duration::from_secs(1)).await;
    }

    // Wait for the subscriber task to finish
    if let Err(e) = subscriber_task.await {
        eprintln!("Subscriber task error: {}", e);
    }

    Ok(())
}
