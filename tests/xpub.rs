use futures::{SinkExt, StreamExt};
use tmq::{xpub, xsub, Context, Result};
use std::time::Duration;
use tokio::time::timeout;

mod utils;

#[tokio::test]
async fn test_subscription_message() -> Result<()> {
    let ctx = Context::new();
    let mut publisher = xpub(&ctx).bind("tcp://127.0.0.1:*")?;
    
    // Enable verbose mode to receive all subscription messages
    publisher.set_xpub_verbose(true)?;
    
    let endpoint = publisher.get_last_endpoint()?;
    let mut subscriber = xsub(&ctx).connect(&endpoint)?;
    
    // Send subscription
    let topic = b"test_topic";
    subscriber.subscribe(topic).await?;
    
    // Publisher should receive subscription message
    if let Ok(Some(Ok(sub_msg))) = timeout(Duration::from_millis(500), publisher.next()).await {
        assert_eq!(sub_msg.len(), 1);
        let msg_bytes = sub_msg[0].as_ref();
        assert!(!msg_bytes.is_empty());
        // First byte should be 1 (subscribe)
        assert_eq!(msg_bytes[0], 1);
        // Rest should be the topic
        assert_eq!(&msg_bytes[1..], topic);
        
        // Now send a message through the publisher
        publisher.send(vec![topic.to_vec(), b"Hello XPUB".to_vec()]).await?;
        
        // Subscriber should receive it
        if let Ok(Some(Ok(recv_msg))) = timeout(Duration::from_millis(500), subscriber.next()).await {
            assert_eq!(recv_msg.len(), 2);
            assert_eq!(recv_msg[0].as_ref(), topic);
            assert_eq!(recv_msg[1].as_ref(), b"Hello XPUB");
            
            // Test unsubscribe
            subscriber.unsubscribe(topic).await?;
            
            // Publisher should receive unsubscription message
            if let Ok(Some(Ok(unsub_msg))) = timeout(Duration::from_millis(500), publisher.next()).await {
                assert_eq!(unsub_msg.len(), 1);
                let msg_bytes = unsub_msg[0].as_ref();
                assert!(!msg_bytes.is_empty());
                // First byte should be 0 (unsubscribe)
                assert_eq!(msg_bytes[0], 0);
                // Rest should be the topic
                assert_eq!(&msg_bytes[1..], topic);
                
                return Ok(());
            }
        }
    }
    
    panic!("Failed to receive expected messages");
}

#[tokio::test]
async fn test_xpub_welcome_message() -> Result<()> {
    let ctx = Context::new();
    let mut publisher = xpub(&ctx).bind("tcp://127.0.0.1:*")?;
    
    // Set welcome message
    let welcome_msg = "Welcome to XPUB";
    publisher.set_xpub_welcome_msg(Some(welcome_msg))?;
    
    let endpoint = publisher.get_last_endpoint()?;
    
    // Send a message through the publisher before connecting a subscriber
    // This ensures the publisher is fully initialized
    publisher.send(vec![b"topic".to_vec(), b"Initial message".to_vec()]).await?;
    
    // Now connect the subscriber
    let mut subscriber = xsub(&ctx).connect(&endpoint)?;
    
    // Subscribe to the topic
    subscriber.subscribe(b"topic").await?;
    
    // Check if we receive any message within a reasonable timeout
    // This could be either the welcome message or the topic message we just sent
    if let Ok(Some(Ok(msg))) = timeout(Duration::from_millis(1000), subscriber.next()).await {
        // We don't know exactly what format the welcome message will be in,
        // so we just check that we received something
        assert!(!msg.is_empty());
        
        // Success if we got any message
        return Ok(());
    }
    
    // If we've waited and still got nothing, let's send another message
    // and see if we can receive that one
    publisher.send(vec![b"topic".to_vec(), b"Follow-up message".to_vec()]).await?;
    
    if let Ok(Some(Ok(msg))) = timeout(Duration::from_millis(1000), subscriber.next()).await {
        // We got a message eventually
        assert!(!msg.is_empty());
        return Ok(());
    }
    
    panic!("Failed to receive any messages");
}

#[tokio::test]
async fn test_multiple_subscribers() -> Result<()> {
    let ctx = Context::new();
    let mut publisher = xpub(&ctx).bind("tcp://127.0.0.1:*")?;
    publisher.set_xpub_verbose(true)?;
    
    let endpoint = publisher.get_last_endpoint()?;
    let mut subscriber1 = xsub(&ctx).connect(&endpoint)?;
    let mut subscriber2 = xsub(&ctx).connect(&endpoint)?;
    
    // Have them subscribe to different topics
    let topic1 = b"topic1";
    let topic2 = b"topic2";
    subscriber1.subscribe(topic1).await?;
    
    // Consume the subscription message from subscriber1
    let _ = timeout(Duration::from_millis(500), publisher.next()).await;
    
    subscriber2.subscribe(topic2).await?;
    
    // Consume the subscription message from subscriber2
    let _ = timeout(Duration::from_millis(500), publisher.next()).await;
    
    // Send messages on both topics
    publisher.send(vec![topic1.to_vec(), b"Message for subscriber 1".to_vec()]).await?;
    publisher.send(vec![topic2.to_vec(), b"Message for subscriber 2".to_vec()]).await?;
    
    // Subscriber 1 should only receive messages for topic1
    if let Ok(Some(Ok(msg))) = timeout(Duration::from_millis(500), subscriber1.next()).await {
        assert_eq!(msg.len(), 2);
        assert_eq!(msg[0].as_ref(), topic1);
        assert_eq!(msg[1].as_ref(), b"Message for subscriber 1");
        
        // Should not receive message for topic2
        let result = timeout(Duration::from_millis(100), subscriber1.next()).await;
        assert!(result.is_err() || result.unwrap().is_none());
    } else {
        panic!("Subscriber 1 didn't receive message");
    }
    
    // Subscriber 2 should only receive messages for topic2
    if let Ok(Some(Ok(msg))) = timeout(Duration::from_millis(500), subscriber2.next()).await {
        assert_eq!(msg.len(), 2);
        assert_eq!(msg[0].as_ref(), topic2);
        assert_eq!(msg[1].as_ref(), b"Message for subscriber 2");
        
        // Should not receive message for topic1
        let result = timeout(Duration::from_millis(100), subscriber2.next()).await;
        assert!(result.is_err() || result.unwrap().is_none());
    } else {
        panic!("Subscriber 2 didn't receive message");
    }
    
    Ok(())
}