use std::{
    sync::{Arc, atomic::AtomicU64},
    time::Duration,
};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use gugugaga::fragment_connect;
use tokio::time::interval;

async fn run(
    cnt: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
    send_cnt: Arc<AtomicU64>, // 新增：发送计数器
    message: Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = "ws://127.0.0.1:8080";
    let (mut reader, mut writer, response) = fragment_connect(url, None).await?;
    println!("Connected with response: {:?}", response);

    tokio::spawn(async move {
        while let Some(msg) = reader.next().await {
            match msg {
                Ok(message) => {
                    cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    match &message {
                        gugugaga::Message::Text(text) => {
                            bytes
                                .fetch_add(text.len() as u64, std::sync::atomic::Ordering::Relaxed);
                        }
                        gugugaga::Message::Binary(bin) => {
                            bytes.fetch_add(bin.len() as u64, std::sync::atomic::Ordering::Relaxed);
                        }
                        _ => {}
                    }
                }
                Err(e) => {
                    eprintln!("Error receiving message: {}", e);
                    break;
                }
            }
        }
    });

    loop {
        writer
            .send(gugugaga::Message::Binary(message.clone()))
            .await?;
        send_cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cnt = Arc::new(AtomicU64::new(0));
    let bytes = Arc::new(AtomicU64::new(0));
    let send_cnt = Arc::new(AtomicU64::new(0));

    // let message_20b = Bytes::from_static("xxxxxxxxxxxxxxxxxxxx".as_bytes());
    let message_16384b = Bytes::from_static("x".repeat(16384).leak().as_bytes());
    // 统计任务
    {
        let send_cnt = send_cnt.clone();
        let recv_cnt = cnt.clone();
        let recv_bytes = bytes.clone();
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(1));
            let mut last_send = 0u64;
            let mut last_recv = 0u64;
            loop {
                interval.tick().await;
                let current_send = send_cnt.load(std::sync::atomic::Ordering::Relaxed);
                let current_recv = recv_cnt.load(std::sync::atomic::Ordering::Relaxed);
                let current_bytes = recv_bytes.load(std::sync::atomic::Ordering::Relaxed);

                println!(
                    "Send: {}/s, Recv: {}/s, Total recv: {} ({} bytes)",
                    current_send - last_send,
                    current_recv - last_recv,
                    current_recv,
                    current_bytes
                );

                last_send = current_send;
                last_recv = current_recv;
            }
        });
    }

    for i in 0..20 {
        println!("Starting client {}", i);
        let cnt = cnt.clone();
        let bytes = bytes.clone();
        let send_cnt = send_cnt.clone();
        // let message_20b = message_20b.clone();
        let message_16384b = message_16384b.clone();
        tokio::spawn({
            async move {
                if let Err(e) = run(cnt, bytes, send_cnt, message_16384b).await {
                    eprintln!("Client {} error: {}", i, e);
                }
            }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    loop {
        tokio::time::sleep(Duration::from_secs(60)).await;
    }
}
