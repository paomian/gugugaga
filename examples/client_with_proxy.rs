use std::{
    sync::{Arc, atomic::AtomicU64},
    time::Duration,
};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt, future::Either};
use gugugaga::Message;
use gugugaga::fragment_connect_with_proxy;
use log::warn;
use tokio::time::interval;

async fn run(
    cnt: Arc<AtomicU64>,
    bytes: Arc<AtomicU64>,
    send_cnt: Arc<AtomicU64>, // 新增：发送计数器
    message: Bytes,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
    let (mut reader, mut writer, response) =
        fragment_connect_with_proxy(url, "socks5h://127.0.0.1:7890", Default::default()).await?;
    println!("Connected with response: {:?}", response);
    tokio::spawn(async move {
        while let Some(msg) = reader.next().await {
            match msg {
                Ok(either) => {
                    cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    match either {
                        Either::Left(message) => match message {
                            Message::Text(text) => {
                                println!("Received text message: {}", text);
                                bytes.fetch_add(
                                    text.len() as u64,
                                    std::sync::atomic::Ordering::Relaxed,
                                );
                            }
                            Message::Binary(bin) => {
                                println!("Received binary message: {:x?}", bin);
                                bytes.fetch_add(
                                    bin.len() as u64,
                                    std::sync::atomic::Ordering::Relaxed,
                                );
                            }
                            Message::Ping(payload) => {
                                println!("Received ping with payload: {:x?}", payload);
                            }
                            Message::Pong(payload) => {
                                println!("Received pong with payload: {:x?}", payload);
                            }
                            Message::Close(code, reason) => {
                                println!(
                                    "Received close message: code={:?}, reason={:?}",
                                    code, reason
                                );
                            }
                            _ => {}
                        },
                        Either::Right(_) => {
                            warn!(
                                "Received fragmented message, which more than 10MB, Streaming Interface is ignored in this example."
                            );
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error receiving message: {}", e);
                    break;
                }
            }
        }
    });
    writer
        .send(Message::Text(message.try_into().unwrap()))
        .await?;
    send_cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mut interval = interval(Duration::from_secs(10));
    loop {
        interval.tick().await;
        writer.send(Message::Ping(Bytes::new())).await?;
        send_cnt.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cnt = Arc::new(AtomicU64::new(0));
    let bytes = Arc::new(AtomicU64::new(0));
    let send_cnt = Arc::new(AtomicU64::new(0));

    // 统计任务
    // {
    //     let send_cnt = send_cnt.clone();
    //     let recv_cnt = cnt.clone();
    //     let recv_bytes = bytes.clone();
    //     tokio::spawn(async move {
    //         let mut interval = interval(Duration::from_secs(1));
    //         let mut last_send = 0u64;
    //         let mut last_recv = 0u64;
    //         loop {
    //             interval.tick().await;
    //             let current_send = send_cnt.load(std::sync::atomic::Ordering::Relaxed);
    //             let current_recv = recv_cnt.load(std::sync::atomic::Ordering::Relaxed);
    //             let current_bytes = recv_bytes.load(std::sync::atomic::Ordering::Relaxed);

    //             println!(
    //                 "Send: {}/s, Recv: {}/s, Total recv: {} ({} bytes)",
    //                 current_send - last_send,
    //                 current_recv - last_recv,
    //                 current_recv,
    //                 current_bytes
    //             );

    //             last_send = current_send;
    //             last_recv = current_recv;
    //         }
    //     });
    // }

    for i in 0..1 {
        println!("Starting client {}", i);
        let cnt = cnt.clone();
        let bytes = bytes.clone();
        let send_cnt = send_cnt.clone();
        let message = Bytes::from_static(
            r#"{
    "assets_ids": [
        "31485881327745134150707048280038521990792393011833115608992370801267178227980",
        "34969960179164892621961191856246020571411902381299826005074339654076229989857"
    ],
    "operation": "subscribe",
    "custom_feature_enabled": false
}"#
            .as_bytes(),
        );
        tokio::spawn({
            async move {
                if let Err(e) = run(cnt, bytes, send_cnt, message).await {
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
