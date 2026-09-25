#![allow(dead_code)]

use lapin::options::{BasicPublishOptions, ConfirmSelectOptions, QueueDeclareOptions};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Connection, ConnectionProperties};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage};

pub struct Broker {
    _container: ContainerAsync<GenericImage>,
    pub url: String,
}

pub async fn start_broker() -> Broker {
    let container = GenericImage::new("rabbitmq", "3-management")
        .with_exposed_port(5672.tcp())
        .with_wait_for(WaitFor::message_on_stdout("Server startup complete"))
        .start()
        .await
        .expect("rabbitmq container start");
    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(5672).await.expect("port");
    let url = format!("amqp://guest:guest@{host}:{port}/%2f");
    for _ in 0..60 {
        if Connection::connect(&url, ConnectionProperties::default())
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
    Broker {
        _container: container,
        url,
    }
}

pub async fn connect(url: &str) -> Connection {
    Connection::connect(url, ConnectionProperties::default())
        .await
        .expect("connect")
}

pub async fn declare_queue(url: &str, queue: &str) {
    let conn = connect(url).await;
    let ch = conn.create_channel().await.unwrap();
    ch.queue_declare(
        queue.into(),
        QueueDeclareOptions {
            durable: true,
            ..Default::default()
        },
        FieldTable::default(),
    )
    .await
    .unwrap();
    conn.close(200, "".into()).await.ok();
}

pub async fn publish_raw(url: &str, exchange: &str, routing_key: &str, bodies: &[Vec<u8>]) {
    publish_with(
        url,
        exchange,
        routing_key,
        bodies,
        BasicProperties::default(),
    )
    .await;
}

pub async fn publish_with(
    url: &str,
    exchange: &str,
    routing_key: &str,
    bodies: &[Vec<u8>],
    props: BasicProperties,
) {
    let conn = connect(url).await;
    let ch = conn.create_channel().await.unwrap();
    ch.confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    for body in bodies {
        ch.basic_publish(
            exchange.into(),
            routing_key.into(),
            BasicPublishOptions::default(),
            body,
            props.clone(),
        )
        .await
        .unwrap()
        .await
        .unwrap();
    }
    conn.close(200, "".into()).await.ok();
}

pub async fn publish_json(url: &str, queue: &str, n: usize) {
    let bodies: Vec<Vec<u8>> = (1..=n)
        .map(|i| format!(r#"{{"id":{i}}}"#).into_bytes())
        .collect();
    publish_raw(url, "", queue, &bodies).await;
}

/// Ready (unacknowledged-excluded) message count of a queue.
pub async fn ready_count(url: &str, queue: &str) -> u32 {
    let conn = connect(url).await;
    let ch = conn.create_channel().await.unwrap();
    let q = ch
        .queue_declare(
            queue.into(),
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    conn.close(200, "".into()).await.ok();
    q.message_count()
}

/// Poll until the queue's ready count reaches `want` (requeues are async).
pub async fn wait_ready(url: &str, queue: &str, want: u32) -> u32 {
    let mut last = 0;
    for _ in 0..50 {
        last = ready_count(url, queue).await;
        if last == want {
            return last;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    last
}
