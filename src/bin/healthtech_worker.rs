use patient_safe_appointment_worker::queue_worker::{
    decide_notification, InfraiQueueClient, NotificationDecision, RateLimiter, WorkerError,
};
use std::sync::Arc;
use tokio::{sync::Semaphore, task::JoinSet, time::Duration};

#[tokio::main]
async fn main() -> Result<(), WorkerError> {
    let client = InfraiQueueClient::from_env()?;
    let concurrency = Arc::new(Semaphore::new(4));
    let mut limiter = RateLimiter::per_second(8);

    loop {
        let messages = client.consume(8).await?;
        if messages.is_empty() {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }

        let mut tasks = JoinSet::new();
        for message in messages {
            limiter.acquire().await;
            let permit = concurrency.clone().acquire_owned().await.expect("semaphore is open");
            let client = client.clone();
            tasks.spawn(async move {
                let _permit = permit;
                match decide_notification(&message.payload) {
                    NotificationDecision::Send { text } => {
                        println!("notification_ready appointment={} text={text}", message.payload.appointment_id);
                    }
                    NotificationDecision::Skip { reason } => {
                        println!("notification_skipped appointment={} reason={reason}", message.payload.appointment_id);
                    }
                }
                client.ack(&message.message_id).await
            });
        }

        while let Some(result) = tasks.join_next().await {
            result.expect("worker task completes")?;
        }
    }
}
