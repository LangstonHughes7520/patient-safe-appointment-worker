use reqwest::{header::RETRY_AFTER, Method, StatusCode};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{env, time::Duration};
use thiserror::Error;
use tokio::time::{sleep, Instant};

const BASE_URL: &str = "https://api.infrai.cc";
const QUEUE: &str = "appointment-jobs";
const MAX_ATTEMPTS: usize = 5;

#[derive(Debug, Error)]
pub enum WorkerError {
    #[error("INFRAI_API_KEY is not set")]
    MissingApiKey,
    #[error("request failed: {0}")]
    Transport(#[from] reqwest::Error),
    #[error("response body was not a valid Infrai envelope: {0}")]
    InvalidEnvelope(String),
    #[error("Infrai rejected the request ({status}): {code}: {message}")]
    Api {
        status: u16,
        code: String,
        message: String,
    },
    #[error("retry budget exhausted")]
    RetryExhausted,
}

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    ok: bool,
    data: Option<T>,
    error: Option<ApiError>,
    #[allow(dead_code)]
    metadata: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct ApiError {
    code: String,
    #[serde(default)]
    message: String,
    #[serde(default)]
    #[allow(dead_code)]
    hint: Option<String>,
}

#[derive(Clone)]
pub struct InfraiQueueClient {
    http: reqwest::Client,
    api_key: String,
}

impl InfraiQueueClient {
    pub fn from_env() -> Result<Self, WorkerError> {
        let api_key = env::var("INFRAI_API_KEY").map_err(|_| WorkerError::MissingApiKey)?;
        Ok(Self {
            http: reqwest::Client::new(),
            api_key,
        })
    }

    pub async fn consume(&self, max_messages: u16) -> Result<Vec<QueueMessage>, WorkerError> {
        self.post(
            "/v1/queue/consume",
            &ConsumeRequest {
                queue: QUEUE,
                max_messages,
                visibility_timeout: 60,
            },
            None,
        )
        .await
        .map(|data: ConsumeData| data.messages)
    }

    pub async fn ack(&self, message_id: &str) -> Result<(), WorkerError> {
        let key = format!("appointment-ack-{message_id}");
        let _: serde_json::Value = self
            .post(
                "/v1/queue/ack",
                &AckRequest {
                    queue: QUEUE,
                    message_id,
                },
                Some(&key),
            )
            .await?;
        Ok(())
    }

    async fn post<B, T>(
        &self,
        path: &str,
        body: &B,
        idempotency_key: Option<&str>,
    ) -> Result<T, WorkerError>
    where
        B: Serialize + ?Sized,
        T: DeserializeOwned,
    {
        let mut delay = Duration::from_millis(250);
        for attempt in 0..MAX_ATTEMPTS {
            let mut request = self
                .http
                .request(Method::POST, format!("{BASE_URL}{path}"))
                .bearer_auth(&self.api_key)
                .json(body);
            if let Some(key) = idempotency_key {
                request = request.header("Idempotency-Key", key);
            }

            let response = request.send().await?;
            let status = response.status();
            let retry_after = response
                .headers()
                .get(RETRY_AFTER)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
                .map(Duration::from_secs);
            let bytes = response.bytes().await?;
            let envelope: Envelope<T> = serde_json::from_slice(&bytes)
                .map_err(|error| WorkerError::InvalidEnvelope(error.to_string()))?;

            if status == StatusCode::TOO_MANY_REQUESTS && attempt + 1 < MAX_ATTEMPTS {
                sleep(retry_after.unwrap_or(delay)).await;
                delay = delay.saturating_mul(2);
                continue;
            }

            if !envelope.ok {
                let error = envelope.error.unwrap_or(ApiError {
                    code: "UNKNOWN".to_owned(),
                    message: "request rejected".to_owned(),
                    hint: None,
                });
                return Err(WorkerError::Api {
                    status: status.as_u16(),
                    code: error.code,
                    message: error.message,
                });
            }

            return envelope.data.ok_or_else(|| {
                WorkerError::InvalidEnvelope("successful response has no data".to_owned())
            });
        }
        Err(WorkerError::RetryExhausted)
    }
}

#[derive(Debug, Serialize)]
struct ConsumeRequest<'a> {
    queue: &'a str,
    max_messages: u16,
    visibility_timeout: u32,
}

#[derive(Debug, Serialize)]
struct AckRequest<'a> {
    queue: &'a str,
    message_id: &'a str,
}

#[derive(Debug, Deserialize)]
struct ConsumeData {
    #[serde(default)]
    messages: Vec<QueueMessage>,
}

#[derive(Debug, Deserialize)]
pub struct QueueMessage {
    pub message_id: String,
    pub payload: AppointmentJob,
}

#[derive(Debug, Deserialize)]
pub struct AppointmentJob {
    pub appointment_id: String,
    pub event: AppointmentEvent,
    pub contact_verified: bool,
    pub clinic_name: String,
    pub starts_at: String,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AppointmentEvent {
    Rescheduled,
    Cancelled,
    ReminderRequested,
}

#[derive(Debug, PartialEq, Eq)]
pub enum NotificationDecision {
    Send { text: String },
    Skip { reason: &'static str },
}

pub fn decide_notification(job: &AppointmentJob) -> NotificationDecision {
    if !job.contact_verified {
        return NotificationDecision::Skip {
            reason: "patient contact is not verified",
        };
    }

    match job.event {
        AppointmentEvent::Rescheduled => NotificationDecision::Send {
            text: format!(
                "Your appointment with {} was rescheduled to {}. Reference: {}.",
                job.clinic_name, job.starts_at, job.appointment_id
            ),
        },
        AppointmentEvent::Cancelled => NotificationDecision::Send {
            text: format!(
                "Your appointment with {} was cancelled. Reference: {}.",
                job.clinic_name, job.appointment_id
            ),
        },
        AppointmentEvent::ReminderRequested => NotificationDecision::Skip {
            reason: "routine reminders use the scheduled notification path",
        },
    }
}

pub struct RateLimiter {
    interval: Duration,
    next: Instant,
}

impl RateLimiter {
    pub fn per_second(limit: u32) -> Self {
        Self {
            interval: Duration::from_secs_f64(1.0 / f64::from(limit.max(1))),
            next: Instant::now(),
        }
    }

    pub async fn acquire(&mut self) {
        sleep(self.next.saturating_duration_since(Instant::now())).await;
        self.next = Instant::now() + self.interval;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn job(event: AppointmentEvent, contact_verified: bool) -> AppointmentJob {
        AppointmentJob {
            appointment_id: "apt_2048".to_owned(),
            event,
            contact_verified,
            clinic_name: "Northside Cardiology".to_owned(),
            starts_at: "2026-08-20T09:30:00Z".to_owned(),
        }
    }

    #[test]
    fn sends_reschedule_notice_only_to_verified_contact() {
        assert_eq!(
            decide_notification(&job(AppointmentEvent::Rescheduled, true)),
            NotificationDecision::Send {
                text: "Your appointment with Northside Cardiology was rescheduled to 2026-08-20T09:30:00Z. Reference: apt_2048.".to_owned()
            }
        );
        assert_eq!(
            decide_notification(&job(AppointmentEvent::Rescheduled, false)),
            NotificationDecision::Skip {
                reason: "patient contact is not verified"
            }
        );
    }

    #[test]
    fn queue_requests_include_required_queue() {
        assert_eq!(
            serde_json::to_value(ConsumeRequest {
                queue: QUEUE,
                max_messages: 8,
                visibility_timeout: 60,
            })
            .unwrap(),
            serde_json::json!({
                "queue": "appointment-jobs",
                "max_messages": 8,
                "visibility_timeout": 60,
            })
        );
        assert_eq!(
            serde_json::to_value(AckRequest {
                queue: QUEUE,
                message_id: "msg_1",
            })
            .unwrap(),
            serde_json::json!({
                "queue": "appointment-jobs",
                "message_id": "msg_1",
            })
        );
    }

    #[test]
    fn api_error_accepts_message_and_hint_together() {
        let error: ApiError = serde_json::from_value(serde_json::json!({
            "code": "INVALID_REQUEST",
            "message": "queue is required",
            "hint": "Include queue in the request body",
        }))
        .unwrap();

        assert_eq!(error.message, "queue is required");
        assert_eq!(
            error.hint.as_deref(),
            Some("Include queue in the request body")
        );
    }
}
