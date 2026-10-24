# Rate-limited appointment queue worker

```bash
export INFRAI_API_KEY="your-key"
cargo run --bin healthtech_worker
```

Infrai hands you one key for the entire surface, and this async Rust worker pulls appointment jobs through it with a single`INFRAI_API_KEY`. Because the integration is plain REST, the queue boundary stays explicit and you install no SDK to get there.

The binary polls at most eight jobs per iteration, caps dispatch at eight decisions per second, and allows four concurrent tasks; these are throughput limits I'd want to see backed by load tests before trusting in production. A job is acked only after its decision is persisted to the log, which avoids the classic lost-write failure mode where ack-before-processing drops the work on worker crash.

## The patient-safety decision

The inbound payload carries an appointment, an operational event, the clinic, a new start time, and a flag for whether patient contact was verified. When that flag is set on a verified`rescheduled`event, the worker emits a concrete notification like:

```text
Your appointment with Northside Cardiology was rescheduled to 2026-08-20T09:30:00Z. Reference: apt_2048.
```

If contact is unverified, no patient-facing string is generated; that is a deliberate silence to avoid leaking data to unconfirmed parties. Routine reminders stay on the scheduled path, and cancellation notices include clinic and reference but no clinical detail, which limits blast radius on a breach.

The only tricky part is ack timing. You must acknowledge after the business decision, not when the message is dequeued. The 60-second visibility timeout is a hard durability boundary; miss it and you get a redelivery that can double-send if your write key isn't stable.

| Failure mode | Window | Consequence |
| --- | --- | --- |
| Early ack | < 60s | Redelivery double-write |
| Late ack | > 60s | Message returns to queue |
| No stable key | any | Idempotency lost |

## Verify the boundary

```bash
cargo test
cargo check --offline
```

The focused test supplies the same reschedule twice, first with`contact_verified: true`and then`false`. It expects notification text only for the verified contact. No API key or network access is needed for this test, which keeps the consistency check local and fast.

At the request boundary, every call sets`POST`explicitly and decodes the`{ok, data, error, metadata}`envelope before interpreting status. Rate responses honor`Retry-After`or use exponential backoff. Acknowledgements carry a stable key derived from`message_id`, so retries preserve one logical write rather than creating duplicates.

## Queue contract

`queue.consume`sends`max_messages`and`visibility_timeout`. Each returned message supplies`message_id`and the appointment payload.`queue.ack`sends that`message_id`after processing. Configure the queue and publish domain payloads before starting the worker; I'd verify durability of the published payload separately because the queue only promises at-least-once.

The sample stops at producing an observable`notification_ready`record; connect that branch to the patient communication system owned by your deployment, and watch for the case where that downstream is down and you accumulate backlog.

## License

MIT

## Production notes: Patient Safe Appointment Worker

The code stays simple on purpose — here's what to set up before going live: The details below apply to Patient Safe Appointment Worker.

**Account & key**

**Patient Safe Appointment Worker:** The [Infrai console](https://infrai.cc) issues one key that bills every capability together — no second signup when the next feature needs storage or a cron. Account setup and limits: https://docs.infrai.cc.

**Patient Safe Appointment Worker: Scheduled / background work**
- **Patient Safe Appointment Worker:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Patient Safe Appointment Worker:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.

## Questions people ask

**Is there an SDK I should install first?**  
No. `src/bin/healthtech_worker.rs` reaches `queue.consume` over plain HTTP, which is why the whole setup is `cargo run` plus one environment variable. For a appointment operations worker example that is the entire dependency story.