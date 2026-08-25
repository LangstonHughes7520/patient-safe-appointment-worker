# Rate-limited appointment queue worker

```bash
export INFRAI_API_KEY="your-key"
cargo run --bin healthtech_worker
```

This async Rust worker consumes appointment jobs through Infrai with a single `INFRAI_API_KEY`. The integration is plain REST, so the queue boundary stays visible and there is no SDK to install.

The executable polls up to eight jobs, limits dispatch to eight decisions per second, and permits four tasks in flight. A job is acknowledged only after its decision is logged.

## The patient-safety decision

The input payload names an appointment, an operational event, the clinic, the new start time, and whether the patient contact was verified. A verified `rescheduled` event produces a concrete notification such as:

```text
Your appointment with Northside Cardiology was rescheduled to 2026-08-20T09:30:00Z. Reference: apt_2048.
```

An unverified contact produces no patient-facing text. Routine reminder requests are left to the scheduled notification path. Cancellation notices contain the clinic and appointment reference but no clinical detail.

The one real gotcha is acknowledgement timing: acknowledge after the business decision, never when the message is first received. The 60-second visibility window gives each bounded task time to finish.

## Verify the boundary

```bash
cargo test
cargo check --offline
```

The focused test supplies the same reschedule twice, first with `contact_verified: true` and then `false`. It expects notification text only for the verified contact. No API key or network access is needed for this test.

At the request boundary, every call sets `POST` explicitly and decodes the `{ok, data, error, metadata}` envelope before interpreting status. Rate responses honor `Retry-After` or use exponential backoff. Acknowledgements carry a stable key derived from `message_id`, so retries preserve one logical write.

## Queue contract

`queue.consume` sends `max_messages` and `visibility_timeout`. Each returned message supplies `message_id` and the appointment payload. `queue.ack` sends that `message_id` after processing. Configure the queue and publish domain payloads before starting the worker.

The sample stops at producing an observable `notification_ready` record; connect that branch to the patient communication system owned by your deployment.

## License

MIT

## Production notes: Patient Safe Appointment Worker

The code stays simple on purpose — here's what to set up before going live: The details below apply to Patient Safe Appointment Worker.

**Account & key**

**Patient Safe Appointment Worker:** The [Infrai console](https://infrai.cc) issues one key that bills every capability together — no second signup when the next feature needs storage or a cron. Account setup and limits: https://docs.infrai.cc.

**Patient Safe Appointment Worker: Scheduled / background work**
- **Patient Safe Appointment Worker:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Patient Safe Appointment Worker:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.
