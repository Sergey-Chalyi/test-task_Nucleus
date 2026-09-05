## Task
Build a small async event processing service:

1. HTTP endpoint to receive transaction events: `{id, user_id, amount, currency, timestamp}`.
2. Push events to a queue (your choice).
3. Consumer reads from queue, deduplicates by `id`, converts amount to USD, stores result.
4. Handle a failing downstream: if the DB or rate lookup is unavailable, do not lose events. Retry with backoff. Document your choice (at-least-once vs exactly-once).
5. APIs:
   - `GET /users/{user_id}/summary` → total USD + transaction count.
   - `GET /users/{user_id}/transactions?from=&to=` → paginated list.
6. One basic metric exposed (e.g. events processed, lag, failures).

## Rules
- Language should be python.
- Keep it simple. Assume ~100 events/sec, bursts to ~1k/sec.
- Docker Compose to run everything locally.
- README: how to run, why you picked your queue, one trade-off you made, what you'd change at 10x load.
- Unit tests for dedup + currency conversion.
- Push the solution to a public git repo (GitHub, GitLab, etc.) and share the link.

## Follow-Up Interview
You will walk us through your source code. We will pick random lines and ask you to explain them. **You must show you understand every part of what you submitted.** Be ready to modify a piece live.