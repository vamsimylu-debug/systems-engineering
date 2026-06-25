# AI Notes

## A prompt I used
"Given this Redis Streams worker, redesign it for at-least-once delivery with effectively-once ledger updates under duplicate orders, worker restarts, and flaky downstream HTTP calls. Show a safe ack strategy and failure handling."

## Something the AI got wrong or oversimplified and how I caught it
One generated draft acknowledged (`XACK`) immediately after a successful payment call, before atomically applying ledger state and marking order completion. That is unsafe: if the worker crashes between ack and ledger update, the message is lost and the order is never reflected in the ledger. I corrected this by making ack the last step only after idempotent state transition to `done` and ledger increment succeed together. I also rejected an "exactly-once" claim and documented the actual semantics as at-least-once delivery with effectively-once effects.
