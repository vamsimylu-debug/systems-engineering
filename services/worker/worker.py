"""Order worker with resilient delivery and idempotent effects.

Semantics:
  - at-least-once delivery from Redis Streams consumer groups
  - effectively-once ledger effects via per-order idempotency state in Redis
"""
import json
import os
import random
import socket
import time
import uuid

import redis
import requests

REDIS_URL = os.environ["REDIS_URL"]
PAYMENTS_URL = os.environ["PAYMENTS_URL"]
ORDERS_STREAM = "orders"
WORKER_GROUP = os.environ.get("WORKER_GROUP", "workers")
CONSUMER_NAME = os.environ.get(
    "WORKER_CONSUMER", f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
)

READ_BATCH = int(os.environ.get("READ_BATCH", "20"))
READ_BLOCK_MS = int(os.environ.get("READ_BLOCK_MS", "2000"))
MIN_IDLE_RECLAIM_MS = int(os.environ.get("MIN_IDLE_RECLAIM_MS", "5000"))
MAX_CHARGE_ATTEMPTS = int(os.environ.get("MAX_CHARGE_ATTEMPTS", "6"))
REQUEST_TIMEOUT_SECONDS = float(os.environ.get("REQUEST_TIMEOUT_SECONDS", "7"))
RETRY_BASE_SECONDS = float(os.environ.get("RETRY_BASE_SECONDS", "0.25"))
RETRY_MAX_SECONDS = float(os.environ.get("RETRY_MAX_SECONDS", "4"))
ORDER_LOCK_SECONDS = int(os.environ.get("ORDER_LOCK_SECONDS", "30"))

r = redis.from_url(REDIS_URL, decode_responses=True)


def ensure_group() -> None:
    try:
        r.xgroup_create(ORDERS_STREAM, WORKER_GROUP, id="0", mkstream=True)
        print(f"created consumer group {WORKER_GROUP}", flush=True)
    except redis.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def state_key(order_id: str) -> str:
    return f"order_state:{order_id}"


def lock_key(order_id: str) -> str:
    return f"order_lock:{order_id}"


def acquire_order_lock(order_id: str, token: str) -> bool:
    return bool(r.set(lock_key(order_id), token, nx=True, ex=ORDER_LOCK_SECONDS))


def release_order_lock(order_id: str, token: str) -> None:
    # Compare-and-delete so we never release a lock created by someone else.
    r.eval(
        "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
        1,
        lock_key(order_id),
        token,
    )


def parse_order(fields):
    order = json.loads(fields["data"])
    required = {"order_id", "customer_id", "amount_cents"}
    if set(order.keys()) != required:
        raise ValueError(f"invalid order schema: {order}")
    if int(order["amount_cents"]) <= 0:
        raise ValueError(f"invalid amount: {order}")
    order["amount_cents"] = int(order["amount_cents"])
    return order


def set_status_charged(order):
    r.hset(
        state_key(order["order_id"]),
        mapping={
            "status": "charged",
            "customer_id": order["customer_id"],
            "amount_cents": str(order["amount_cents"]),
            "charged_at": str(time.time()),
        },
    )


def apply_ledger_once(order) -> bool:
    key = state_key(order["order_id"])
    while True:
        try:
            with r.pipeline() as pipe:
                pipe.watch(key)
                status = pipe.hget(key, "status")
                if status == "done":
                    pipe.unwatch()
                    return True
                if status != "charged":
                    pipe.unwatch()
                    return False
                pipe.multi()
                pipe.incrby(f"ledger:{order['customer_id']}", order["amount_cents"])
                pipe.incr("processed_count")
                pipe.hset(key, mapping={"status": "done", "completed_at": str(time.time())})
                pipe.execute()
                return True
        except redis.WatchError:
            continue


def charge_with_retries(order) -> bool:
    for attempt in range(1, MAX_CHARGE_ATTEMPTS + 1):
        try:
            resp = requests.post(
                f"{PAYMENTS_URL}/charge",
                json={"order_id": order["order_id"], "amount_cents": order["amount_cents"]},
                timeout=(2, REQUEST_TIMEOUT_SECONDS),
            )
            resp.raise_for_status()
            set_status_charged(order)
            return True
        except requests.RequestException as exc:
            delay = min(RETRY_MAX_SECONDS, RETRY_BASE_SECONDS * (2 ** (attempt - 1)))
            delay += random.uniform(0, delay * 0.15)
            print(
                f"payment failed for {order['order_id']} attempt={attempt}/{MAX_CHARGE_ATTEMPTS}: {exc}",
                flush=True,
            )
            if attempt == MAX_CHARGE_ATTEMPTS:
                return False
            time.sleep(delay)
    return False


def process_message(msg_id, fields):
    try:
        order = parse_order(fields)
    except Exception as exc:
        # Poison message: acknowledge so it cannot block the stream forever.
        print(f"dropping invalid message {msg_id}: {exc}", flush=True)
        r.xack(ORDERS_STREAM, WORKER_GROUP, msg_id)
        return

    oid = order["order_id"]
    token = uuid.uuid4().hex
    if not acquire_order_lock(oid, token):
        return

    try:
        status = r.hget(state_key(oid), "status")
        if status == "done":
            r.xack(ORDERS_STREAM, WORKER_GROUP, msg_id)
            return

        if status != "charged":
            charged = charge_with_retries(order)
            if not charged:
                # Leave unacked. Another delivery will retry later.
                return

        if apply_ledger_once(order):
            r.xack(ORDERS_STREAM, WORKER_GROUP, msg_id)
            print(f"processed {oid} for {order['customer_id']}", flush=True)
    finally:
        release_order_lock(oid, token)


def claim_stale_pending():
    result = r.xautoclaim(
        ORDERS_STREAM,
        WORKER_GROUP,
        CONSUMER_NAME,
        MIN_IDLE_RECLAIM_MS,
        "0-0",
        count=READ_BATCH,
    )
    if not result:
        return []
    if len(result) >= 2:
        return result[1]
    return []


def read_new_messages():
    resp = r.xreadgroup(
        WORKER_GROUP,
        CONSUMER_NAME,
        {ORDERS_STREAM: ">"},
        count=READ_BATCH,
        block=READ_BLOCK_MS,
    )
    if not resp:
        return []
    return resp[0][1]


def main():
    ensure_group()
    print(f"worker started group={WORKER_GROUP} consumer={CONSUMER_NAME}", flush=True)
    while True:
        messages = claim_stale_pending()
        if not messages:
            messages = read_new_messages()
        for msg_id, fields in messages:
            process_message(msg_id, fields)


if __name__ == "__main__":
    main()
