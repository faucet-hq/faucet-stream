#!/usr/bin/env python3
"""Seed a running `faucet serve` with a demo feed and push events through it.

Used by `scripts/try-local.sh`. It follows "A feed in five minutes" from the
feeds cookbook: register and launch a template whose source is `type: feed`,
create the feed `demo-orders` with one delivery, issue a sender token, post a
few events as a sender would, and wait until the delivery has written them to
`out/feed_orders.jsonl` in the demo workspace.

Idempotent: an existing template or feed is reused (a template left unlaunched
is launched); every start replaces the previous sender token with a new one
and sends a new batch of events.

Usage: demo_feed.py [BASE_URL [DEMO_DIR]]   (default http://127.0.0.1:8899)
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request

TOKEN_PREFIX = "try-local-"
TEMPLATE_ID = "demo-orders-archive"
FEED = "demo-orders"
DELIVERY = "archive"
OUTPUT = "out/feed_orders.jsonl"
EVENTS = 3
WAIT_SECS = 60

TEMPLATE = f"""version: 1
kind: pipeline
name: demo_orders_archive
pipeline:
  source:
    type: feed
  sink:
    type: file
    config:
      path: ./{OUTPUT}
      if_exists: append
  state:
    type: file
    config:
      path: ./state
tests:
  suite:
    auto: {{ defaults_baseline: true }}
"""

FEED_BODY = {
    "name": FEED,
    "description": "Demo order events (scripts/try-local.sh)",
    "ingest": {"auth": {"tokens": "managed"}},
    "deliveries": [
        {"name": DELIVERY, "template": {"id": TEMPLATE_ID}, "flush": {"max_age": "2s"}}
    ],
}


def request(base, method, path, body=None, token=None):
    """One JSON request; returns (status, parsed body or text)."""
    data = None if body is None else json.dumps(body).encode()
    headers = {"content-type": "application/json"} if data is not None else {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(base + path, data=data, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            status, raw = resp.status, resp.read()
    except urllib.error.HTTPError as error:
        status, raw = error.code, error.read()
    except (urllib.error.URLError, OSError) as error:
        return 0, str(error)
    text = raw.decode("utf-8", "replace")
    try:
        return status, json.loads(text) if text else {}
    except ValueError:
        return status, text


def line_count(path):
    try:
        with open(path, encoding="utf-8") as handle:
            return sum(1 for line in handle if line.strip())
    except OSError:
        return 0


def delivered(path, order_ids):
    """How many of `order_ids` have a row in the output file."""
    found = set()
    try:
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except ValueError:
                    continue
                if isinstance(row, dict) and row.get("order_id") in order_ids:
                    found.add(row["order_id"])
    except OSError:
        return 0
    return len(found)


def ensure_template(call):
    status, body = call("GET", f"/v1/templates/{TEMPLATE_ID}")
    if status == 200:
        if isinstance(body, dict) and body.get("stable") is None:
            status, body = call("POST", f"/v1/templates/{TEMPLATE_ID}/launch", {})
            if status not in (200, 201):
                return f"launching template {TEMPLATE_ID} answered {status}: {body}"
        return None
    status, body = call(
        "POST",
        "/v1/templates",
        {"id": TEMPLATE_ID, "config": TEMPLATE, "config_format": "yaml", "launch": True},
    )
    if status not in (200, 201):
        return f"registering template {TEMPLATE_ID} answered {status}: {body}"
    return None


def ensure_feed(call):
    status, _ = call("GET", f"/v1/feeds/{FEED}")
    if status == 200:
        return None
    status, body = call("POST", "/v1/feeds", FEED_BODY)
    if status not in (200, 201, 409):
        return f"creating feed {FEED} answered {status}: {body}"
    return None


def revoke_old_tokens(call):
    status, body = call("GET", f"/v1/feeds/{FEED}/tokens")
    if status != 200 or not isinstance(body, list):
        return
    for token in body:
        if not isinstance(token, dict) or token.get("revoked_at"):
            continue
        if str(token.get("name", "")).startswith(TOKEN_PREFIX) and token.get("id"):
            call("DELETE", f"/v1/feeds/{FEED}/tokens/{token['id']}")


def issue_token(call, name):
    status, body = call("POST", f"/v1/feeds/{FEED}/tokens", {"name": name})
    if status not in (200, 201) or not isinstance(body, dict) or not body.get("token"):
        return None, f"issuing a feed token answered {status}: {body}"
    return body["token"], None


def send_events(call, token, start, count):
    for number in range(start, start + count):
        event = {"order_id": number, "total": f"{number % 90 + 10}.90", "source": "try-local"}
        status, body = call("POST", f"/v1/feeds/{FEED}/events", event, token)
        if status != 202:
            return f"sending event {number} answered {status}: {body}"
    return None


def wait_for_orders(path, order_ids, wait_secs=WAIT_SECS, sleep=time.sleep, clock=time.monotonic):
    deadline = clock() + wait_secs
    while True:
        have = delivered(path, order_ids)
        if have >= len(order_ids):
            return have
        if clock() >= deadline:
            return have
        sleep(1)


def seed(base, demo_dir, call=None, sleep=time.sleep, clock=time.monotonic, log=print, stamp=None):
    """Run the demo; returns the number of failures (0 or 1)."""
    if call is None:
        call = lambda method, path, body=None, token=None: request(base, method, path, body, token)
    status, _ = call("GET", "/v1/feeds")
    if status == 404:
        log("  feeds: this build has no `feeds` feature; skipped")
        return 0
    if status != 200:
        log(f"  feeds: {base} is not answering /v1/feeds ({status})")
        return 1
    stamp = int(time.time()) if stamp is None else stamp
    output = os.path.join(demo_dir, OUTPUT)
    for step in (ensure_template, ensure_feed):
        problem = step(call)
        if problem:
            log(f"  feeds: {problem}")
            return 1
    revoke_old_tokens(call)
    token, problem = issue_token(call, f"{TOKEN_PREFIX}{stamp}")
    if not problem:
        problem = send_events(call, token, stamp, EVENTS)
    if problem:
        log(f"  feeds: {problem}")
        return 1
    sent = set(range(stamp, stamp + EVENTS))
    have = wait_for_orders(output, sent, sleep=sleep, clock=clock)
    if have < EVENTS:
        log(
            f"  feeds: sent {EVENTS} events but {OUTPUT} holds {have} of them "
            f"after {WAIT_SECS}s (see serve.log)"
        )
        return 1
    rows = line_count(output)
    log(f"  feeds: {EVENTS} events posted to {FEED} and delivered to {OUTPUT} ({rows} rows)")
    log(f"  feeds: sender token for this session: {token}")
    return 0


def main(argv):
    base = (argv[0] if argv else "http://127.0.0.1:8899").rstrip("/")
    demo_dir = argv[1] if len(argv) > 1 else "."
    return seed(base, demo_dir)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
