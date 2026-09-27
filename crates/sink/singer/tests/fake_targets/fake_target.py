#!/usr/bin/env python3
"""Dependency-free fake Singer target for faucet-sink-singer tests.

Reads Singer messages from stdin. Config (`--config FILE`) keys:
  path       JSON Lines file every RECORD is appended to (with `_version`)
  log        file each event is appended to: spawn / schema / state / activate / exit
  mode       echo (default) | silent | echo_at_exit | crash | exit_early
  crash_after  records to accept before crashing (mode crash)
  secret     echoed to stderr by mode crash (the sink must redact it)
  delay_start  seconds to sleep before reading stdin (back-pressure tests)
Environment: FAKE_TARGET_TAG, when set, is logged on spawn.
"""
import json
import os
import sys
import time


def main():
    args = sys.argv[1:]
    cfg = json.load(open(args[args.index("--config") + 1]))
    mode = cfg.get("mode", "echo")
    path = cfg["path"]
    log_path = cfg.get("log")

    def log(event):
        if log_path:
            with open(log_path, "a") as f:
                f.write(json.dumps(event) + "\n")

    log({"event": "spawn", "tag": os.environ.get("FAKE_TARGET_TAG"), "args": args[2:]})
    if mode == "exit_early":
        sys.stderr.write("refusing input\n")
        sys.exit(0)
    if cfg.get("delay_start"):
        time.sleep(float(cfg["delay_start"]))

    out = open(path, "a")
    records = 0
    last_state = None
    for line in sys.stdin:
        msg = json.loads(line)
        kind = msg["type"]
        if kind == "SCHEMA":
            log({"event": "schema", "stream": msg["stream"], "schema": msg["schema"],
                 "key_properties": msg.get("key_properties")})
        elif kind == "RECORD":
            rec = dict(msg["record"])
            if "version" in msg:
                rec["_version"] = msg["version"]
            rec["_stream"] = msg["stream"]
            out.write(json.dumps(rec) + "\n")
            records += 1
            if mode == "crash" and records >= int(cfg.get("crash_after", 1)):
                out.flush()
                sys.stderr.write("fatal: cannot load record with key %s\n" % cfg.get("secret"))
                sys.stderr.flush()
                sys.exit(3)
        elif kind == "STATE":
            out.flush()
            os.fsync(out.fileno())
            last_state = msg["value"]
            log({"event": "state", "value": last_state, "records": records})
            if mode == "echo":
                print(json.dumps({"type": "STATE", "value": last_state}), flush=True)
        elif kind == "ACTIVATE_VERSION":
            out.flush()
            out.close()
            version = msg["version"]
            kept = [l for l in open(path) if json.loads(l).get("_version", version) >= version]
            with open(path, "w") as f:
                f.writelines(kept)
            out = open(path, "a")
            log({"event": "activate", "version": version})
    out.close()
    if mode == "echo_at_exit" and last_state is not None:
        print(json.dumps({"type": "STATE", "value": last_state}), flush=True)
    log({"event": "exit", "records": records})


if __name__ == "__main__":
    main()
