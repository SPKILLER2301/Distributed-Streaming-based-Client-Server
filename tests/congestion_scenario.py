"""Reproducible A4 comparison: python -m tests.congestion_scenario --output ...

Real packet encoding/crypto/ACK/credit/recovery, virtual monotonic time, 25ms
one-way delay, 20ms transport ticks. No sockets or bandwidth/queue model:
reported goodput describes this simulator, not physical network capacity.
"""
import argparse
import csv
import hashlib
import heapq
import json
import random
from pathlib import Path

from common import constants as C
from common.packet import decode
from transport.connection import Connection
from transport.crypto import KeyExchange


def run_scenario(algorithm, scenario, *, seed=1, payload_bytes=240 * 1024):
    now = 100.0
    start = now
    queue, trace, received = [], [], bytearray()
    nodes, attempts = {}, {}
    rng = {"client": random.Random(seed), "server": random.Random(seed)}
    serial = 0
    dropped = {"client": 0, "server": 0}
    payload = (bytes(range(256)) * ((payload_bytes + 255) // 256))[:payload_bytes]
    a, b = KeyExchange(), KeyExchange()
    for source, cipher in (("client", a.derive_cipher(b.public_bytes, True)),
                           ("server", b.derive_cipher(a.public_bytes, False))):
        def send(raw, addr, side=source):
            nonlocal serial
            pkt = decode(raw)
            key = side, pkt.ptype, pkt.stream_id, pkt.seq
            attempt = attempts.get(key, 0) + 1
            attempts[key] = attempt
            targeted = (scenario == "targeted" and side == "client" and
                        pkt.ptype == C.PKT_DATA and pkt.seq == 1 and attempt == 1)
            lost = targeted or (scenario == "loss10" and rng[side].random() < 0.1)
            if lost:
                dropped[side] += 1
                return
            serial += 1
            heapq.heappush(queue, (now + 0.025, serial,
                                  "server" if side == "client" else "client", raw))

        nodes[source] = Connection(7, ("peer", 9000), cipher, send,
            lambda cid, sid, data: received.extend(data),
            lambda *args: None, clock=lambda: now, congestion_algorithm=algorithm)
    client = nodes["client"]
    for offset in range(0, len(payload), C.FILE_CHUNK_SIZE):
        client.send_message(2, payload[offset:offset + C.FILE_CHUNK_SIZE])

    previous = None

    def record():
        nonlocal previous
        state = (round(client.cc.cwnd, 6), client._inflight_total(),
                 client.stats["acked_payload_bytes"], client.stats["retransmitted"],
                 client.stats["loss_reductions"], client.stats["timeout_reductions"])
        if state != previous:
            trace.append({"algorithm": algorithm, "scenario": scenario,
                          "time_seconds": round(now - start, 6),
                          **dict(zip(("cwnd_packets", "inflight_packets", "acked_bytes",
                                      "retransmitted", "loss_reductions", "timeout_reductions"), state))})
            previous = state

    record()
    # Integer millisecond steps avoid accumulating floating-point timer drift.
    for step in range(1, 120_001):
        now = start + step / 1000
        while queue and queue[0][0] <= now + 1e-10:
            _, _, dest, raw = heapq.heappop(queue)
            nodes[dest].handle_packet(decode(raw))
            record()
        if step % 20 == 0:
            for conn in nodes.values():
                conn.on_tick(now)
            record()
        assert not any(conn.closed for conn in nodes.values()), "connection closed before transfer completed"
        if client.stats["acked_payload_bytes"] == len(payload):
            break
    else:
        raise AssertionError("virtual transfer exceeded 120 seconds")
    assert bytes(received) == payload, "corrupt, duplicated or missing payload"
    assert not client.send_streams[2].pending and not client.send_streams[2].unacked
    duration = now - start
    result = {"algorithm": algorithm, "scenario": scenario, "seed_each_direction": seed,
              "one_way_delay_seconds": 0.025, "tick_seconds": 0.02,
              "loss_each_direction": 0.1 if scenario == "loss10" else 0,
              "payload_bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest(),
              "duration_seconds": round(duration, 6),
              "goodput_mib_per_second": round(len(payload) / duration / 1024**2, 6),
              "final_cwnd_packets": round(client.cc.cwnd, 6),
              "dropped": dropped, "stats": dict(client.stats)}
    for conn in nodes.values():
        conn._finish("scenario completed")
    return result, trace


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--trace", required=True, type=Path)
    args = parser.parse_args()
    results, trace = [], []
    for scenario in ("clean", "targeted", "loss10"):
        for algorithm in ("aimd", "cubic"):
            result, events = run_scenario(algorithm, scenario)
            results.append(result)
            trace.extend(events)
            print(json.dumps(result))
    args.output.write_text(json.dumps({"results": results}, indent=2) + "\n", encoding="utf-8")
    with args.trace.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(trace[0]))
        writer.writeheader()
        writer.writerows(trace)


if __name__ == "__main__":
    main()
