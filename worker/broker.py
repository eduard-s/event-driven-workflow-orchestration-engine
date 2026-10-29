# Redis owns task claims and durable reports. Workers never access PostgreSQL.

import hashlib
import json
import math
import time
from collections.abc import Mapping
from typing import Any
from uuid import uuid4

from shared.broker import MAX_ENVELOPE_BYTES, task_claim_key
from shared.broker import Broker as ConsumerBroker
from shared.output import PreparedOutput, safe_runtime
from worker.messages import Claim

_CLAIM = """
local now = redis.call('TIME')
local started = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local deadline = tonumber(ARGV[2])
if started >= deadline or redis.call('EXISTS', KEYS[1]) == 1 then
    return {}
end
local lease = math.min(started + tonumber(ARGV[3]), deadline)
local event = cjson.decode(ARGV[4])
event.kind = 'started'
event.token = ARGV[1]
event.started_ms = started
event.lease_ms = lease
redis.call('XADD', KEYS[2], '*', 'envelope', cjson.encode(event))
redis.call('HSET', KEYS[1], 'token', ARGV[1], 'lease_ms', lease, 'started', cjson.encode(event))
redis.call('PEXPIREAT', KEYS[1], deadline + 3000)
return {started, lease}
"""

_COMPLETE = """
if redis.call('HGET', KEYS[1], 'token') ~= ARGV[1] then
    return 0
end
local stored = redis.call('HGET', KEYS[1], 'result')
if stored then
    if stored == ARGV[2] then return 1 else return 0 end
end
local now = redis.call('TIME')
local current = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
if current >= tonumber(redis.call('HGET', KEYS[1], 'lease_ms')) then
    return 0
end
local report = string.sub(ARGV[3], 1, -2) .. ',"reported_ms":' .. current .. '}'
redis.call('HSET', KEYS[1], 'result', ARGV[2], 'outcome', report)
redis.call('XADD', KEYS[2], '*', 'envelope', ARGV[4])
return 1
"""


def outcome_envelope(
    envelope: Mapping[str, Any], claim: Claim, prepared: PreparedOutput, runtime: dict | None
) -> bytes:
    # Encode one immutable report before Redis retries so retries keep the same content.
    # Args: envelope identifies the task, claim identifies its owner, prepared contains JSON, and runtime contains timings.
    # Returns: Bounded JSON bytes. Invalid output becomes a permanent failure report.
    success = prepared.success and prepared.payload is not None
    payload = prepared.payload
    if payload is None:
        payload = '{"code":"INVALID_OUTPUT","message":"Handler output exceeds its contract.","retryable":false}'
    event = {
        **{
            key: envelope[key]
            for key in ("schema_version", "event_id", "execution_id", "node_id", "try_number")
        },
        "kind": "outcome",
        "token": claim["token"],
        "started_ms": claim["started_ms"],
        "lease_ms": claim["lease_ms"],
        "success": success,
        "payload": payload,
        "runtime": safe_runtime(runtime) or {},
    }
    encoded = json.dumps(event, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()
    if len(encoded) > MAX_ENVELOPE_BYTES:
        raise ValueError("Broker envelope exceeds its byte limit")
    return encoded


class Broker(ConsumerBroker):
    # Add atomic per-try claims and result publication to the common Redis consumer.
    # Args: settings selects Redis and stream names, and client optionally supplies an existing Redis client.
    # Returns: A Redis adapter with atomic claims and result reports.

    async def claim(self, envelope: Mapping[str, Any]) -> Claim | None:
        # Claim one try and publish its start event before handler work.
        # Args: envelope contains frozen input, saved limits, and the execution deadline.
        # Returns: The owner and local time budget, or None for duplicate or expired work.
        token = str(uuid4())
        identity = {
            key: envelope[key]
            for key in ("schema_version", "event_id", "execution_id", "node_id", "try_number")
        }
        measured = time.monotonic()
        times = await self.client.eval(
            _CLAIM,
            2,
            task_claim_key(self.settings, envelope),
            self.settings.event_stream,
            token,
            envelope["deadline_ms"],
            max(1, math.floor(envelope["handler_timeout"] * 1000)),
            json.dumps(identity),
        )
        if not times:
            return None
        started, lease = map(int, times)
        return {
            "token": token,
            "input": envelope["input"],
            "deadline": measured + (lease - started) / 1000,
            "started_ms": started,
            "lease_ms": lease,
            "max_output_bytes": envelope["max_output_bytes"],
            "max_depth": envelope["max_depth"],
        }

    async def complete(self, envelope: Mapping[str, Any], token: str, encoded: bytes) -> bool:
        # Publish a result once while the same Redis owner still holds its lease.
        # Args: envelope identifies the try, token identifies its owner, and encoded contains the immutable report.
        # Returns: True for an accepted or repeated identical report. Otherwise False.
        digest = hashlib.sha256(encoded).hexdigest()
        notice = json.dumps(
            {
                **{
                    key: envelope[key]
                    for key in (
                        "schema_version",
                        "event_id",
                        "execution_id",
                        "node_id",
                        "try_number",
                    )
                },
                "kind": "outcome",
                "token": token,
            },
            separators=(",", ":"),
        )
        return bool(
            await self.client.eval(
                _COMPLETE,
                2,
                task_claim_key(self.settings, envelope),
                self.settings.event_stream,
                token,
                digest,
                encoded,
                notice,
            )
        )
