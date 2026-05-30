# Created by: Mustafa Can Caliskan
# Date: 2026-04-25

"""Metrics collection layer.

Three independent sinks subscribe to the per-request event stream emitted by
the harness workers:

- `LatencySink` aggregates latencies into HDR histograms per query class.
- `RequestLogSink` persists full per-request records as Parquet.
- `ThroughputSink` computes 1-second rolling throughput counters.

Additionally, `EventLog` records phase-change events (warmup end, scale-out
trigger, etc.) as JSONL, and `ResourceSampler` records per-node CPU / memory /
disk / network samples.

All sinks are strictly write-only during a run. Analysis reads the serialized
output artifacts later; it never imports from this module.
"""

from graph_bench.metrics.collector import MetricsCollector
from graph_bench.metrics.events import EventLog
from graph_bench.metrics.latency import LatencySink
from graph_bench.metrics.requests import RequestLogSink, RequestRecord
from graph_bench.metrics.resource import ResourceSampler
from graph_bench.metrics.throughput import ThroughputSink

__all__ = [
    "EventLog",
    "LatencySink",
    "MetricsCollector",
    "RequestLogSink",
    "RequestRecord",
    "ResourceSampler",
    "ThroughputSink",
]
