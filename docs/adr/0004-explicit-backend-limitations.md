# ADR 0004: Explicit Backend Limitations

## Status

Accepted

## Context

`DeltaTableServiceClient` exposes one public API surface over heterogeneous backends. V1 and V2 are service-backed Flight paths. V3 is an in-process native path.

Not every backend supports every public method.

## Problem

The SDK can either hide differences behind degraded behavior or make unsupported features fail clearly. Silent degradation is dangerous for data workflows.

## Decision

Keep backend-specific limitations explicit. Unsupported operations throw clear exceptions rather than silently emulating, ignoring, or returning empty data.

## Rationale

- Data integrations should fail loudly when a capability is unavailable.
- Agents need backend capability signals to choose correct code paths.
- Silent fallback can corrupt assumptions about CDF, partitions, and schema evolution.

## Consequences

Positive:

- Safer generated code.
- Clearer troubleshooting.
- Honest capability matrix.

Negative:

- Consumers must understand backend differences.
- One client type can still suggest more uniformity than actually exists.

## Alternatives Considered

- Automatically fallback to full reads or empty results for unsupported APIs. Rejected because it hides semantic changes.
- Split every backend into a separate public client type. Deferred because the single client preserves compatibility and discoverability.
