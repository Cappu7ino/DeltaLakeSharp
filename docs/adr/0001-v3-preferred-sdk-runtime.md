# ADR 0001: V3 Native Rust As Preferred SDK Runtime

## Status

Accepted

## Context

DeltaLakeSharp exposes V1 Spark, V2 DataFusion, and V3 Rust execution modes through `ServiceMode`. V1 and V2 require Arrow Flight services. V3 runs in process through native Rust interop.

ADBC is implemented on top of the V3 path, and the advanced SDK surfaces such as CDF and partitioned reads are V3-oriented.

## Problem

External NuGet consumers need a clear default runtime. Without guidance, agents may choose the URI constructor, which defaults to V1 Spark and requires a service endpoint.

## Decision

V3 Rust is the preferred and de-facto runtime for external client SDK consumption and the required runtime for ADBC. V1 Spark and V2 DataFusion remain public `ServiceMode` values for service-backed compatibility and integration infrastructure.

## Rationale

- V3 avoids requiring external service orchestration for typical SDK consumers.
- V3 enables CDF, partitioned reads, schema-mode writes, and ADBC-backed behavior.
- V1/V2 remain useful for compatibility, backend comparison, and test harness coverage.

## Consequences

Positive:

- Clear guidance for new integrations.
- Lower operational friction for package consumers.
- More accurate AI-generated examples.

Negative:

- V3 native library packaging becomes part of the consumer story.
- Public constructors still expose V1 default behavior, so docs must be explicit.

## Alternatives Considered

- Make V1 the documented default because the URI constructor defaults to V1. Rejected because it misrepresents the preferred package-consumption path.
- Hide V1/V2 from docs. Rejected because they remain public compatibility modes.
