# Client-Side Statistics

The Rust SDK statistics implementation follows the Oracle NoSQL Java SDK stats
contract for profile names, profile behavior, emitted JSON field names, and
periodic lifecycle behavior. Treat the Java SDK output as the compatibility
target. Do not add Rust-only fields to the emitted periodic payload unless the
Java contract changes.

This document is for maintainers and reviewers. User-facing setup lives in
`README.md`.

## Goals

- Collect client-side request statistics with Java-compatible profile behavior.
- Emit Java-style periodic JSON snapshots.
- Support runtime control through `StatsControl`.
- Keep request instrumentation centralized in the handle lifecycle.
- Keep query-level statistics limited to `StatsProfile::All`.
- Provide tests that validate functionality without depending on exact machine
  latency values.

## Files Involved

- `src/stats.rs`: public stats types, runtime control, aggregation,
  percentile calculation, Java-style JSON serialization, and core stats unit
  tests.
- `src/handle_builder.rs`: builder configuration and environment-variable
  parsing for stats profile, interval, pretty print, logging, handler, and
  percentile mode.
- `src/handle.rs`: common request lifecycle. This is the central hook that
  records request success/error, latency, request size, response size, retry
  count, and auth retry count.
- Request files such as `src/put_request.rs`, `src/get_request.rs`,
  `src/delete_request.rs`, `src/write_multiple_request.rs`,
  `src/multi_delete_request.rs`, `src/table_request.rs`,
  `src/list_tables_request.rs`, `src/get_indexes_request.rs`,
  `src/system_request.rs`, and `src/table_usage_request.rs`: set the
  Java-compatible request name through `SendOptions`.
- `src/query_request.rs`: handles query request naming, prepare-only behavior,
  logical query observation, query metadata wiring, and ALL-profile query
  entries.
- `src/writer.rs`, `src/reader.rs`, and `src/nson.rs`: protocol
  serialization/deserialization support. These files are useful when reviewing
  request/result byte accounting.
- `src/stats_functional_tests.rs`: Java `StatsTest`-style functional tests for
  schema shape, profile behavior, deterministic counters, errors, throttling
  fields, and latency field presence.
- `tests/stats_e2e_tests.rs`: opt-in end-to-end test that sends real SDK
  `Put`, `Get`, and `Query` requests and validates stats counts, average
  latency range, and p95 presence.
- `examples/stats`: compact usage example.
- `examples/stats_profile_output_demo`: Java-comparison demo for one selected
  profile.
- `examples/stats_periodic_workload`: periodic workload/stress validation,
  interval emission, throughput checks, and `EXACT`/`HDR` comparison.

## Request Lifecycle

The request path is:

1. User code builds a request, for example `PutRequest`, `GetRequest`, or
   `QueryRequest`.
2. The request's `execute()` method creates a `Writer`.
3. The request serializes itself into the NoSQL protocol payload.
4. The request creates `SendOptions` with:
   - Java-compatible request name, such as `Put`, `Get`, `Query`, or `Prepare`.
   - Retryability.
   - Timeout.
   - Compartment/namespace metadata.
   - Optional query stats metadata.
5. The request calls `Handle::send_and_receive()`.
6. `send_and_receive()` captures request size and enters the retry loop.
7. `send_and_receive_once()` calls `post_data()`.
8. `post_data()` builds headers, sends the HTTP request, reads the response body
   bytes, and stores the measured latency.
9. `send_and_receive_once()` wraps the bytes in a `Reader` and checks for
   protocol-level errors.
10. `send_and_receive()` records a `StatsObservation` for success or final
    non-retry error.
11. The request-specific code deserializes the `Reader` into the final result
    object, such as `PutResult`, `GetResult`, or `QueryResult`.
12. The result object is returned to user code.

## Latency Boundary

Current Rust stats latency is SDK-side HTTP latency.

The timer starts in `Handle::post_data()` immediately before the HTTP request is
sent:

```text
request_start = Instant::now()
```

The timer stops immediately after the full HTTP response body has been read:

```text
resp.bytes().await
last_request_latency = request_start.elapsed()
```

This means stats latency includes:

- reqwest/client HTTP send work after the timer starts.
- Network/proxy/server wait time.
- Server-side execution time as observed by the client.
- HTTP response transfer.
- Reading the response body bytes.

This means stats latency excludes:

- User-side request object construction.
- Request serialization before the timer starts.
- Full NSON result deserialization after response bytes are received.
- Final SDK result object creation.
- User code after `execute()` returns.

This boundary follows the Java-style request lifecycle statistic rather than a
full application-perceived `execute()` duration. If the boundary changes later,
tests and documentation must be updated together.

Latency values are stored and emitted in whole milliseconds. Very fast local
CloudSim requests can round to `0ms`; the SDK still emits
`httpRequestLatencyMs` when latency samples exist, including p95/p99 for
`MORE` and `ALL`.

## Throughput Boundary

The stats system does not emit an explicit throughput field. Throughput is
derived from request counts over a stats interval:

```text
throughput = sum(httpRequestCount) / interval_seconds
```

Correctness depends on:

- Recording every completed request exactly once.
- Emitting snapshots on the configured interval.
- Using the actual interval duration when doing external throughput analysis.

The periodic workload example prints a rough throughput summary from the
captured stats counts.

## Stats Aggregation

`StatsControl::observe()` is the main aggregation entry point. It updates the
current interval under a lock.

For every observed request, aggregate request stats update:

- `httpRequestCount`
- success/error counts
- request size
- response/result size
- latency
- retry count
- auth retry count
- throttle retry count
- retry delay
- rate limit delay

For `StatsProfile::All`, query requests with query metadata also update a
logical query entry. Query stats track logical query count and HTTP request
count separately because one logical query can involve multiple HTTP requests.

`StatsProfile::All` may emit raw SQL text and query plan text through the
default log output and through registered handlers. For sensitive workloads,
disable automatic logging with `stats_enable_log(false)?` and use a handler that
redacts SQL, literals, table names, or plan details before storing or forwarding
snapshots.

Prepare-only requests are counted as aggregate `Prepare` request stats. They do
not create ALL-profile query entries, matching the Java SDK behavior.

## Profile Behavior

- `NONE`: no collection and no periodic stats payload.
- `REGULAR`: aggregate request stats without latency percentiles and without
  query entries.
- `MORE`: aggregate request stats with `95th` and `99th` latency percentile
  fields.
- `ALL`: `MORE` behavior plus query-level entries for actual query executions.

`StatsControl::stop()` stops collection but does not disable the periodic task.
For non-`NONE` profiles, empty interval snapshots can still be emitted, matching
the Java SDK lifecycle. The handle installs this scheduler even when the initial
profile is `NONE`; it emits no payload while the profile remains `NONE`, but it
allows later synchronous `StatsControl::set_profile()` and `StatsControl::start()`
calls to begin interval emission without needing to spawn an async task from
those setters.

## JSON Contract

The initialization/config log includes metadata such as:

- `sdkName`
- `sdkVersion`
- `clientId`
- `profile`
- `intervalSec`
- `prettyPrint`
- `percentileMode`
- `rateLimitingEnabled`

Periodic interval payloads are intentionally smaller and Java-compatible:

```text
clientId
startTime
endTime
requests
queries      only for ALL and only when query entries exist
connections  only for non-empty intervals with connection data
```

Periodic payloads must not include Rust-only top-level fields such as
`profile`, `sdkName`, or `sdkVersion`.

Request entries use Java field names:

```text
httpRequestCount
resultSize
name
httpRequestLatencyMs
requestSize
rateLimitDelayMs
errors
retry
```

`requestSize` and `resultSize` contain only:

```text
min
avg
max
```

`httpRequestLatencyMs` contains:

```text
min
avg
max
```

For `MORE` and `ALL`, it also contains:

```text
95th
99th
```

Do not add `count` or `total` to emitted metric objects. Those are internal
aggregation details only.

Timestamps use UTC RFC3339 seconds precision, for example:

```text
2026-06-09T06:42:45Z
```

## Percentile Modes

`StatsPercentileMode::Exact` is the default. It stores latency samples for the
current interval and sorts them when the snapshot is emitted. This is closest
to the Java SDK implementation and is preferred for parity validation.

`StatsPercentileMode::Hdr` uses a bounded HDR-style histogram. This reduces
per-interval memory growth and avoids sorting large sample arrays, which is
better for high-volume clients. Percentile values are approximate and can be
more conservative because values are grouped into buckets.

The emitted JSON schema does not change between modes. Only the percentile
calculation strategy changes.

Choose the mode through the builder, the builder environment variables, or the
examples. The compact `examples/stats` example reads `STATS_PERCENTILE_MODE`:

```sh
STATS_PERCENTILE_MODE=HDR cargo run --example stats
cargo run --example stats_profile_output_demo -- localhost 8080 MORE HDR
cargo run --example stats_periodic_workload -- localhost 8080 300 5 MORE 1 HDR
```

## Connection Stats

The Java SDK reports connection statistics from its HTTP client. Rust's
reqwest/hyper stack does not expose the same Java-style pool connection count.

The Rust SDK currently records the closest compatibility signal: a non-empty
interval with observed HTTP traffic reports one active client-side connection.
This keeps local proxy/CloudSim output aligned with Java's non-zero connection
behavior without adding a Rust-specific JSON field.

If reqwest exposes stable connection pool metrics in the future, this area can
be revisited.

## Tests

Run the baseline library checks with the known QTF baseline skipped:

```sh
cargo fmt --check
cargo check
cargo test --lib -- --skip qtf_test
```

Run stats functional tests with expected/actual output:

```sh
cargo test --lib stats::functional_tests -- --nocapture --skip qtf_test
```

These tests validate:

- Java-compatible periodic JSON shape.
- Profile behavior.
- Request counts.
- Request/result size summaries.
- Retry, auth retry, throttle retry, retry delay, and rate limit delay fields.
- Latency fields and percentile field presence.
- Query stats invariants for `StatsProfile::All`.
- Empty interval behavior.

Run the ignored opt-in end-to-end stats test with CloudSim on `localhost:8080`:

```sh
RUN_NOSQL_STATS_E2E=1 \
NOSQL_STATS_E2E_MODE=cloudsim \
NOSQL_STATS_E2E_ENDPOINT=http://localhost:8080 \
cargo test --test stats_e2e_tests -- --ignored --nocapture
```

The E2E test runs a mixed real SDK workload:

- `Table`
- `Put`
- `Get`
- `Query`
- `Prepare`
- `WriteMultiple`
- `MultiDelete`
- `Delete`
- `GetTable`
- `ListTables`
- `GetIndexes`

It validates:

- SDK request counts match independently measured request counts.
- SDK-derived throughput matches independently measured throughput.
- SDK average latency approximately matches independently measured average
  latency.
- SDK p95 and p99 latency approximately match independently measured p95 and
  p99 latency.

The default repeated workload count is intentionally local-friendly:

```sh
NOSQL_STATS_E2E_REQUEST_COUNT=10
NOSQL_STATS_E2E_INTERVAL_SECS=10
```

Metric tolerances can be overridden:

```sh
NOSQL_STATS_E2E_LATENCY_ABS_TOLERANCE_MS=100
NOSQL_STATS_E2E_LATENCY_REL_TOLERANCE=2.0
NOSQL_STATS_E2E_THROUGHPUT_REL_TOLERANCE=0.05
```

## Demo Commands

With CloudSim running:

```sh
NOSQL_DEMO_MODE=cloudsim cargo run --example stats
NOSQL_DEMO_MODE=cloudsim cargo run --example stats_profile_output_demo -- localhost 8080 MORE EXACT
NOSQL_DEMO_MODE=cloudsim cargo run --example stats_profile_output_demo -- localhost 8080 ALL EXACT
NOSQL_DEMO_MODE=cloudsim cargo run --example stats_periodic_workload -- localhost 8080 300 5 MORE 1 EXACT
```

For high-volume comparison:

```sh
NOSQL_DEMO_MODE=cloudsim \
STATS_TABLE_NAME=sdk_stats_periodic_hdr \
STATS_TABLE_READ_UNITS=100000 \
STATS_TABLE_WRITE_UNITS=100000 \
cargo run --example stats_periodic_workload -- localhost 8080 600 5 MORE 5000 HDR 512
```

Use a fresh table name when changing CloudSim table limits because
`CREATE TABLE IF NOT EXISTS` does not raise limits on an existing table.

## Review Checklist

When reviewing stats changes, check:

- Does the change preserve Java-compatible field names and profile behavior?
- Does it avoid request-path instrumentation outside `Handle::send_and_receive`
  unless there is a request-specific reason?
- Does it keep prepare-only requests out of ALL-profile query entries?
- Does it preserve `sum(queries[*].count) == Query.httpRequestCount` for ALL
  profile query workloads?
- Does it keep latency units in milliseconds?
- Does it keep percentile fields only for `MORE` and `ALL`?
- Does it avoid exact latency assertions in environment-dependent tests?
- Does it keep normal test runs independent of a live CloudSim/proxy?

## Known Limitations

- Connection stats are an approximation because reqwest does not expose
  Java-style pool counts.
- Latency is measured at the HTTP response-byte boundary, not after full NSON
  deserialization or result object construction.
- Latency is emitted in whole milliseconds, so very fast local requests can
  show `0ms`.
- HDR percentile mode is approximate by design.
- The E2E stats test requires a running CloudSim/proxy and is therefore opt-in.
