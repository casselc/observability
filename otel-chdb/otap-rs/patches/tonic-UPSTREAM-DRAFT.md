# DRAFT — tonic PR: jitter `max_connection_age` per connection (gRFC A9)

**Status: draft for the owner's review. Not posted: no PR, issue or comment
has been made on grpc/grpc-rust (hyperium/tonic).** The local fix
(`tonic-0001-…patch`) is the same code backported to 0.14.6 together with
the two upstream fixes it depends on.

## Where upstream stands (checked 2026-09-27, read-only)

| # | Bug in 0.14.6 | Upstream |
|---|---|---|
| 1 | With `max_connection_age_grace`, no GOAWAY is sent; the connection is dropped at age + grace | open PR [#2877](https://github.com/grpc/grpc-rust/pull/2877) "fix(transport): drain connections before the grace timeout" (2026-09-18), approved in substance by a reviewer, not merged |
| 2 | Without a grace, the finished timer future is polled again: panic "`async fn` resumed after completion" kills the connection's in-flight RPCs | issue [#2522](https://github.com/grpc/grpc-rust/issues/2522), fixed by [#2780](https://github.com/grpc/grpc-rust/pull/2780) (merged 2026-07-30); no release since 0.14.6 |
| 3 | No per-connection jitter | nothing (searched issues/PRs for "jitter", "max_connection_age", "goaway") |

So the upstream PR worth sending is only (3), stacked on #2877 (it touches the
same timer). Options for the owner:

- **A (recommended):** wait for #2877 to merge, then open the PR below
  against master.
- **B:** leave a review comment on #2877 suggesting the jitter (and the
  raw-frame GOAWAY test) be folded in.
- **C:** do nothing upstream; keep `tonic-0001` until a release has #2780 +
  #2877, then shrink it to the jitter.

Design choice to confirm: **always-on +/-10%** (what gRFC A9 specifies and
grpc-go, grpc-java and grpc-core do) rather than an opt-in
`max_connection_age_jitter(bool)`. It changes when an existing
`max_connection_age` fires by up to 10%, which A9 explicitly allows; an
opt-in builder flag is the fallback if maintainers object.

---

## Title

transport: jitter max_connection_age per connection (gRFC A9)

## Description

### Motivation

`Server::max_connection_age` closes every connection at exactly the same age.
Connections opened together (a client fleet started together, or all clients
reconnecting after a server restart) are therefore all sent their GOAWAY at
the same moment, and all reconnect at the same moment, again and again: a
reconnect storm every `max_connection_age`.

[gRFC A9](https://github.com/grpc/proposal/blob/master/A9-server-side-conn-mgt.md)
specifies: "A per-connection random jitter of +/-10% will be added to
`MAX_CONNECTION_AGE` to spread out connection storms." grpc-go, grpc-java and
grpc-core implement it; tonic does not.

### Solution

When a connection is accepted, its age timer is set to `max_connection_age`
multiplied by a uniformly random factor in `[0.9, 1.1)`. The grace period is
unchanged and still starts when the (jittered) age fires.

- No new dependency: the random value comes from std's randomly keyed
  `RandomState` hasher fed a counter (only has to decorrelate connections).
- Ages too large to scale (e.g. `Duration::MAX` used as "forever") are left
  unchanged instead of panicking in `Duration` arithmetic.
- Docs of `max_connection_age` mention the jitter and link A9.

Builds on #2877 (the age / grace timers in `serve_connection`).

### Testing

- New unit test `connection_age_jitter_bounds`: fixed-factor cases (0 → 90%,
  0.5 → 100%, <1 → <110%), 1000 random draws all within [90%, 110%) and not
  skewed to one side, `Duration::MAX` and `Duration::ZERO` do not panic.
- #2877's connection tests widened to the jitter range (`AGE_MIN` = 0.9 x
  age, `AGE_MAX` = 1.1 x age): GOAWAY checked after `AGE_MAX`, the call is
  still pending just before `AGE_MIN + grace` and cut after `AGE_MAX +
  grace`.
- Optionally (from the local patch, not in the diff below): a raw-HTTP/2
  test that checks the wire sequence at the jittered age — GOAWAY with
  last-stream-id 2^31-1 and NO_ERROR plus a PING, then after the PING ack a
  second GOAWAY with the real last stream id, then the close — with and
  without a grace.

Local verification: see "Test notes" below.

---

## Diff (against PR #2877's head, `tonic/src/transport/server/mod.rs`)

```diff
--- a/tonic/src/transport/server/mod.rs
+++ b/tonic/src/transport/server/mod.rs
@@ -294,8 +294,14 @@
 
     /// Sets the maximum age of a connection before graceful shutdown begins.
     ///
+    /// As specified by [gRFC A9], each connection's age is jittered by a random
+    /// +/-10%, so that connections opened together do not all reconnect
+    /// together.
+    ///
     /// Default is no limit (`None`).
     ///
+    /// [gRFC A9]: https://github.com/grpc/proposal/blob/master/A9-server-side-conn-mgt.md
+    ///
     /// # Example
     ///
     /// ```
@@ -916,6 +922,30 @@
     }
 }
 
+/// Scales `max_connection_age` by a random factor in `[0.9, 1.1)`, the
+/// per-connection jitter of gRFC A9.
+fn jittered_connection_age(max_connection_age: Duration) -> Duration {
+    jitter_connection_age(max_connection_age, random_unit())
+}
+
+/// Scales `age` by `0.9 + 0.2 * unit`, for `unit` in `[0, 1)`. Ages too large to
+/// scale are returned unchanged.
+fn jitter_connection_age(age: Duration, unit: f64) -> Duration {
+    Duration::try_from_secs_f64(age.as_secs_f64() * (0.9 + 0.2 * unit)).unwrap_or(age)
+}
+
+/// A uniformly distributed value in `[0, 1)`. Only has to decorrelate
+/// connections, so it uses std's randomly keyed hasher instead of a dependency.
+fn random_unit() -> f64 {
+    use std::hash::{BuildHasher, Hasher, RandomState};
+    use std::sync::atomic::{AtomicU64, Ordering};
+
+    static COUNTER: AtomicU64 = AtomicU64::new(0);
+    let mut hasher = RandomState::new().build_hasher();
+    hasher.write_u64(COUNTER.fetch_add(1, Ordering::Relaxed));
+    (hasher.finish() >> 11) as f64 / (1u64 << 53) as f64
+}
+
 // This is moved to its own function as a way to get around
 // https://github.com/rust-lang/rust/issues/102211
 fn serve_connection<B, IO, S, E>(
@@ -944,7 +974,7 @@
             let mut conn = pin!(builder.serve_connection(hyper_io, hyper_svc));
 
             let mut age_timeout = pin!(Fuse {
-                inner: max_connection_age.map(sleep),
+                inner: max_connection_age.map(|age| sleep(jittered_connection_age(age))),
             });
             let mut grace_timeout = pin!(Fuse {
                 inner: None::<Sleep>,
@@ -1311,6 +1341,9 @@
     const CONNECTION_AGE: Duration = Duration::from_secs(10);
     const CONNECTION_GRACE: Duration = Duration::from_secs(5);
     const TEST_TIMEOUT: Duration = Duration::from_secs(1);
+    // `max_connection_age` is jittered by +/-10% per connection.
+    const AGE_MIN: Duration = Duration::from_secs(9);
+    const AGE_MAX: Duration = Duration::from_secs(11);
 
     struct TestConnection {
         client: SendRequest<Bytes>,
@@ -1411,13 +1444,33 @@
         assert_eq!(body, "ok");
     }
 
+    #[test]
+    fn connection_age_jitter_bounds() {
+        let age = Duration::from_secs(100);
+        assert_eq!(jitter_connection_age(age, 0.0), Duration::from_secs(90));
+        assert_eq!(jitter_connection_age(age, 0.5), Duration::from_secs(100));
+        assert!(jitter_connection_age(age, 0.999_999) < Duration::from_secs(110));
+        let ages: Vec<Duration> = (0..1000).map(|_| jittered_connection_age(age)).collect();
+        assert!(
+            ages.iter()
+                .all(|a| (Duration::from_secs(90)..Duration::from_secs(110)).contains(a))
+        );
+        let shorter = ages.iter().filter(|a| **a < age).count();
+        assert!(
+            (350..=650).contains(&shorter),
+            "skewed jitter: {shorter}/1000"
+        );
+        assert_eq!(jitter_connection_age(Duration::MAX, 0.999), Duration::MAX);
+        assert_eq!(jittered_connection_age(Duration::ZERO), Duration::ZERO);
+    }
+
     #[tokio::test(start_paused = true)]
     async fn connection_age_sends_goaway_and_allows_in_flight_calls_to_finish() {
         let mut connection =
             TestConnection::new(Some(CONNECTION_AGE), Some(CONNECTION_GRACE)).await;
         let (mut response, complete) = connection.start_call().await;
 
-        sleep(CONNECTION_AGE + TEST_TIMEOUT).await;
+        sleep(AGE_MAX + TEST_TIMEOUT).await;
         connection.assert_goaway().await;
         assert_call_pending(&mut response).await;
         finish_call(response, complete).await;
@@ -1435,9 +1488,9 @@
             TestConnection::new(Some(CONNECTION_AGE), Some(CONNECTION_GRACE)).await;
         let (mut response, complete) = connection.start_call().await;
 
-        sleep(CONNECTION_AGE + CONNECTION_GRACE - TEST_TIMEOUT).await;
+        sleep(AGE_MIN + CONNECTION_GRACE - TEST_TIMEOUT).await;
         assert_call_pending(&mut response).await;
-        sleep(TEST_TIMEOUT).await;
+        sleep(AGE_MAX - AGE_MIN + TEST_TIMEOUT).await;
         assert!(
             timeout(TEST_TIMEOUT, response)
                 .await
@@ -1454,7 +1507,7 @@
         let mut connection = TestConnection::new(Some(CONNECTION_AGE), None).await;
         let (mut response, complete) = connection.start_call().await;
 
-        sleep(CONNECTION_AGE + CONNECTION_GRACE + TEST_TIMEOUT).await;
+        sleep(AGE_MAX + CONNECTION_GRACE + TEST_TIMEOUT).await;
         connection.assert_goaway().await;
         assert_call_pending(&mut response).await;
         finish_call(response, complete).await;
@@ -1467,7 +1520,7 @@
             let mut connection = TestConnection::new(None, grace).await;
             let (mut response, complete) = connection.start_call().await;
 
-            sleep(CONNECTION_AGE + CONNECTION_GRACE + TEST_TIMEOUT).await;
+            sleep(AGE_MAX + CONNECTION_GRACE + TEST_TIMEOUT).await;
             assert_call_pending(&mut response).await;
             let (next_response, next_complete) = connection.start_call().await;
             finish_call(next_response, next_complete).await;
@@ -1481,7 +1534,7 @@
         let mut connection = TestConnection::new(Some(CONNECTION_AGE), Some(Duration::ZERO)).await;
         let (response, complete) = connection.start_call().await;
 
-        sleep(CONNECTION_AGE + TEST_TIMEOUT).await;
+        sleep(AGE_MAX + TEST_TIMEOUT).await;
         assert!(
             timeout(TEST_TIMEOUT, response).await.unwrap().is_err(),
             "zero grace should terminate unfinished calls at the age limit",
```

## Test notes

- **This diff** was applied to a copy of `tonic/` at #2877's head
  (`e57ad31`, which includes #2780) in a one-member workspace, Rust 1.98.1,
  release profile: `cargo test -p tonic --lib` 82 passed; the six
  `connection_age*` tests passed 30 runs of 30 (fresh jitter each run);
  `cargo clippy -p tonic --lib --tests` with the workspace lints: no
  findings. Not run: the rest of the grpc-rust workspace (integration tests,
  other crates), for lack of disk here; CI would.
- **The local backport** (`tonic-0001`, 0.14.6 + the #2780/#2877 behaviour +
  this jitter) has its own tests: raw-frame two-step GOAWAY at the jittered
  age with and without a grace; in-flight call completes within the grace
  while a new call gets GOAWAY/NO_ERROR and the connection closes as soon as
  it is done; a call longer than the grace is cut at age + grace; no grace
  and a busy connection does not panic; no age keeps the connection; jitter
  bounds. 79/79 lib tests pass; the connection tests passed 30/30 runs. On
  unmodified 0.14.6, three of them fail (no GOAWAY with a grace; a new call
  accepted after the age; `async fn` resumed after completion).
- **End to end** (otap-rs `scripts/goaway_e2e.sh`, stock otelcol agents with
  `dns:///` + round_robin, 2 → 3 receivers, age 20 s): the added receiver
  gets its share ~20 s after joining the DNS answer, with a 15 s grace every
  span arrives exactly once, with grace 0 in-flight requests are cut and
  resent (duplicates, no loss). Results in `otap-rs/results/goaway/tonic-*.txt`.

## Not in this PR (possible follow-ups)

- gRFC A9 also asks for GOAWAY debug data `max_age`; hyper's
  `graceful_shutdown()` has no way to pass debug data.
- No hook reports the GOAWAY or the forced close to the application (only
  `debug!` logs); otap-rs lost its `aged_out` / `force_closed` counters for
  that reason. A callback or a metric would need a new API; not proposed.
