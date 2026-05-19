# mini-redis Learning Plan

A step-by-step guide to learning async Rust by reading this codebase.
Target audience: you know Rust syntax (structs, enums, traits, lifetimes, pattern matching) but haven't built a real async project.

---

## How This Document Works

The codebase has **20 Rust files**. You don't read them all at once. This plan
divides them into **6 phases**, ordered from foundational to advanced. Each phase:

- Lists the exact files to read, in order
- Explains what Rust/Tokio concepts you'll learn
- Gives you concrete questions to answer before moving on

**Rule of thumb**: if you can't answer the questions at the end of a phase,
re-read that phase. Don't rush.

---

## Phase 1: The Wire Protocol (start here)

This is the foundation of everything. A Redis client and server communicate
by sending and receiving bytes over TCP. The Redis protocol (RESP) defines
how those bytes are structured. Understanding this is prerequisite to
everything else.

### Redis Protocol (RESP) Overview

> from <https://redis.io/docs/latest/develop/reference/protocol-spec/>

| First byte | Type          | Format                             |
| ---------- | ------------- | ---------------------------------- |
| `+`        | Simple string | `+OK\r\n`                          |
| `-`        | Error         | `-ERR message\r\n`                 |
| `:`        | Integer       | `:42\r\n`                          |
| `$`        | Bulk string   | `$5\r\nhello\r\n`                  |
| `*`        | Array         | `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n` |

### Files to read (in order)

1. **`src/frame.rs`** — The RESP data model
2. **`src/parse.rs`** — A cursor for extracting typed values from a Frame

### What you'll learn from `frame.rs`

- **Enums as data models**: `Frame` is an enum with 6 variants
  (`Simple`, `Error`, `Integer`, `Bulk`, `Null`, `Array`). This is how
  Rust represents "a value that could be one of several types."
- **The `bytes` crate**: `Bytes` is a reference-counted byte buffer.
  Unlike `Vec<u8>`, cloning `Bytes` is cheap (no data copy). You'll see
  this everywhere in async Rust.
- **`Cursor<&[u8]>`**: wrapping a byte slice in a `Cursor` lets you read
  bytes sequentially, tracking position. This is the standard pattern for
  parsing binary protocols.
- **Recursive parsing**: `Frame::parse` and `Frame::check` call themselves
  for array frames. Notice how `check` is separate from `parse` — first
  validate the frame is complete, then allocate and parse.
- **Custom error types**: `frame::Error` has two variants: `Incomplete`
  (not enough data yet — normal during streaming) and `Other` (malformed
  data). This distinction is critical in networked code.
- **`impl From<X> for Error`**: these let you use `?` to convert errors
  automatically. This is idiomatic Rust error handling.

### What you'll learn from `parse.rs`

- **Wrapper types for safety**: `Parse` wraps a `Frame` array and exposes
  a typed cursor API (`next_string()`, `next_bytes()`, `next_int()`).
  This prevents commands from accidentally reading the wrong type.
- **The iterator pattern**: `Parse` owns a `vec::IntoIter<Frame>`. Each
  `next_*` call consumes one element. `finish()` verifies nothing remains.
- **Separation of concerns**: `parse.rs` doesn't know about any specific
  command. It's a generic tool. Each command uses it independently.

### Questions to answer before moving on

1. What are the 6 variants of `Frame`, and what does each represent in the
   Redis protocol?

   => `Simple` - simple string, `Error` - simple error, `Integer` - Integer,
   `Bulk` - bulk string, `Null` - null bulk string, `Array` - array

2. Why does `Frame::check` exist separately from `Frame::parse`? What
   problem does this solve?

   => Check if the entire message can be decoded from `src`, validate the
   frame avoid allocating for malformed frames.

3. What's the difference between `Error::Incomplete` and `Error::Other`?

   => `Error::Incomplete` implies haven't read completely from Redis
   `Error::Other` handles the other, such as not match the protocol

   When would each occur?

4. Why is `Bytes` used instead of `Vec<u8>`?

   => more efficiency when cloning

---

## Phase 2: The Connection Layer

Now that you understand the data model (Frame), the next layer reads and
writes Frames over a TCP socket.

### Files to read (in order)

1. **`src/connection.rs`** — Buffered TCP stream that reads/writes Frames
2. **`src/shutdown.rs`** — Graceful shutdown signal wrapper

### What you'll learn from `connection.rs`

- **Buffered I/O**: `Connection` wraps `TcpStream` in a `BufWriter` (for
  writes) and a `BytesMut` (for reads). Writing to a buffer and flushing
  in bulk is dramatically faster than writing to the socket each time.
- **The read loop pattern**: `read_frame()` loops: try to parse from the
  buffer → if not enough data, read more from the socket → repeat. This is
  the fundamental pattern for reading streaming protocols.
- **`AsyncReadExt` / `AsyncWriteExt`**: Tokio's traits for async I/O.
  `read_buf` reads into a `BytesMut`, `write_u8` / `write_all` write
  bytes. These are the async equivalents of `std::io::Read` / `Write`.
- **Two-phase parsing**: `check` (fast, no allocation) runs first. Only if
  it succeeds does `parse` (slower, allocates) run. Then `advance` removes
  the consumed bytes from the buffer.
- **Frame encoding**: `write_frame` / `write_value` show the reverse
  process — converting a `Frame` back to bytes on the wire.

### What you'll learn from `shutdown.rs`

- **`broadcast` channel**: a multi-producer, multi-consumer channel where
  every receiver gets every message. Used here to signal all connections
  simultaneously.
- **`select!` pattern**: `shutdown.rs` wraps the broadcast receiver and
  provides an `is_shutdown()` check. This is used with `tokio::select!`
  in the server to race between "read a frame" and "receive shutdown."

### Questions to answer before moving on

1. Why is `BufWriter` used for the write side but `BytesMut` for the read
   side? Why not use the same approach for both?

   ~=> `ButesMut` is a buffer to store data for read, read in loop and check then
   write to the write buffer `BufWriter`, which wait until it has enough data
   to flush to the socket. If use the same approach, it needs to control the
   concurrency option, this may reduce the performance.~

   => Read side (`BytesMut`): data arrives from the socket in unpredictable chunks.
   You need a growable buffer to accumulate partial bytes until a complete frame
   is present. `BytesMut` grows as needed.


    Write side (`BufWriter`): you're writing complete, known-size frames. You need
    batching to avoid many small syscalls. `BufWriter` accumulates writes and flushes
    once.

2. Trace through `read_frame()`: what happens if the socket sends half a
   frame? What about a complete frame followed by the start of another?

   ~=> When it read a incomplete frame, it try to parse it, `check()` return a
   `Ok(None)`, then check if the write buffer `stream` has any data, if yes,
   then return a error `Err("connection reset by peer".into())`, if no,
   continue read;~

   => For a half frame (e.g., `$5\r\nhel` — incomplete):
   - `read_frame()` calls `parse_frame()`
   - `Frame::check()` returns `Err(Incomplete)`
   - `parse_frame()` maps that to `Ok(None)`
   - Back in `read_frame()`, since `Ok(None)` means "not enough data yet,"
   it reads more from socket: `self.stream.read_buf(&mut self.buffer).await?`
   - If the read returns 0 (socket closed), then it checks if the buffer is empty.
   If empty → clean close → `Ok(None)`. If not empty →
   `Err("connection reset by peer")`
   - If read returns more bytes → loop back, try `parse_frame()` again

   When it read a complete one followed by the start of another,
   it parses the complete frame and the left stay in read buffer.

3. Why does `Connection::write_frame` call `flush()` at the end? What
   would happen without it?

   => Send all the data in write buffer to socket, it not, it'll stay in
   buffer, the caller won't receive any data, the data in buffer will be
   discarded later.

---

## Phase 3: Commands — One at a Time

Commands are the business logic. Each Redis command (GET, SET, etc.) is a
struct that knows how to parse itself from a Frame and apply itself to the
database. Start with the simplest one.

### Files to read (in order)

1. **`src/cmd/ping.rs`** — Simplest command (echo back or return PONG)
2. **`src/cmd/get.rs`** — Read a value from the database
3. **`src/cmd/set.rs`** — Write a value (with optional expiration)
4. **`src/cmd/unknown.rs`** — Fallback for unrecognized commands
5. **`src/cmd/mod.rs`** — The Command enum that dispatches to each command

### What you'll learn

- **The command pattern**: each command struct has three methods:
  - `parse_frames(&mut Parse)` — extract fields from the frame
  - `apply(db, dst, ...)` — execute the command, write the response
  - `into_frame(self)` — convert back to a Frame (used by the client)
- **`#[instrument]` and `debug!`**: the `tracing` crate for structured
  logging. `#[instrument]` auto-instruments async functions. `debug!`
  logs key-value pairs.
- **`Bytes::from()`**: converting strings and byte slices into `Bytes`.
  You'll see `"get".as_bytes()` → `Bytes::from(...)` a lot.
- **Enum dispatch**: `cmd/mod.rs` has a `Command` enum and a `match`
  statement that delegates to each command's `apply`. This is how Rust
  does polymorphism without trait objects.
- **`impl ToString`**: `Get::new(key: impl ToString)` accepts `&str`,
  `String`, etc. This is a common ergonomic pattern.

### Questions to answer before moving on

1. What are the three methods every command must implement, and what does
   each do?
2. How does `Command::from_frame` decide which command to create?
3. Why does `Get::into_frame` use `Bytes::from("get".as_bytes())` instead
   of just `Bytes::from("get")`?
4. What happens when the server receives an unrecognized command?

---

## Phase 4: The Server — Putting It All Together

The server ties everything together: accept TCP connections, read frames,
dispatch commands, write responses.

### Files to read (in order)

1. **`src/bin/server.rs`** — The entry point (very short)
2. **`src/server.rs`** — The main server logic
3. **`src/db.rs`** — The in-memory database with expiration

### What you'll learn from `bin/server.rs`

- **`#[tokio::main]`**: this macro turns `async fn main()` into a regular
  `fn main()` that starts the Tokio runtime. You'll use this in every
  Tokio project.
- **`tracing-subscriber`**: sets up logging with `env-filter` so you can
  control log levels via `RUST_LOG`.

### What you'll learn from `server.rs`

- **Task-per-connection**: `tokio::spawn(async move { ... })` creates a
  new concurrent task for each connection. Tasks are Tokio's lightweight
  green threads.
- **`Semaphore` for connection limiting**: `Arc<Semaphore>` limits
  concurrent connections to 250. `acquire_owned()` returns a permit;
  dropping the permit releases it.
- **`tokio::select!`**: races two async operations. The server uses it to
  simultaneously listen for new connections and the shutdown signal.
  Each connection handler uses it to race "read a frame" vs "receive
  shutdown."
- **Graceful shutdown**: the shutdown protocol uses three channels:
  - `broadcast` to notify all connections to stop
  - `mpsc` to track when all connections have finished
  - Drop-based cleanup (when `_shutdown_complete` sender drops, the
    receiver knows that handler is done)
- **Exponential backoff**: `accept()` retries with doubling delays (1s,
  2s, 4s, ... up to 64s) on transient errors.

### What you'll learn from `db.rs`

- **`std::sync::Mutex` in async code**: the comments explain why a std
  mutex is used instead of a Tokio mutex. Key insight: if you never hold
  the lock across `.await` points, a std mutex is fine and faster.
- **`Arc` for shared state**: `Db` is a thin wrapper around `Arc<Shared>`.
  Cloning is cheap. All connections share the same data.
- **`BTreeSet` for expiration tracking**: entries are sorted by
  expiration time. The background task sleeps until the next expiration.
- **Background task pattern**: `tokio::spawn(purge_expired_tasks(...))`
  runs a long-lived task that cleans up expired keys. It uses
  `tokio::select!` to race between "sleep until next expiration" and
  "be notified of a new key."
- **`Drop` for cleanup**: `DbDropGuard` implements `Drop` to signal the
  background task to shut down. This is RAII — the idiomatic Rust way
  to ensure cleanup happens.

### Questions to answer before moving on

1. Trace the full lifecycle of a `SET foo bar` command: from TCP accept
   through to the response being written. Which functions are called?
2. Why is `std::sync::Mutex` used instead of `tokio::sync::Mutex`?
   What would go wrong with a Tokio mutex here?
3. How does the server know when all connections have finished during
   shutdown? Explain the `mpsc` channel trick.
4. How does key expiration work? What triggers the background task to
   wake up?

---

## Phase 5: The Client Library

The client is the other side of the protocol. It connects to the server,
sends commands, and reads responses. This phase also introduces pub/sub.

### Files to read (in order)

1. **`src/clients/client.rs`** — Async client with get/set/publish/subscribe
2. **`src/clients/blocking_client.rs`** — Blocking wrapper around the async client
3. **`src/clients/buffered_client.rs`** — Client with command buffering
4. **`src/clients/mod.rs`** — Module exports
5. **`src/cmd/publish.rs`** — Publish command implementation
6. **`src/cmd/subscribe.rs`** — Subscribe command (the most complex command)

### What you'll learn from `client.rs`

- **Typestate pattern**: `Client` transitions to `Subscriber` when you
  call `subscribe()`. This prevents calling non-pub/sub methods after
  subscribing — enforced at compile time, not runtime.
- **`async-stream` crate**: `Subscriber::into_stream()` uses
  `try_stream!` to create an async `Stream` from an async function.
  Rust doesn't have generators yet, so this macro fills the gap.
- **Request-response pattern**: every client method does the same thing:
  build a command → convert to frame → write frame → read response →
  match on response type.

### What you'll learn from `blocking_client.rs`

- **`tokio::runtime::Runtime`**: wraps an async client to provide a
  synchronous API. This shows how to bridge async and sync code.
- **`tokio::task::block_in_place`**: runs async code from a blocking
  context without starving the runtime.

### What you'll learn from `subscribe.rs`

- **`StreamMap`**: tracks multiple active subscriptions. Messages from
  any channel are merged into a single stream.
- **Complex state management**: `Subscribe::apply` is the most complex
  function in the codebase. It handles: initial subscription, receiving
  messages, and processing unsubscribe commands — all in one loop.

### Questions to answer before moving on

1. How does the typestate pattern prevent misuse of the Subscriber?
2. What does `into_stream()` do, and why can't you just implement
   `Stream` directly on `Subscriber`?
3. How does `subscribe.rs` handle a client that subscribes to multiple
   channels and then unsubscribes from one?

---

## Phase 6: The Entry Points and Examples

These tie everything together and show real usage patterns.

### Files to read (in order)

1. **`src/bin/cli.rs`** — Command-line client
2. **`src/lib.rs`** — The library root (module structure + error types)
3. **`examples/hello_world.rs`** — Simplest client example
4. **`examples/pub.rs`** — Publish example
5. **`examples/sub.rs`** — Subscribe example
6. **`examples/chat.rs`** — Multi-client chat using pub/sub

### What you'll learn

- **`clap` with derive macros**: `cli.rs` uses `#[derive(Parser)]` for
  CLI argument parsing. This is the standard approach in Rust CLIs.
- **Module visibility**: `lib.rs` shows which types are `pub` vs `pub(crate)`.
  Notice that `Db`, `Parse`, and `Shutdown` are private — they're
  implementation details.
- **Error type strategy**: the crate uses `Box<dyn Error>` as the
  top-level error type, but custom enums in hot paths (like `ParseError`).
  The comments explain why.

### Questions to answer before moving on

1. What's the difference between `pub`, `pub(crate)`, and private
   visibility? Find examples of each.
2. Why are some modules `pub mod` and others just `mod` in `lib.rs`?
3. Run `cargo run --bin mini-redis-server` and `cargo run --example hello_world`.
   Does it work? Now read the code again with fresh eyes.

---

## Phase 7 (Bonus): Tests

Once you understand the codebase, read the tests to see how async code
is tested.

### Files to read

1. **`tests/server.rs`** — Integration tests (server + client together)
2. **`tests/client.rs`** — Client-specific tests
3. **`tests/frame_validation.rs`** — Frame parsing edge cases
4. **`tests/buffered_client.rs`** — Buffered client tests

### What you'll learn

- **`#[tokio::test]`**: async test functions.
- **`tokio::time::pause()`**: mock time in tests. The expiration tests
  use this to avoid real `sleep()`.
- **Integration testing pattern**: start a server in the background,
  connect a client, run assertions, shut down.

---

## Summary: File Reading Order

Here's the complete reading order in one list:

```
Phase 1: Wire Protocol
  src/frame.rs
  src/parse.rs

Phase 2: Connection Layer
  src/connection.rs
  src/shutdown.rs

Phase 3: Commands
  src/cmd/ping.rs
  src/cmd/get.rs
  src/cmd/set.rs
  src/cmd/unknown.rs
  src/cmd/mod.rs

Phase 4: Server
  src/bin/server.rs
  src/server.rs
  src/db.rs

Phase 5: Client
  src/clients/client.rs
  src/clients/blocking_client.rs
  src/clients/buffered_client.rs
  src/clients/mod.rs
  src/cmd/publish.rs
  src/cmd/subscribe.rs

Phase 6: Entry Points
  src/bin/cli.rs
  src/lib.rs
  examples/hello_world.rs
  examples/pub.rs
  examples/sub.rs
  examples/chat.rs

Phase 7: Tests (bonus)
  tests/server.rs
  tests/client.rs
  tests/frame_validation.rs
  tests/buffered_client.rs
```

---

## Key Concepts Cheat Sheet

| Concept             | Where it appears          | What to look for                                     |
| ------------------- | ------------------------- | ---------------------------------------------------- |
| `async` / `await`   | Everywhere                | Any `async fn` returns a future; `.await` drives it  |
| `tokio::spawn`      | `server.rs`               | Creates a concurrent task (like a green thread)      |
| `tokio::select!`    | `server.rs`, `db.rs`      | Races multiple async ops, runs the first to complete |
| `Arc<Mutex<T>>`     | `db.rs`                   | Shared mutable state across tasks                    |
| `broadcast` channel | `server.rs`, `db.rs`      | One-to-many notification (shutdown, pub/sub)         |
| `mpsc` channel      | `server.rs`               | Many-to-one communication (shutdown tracking)        |
| `Bytes`             | Everywhere                | Cheap-to-clone byte buffer (no copy on clone)        |
| `BufWriter`         | `connection.rs`           | Buffered async writes (batch before sending)         |
| `BytesMut`          | `connection.rs`           | Growable byte buffer for reading                     |
| `Cursor<&[u8]>`     | `frame.rs`                | Sequential byte reading with position tracking       |
| `impl Trait` args   | `cmd/get.rs`, `server.rs` | Accept any type implementing a trait                 |
| Typestate           | `clients/client.rs`       | `Client` → `Subscriber` transition at compile time   |
| RAII / Drop         | `db.rs`                   | Cleanup via `Drop` (shutdown background task)        |

---

## Next Steps After Finishing

1. **Add a new command** (e.g., `DEL` or `INCR`). You'll need to:
   - Create `src/cmd/del.rs`
   - Add a variant to the `Command` enum
   - Add a method to `Client`
   - Add parsing in `Command::from_frame`

2. **Add persistence** — save the database to disk on shutdown and load
   it on startup.

3. **Read the Tokio tutorial** — after understanding this codebase, the
   official Tokio tutorial will make much more sense:
   <https://tokio.rs/tokio/tutorial>

4. **Compare with real Redis protocol** — read the RESP spec at
   <https://redis.io/topics/protocol> and see how mini-redis maps to it.
