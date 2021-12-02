### Required

Rust 2018

### Cargo Features

- no_trace_release : disable trace logs in release, useful if there is sensitive information in the URL
- zstd : zstd compression for actix

defaults : zstd

### Purpose

Signalr actor client for actix and rust, using futures 0.3 async/await

### bittrex example

There is a
[bittrex example](https://github.com/actix/actix/tree/master/examples/bittrex.rs)
which provides a basic example of using the actor
