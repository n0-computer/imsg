# iroh messages

A base protocol providing streams of messages.

The purpose of imsg is to be a building block for your own protocol.
It simplifies handling a full-blown QUIC connection with all its
subtelties into having a protocol which sends and receives messages.

The short summary of the features is:

- Always send and receive an entire message, as a `Bytes` object.
- Either side can send the first message.
- When closing a connection, it is easy to reason about which message
  may be lost.


## Work In Progress

This is currently highly experimental and more a prototype under
development.


## License

Copyright 2025 N0, INC.

This project is licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or
   http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or
   http://opensource.org/licenses/MIT)

at your option.

## Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this project by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
