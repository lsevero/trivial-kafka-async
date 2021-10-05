# trivial-kafka-async

A simple kafka client using core.async

[![Clojars Project](https://img.shields.io/clojars/v/org.clojars.lsevero/trivial-kafka-async.svg)](https://clojars.org/org.clojars.lsevero/trivial-kafka-async)

## Usage

Just instantiate `producer!`s and `consumer!`s when you need.
Both those functions return a `core.async/chan`.

To send send a message to topic, send a message to the producer channel returned by `producer!`.

To receive a message from a topic, read the messages on the consumer channel returned by `consumer!`.

There is also a `worker!` facility that consumes a consumer channel and apply a function to each message received.

## Troubleshooting 
### Unresponsive kafka channels
core.async default pool-size may be too small if you have to many producers and consumers instantiated.
To increase the pool size you'll need to pass this java property to your app, or in java or in `:jvm-opts` project.clj lein section:
`-Dclojure.core.async.pool-size=<number of threads>`
The default for this property is 8.

## Working example

See [kafka-client-template](https://github.com/lsevero/kafka-client-template)

## License

Copyright © 2021 Lucas Severo

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
