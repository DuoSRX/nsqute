# NSQute - NSQ Client

Work in progress

## TODO

* ~~Consumer~~
  * Message Flow Starvation
  * Backoff on failure
* ~~Producer~~
  * MPUB
  * DPUB
* Error handling
  * Reconnection on failure
  * Acknowledge non-fatal errors (`FIN_FAILED`...etc)
* Nsqlookupd
  * ~~Connect to multiple nsqds~~
  * Polling for new servers
  * Handle servers added/removed
* Configuration
  * Pick a configuration style (TOML, JSON, flags...)
* RDY and max_in_flight rebalancing
* IDENTIFY and feature negotiation
* Better logging
* Do some benchmarking
* Additional stuff:
  * Deflate
  * Snappy
  * TLS