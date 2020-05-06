# NSQute - NSQ Client

Work in progress

## TODO

* ~~Producer~~
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
* Better logging
* Do some benchmarking
* Mayyyyyybe use Async
