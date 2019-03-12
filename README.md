### StatsNozzle

Listens on the firehose for events and counts them, by EventType, Origin, Job, Deployment and IP. Every 5 seconds it outputs the numbers to stdout.

Environment variables used:

* API_ENDPOINT
* CF_USERNAME
* CF_PASSWORD