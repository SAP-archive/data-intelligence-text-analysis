# Topic dispatcher - textanalysis.topic_identification (Version: 0.0.1)

Sends input topics to SQL processor and topic frequency operator.

## Inport

* **topic** (Type: message.table) Message with body as table.

## outports

* **log** (Type: string) Logging data
* **sql** (Type: message) message with body is sql and attributes contains the topic

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port


# Tags
sdi_utils : 

