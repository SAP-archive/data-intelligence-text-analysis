# Topic Frequency - textanalysis.topic_frequency (Version: 0.0.1)

Find frequency of topics in keyword index.

## Inport

* **topic** (Type: message.dicts) Message with body as dictionary.
* **keyword_index** (Type: message.table) Message with body as table.

## outports

* **log** (Type: string) Logging data
* **data** (Type: message) Output List of dicts with word frequency

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **tolerance** - Tolerance (Type: integer) Factor tolerated to count as has been found


# Tags
sdi_utils : pandas : 

