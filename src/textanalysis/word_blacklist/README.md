# Remove Blacklisted Words - textanalysis.word_blacklist (Version: 0.1.0)

Remove Blacklisted Words

## Inport

* **blacklist** (Type: message.list) Message with body as dictionary.
* **table** (Type: message.DataFrame) Message with body as table.

## outports

* **log** (Type: string) Logging data
* **data** (Type: message.DataFrame) Message table after blacklist removals

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **word_types** - Word types (Type: string) Setting word type selection for delete
* **language_filter** - Language filter (Type: string) Filter for languages of media.


# Tags
sdi_utils : pandas : 

