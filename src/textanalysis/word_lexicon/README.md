# Lexicon Words - textanalysis.word_lexicon (Version: 0.1.0)

Map words according to lexicon.

## Inport

* **lexicon** (Type: message.DataFrame) Message with body as  DataFrame.
* **table** (Type: message.DataFrame) Message with body as  DataFrame.

## outports

* **log** (Type: string) Logging data
* **data** (Type: message.DataFrame) Data after mapping lexicon

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **language_filter** - Language Filter (Type: string) Filter for language of media.
* **word_types** - Type (Type: string) Setting word type selection for mapping.


# Tags
sdi_utils : pandas : 

