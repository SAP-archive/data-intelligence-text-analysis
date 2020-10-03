# sql word index - textanalysis.sql_word_index (Version: 0.1.0)

Creates SQL statement for creating a word _index from text_word.

## Inport

* **sentiment_list** (Type: message.DataFrame) Sentiment list

## outports

* **log** (Type: string) Logging data
* **data** (Type: message) sql statement

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **type_limit_map** - Limit of each type (Type: string) Minimum frequency of words for each type (map)


# Tags
sdi_utils : spacy : 

