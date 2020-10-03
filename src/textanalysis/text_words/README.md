# Text Cleansing - textanalysis.text_cleansing (Version: 0.0.1)

Prepares text for further processings

## Inport

* **blacklist** (Type: message.list) Message with body as dictionary.
* **articles** (Type: message.dicts) Message with body as dictionary.

## outports

* **log** (Type: string) Logging data
* **data** (Type: message) Output List of dicts with word frequency

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **language** - Language (Type: string) Filter for language of media.
* **mode** - Mode (Type: string) Define the kind of data extraction. "NOUN": nouns,"PNOUN": proper nouns. "P+NOUN": proper nouns and nouns Default is removing stopwords.
* **counter** - Counter (Type: boolean) Returns counter if "TRUE" or a list of words.
* **max_word_len** - Maximum word lenght (Type: integer) Maximum word lenght.


# Tags
sdi_utils : spacy : 

