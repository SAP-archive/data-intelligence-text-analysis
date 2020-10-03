# Doc Prepare - textanalysis.doc_prepare (Version: 0.0.18)

Prepares documents read from DB by select the values and remove formats.

## Inport

* **docs** (Type: message.DataFrame) Message table.

## outports

* **log** (Type: string) Logging data
* **data** (Type: message.DataFrame) Table with word index

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **text_column** - TEXT Column (Type: string) Name of incoming text column
* **id_column** - ID Column (Type: string) Name of incoming id column
* **language_column** - Name of language column (Type: string) Name of language column in input data.
* **default_language** - Default language (Type: string) Default language of the texts
* **remove_html_tags** - Remove html tags (Type: boolean) Remove html tags from text
* **media_docs** - Media articles (Type: boolean) Name of media article determines language.


# Tags
sdi_utils : 

