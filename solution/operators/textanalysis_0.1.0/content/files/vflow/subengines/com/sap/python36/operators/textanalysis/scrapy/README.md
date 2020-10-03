# scrapy - textanalysis.scrapy (Version: 0.0.2)

Starts scrapy and sends output to data port as dictionary or as json string tojsonstr port. Log output send to stderr(scrapy) to log port.

## Inport

* **spider** (Type: message.file) spider.py file
* **pipelines** (Type: message.file) pipelines.py file
* **items** (Type: message.file) pipelines.py file
* **middlewares** (Type: message.file) middlewares.py file
* **settings** (Type: message.file) settings.py file

## outports

* **log** (Type: string) logging
* **data** (Type: message.DataFrame) data

## Config

* **debug_mode** - Debug mode (Type: boolean) Sending debug level information to log port
* **spider** - Spider (Type: string) List of spiders for the command 'scrapy crawl <spider>'. Naming convention forspider = [media]_spider.py
* **scrapy_dir** - Scrapy directory (Type: string) Scrapy directory (absolute path)
* **project_dir** - Project Directory (Type: string) Scrapy project directory on container


# Tags
pandas : sdi_utils : scrapy : 

