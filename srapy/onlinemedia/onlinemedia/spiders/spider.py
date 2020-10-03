import os
import scrapy
from onlinemedia.items import newsItem

#os.chdir('')

class SpiegelSpider(scrapy.Spider):
    name = "Spiegel_spider"
    '''this spider crawl the news on the website of Spiegel'''
    def start_requests(self):
        # the schema of rubrics pages are identical to that of homepage, therefore all put in the same list
        homepage_urls = ["https://www.spiegel.de", "https://www.spiegel.de/politik/deutschland", "https://www.spiegel.de/politik/ausland", "https://www.spiegel.de/wirtschaft/"]

        for url in homepage_urls:
            yield scrapy.Request(url = url, callback = self.parse_homepage)
        for url in politics_urls:
            yield scrapy.Request(url = url, callback = self.parse_rubrics)

    def parse_homepage(self, response):
        article_urls_raw = response.xpath("//a[@class = 'text-black block']//@href").extract()
        article_urls = [url for url in article_urls_raw if url[0:5] == "https"]
        # article_titles = response.xpath("//a[@class = 'text-black block']//@title").extract()

        for ii, article_url in enumerate(article_urls):
            # In principle, only one kind of item should be generated, because they will be all equally treated by the pipeline
            article = newsItem()
            article['index'] = ii
            article['url'] = article_url
            # check the parse_rubrics
            if 'politik' in article_url.split('/'):
                article['rubrics'] = 'politics'
            elif 'wirtschaft' in article_url.split('/'):
                article['rubrics'] = 'economics'
            else:
                article['rubrics'] = 'homepage'

            article_request = scrapy.Request(article_url, callback = self.parse_article)
            # the parser seems to be only able to catch response, not items
            # the item can be stored in the request/response and transfered to the next parser
            article_request.meta['item'] = article
            yield article_request

    def parse_article(self, response):
        article = response.meta['item']
        article['title'] = response.xpath("//article//header//span[contains(@class, 'align-middle')]//text()").extract()
        article['text'] = response.xpath("//article//p//text()").extract()
        if 'www' not in response.url.split('.')[0]:
            article['website'] = response.url.split('.')[0][8:]
        else:
            article['website'] = response.url.split('.')[1]
        # check if the article is behind the paywall
        # Spiegel uses paywall in div to indicate premium articles
        paywall_test = response.xpath("//div[@data-component='Paywall']//text()").extract()
        if len(paywall_test) == 0:
            article['paywall'] = False
        else:
            article['paywall'] = True
        # each time, when an item is generated, it will be passed on to the pipeline
        yield article

class FazSpider(scrapy.Spider):
    name = "FAZ_spider"
    '''this spider crawl the news on the website of FAZ'''
    def start_requests(self):
        # the schema of rubrics pages are identical to that of homepage, therefore all put in the same list
        homepage_urls = ["https://www.faz.net", "https://www.faz.net/aktuell/politik/", "https://www.faz.net/aktuell/wirtschaft/"]
        for url in homepage_urls:
            yield scrapy.Request(url = url, callback = self.parse_homepage)

    def parse_homepage(self, response):
        article_urls_raw = response.xpath("//a[@class = 'js-hlp-LinkSwap js-tsr-Base_ContentLink tsr-Base_ContentLink']//@href").extract()
        article_urls = [url for url in article_urls_raw if url[0:5] == "https"]
        # article_titles = response.xpath("//a[@class = 'js-hlp-LinkSwap js-tsr-Base_ContentLink tsr-Base_ContentLink']//@title").extract()

        for ii, article_url in enumerate(article_urls):
            # In principle, only one kind of item should be generated, because they will be all equally treated by the pipeline
            article = newsItem()
            article['index'] = ii
            article['url'] = article_url
            # check the parse_rubrics
            if 'politik' in article_url.split('/'):
                article['rubrics'] = 'politics'
            elif 'wirtschaft' in article_url.split('/'):
                article['rubrics'] = 'economics'
            else:
                article['rubrics'] = 'homepage'

            article_request = scrapy.Request(article_url, callback = self.parse_article)
            # the parser seems to be only able to catch response, not items
            # the item can be stored in the request/response and transfered to the next parser
            article_request.meta['item'] = article
            yield article_request

    def parse_article(self, response):
        article = response.meta['item']
        article['title'] = response.xpath("//span[@class='atc-HeadlineText']//text()").extract()
        article['text'] = response.xpath("//p[contains(@class, 'atc-TextParagraph')]//text()").extract()
        if 'www' not in response.url.split('.')[0]:
            article['website'] = response.url.split('.')[0][8:]
        else:
            article['website'] = response.url.split('.')[1]
        # check if the article is behind the paywall
        # faz uses paywall element to indicate premium articles
        paywall_test = response.xpath("//div[contains(@class, 'Paywall')]//text()").extract()
        if len(paywall_test) == 0:
            article['paywall'] = False
        else:
            article['paywall'] = True
        # each time, when an item is generated, it will be passed on to the pipeline
        yield article

class SuedSpider(scrapy.Spider):
    name = "Sueddeutsche_spider"
    '''this spider crawl the news on the website of Suedeutsche'''
    def start_requests(self):
        # the schema of rubrics pages are identical to that of homepage, therefore all put in the same list
        homepage_urls = ["https://www.sueddeutsche.de", "https://www.sueddeutsche.de/politik", "https://www.sueddeutsche.de/wirtschaft"]
        for url in homepage_urls:
            yield scrapy.Request(url = url, callback = self.parse_homepage)

    def parse_homepage(self, response):
        article_urls_raw = response.xpath("//a[contains(@class, 'sz-teaser')]//@href").extract()
        article_urls = [url for url in article_urls_raw if url[0:5] == "https"]
        # article_titles = response.xpath("//a[contains(@class, 'sz-teaser')]//h3[contains(@class, 'sz-teaser__title')]//text()").extract()

        for ii, article_url in enumerate(article_urls):
            # In principle, only one kind of item should be generated, because they will be all equally treated by the pipeline
            article = newsItem()
            article['index'] = ii
            article['url'] = article_url
            # check the parse_rubrics
            if 'politik' in article_url.split('/'):
                article['rubrics'] = 'politics'
            elif 'wirtschaft' in article_url.split('/'):
                article['rubrics'] = 'economics'
            else:
                article['rubrics'] = 'homepage'
            article_request = scrapy.Request(article_url, callback = self.parse_article)
            # the parser seems to be only able to catch response, not items
            # the item can be stored in the request/response and transfered to the next parser
            article_request.meta['item'] = article
            yield article_request

    def parse_article(self, response):
        article = response.meta['item']
        article['title'] = response.xpath("//h2//span[@class='css-dtx5jp']//text()").extract()
        article['text'] = response.xpath("//div[contains(@class, 'sz-article__body')]//p[@class]//text()").extract()
        if 'www' not in response.url.split('.')[0]:
            article['website'] = response.url.split('.')[0][8:]
        else:
            article['website'] = response.url.split('.')[1]
        # check if the article is behind the paywall
        # sueddeutsche uses reduced = True to indicate premium articles
        paywall_test = response.url.split("?")
        if paywall_test == 'reduced=true':
            article['paywall'] = True
        else:
            article['paywall'] = False
        # each time, when an item is generated, it will be passed on to the pipeline
        yield article

class BildSpider(scrapy.Spider):
    name = "Bild_spider"
    '''this spider crawl the news on the website of Bild'''
    def start_requests(self):
        homepage_urls = ["https://www.bild.de"]
        for url in homepage_urls:
            yield scrapy.Request(url = url, callback = self.parse_homepage)

    def parse_homepage(self, response):
        # most bild internal articles ends with .html
        article_urls_raw = response.xpath("//a[contains(@href, '.html')]//@href").extract()
        # Bild.de does not use absolute path for its internal web pages
        article_urls = ['https://www.bild.de'+url for url in article_urls_raw if not (url[0:4] == "http")]
        # article_titles = response.xpath("//a[contains(@class, 'sz-teaser')]//h3[contains(@class, 'sz-teaser__title')]//text()").extract()

        for ii, article_url in enumerate(article_urls):
            # In principle, only one kind of item should be generated, because they will be all equally treated by the pipeline
            article = newsItem()
            article['index'] = ii
            article['url'] = article_url
            article_request = scrapy.Request(article_url, callback = self.parse_article)
            # the parser seems to be only able to catch response, not items
            # the item can be stored in the request/response and transfered to the next parser
            article_request.meta['item'] = article
            yield article_request

    def parse_article(self, response):
        article = response.meta['item']
        article['title'] = response.xpath("//h2//span[@class = 'headline']//text()").extract()
        article['text'] = response.xpath("//div[@class = 'txt']//p//text()").extract()
        if 'www' not in response.url.split('.')[0]:
            article['website'] = response.url.split('.')[0][8:]
        else:
            article['website'] = response.url.split('.')[1]
        # each time, when an item is generated, it will be passed on to the pipeline
        yield article

class ElpaisSpider(scrapy.Spider):
    name = "Elpais_spider"
    '''this spider crawl the news on the website of El Pais'''
    def start_requests(self):
        # the schema of rubrics pages are identical to that of homepage, therefore all put in the same list
        # it seems el pais does not have a rubric for politics
        homepage_urls = ["https://www.elpais.com", "https://www.elpais.com/economia"]
        for url in homepage_urls:
            yield scrapy.Request(url = url, callback = self.parse_homepage)

    def parse_homepage(self, response):
        article_urls_raw = response.xpath("//article[contains(@class, 'articulo')]//@href").extract()
        article_urls = [url for url in article_urls_raw if url[0:18] == "https://elpais.com"]
        # article_titles = response.xpath("//a[contains(@class, 'sz-teaser')]//h3[contains(@class, 'sz-teaser__title')]//text()").extract()

        for ii, article_url in enumerate(article_urls):
            # In principle, only one kind of item should be generated, because they will be all equally treated by the pipeline
            article = newsItem()
            article['index'] = ii
            article['url'] = article_url
            # check the parse_rubrics
            if 'politica' in article_url.split('/'):
                article['rubrics'] = 'politics'
            elif 'economia' in article_url.split('/'):
                article['rubrics'] = 'economics'
            else:
                article['rubrics'] = 'homepage'
            article_request = scrapy.Request(article_url, callback = self.parse_article)
            # the parser seems to be only able to catch response, not items
            # the item can be stored in the request/response and transfered to the next parser
            article_request.meta['item'] = article
            yield article_request

    def parse_article(self, response):
        article = response.meta['item']
        article['title'] = response.xpath("//h1[contains(@class, 'articulo-titulo')]//text()").extract()
        article['text'] = response.xpath("//div[contains(@class, 'articulo-cuerpo')]//p//text()").extract()
        if 'www' not in response.url.split('.')[0]:
            article['website'] = response.url.split('.')[0][8:]
        else:
            article['website'] = response.url.split('.')[1]

        # check if the article is behind the paywall
        # elpais appears to have no paywall
        article['paywall'] = False
        # each time, when an item is generated, it will be passed on to the pipeline
        yield article

class ElmundoSpider(scrapy.Spider):
    name = "Elmundo_spider"
    '''this spider crawl the news on the website of El Elmundo_spider'''
    def start_requests(self):
        # the schema of rubrics pages are identical to that of homepage, therefore all put in the same list
        homepage_urls = ["https://www.elmundo.es"]
        for url in homepage_urls:
            yield scrapy.Request(url = url, callback = self.parse_homepage)

    def parse_homepage(self, response):
        article_urls_raw = response.xpath("//a[contains(@class, 'cover-content')]//@href").extract()
        # el mundo articles links usually start with https://www.elmundo.es
        article_urls = [url for url in article_urls_raw if (url[0:22] == "https://www.elmundo.es")]
        # article_titles = response.xpath("//a[contains(@class, 'sz-teaser')]//h3[contains(@class, 'sz-teaser__title')]//text()").extract()

        for ii, article_url in enumerate(article_urls):
            # In principle, only one kind of item should be generated, because they will be all equally treated by the pipeline
            article = newsItem()
            article['index'] = ii
            article['url'] = article_url
            # check the parse_rubrics
            if 'politique' in article_url.split('/'):
                article['rubrics'] = 'politics'
            elif 'economie' in article_url.split('/'):
                article['rubrics'] = 'economics'
            else:
                article['rubrics'] = 'homepage'
            article_request = scrapy.Request(article_url, callback = self.parse_article)
            # the parser seems to be only able to catch response, not items
            # the item can be stored in the request/response and transfered to the next parser
            article_request.meta['item'] = article
            yield article_request

    def parse_article(self, response):
        article = response.meta['item']
        article['title'] = response.xpath("//h1[contains(@class, 'article__headline')]//text()").extract()
        # Le Figaro has live articles centered in the homepage with a different html schema
        article['text'] = response.xpath("//div[contains(@class, 'article__body')]//p//text()").extract()
        # article['text'] = response.xpath("//article[contains(@id, 'fig-article')]//text()").extract()
        if 'www' not in response.url.split('.')[0]:
            article['website'] = response.url.split('.')[0][8:]
        else:
            article['website'] = response.url.split('.')[1]
        # check if the article is behind the paywall
        # el mundo uses premium sections to indicate premium articles
        paywall_test = response.xpath("//div[contains(@class, 'article__premium')]//text()").extract()
        if (len(paywall_test) != 0):
            article['paywall'] = True
        else:
            article['paywall'] = False
        # each time, when an item is generated, it will be passed on to the pipeline
        yield article

class LefigaroSpider(scrapy.Spider):
    name = "Lefigaro_spider"
    '''this spider crawl the news on the website of Le Figaro'''
    def start_requests(self):
        # the schema of rubrics pages are identical to that of homepage, therefore all put in the same list
        homepage_urls = ["https://www.lefigaro.fr", "https://www.lefigaro.fr/politique", "https://www.lefigaro.fr/economie"]
        for url in homepage_urls:
            yield scrapy.Request(url = url, callback = self.parse_homepage)

    def parse_homepage(self, response):
        article_urls_raw = response.xpath("//a[contains(@class, 'css-m47150 esdb6og4')]//@href").extract()
        # links starting with http are likely to external sites
        article_urls = ['https://www.lefigaro.fr' + url for url in article_urls_raw if not (url[0:4] == "http")]
        # article_titles = response.xpath("//a[contains(@class, 'sz-teaser')]//h3[contains(@class, 'sz-teaser__title')]//text()").extract()

        for ii, article_url in enumerate(article_urls):
            # In principle, only one kind of item should be generated, because they will be all equally treated by the pipeline
            article = newsItem()
            article['index'] = ii
            article['url'] = article_url
            # check the parse_rubrics
            if 'politique' in article_url.split('/'):
                article['rubrics'] = 'politics'
            elif 'economie' in article_url.split('/'):
                article['rubrics'] = 'economics'
            else:
                article['rubrics'] = 'homepage'
            article_request = scrapy.Request(article_url, callback = self.parse_article)
            # the parser seems to be only able to catch response, not items
            # the item can be stored in the request/response and transfered to the next parser
            article_request.meta['item'] = article
            yield article_request

    def parse_article(self, response):
        article = response.meta['item']
        article['title'] = response.xpath("//h1[contains(@class, 'eziudsn0')]//text()").extract()
        # Le Figaro has live articles centered in the homepage with a different html schema
        live_title = response.xpath("//h1[contains(@class, 'css-1ivsdme')]//text()")
        if (len(live_title) != 0):
            # only the first paragraph of a live article
            article['text'] = response.xpath("//div[contains(@class, 'live-article')]//text()").extract()
        else:
            article['text'] = response.xpath("//p[contains(@class, 'ekabp3u0')]//text()").extract()
        # article['text'] = response.xpath("//article[contains(@id, 'fig-article')]//text()").extract()
        if 'www' not in response.url.split('.')[0]:
            article['website'] = response.url.split('.')[0][8:]
        else:
            article['website'] = response.url.split('.')[1]
        # check if the article is behind the paywall
        # lefigaro does not use very clear identifier for premium articles
        paywall_test = response.xpath("//div[contains(@class, 'essq5k66')]//text()").extract()
        if (len(paywall_test) != 0):
            article['paywall'] = True
        else:
            article['paywall'] = False
        # each time, when an item is generated, it will be passed on to the pipeline
        yield article

class LemondeSpider(scrapy.Spider):
    name = "Lemonde_spider"
    '''this spider crawl the news on the website of Le Monde'''
    def start_requests(self):
        # the schema of rubrics pages are identical to that of homepage, therefore all put in the same list
        homepage_urls = ["https://www.lemonde.fr", "https://www.lemonde.fr/politique", "https://www.lemonde.fr/economie"]
        for url in homepage_urls:
            yield scrapy.Request(url = url, callback = self.parse_homepage)

    def parse_homepage(self, response):
        article_urls_raw = response.xpath("//div[contains(@class, 'article')]//@href").extract()
        article_urls = [url for url in article_urls_raw]
        # article_titles = response.xpath("//a[contains(@class, 'sz-teaser')]//h3[contains(@class, 'sz-teaser__title')]//text()").extract()

        for ii, article_url in enumerate(article_urls):
            # In principle, only one kind of item should be generated, because they will be all equally treated by the pipeline
            article = newsItem()
            article['index'] = ii
            article['url'] = article_url
            # check the parse_rubrics
            if 'politique' in article_url.split('/'):
                article['rubrics'] = 'politics'
            elif 'economie' in article_url.split('/'):
                article['rubrics'] = 'economics'
            else:
                article['rubrics'] = 'homepage'
            article_request = scrapy.Request(article_url, callback = self.parse_article)
            # the parser seems to be only able to catch response, not items
            # the item can be stored in the request/response and transfered to the next parser
            article_request.meta['item'] = article
            yield article_request

    def parse_article(self, response):
        article = response.meta['item']
        article['title'] = response.xpath("//h1[contains(@class, 'article__title')]//text()").extract()
        article['text'] = response.xpath("//p[contains(@class, 'article__paragraph')]//text()").extract()
        # article['text'] = response.xpath("//article[contains(@id, 'fig-article')]//text()").extract()
        if 'www' not in response.url.split('.')[0]:
            article['website'] = response.url.split('.')[0][8:]
        else:
            article['website'] = response.url.split('.')[1]
        # check if the article is behind the paywall
        # lemonde paywall section to indicate premium articles
        paywall_test = response.xpath("//section[contains(@id, 'js-paywall-content')]//text()").extract()
        if (len(paywall_test) != 0):
            article['paywall'] = True
        else:
            article['paywall'] = False
        # each time, when an item is generated, it will be passed on to the pipeline
        yield article
