FROM opensuse/leap:15.0

RUN zypper --non-interactive update && \
    # Install tar, gzip, python, python3, pip, pip3, gcc and libgthread
    zypper --non-interactive install --no-recommends --force-resolution \
    tar \
    gzip \
    python3 \
    python3-pip \
    gcc=7 \
    gcc-c++=7 \
    libgthread-2_0-0=2.54.3

RUN groupadd -g 1972 textanalysis && useradd -g 1972 -u 1972 -m textanalysis
USER 1972:1972
WORKDIR "/home/textanalysis"
ENV HOME=/home/textanalysis
ENV PATH="${PATH}:${HOME}/.local/bin"

RUN python3 -m pip install --upgrade pip --user
RUN python3 -m pip install tornado==5.0.2 --user
RUN python3 -m pip install pandas --user
RUN python3 -m pip --no-cache-dir install sdi_utils --user

### Scrapy

RUN python3 -m pip install scrapy --user

RUN python3 -m pip --no-cache-dir install nltk --user

RUN python3 -m pip --no-cache-dir install spacy --user
RUN python3 -m spacy download de_core_news_sm --user
# RUN python3 -m spacy download fr_core_news_sm --user
RUN python3 -m spacy download es_core_news_sm --user
RUN python3 -m spacy download en_core_web_sm --user

ENV PATH="${PATH}:${HOME}/.local/bin"
RUN scrapy startproject onlinemedia

COPY source_files/items.py ./onlinemedia/onlinemedia
COPY source_files/middlewares.py ./onlinemedia/onlinemedia
COPY source_files/pipelines.py ./onlinemedia/onlinemedia
COPY source_files/settings.py ./onlinemedia/onlinemedia
COPY source_files/spider.py ./onlinemedia/onlinemedia/spiders


#
# For interactive testing:
# Uncomment
#COPY source_files/items.py ./onlinemedia/onlinemedia
#COPY source_files/middlewares.py ./onlinemedia/onlinemedia
#COPY source_files/pipelines.py ./onlinemedia/onlinemedia
#COPY source_files/settings.py ./onlinemedia/onlinemedia
#COPY source_files/spider.py ./onlinemedia/onlinemedia/spiders

# build docker image locally: docker build --tag scrapy .
# run docker intactively: docker run -it scrapy bash
# change directory: cd onlinemedia
# run scrapy: scrapy crawl Spiegel_spider
