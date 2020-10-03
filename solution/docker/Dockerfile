FROM §/com.sap.datahub.linuxx86_64/sles:15.0-sap-007
#FROM opensuse/leap:15.0

RUN groupadd -g 1972 textanalysis && useradd -g 1972 -u 1972 -m textanalysis
USER 1972:1972
WORKDIR "/home/textanalysis"
ENV HOME=/home/textanalysis
ENV PATH="${PATH}:${HOME}/.local/bin"

### Scrapy

RUN python3 -m pip install scrapy --user

RUN python3 -m pip --no-cache-dir install pandas --user

RUN python3 -m pip --no-cache-dir install nltk --user

RUN python3 -m pip --no-cache-dir install spacy --user
RUN python3 -m spacy download de_core_news_sm --user
RUN python3 -m spacy download fr_core_news_sm --user
RUN python3 -m spacy download es_core_news_sm --user
RUN python3 -m spacy download en_core_web_sm --user

RUN python3 -m pip --no-cache-dir install sdi_utils --user

RUN python3 -m pip --no-cache-dir install requests --user
RUN python3 -m pip --no-cache-dir install textblob --user
RUN python3 -m pip --no-cache-dir install textblob-de --user
RUN python3 -m pip --no-cache-dir install textblob-fr --user

RUN scrapy startproject onlinemedia 
