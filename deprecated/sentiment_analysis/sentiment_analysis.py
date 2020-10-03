import io
import json
import os
import re
import pickle
import subprocess

import pandas as pd


from textblob import TextBlob, Blobber
from textblob_de import TextBlobDE as TextBlobDE
from textblob_fr import PatternTagger as PatternTaggerFR, PatternAnalyzer as PatternAnalyzerFR

import nltk
nltk.download('punkt')

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

language_dict = {'Lefigaro': 'FR', 'Lemonde': 'FR', 'Spiegel': 'DE', 'FAZ': 'DE', 'Sueddeutsche': 'DE', 'Elpais': 'ES',
                 'Elmundo': 'ES'}

supported_languages = ['EN','DE','FR']

try:
    api
except NameError:
    class api:
        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if port == outports[1]['name']:
                pass
                #new_filename = os.path.basename(msg.attributes['storage.filename']).split('.')[0] + '-sentiment.json'
                #with open(os.path.join('/Users/Shared/data/onlinemedia/prep_texts', new_filename), mode='w') as f:
                #    f.write(json.dumps(msg.body, ensure_ascii=False, indent=4))

        def set_config(config):
            api.config = config

        def set_port_callback(port, callback):
            pass

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': '','textblob':''}
            version = "0.0.18"
            operator_name = "sentiment_analysis"
            operator_description = "Sentiment Analysis"
            operator_description_long = "Sentiment Analysis using lexicographic approach. "
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

last_msg = None
hash_text_list = list()


def get_article_sentiment(article):
    """
    Extracts sentiment analysis for article.
    @param article: article dictionary (retrieved from the Data Lake)
    @returns: (article_level_polarity, article_level_subjectivity)
    """
    if language_dict[article['media']] == 'DE':
        blob = TextBlobDE(article['text'])
        polarity, subjectivity = (blob.sentiment.polarity, blob.sentiment.subjectivity)
    elif language_dict[article['media']] == 'FR':
        tb = Blobber(pos_tagger=PatternTaggerFR(), analyzer=PatternAnalyzerFR())
        blob = tb(article['text'])
        polarity, subjectivity = blob.sentiment
    else:  # for now defaults to FR (just for PoC)
        blob = TextBlob(article['text'])
        polarity, subjectivity = (blob.sentiment.polarity, blob.sentiment.subjectivity)
    return polarity, subjectivity


def process(msg):
    global setup_data
    global last_msg
    global hash_text_list

    operator_name = 'sentiment analysis'
    logger, log_stream = slog.set_logging(operator_name, loglevel=api.config.debug_mode)
    logger.info("Process started")
    time_monitor = tp.progress()

    if api.config.debug_mode:
        api.send(outports[0]['name'], log_stream.getvalue())

    article_list = msg.body
    att_dict = msg.attributes
    att_dict['operator'] = operator_name

    sentiments_list = list()
    sentiments_table = list()
    media_set = set()
    for article in article_list:
        media_set.add(article['media'])

        # Ensure that text only analysed once
        if article['hash_text'] in hash_text_list:
            continue
        hash_text_list.append(article['hash_text'])

        if not language_dict[article['media']] in supported_languages :
            continue

        polarity, subjectivity =  get_article_sentiment(article)

        sentiments_list.append({'HASH_TEXT': article['hash_text'],'POLARITY': polarity, 'SUBJECTIVITY': subjectivity})
        sentiments_table.append([article['hash_text'],polarity,subjectivity])


    logger.debug('Process ended, analysed media: {} - article sentiments analysed {}  - {}'.format(str(media_set), len(sentiments_list),\
                                                                                      time_monitor.elapsed_time()))


    table_att = {"columns": [
        {"class": "string", "name": "HASH_TEXT", "nullable": False, "type": {"hana": "INTEGER"}},
        {"class": "string", "name": "POLARITY", "nullable": True,"type": {"hana": "DOUBLE"}},
        {"class": "string", "name": "SUBJECTIVITY", "nullable": True, "type": {"hana": "DOUBLE"}}],
              "name": "DIPROJECTS.SENTIMENTS", "version": 1}

    api.send(outports[0]['name'], log_stream.getvalue())
    if len(sentiments_list) :
        logger.debug("First Record: {}".format(str(sentiments_list[0])))
    api.send(outports[2]['name'], api.Message(attributes=att_dict, body=sentiments_list))

    att_dict['table'] = table_att
    if len(sentiments_table) :
        logger.debug("First Record: {}".format(str(sentiments_table[0])))
    msg = api.Message(attributes=att_dict, body=sentiments_table)
    api.send(outports[1]['name'], msg)


inports = [{'name': 'articles', 'type': 'message.dicts', "description": "Message with body as dictionary "}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"},
            {'name': 'table', 'type': 'message.table', "description": "table of sentiments articles"},             \
            {'name': 'data', 'type': 'message', "description": "Output List of sentiment records"}]

# api.set_port_callback(inports[0]['name'], process)

def test_operator():
    config = api.config
    config.debug_mode = True
    api.set_config(config)

    crawled_texts = False
    if crawled_texts :
        in_dir = '/Users/Shared/data/onlinemedia/crawled_texts/'
        files_in_dir = [f for f in os.listdir(in_dir) if
                        os.path.isfile(os.path.join(in_dir, f)) and re.match('Spiegel_2020-04-01.json', f)]
        for i, fname in enumerate(files_in_dir):
            fbase = fname.split('.')[0]
            eos = True if len(files_in_dir) == i + 1 else False
            attributes = {'format': 'csv', "storage.filename": fbase, 'storage.endOfSequence': eos, \
                          'storage.fileIndex': i, 'storage.fileCount': len(files_in_dir), 'process_list': []}
            with open(os.path.join(in_dir, fname)) as json_file:
                data = json.load(json_file)
            msg_data = api.Message(attributes=attributes, body=data)
            process(msg_data)
    else:
        doc_file = '/Users/Shared/data/onlinemedia/data/doc_data_cleansed.csv'
        df = pd.read_csv(doc_file, sep=',', nrows=1000000000)
        msg = api.Message(attributes={'file': {'path': doc_file}, 'format': 'pandas'}, body=df)
        process(msg)

if __name__ == '__main__':
    test_operator()
    if False:
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(["vctl", "solution", "bundle",
                        '/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_0.0.18', \
                        "-t", solution_name])
        subprocess.run(["mv", solution_name + '.zip', '../../../solution/operators'])


