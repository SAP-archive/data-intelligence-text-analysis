import io
import json
import os
import re
import pickle
import subprocess

import pandas as pd
import numpy as np

from textblob import TextBlob, Blobber
from textblob_de import TextBlobDE as TextBlobDE
from textblob_fr import PatternTagger as PatternTaggerFR, PatternAnalyzer as PatternAnalyzerFR

import nltk
nltk.download('punkt')

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp


supported_languages = ['EN','DE','FR']

try:
    api
except NameError:
    class api:

        queue = list()

        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if port == outports[1]['name']:
                api.queue.append(msg)

        def set_config(config):
            api.config = config

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': '','textblob':''}
            version = "0.1.0"
            operator_name = "text_sentiment"
            operator_description = "Text Sentiment Analysis"
            operator_description_long = "Text Sentiment Analysis using Textblob. "
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

last_msg = None
id_set = set()

operator_name = 'sentiment analysis'
logger, log_stream = slog.set_logging(operator_name, loglevel=api.config.debug_mode)
logger.info("Process started")
time_monitor = tp.progress()

def get_sentiment(text, language):
    if isinstance(text,str) :
        if language == 'DE':
            blob = TextBlobDE(text)
            return [blob.sentiment.polarity, blob.sentiment.subjectivity]
        elif language == 'FR' :
            tb = Blobber(pos_tagger=PatternTaggerFR(), analyzer=PatternAnalyzerFR())
            blob = tb(text)
            return  blob.sentiment
        else:
            blob = TextBlob(text)
            return [blob.sentiment.polarity, blob.sentiment.subjectivity]



def process(msg):
    global setup_data
    global last_msg
    global hash_text_list

    df = msg.body
    att_dict = msg.attributes
    df['polarity'] = np.nan
    df['subjectivity'] = np.nan
    df[['polarity','subjectivity']] = df.apply(lambda row: pd.Series(get_sentiment(row['text'], row['language']),dtype='object'),axis=1)
    df['polarity'] = df['polarity'].round(2)
    df['subjectivity'] = df['subjectivity'].round(2)

    logger.info('Text processed: {}'.format(df.shape[0]))
    logger.debug('Process ended{}'.format(time_monitor.elapsed_time()))

    api.send(outports[0]['name'], log_stream.getvalue())
    api.send(outports[1]['name'], api.Message(attributes=att_dict,body=df[['text_id','polarity','subjectivity']]))



inports = [{'name': 'sentiment_list', 'type': 'message.DataFrame', "description": "Sentiment list"},
           {'name': 'texts', 'type': 'message.DataFrame', "description": "DataFrame with texts"}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"},
            {'name': 'data', 'type': 'message.DataFrame', "description": "data of sentiments"}]

# api.set_port_callback(inports[0]['name'], setup_sentiment_list)
# api.set_port_callback(inports[1]['name'], process)

def test_operator():
    config = api.config
    config.debug_mode = True
    config.language_filter = 'DE'
    config.use_sentiment_word_list = True
    api.set_config(config)

    doc_file = '/Users/Shared/data/onlinemedia/data/doc_data_cleansed.csv'
    df = pd.read_csv(doc_file, sep=',', nrows=1000000000)
    msg = api.Message(attributes={'file': {'path': doc_file}, 'format': 'pandas'}, body=df)
    process(msg)

    out_file = '/Users/Shared/data/onlinemedia/data/text_sentiment.csv'
    df_list = [d.body for d in api.queue]
    pd.concat(df_list).to_csv(out_file,index=False)

if __name__ == '__main__':
    #test_operator()
    if True:
        subprocess.run(["rm",'-r','/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name+'_'+api.config.version
        subprocess.run(["vctl", "solution", "bundle", '/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_' + api.config.version,\
                                  "-t", solution_name])
        subprocess.run(["mv", solution_name+'.zip', '../../../solution/operators'])


