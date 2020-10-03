
import csv

import os
import pandas as pd
from datetime import date
from sklearn.decomposition import LatentDirichletAllocation, TruncatedSVD
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

#d.set_option('max_colwidth', 50)
pd.set_option('expand_frame_repr', True)
pd.set_option('display.max_columns', 100)


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
                fname = '/Users/Shared/data/onlinemedia/repository/topic_lda.csv'
                with open(fname, 'w') as f:
                    writer = csv.writer(f)
                    cols = [c['name'] for c in msg.attributes['table']['columns']]
                    writer.writerow(cols)
                    print(msg.body)
                    writer.writerows(msg.body)
            else:
                pass
                #print(msg)

        def set_config(config):
            api.config = config

        def set_port_callback(port, callback):
            pass

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': '', 'sklearn':'', 'pandas':''}
            version = "0.0.1"
            operator_description = "Topic Identification"
            operator_description_long = "Runs through all article words and identifies topics."
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            num_topics = 20
            config_params['num_topics'] = {'title': 'Number of Topics',
                                           'description': 'Number of topics searched in provided articles.',
                                           'type': 'number'}

            language_filter = "None"
            config_params['language_filter'] = {'title': 'Language Filter',
                                           'description': 'List of languages that should be processed',
                                           'type': 'string'}

            topic_num_words = 10
            config_params['topic_num_words'] = {'title': 'Number of words per topic',
                                           'description': 'Number of words per topic',
                                           'type': 'integer'}

            word_type_filter = "P"
            config_params['word_type_filter'] = {'title': 'Word type filter',
                                           'description': 'Filter based on word types',
                                           'type': 'string'}



def process(db_msg):

    logger, log_stream = slog.set_logging('topic_identification', loglevel=api.config.debug_mode)
    logger.info("Process started")
    time_monitor = tp.progress()

    columns = [c['name'] for c in db_msg.attributes['table']['columns']]
    df = pd.DataFrame(db_msg.body, columns=columns)

    # Language filter
    language_filter = tfp.read_list(api.config.language_filter)
    if language_filter :
        df = df.loc[df["LANGUAGE"].isin(language_filter)]
    else :
        language_filter = list(df['LANGUAGE'].unique())
    logger.info('Languages : {}'.format(language_filter))

    # Word type filter
    word_type_filter = tfp.read_value(api.config.word_type_filter)
    if word_type_filter:
        types = [ c for c in word_type_filter]
        df = df.loc[df["TYPE"].isin(types)]
    logger.info('Word restricted to types : {}'.format(word_type_filter))

    # groupby and concatenate words
    gdf = df.groupby(by =['HASH_TEXT','LANGUAGE'])['WORD'].apply(lambda x: ' '.join(x)).reset_index()

    logger.info('Topic identification: ')
    for lang in language_filter :
        logger.info('Language: {}  #Documents: {}  #Words: {}'.format(lang,gdf.loc[gdf['LANGUAGE']==lang].shape[0],\
                                                                      df.loc[df['LANGUAGE'] == lang].shape[0]))

    api.send(outports[0]['name'],log_stream.getvalue())
    log_stream.seek(0)

    # create document-term matrix - no tokenization or text prep are needed
    tf_vectorizer = CountVectorizer(analyzer='word', min_df=1, lowercase=False,tokenizer=str.split)

    # tf means term-frequency in a document for each language
    date_today = str(date.today())

    # 2-array with TOPIC, LANGUAGE, TYPE, DATE, EXPIRY_DATE, ATTRIBUTE, KEYWORD_i (num of topics)
    topic_list = list()
    for lang in language_filter :
        logger.info('Process all texts for language: {}'.format(lang))
        lang_gdf = gdf.loc[gdf['LANGUAGE']==lang]
        dtm_tf = tf_vectorizer.fit_transform(lang_gdf['WORD'])
        # for tf dtm
        lda_tf = LatentDirichletAllocation(n_components=api.config.num_topics, learning_method='online', evaluate_every=-1, n_jobs=-1)
        lda_tf.fit(dtm_tf)
        feature_names = tf_vectorizer.get_feature_names()

        for i, topic in enumerate(lda_tf.components_):
            topic_words = [feature_names[f] for f in topic.argsort()[:-api.config.topic_num_words-1:-1]]
            logger.debug('Len: {}  topic_words:{}'.format(len(topic_words),topic_words))
            row = [date_today+"-"+ str(i), lang, 'ALGO', date_today, None, None] + topic_words
            topic_list.append(row)

    attributes = {
        "table": {"columns": [{"class": "string", "name": "TOPIC", "nullable": False, "size": 80, "type": {"hana": "NVARCHAR"}},
                              {"class": "string", "name": "LANGUAGE", "nullable": False, "size": 2,"type": {"hana": "NVARCHAR"}},
                              {"class": "string", "name": "TYPE", "nullable": False, "size": 10,"type": {"hana": "NVARCHAR"}},
                              {"class": "string", "name": "DATE", "nullable": True, "type": {"hana": "DATE"}},
                              {"class": "string", "name": "EXPIRY_DATE", "nullable": True, "type": {"hana": "DATE"}},
                              {"class": "string", "name": "ATTRIBUTE", "nullable": True, "size": 25,"type": {"hana": "NVACHAR"}}],
                  "name": "DIPROJECTS.WORD_INDEX", "version": 1}}
    for i in range(1,api.config.topic_num_words+1) :
        attributes['table']['columns'].append({"class": "string", "name": "KEYWORD_"+str(i), "nullable": True, "size": 80, "type": {"hana": "NVARCHAR"}})

    msg = api.Message(attributes = attributes, body = topic_list)
    logger.info('Process ended, topics processed {}'.format(time_monitor.elapsed_time()))
    logger.debug("Attributes: \n{}".format(attributes))
    logger.debug('Topics: ')
    for t in topic_list :
        logger.debug(t)
    api.send(outports[0]['name'], log_stream.getvalue())
    api.send(outports[1]['name'], msg)


outports = [{'name': 'log', 'type': 'string', "description": "Logging data"},
            {'name': 'sql', 'type': 'message.table', "description": "message with body is sql and attributes contains the topic"}]
inports = [{'name': 'words', 'type': 'message.table', "description": "Message with body as table."}]


#api.set_port_callback(inports[0]['name'], process)


def test_operator():
    config = api.config
    config.language_filter = "DE, FR"
    config.num_topics = 30
    config.debug_mode = True
    config.topic_words = 10

    word_filename = '/Users/Shared/data/onlinemedia/repository/word_index.csv'
    articles = list()
    with open(word_filename, mode='r') as csv_file:
        rows = csv.reader(csv_file, delimiter=',')
        next(rows,None)
        for r in rows:
            # hash_text - language - type - words -  count
            articles.append(r)

    attributes = {"table":{"columns":[{"class":"string","name":"HASH_TEXT","nullable":True,"type":{"hana":"INTEGER"}},
                                      {"class": "string", "name": "LANGUAGE", "nullable": True, "size": 2,
                                       "type": {"hana": "NVARCHAR"}},
                                      {"class": "string", "name": "TYPE", "nullable": True, "size": 1,
                                       "type": {"hana": "NVARCHAR"}},
                                      {"class":"string","name":"WORD","nullable":True,"size":80,"type":{"hana":"NVARCHAR"}},
                                      {"class":"string","name":"COUNT","nullable":True,"type":{"hana":"INTEGER"}}],
                           "name":"DIPROJECTS.WORD_INDEX2","version":1}}

    print('Number articles : {}'.format(len(articles)))

    word_msg = api.Message(attributes=attributes, body=articles)
    process(word_msg)

if __name__ == '__main__':
    test_operator()
    #gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)





