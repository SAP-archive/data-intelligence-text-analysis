import json
import os
import csv
import pandas as pd
import numpy as np
import re
import pickle
import collections
import subprocess

import spacy

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

# global variable settings


supported_languages = ['DE', 'EN', 'ES', 'FR']
lexicon_languages = {lang: False for lang in supported_languages}
supported_word_types = ['PROPN','NOUN']

# ensure the models are downloaded in the first place by 'python -m spacy download <language_model>'
language_models = {'EN': 'en_core_web_sm', 'DE': 'de_core_news_sm', 'FR': 'fr_core_news_sm', 'ES': 'es_core_news_sm',
                   'IT': 'it_core_news_sm', 'LT': 'lt_core_news_sm', 'NB': 'nb_core_news_sm', 'nl': 'nl_core_news_sm',
                   'PT': 'pt_core_news_sm'}


try:
    api
except NameError:
    class api:

        queue_data = list()
        queue_sentiments = list()

        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if port == outports[2]['name']:
                api.queue_data.append(msg)
            elif port == outports[1]['name']:
                api.queue_sentiments.append(msg)
            else:
                # print('{}: {}'.format(port, msg))
                pass

        def set_config(config):
            api.config = config

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': '', 'spacy': ''}
            version = "0.1.0"
            operator_name = "text_words"
            operator_description = "Text-Words"
            operator_description_long = "Extracts words from text for further analysis."
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            language = 'None'
            config_params['language'] = {'title': 'Language', 'description': 'Filter for language of media.',
                                         'type': 'string'}

            types = 'PROPN, NOUN'
            config_params['types'] = {'title': 'Types',
                                     'description': 'Define the kind of data extraction.', 'type': 'string'}

            entity_types = 'ORG, PER, LOC'
            config_params['entity_types'] = {'title': 'Entity types',
                                     'description': 'List of entity types', 'type': 'string'}

            max_word_len = 80
            config_params['max_word_len'] = {'title': 'Maximum word lenght', 'description': 'Maximum word length.',
                                             'type': 'integer'}

            min_word_len = 3
            config_params['min_word_len'] = {'title': 'Minimum word length', 'description': 'Minimum word length.',
                                             'type': 'integer'}

            sentiments = False
            config_params['sentiments'] = {'title': 'Do sentiment analysis using word list',
                                           'description': 'Do sentiment analysis using word list.',
                                           'type': 'boolean'}

# global variables
id_set = set()
list_df = pd.DataFrame()
att_dict = dict()
sentiment_words_df = pd.DataFrame()
operator_name = 'text_words'


def setup_sentiment_list(msg):
    global sentiment_words_df
    logger, log_stream = slog.set_logging(operator_name, api.config.debug_mode)
    logger.info('Set sentiment_list')
    logger.debug('Attributes: {}'.format(msg.attributes))
    sentiment_words_df = msg.body
    logger.debug('Sentiment list size: {}'.format(sentiment_words_df.shape[0]))
    api.send(outports[0]['name'], log_stream.getvalue())
    process(None)


def process(msg):
    global id_set
    global list_df
    global sentiment_words_df
    global att_dict

    logger, log_stream = slog.set_logging(operator_name, api.config.debug_mode)
    logger.info("Main Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()

    if msg:
        att_dict = msg.attributes

    # sync data and setup msg
    # Case: do sentiment AND sentiment_setup not done/msg with data
    if api.config.sentiments and sentiment_words_df.empty:
        logger.info('Sentiment word list not setup yet!')
        api.send(outports[0]['name'], log_stream.getvalue())
        att_dict = msg.attributes
        if list_df.empty:
            list_df = msg.body
        else:
            list_df = pd.concat(list_df, msg.body)
        return 0
    # Case: Sentiment setup called process and data has been sent previously
    elif msg == None and not list_df.empty:
        df = list_df
    # Case: Sentiment setup called process and data has NOT been sent previously
    elif msg == None and list_df.empty:
        return 0
    # Case:  data sent and no sentiment analysis is required
    else:
        df = msg.body

    logger.debug('Attributes: {}'.format(att_dict))
    logger.debug('DataFrame: {} - {}'.format(df.shape[0], df.shape[1]))

    if df.shape[0] == 0:
        logger.warning('Empty DataFrame')
        return 0

        # Remove ID that has been processed
    # Should already been filtered out by 'Doc Prepare'
    prev_num_rows = df.shape[0]
    df = df.loc[~df['text_id'].isin(id_set)]
    post_num_rows = df.shape[0]
    if prev_num_rows != post_num_rows:
        logger.warning('Processed text_id has been found:  {} -> {}'.format(prev_num_rows, post_num_rows))
    id_set.update(df['text_id'].unique().tolist())

    # Languages
    language_filter = tfp.read_value(api.config.language)
    logger.info('Language filter: {}'.format(language_filter))
    if not language_filter:
        language_filter = df['language'].unique().tolist()
    language_filter = [lang for lang in language_filter if lang in language_models.keys()]
    nlp = dict()
    for lc in language_filter:
        nlp[lc] = spacy.load(language_models[lc])
    df = df.loc[df['language'].isin(language_filter)]

    # Warning for languages not supported
    languages_not_supported = [lang for lang in language_filter if not lang in language_models.keys()]
    if languages_not_supported:
        logger.warning(('The text of following langauges not analysed due to unsupported language: {}'.format(
            languages_not_supported)))

    # word types
    types = tfp.read_list(api.config.types)
    logger.info('Word types to be extracted: {}'.format(types))

    entity_types = tfp.read_list(api.config.entity_types)
    logger.info('Entity types to be extracted: {}'.format(entity_types))

    # Create doc for all
    word_bag_list = list()
    sentiment_list = list()

    def get_words(id, language, text):
        if not isinstance(text, str):
            logger.warning(('Record with error - ID: {} - {}'.format(id, text)))
            return -1
        doc = nlp[language](text)
        words = list()
        for t in types:
            words.extend(
                [[id, language, t, token.lemma_[:api.config.max_word_len]] for token in doc if token.pos_ == t])
        for et in entity_types:
            words.extend(
                [[id, language, et, ent.text[:api.config.max_word_len]] for ent in doc.ents if ent.label_ == et])
        # sentiment
        if language == 'DE' and api.config.sentiments:
            # collect all the lemmatized words of text
            # get all rows from sentiment_words_df and calculate mean
            lemmatized = [token.lemma_.lower() for token in doc if token.pos_ in ['NOUN', 'VERB', 'ADJ']]
            # logger.debug('lemmatized words: {}'.format(lemmatized[0:10]))
            text_sentiment_words = sentiment_words_df.loc[sentiment_words_df.index.isin(lemmatized), 'value']
            text_sentiment_value = text_sentiment_words.mean()
            sentiment_list.append([id, text_sentiment_value])
            # logger.debug('id: {}  #sentiment words: {}   sentitment:{}'.format(id,text_sentiment_words.shape[0],text_sentiment_value))

        word_bag_list.append(pd.DataFrame(words, columns=['text_id', 'language', 'type', 'word']))

    df.apply(lambda x: get_words(x['text_id'], x['language'], x['text']), axis=1)

    # data message
    try:
        word_bag = pd.concat(word_bag_list)
    except ValueError as ex:
        logger.error('No words in message: {}'.format(att_dict))
        api.send(outports[0]['name'], log_stream.getvalue())
        log_stream.seek(0)
        log_stream.truncate()
        return 0

    word_bag = word_bag.loc[word_bag['word'].str.len() >= api.config.min_word_len]
    word_bag['count'] = 1
    word_bag = word_bag.groupby(['text_id', 'language', 'type', 'word'])['count'].sum().reset_index()

    # check for duplicates - should be unnecessary
    prev_num_rows = word_bag.shape[0]
    word_bag.drop_duplicates(ignore_index=True, inplace=True)
    post_num_rows = word_bag.shape[0]
    if not prev_num_rows == post_num_rows:
        logger.warning('Duplicates has been found:  {} -> {}'.format(prev_num_rows, post_num_rows))
        api.send(outports[0]['name'], log_stream.getvalue())
        log_stream.seek(0)
        log_stream.truncate()

    data_msg = api.Message(attributes=att_dict, body=word_bag)
    logger.info('Labels in document: {}'.format(word_bag['type'].unique().tolist()))
    logger.debug('DataFrame shape: {} - {}'.format(word_bag.shape[0], word_bag.shape[1]))
    api.send(outports[2]['name'], data_msg)

    # sentiment message
    if api.config.sentiments and len(sentiment_list) > 0:
        sentiment_df = pd.DataFrame(sentiment_list, columns=['text_id', 'sentiment'])
        sentiment_df.drop_duplicates(inplace=True)
        sentiment_csv = sentiment_df.to_csv(index=False)
        sentiment_msg = api.Message(attributes=att_dict, body=sentiment_csv)
        api.send(outports[1]['name'], sentiment_msg)

    # log message
    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'sentimentlist', 'type': 'message.DataFrame', "description": "Sentiment list"},
           {'name': 'docs', 'type': 'message.DataFrame', "description": "Message with body as dictionary."}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'sentiments', 'type': 'message', "description": "csv with sentiments"},\
            {'name': 'data', 'type': 'message.DataFrame', "description": "Table with word index"}]

#api.set_port_callback(inports[0]['name'], setup_sentiment_list)
#api.set_port_callback(inports[1]['name'], process)



def test_operator():

    config = api.config
    config.debug_mode = True
    config.types = 'PROPN'
    config.entity_types = 'PER, ORG, LOC'
    config.language = 'None'
    config.max_word_len = 80
    config.min_word_len = 3
    config.batch_size = 10
    config.sentiments = True
    api.set_config(config)


    doc_file = '/Users/Shared/data/onlinemedia/data/doc_data_cleansed.csv'
    df = pd.read_csv(doc_file,sep=',',nrows=1000000000)
    msg = api.Message(attributes={'file': {'path': doc_file},'format':'pandas'}, body=df)
    process(msg)

    df_s = pd.read_csv('../../../data_repository/sentiments_DE.csv', sep=',',index_col = 'word' )
    msg = api.Message(attributes={'file': {'path': doc_file},'format':'pandas'}, body=df_s)
    setup_sentiment_list(msg)

    # saving sentiment csv
    out_file = '/Users/Shared/data/onlinemedia/data/text_sentiment_words.csv'
    str_list = [d.body for d in api.queue_sentiments]
    csv = '\n'.join(str_list)
    csv_file = open(out_file,'w')
    csv_file.write(csv)
    csv_file.close()

    out_file = '/Users/Shared/data/onlinemedia/data/word_index.csv'
    df_list = [d.body for d in api.queue_data]
    pd.concat(df_list).to_csv(out_file,index=False)

if __name__ == '__main__':
    #test_operator()

    if True :
        subprocess.run(["rm",'-r','/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name+'_'+api.config.version
        subprocess.run(["vctl", "solution", "bundle", '/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_'+ api.config.version,\
                                  "-t", solution_name])
        subprocess.run(["mv", solution_name+'.zip', '../../../solution/operators'])

