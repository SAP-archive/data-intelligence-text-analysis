import json
import os
import csv
import pandas as pd
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

        queue = list()

        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if port == outports[1]['name']:
                api.queue.append(msg)
            else:
                # print('{}: {}'.format(port, msg))
                pass

        def set_config(config):
            api.config = config

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': '', 'spacy': ''}
            version = "0.0.18"
            operator_name = "text_words"
            operator_description = "Words from Text"
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


# global variables
id_set = set()

def process(msg):
    global id_set

    operator_name = 'text_words'
    logger, log_stream = slog.set_logging(operator_name, api.config.debug_mode)

    logger.info("Main Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()

    df = msg.body
    att_dict = msg.attributes

    # Remove ID that has been processed
    df = df.loc[~df['ID'].isin(id_set)]
    id_set.update(df['ID'].unique().tolist())

    # Languages
    language_filter = tfp.read_value(api.config.language)
    logger.info('Language filter: {}'.format(language_filter))
    if not language_filter :
        language_filter = df['LANGUAGE'].unique().tolist()
    language_filter = [ lang for lang in language_filter if lang in language_models.keys()]
    nlp = dict()
    for lc in language_filter :
        nlp[lc] = spacy.load(language_models[lc])
    df = df.loc[df['LANGUAGE'].isin(language_filter)]

    # Warning for languages not supported
    languages_not_supported =  [ lang for lang in language_filter if not lang in language_models.keys()]
    if languages_not_supported :
        logger.warning(('The text of following langauges not analysed due to unsupported language: {}'.format(languages_not_supported)))

    # word types
    types = tfp.read_list(api.config.types)
    logger.info('Word types to be extracted: {}'.format(types))

    entity_types = tfp.read_list(api.config.entity_types)
    logger.info('Entity types to be extracted: {}'.format(entity_types))

    # Create doc for all
    word_bag_list = list()

    def get_words(id, language, text) :
        if not isinstance(text,str) :
            logger.warning(('Record with error - ID: {} - {}'.format(id,text) ))
            return -1
        doc = nlp[language](text)
        words = list()
        for t in types:
            words.extend([[id,language,t,token.lemma_[:api.config.max_word_len]] for token in doc if token.pos_ == t])
        for et in entity_types:
            words.extend([[id,language,et,ent.text[:api.config.max_word_len]] for ent in doc.ents if ent.label_ == et])
        word_bag_list.append(pd.DataFrame(words,columns = ['ID','LANGUAGE','TYPE','WORD']))

    df.apply(lambda x : get_words(x['ID'],x['LANGUAGE'],x['TEXT']),axis=1)
    word_bag = pd.concat(word_bag_list)
    word_bag =  word_bag.loc[word_bag['WORD'].str.len() >= api.config.min_word_len ]
    word_bag['COUNT'] = 1
    word_bag = word_bag.groupby(['ID','LANGUAGE','TYPE','WORD'])['COUNT'].sum().reset_index()

    # test for duplicates
    dup_s = word_bag.duplicated(subset=['ID','LANGUAGE','TYPE','WORD']).value_counts()
    num_duplicates = dup_s[True] if True in dup_s  else 0
    logger.info('Duplicates: {} / {}'.format(num_duplicates, word_bag.shape[0]))

    att_dict['message.lastBatch'] = True
    table_msg = api.Message(attributes=att_dict, body=word_bag)

    logger.info('Labels in document: {}'.format(word_bag['TYPE'].unique().tolist()))
    api.send(outports[0]['name'], log_stream.getvalue())
    api.send(outports[1]['name'], table_msg)


inports = [{'name': 'docs', 'type': 'message.DataFrame', "description": "Message with body as dictionary."}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'data', 'type': 'message.DataFrame', "description": "Table with word index"}]


#api.set_port_callback(inports[0]['name'], process)


def test_operator():

    config = api.config
    config.debug_mode = True
    config.types = 'PROPN'
    config.entity_types = 'PER, ORG, LOC'
    config.language = 'None'
    config.max_word_len = 80
    config.min_word_len = 3
    config.batch_size = 10
    api.set_config(config)


    doc_file = '/Users/Shared/data/onlinemedia/data/doc_data_cleansed.csv'
    df = pd.read_csv(doc_file,sep=',',nrows=1000000000)
    msg = api.Message(attributes={'file': {'path': doc_file},'format':'pandas'}, body=df)
    process(msg)

    out_file = '/Users/Shared/data/onlinemedia/data/word_extraction.csv'
    df_list = [d.body for d in api.queue]
    pd.concat(df_list).to_csv(out_file,index=False)



if __name__ == '__main__':
    test_operator()

    if True :
        subprocess.run(["rm",'-r','/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name+'_'+api.config.version
        subprocess.run(["vctl", "solution", "bundle", '/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_0.0.18',\
                                  "-t", solution_name])
        subprocess.run(["mv", solution_name+'.zip', '../../../solution/operators'])

