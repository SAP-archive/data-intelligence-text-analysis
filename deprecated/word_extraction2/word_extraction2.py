import json
import os
import csv
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
media_languages = {'Lefigaro': 'FR', 'Lemonde': 'FR', 'Spiegel': 'DE', 'FAZ': 'DE', 'Sueddeutsche': 'DE',
                   'Elpais': 'ES',
                   'Elmundo': 'ES'}


supported_languages = ['DE', 'EN', 'ES', 'FR']
lexicon_languages = {lang: False for lang in supported_languages}
supported_word_types = ['PROPN','NOUN']

# ensure the models are downloaded in the first place by 'python -m spacy download <language_model>'
language_models = {'EN': 'en_core_web_sm', 'DE': 'de_core_news_sm', 'FR': 'fr_core_news_sm', 'ES': 'es_core_news_sm',
                   'IT': 'it_core_news_sm', 'LT': 'lt_core_news_sm', 'NB': 'nb_core_news_sm', 'nl': 'nl_core_news_sm',
                   'PT': 'pt_core_news_sm'}
nlp = dict()


def nlp_doc(logger, language_code, text):
    global nlp
    if language_code in language_models.keys():
        if not language_code in nlp.keys():
            nlp[language_code] = spacy.load(language_models[language_code])
            logger.info('Spacy language package loaded: {}'.format(language_models[language_code]))
    else:
        logger.warning('No language model available for: {}'.format(language_code))
        return None
    return nlp[language_code](text)


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
            operator_name = "word_extraction"
            operator_description = "Word Extraction2"
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
            config_params['mode'] = {'title': 'Mode',
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

#
hash_list = list()
all_labels = set()

def process(msg):
    global hash_list
    global all_labels

    operator_name = 'word_extraction2'
    logger, log_stream = slog.set_logging(operator_name, api.config.debug_mode)

    logger.info("Main Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()

    adict = msg.body
    att_dict = msg.attributes

    language_filter = tfp.read_value(api.config.language)
    logger.info('Language filter: {}'.format(language_filter))

    # word types
    types = tfp.read_list(api.config.types)
    logger.info('Word types to be extracted: {}'.format(types))

    entity_types = tfp.read_list(api.config.entity_types)
    logger.info('Entity types to be extracted: {}'.format(entity_types))

    if not types and not entity_types :
        logger.warning('Neither word types nor entity types defined.')

    article_words = list()
    article_count = 0
    for index_article, article in enumerate(adict):

        # filter language
        language = media_languages[article['media']]
        if language_filter and not language_filter == language:
            # logger.debug('Language filtered out: {} ({})'.format(language, language_filter))
            continue

        article_count += 1

        # check if article has been processed
        if article['hash_text'] in hash_list:
            logger.debug('Article has already been processed: {} - {} - {}'.format(article['date'], article['media'],                                                                   article['hash_text']))
            continue
        hash_list.append(article['hash_text'])

        doc = nlp_doc(logger, language, article['text'])

        words = dict()
        # only when doc has been created - language exists
        entities = dict()
        if doc:
            for t in types :
                words[t] = [ token.lemma_[:api.config.max_word_len] for token in doc if token.pos_ == t]
            for et in entity_types:
                words[et] = [ent.text[:api.config.max_word_len] for ent in doc.ents if ent.label_ == et]
            for ety in doc.ents :
                all_labels.add(ety.label_)

        for m in words:
            # minimum word length
            words[m] = [w for w in words[m] if len(w) >= api.config.min_word_len]
            article_words.extend([[article['hash_text'], language, m, cw[0],cw[1]] for cw in collections.Counter(words[m]).most_common()])



    att_dict['table'] =  {"columns": [{"class": "string", "name": "HASH_TEXT", "nullable": True, "type": {"hana": "BIGINT"}},
                                      {"class": "string", "name": "LANGUAGE", "nullable": True, "size": 2,"type": {"hana": "NVARCHAR"}},
                                      {"class": "string", "name": "TYPE", "nullable": True, "size": 15,"type": {"hana": "NVARCHAR"}},
                                      {"class": "string", "name": "WORD", "nullable": True, "size": 80,"type": {"hana": "NVARCHAR"}},
                                      {"class": "string", "name": "COUNT", "nullable": True, "type": {"hana": "INTEGER"}}],
                              "name": "DIPROJECTS.WORD_INDEX2", "version": 1}

    print(article_words)

    table_msg = api.Message(attributes=att_dict, body=article_words)
    logger.info('Labels in document: {}'.format(all_labels))
    api.send(outports[0]['name'], log_stream.getvalue())
    api.send(outports[1]['name'], table_msg)


inports = [{'name': 'articles', 'type': 'message.dicts', "description": "Message with body as dictionary."}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'data', 'type': 'message.table', "description": "Table with word index"}]

#api.set_port_callback(inports[0]['name'], setup_blacklist)
#api.set_port_callback(inports[1]['name'], setup_keywords)
#api.set_port_callback(inports[2]['name'], setup_lexicon)
#api.set_port_callback(inports[3]['name'], process)


def test_operator():
    global labels

    config = api.config
    config.debug_mode = True
    config.mode = 'PNOUN'
    config.entity_types = 'PER, ORG, LOC'
    config.language = 'None'
    config.max_word_len = 80
    config.min_word_len = 3

    api.set_config(config)

    in_dir = '/Users/Shared/data/onlinemedia/crawled_texts/'
    files_in_dir = [f for f in os.listdir(in_dir) if os.path.isfile(os.path.join(in_dir, f)) and re.match('Spieg.*', f)]

    for i, fname in enumerate(files_in_dir):
        if i > 1 :
            break
        fbase = fname.split('.')[0]
        eos = True if len(files_in_dir) == i + 1 else False
        attributes = {'format': 'csv', "storage.filename": fbase, 'storage.endOfSequence': eos, \
                      'storage.fileIndex': i, 'storage.fileCount': len(files_in_dir), 'process_list': []}
        with open(os.path.join(in_dir, fname)) as json_file:
            try:
                data = json.load(json_file)
            except UnicodeDecodeError as e:
                print('Error reading {} ({})'.format(fbase, e))

        print('Read file: {} ({}/{}'.format(fbase, attributes['storage.fileIndex'], attributes['storage.fileCount']))
        msg_data = api.Message(attributes=attributes, body=data)

        process(msg_data)

    print("All labels found: {}".format(all_labels))

    # saving outcome as word index
    with open('/Users/Shared/data/onlinemedia/data/word_extraction.csv', 'w') as f:
        writer = csv.writer(f)
        cols = ['HASH_TEXT','LANGUAGE','TYPE','WORD', 'COUNT']
        writer.writerow(cols)
        for msg in api.queue :
            #print(msg.body)
            for rec in msg.body:
                writer.writerow(rec)

if __name__ == '__main__':
    test_operator()

    if False :
        subprocess.run(["rm",'-r','/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name+'_'+api.config.version
        subprocess.run(["vctl", "solution", "bundle", '/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_0.0.18',\
                                  "-t", solution_name])
        subprocess.run(["mv", solution_name+'.zip', '../../../solution/operators'])


