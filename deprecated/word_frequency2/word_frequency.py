import io
import json
import os
import csv
import re
from datetime import datetime, timedelta
from collections import Counter
import nltk

import spacy
nlp_g = spacy.load('de_core_news_sm')
nlp_fr = spacy.load('fr_core_news_sm')

nlp_es = spacy.load('es_core_news_sm')

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

language_dict = {'Lefigaro': 'F', 'Lemonde': 'F', 'Spiegel': 'G', 'FAZ': 'G', 'Sueddeutsche': 'G', 'Elpais': 'S',
            'Elmundo': 'S'}

try:
    api
except NameError:
    class api:
        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if port == outports[2]['name']:
                new_filename = 'word_count.json'
                with open(os.path.join('/Users/Shared/data/onlinemedia/prep_texts', new_filename), mode='w') as f:
                    f.write(json.dumps(msg.body, ensure_ascii=False,indent=4))
            else:
                #print('{}: {}'.format(port, msg))
                pass

        def set_config(config):
            api.config = config

        def set_port_callback(port, callback):
            pass

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': '', 'spacy': ''}
            version = "0.0.1"
            operator_description = "Word Frequency"
            operator_description_long = "Find specified words for a certain time period in articles."
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            collect = True
            config_params['collect'] = {'title': 'Collect input',
                                           'description': 'Collect input until last batch received',
                                           'type': 'boolean'}

            limit = 100
            config_params['limit'] = {'title': 'Limit the number of words. Necessary if stored in database.',
                                                    'description': 'Number of words to be extracted.',
                                                    'type': 'integer'}
            date = '2020-02-24'
            config_params['date'] = {'title': 'Date','description': 'Date of indexing the words.','type': 'string'}

            days_into_past = '7'
            config_params['days_into_past'] = {'title': 'Days into past','description': 'Days into past.','type': 'integer'}

            language = 'None'
            config_params['language'] = {'title': 'Language','description': 'Filter for language of media.','type': 'string'}

            media = 'None'
            config_params['media'] = {'title': 'Media','description': 'Filter for media','type': 'string'}

            mode = 'NOUN'
            config_params['mode'] = {'title': 'Mode','description': 'Define the kind of data extraction. \"NOUN\": nouns,'
                                        '\"PNOUN\": proper nouns. Default is removing stopwords.','type': 'string'}

            max_word_len = 80
            config_params['max_word_len'] = {'title': 'Maximum word lenght','description': 'Maximum word lenght.','type': 'integer'}

            json_string_output = False
            config_params['json_string_output'] = {'title': 'JSON string output',
                                           'description': 'Sending JSON string to json output',
                                           'type': 'boolean'}


blacklist = list()
last_msg = None
word_counter = Counter()
id_set = dict()

def setup(msg):
    global blacklist
    setup_list = msg.body
    process(None)

# Checks if corrections has been set
def check_for_setup(logger, msg):
    global blacklist, last_msg

    # Case: Keywords has been set before
    if msg == None:
        logger.debug('Setup data received!')
        if last_msg == None:
            logger.info('Prerequisite message has been set, but waiting for data')
            return None
        else:
            logger.info("Last msg list has been retrieved")
            return last_msg
    else:
        logger.debug('Processing data received!')
        if last_msg == None:
            last_msg = msg
        else:
            last_msg.attributes = msg.attributes
            last_msg.body.extend(msg.body)
        if len(setup_list) == 0:
            logger.info('No setup message - msg has been saved')
            return None
        else:
            logger.info('Setup is been set. Saved msg retrieved.')
            msg = last_msg
            last_msg = None
            return msg

def test_last_batch (attributes, collect = False) :
    if not collect :
        progress_str = '<BATCH ENDED><1>'
        return True, progress_str
    elif ('batch.number' in attributes and 'batch.index' in attributes) or \
        ('storage.fileCount' in attributes and 'storage.fileIndex' in attributes) :
        # just in case the batch attributes are not set
        if not 'batch.number' in attributes or not 'batch.index' in attributes :
            attributes['batch.index'] = attributes['storage.fileIndex']
            attributes['batch.number'] = attributes['storage.fileCount']
        if attributes['batch.index'] + 1 == attributes['batch.number'] :
            progress_str = '<BATCH ENDED><{}>'.format(attributes['batch.number'])
            return True, progress_str
        else:
            progress_str = '<BATCH IN-PROCESS><{}/{}>'.format(attributes['batch.index'] + 1,
                                                              attributes['batch.number'])
            return False, progress_str
    else :
        raise ValueError('For collecting data batch.index or storage.fileIndex is necessary in Message attributes.')

def process(msg):
    global blacklist
    global last_msg
    global word_counter
    global id_set

    logger, log_stream = slog.set_logging('word_frequency', loglevel=api.config.debug_mode)
    logger.info("Process started")
    time_monitor = tp.progress()

    msg = check_for_setup(logger, msg)
    if not msg:
        api.send(outports[0]['name'], log_stream.flush())
        return 0

    adict = msg.body
    att_dict = msg.attributes

    end_date = datetime.strptime(api.config.date,'%Y-%m-%d')
    start_date = end_date - timedelta(days=api.config.days_into_past)

    language_filter = tfp.read_value(api.config.language)
    media_filter = tfp.read_value(api.config.media)

    for index_article, article in enumerate(adict):

        # filter article
        adate = datetime.strptime(article['date'],'%Y-%m-%d')
        if not start_date <= adate <= end_date :
            #logger.debug('Date of article out of range: {} ({} - {})'.format(adate,start_date,end_date))
            continue

        # filter language
        if language_filter and not language_filter == language_dict[article['media']] :
            #logger.debug('Language filtered out: {} ({})'.format(language_dict[article['media']], language_filter))
            continue

        # filter media
        if media_filter and not media_filter == article['media'] :
            #logger.debug('Media filtered out: {} ({})'.format(article['media'], media_filter))
            continue

        # check if article has been processed
        if article['hash_text'] in hash_list :
            logger.debug('Article has already been processed: {} - {} - {}'.format(article['date'],article['media'],article['hash_text']))
            word_counter.update(hash_list[article['hash_text']])
            continue

        language = language_dict[article['media']]
        text = article['text']

        # Language settings
        if language == 'G':
            doc = nlp_g(text)
        elif language == 'F':
            doc = nlp_fr(text)
        elif language == 'S':
            doc = nlp_es(text)
        else :
            logger.warning('Language not implmented')
            doc = None
            words = []

        # only when doc has been created - language exists
        if doc :
            if api.config.mode == 'NOUN' :
                words = [token.lemma_[:api.config.max_word_len] for token in doc if token.pos_  in ['PROPN', 'NOUN'] ]
            elif api.config.mode == 'PROPN' :
                words = [token.lemma_[:api.config.max_word_len] for token in doc if token.pos_ == 'PROPN']
            else :
                words = [token.text[:api.config.max_word_len] for token in doc if not token.is_stop]

            word_counter.update(words)

        hash_list[article['hash_text']] = words

    if api.config.limit > 0 :
        common_words = word_counter.most_common(api.config.limit)

    result, progress_str = test_last_batch(attributes=att_dict, collect=api.config.collect)
    if result:
        word_freq = [ {'date':api.config.date,'days_into_past':api.config.days_into_past, 'language':api.config.language, \
                       'media':api.config.media,'mode':api.config.mode, 'word':w, 'frequency': f} for w,f in common_words ]
        msg = api.Message(attributes=att_dict,body=word_freq)
        api.send(outports[2]['name'], msg)

        if api.config.json_string_output :
            json_data = json.dumps(word_freq)
            api.send(outports[1]['name'], msg)

    logger.debug('Process ended,  {}  - {}  '.format(progress_str, time_monitor.elapsed_time()))
    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'blacklist', 'type': 'message.list', "description": "Message with body as dictionary."},\
            {'name': 'articles', 'type': 'message.dicts', "description": "Message with body as dictionary."}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"},\
            {'name': 'json', 'type': 'string', "description": "JSON data"},\
            {'name': 'data', 'type': 'message.dict', "description": "Output List of index words"}]

#api.set_port_callback(inports[0]['name'], setup)
#api.set_port_callback(inports[1]['name'], process)

def main():
    config = api.config
    config.debug_mode = True
    config.limit = 50
    config.mode = 'PROPN'
    config.collect = True
    config.days_into_past = 5
    config.date = '2020-02-17'
    config.language = 'G'
    config.media = 'Spiegel'
    config.json_string_output = True
    api.set_config(config)

    bl_filename = '/Users/Shared/data/onlinemedia/repository/blacklist.txt'
    blacklist = list()
    with open(bl_filename,mode='r') as csv_file:
        rows = csv.reader(csv_file,delimiter=',')
        for r in rows :
            blacklist.append(r[0])
        #print(csv_file.read())
    kw_msg = api.Message(attributes={'filename': bl_filename}, body=blacklist)
    setup(kw_msg)

    in_dir = '/Users/Shared/data/onlinemedia/crawled_texts/'
    files_in_dir = [f for f in os.listdir(in_dir) if os.path.isfile(os.path.join(in_dir, f)) and re.match('.*json',f)]

    for i, fname in enumerate(files_in_dir):

        fbase = fname.split('.')[0]
        eos = True if len(files_in_dir) == i + 1 else False
        attributes = {'format': 'csv', "storage.filename": fbase, 'storage.endOfSequence': eos, \
                      'storage.fileIndex': i, 'storage.fileCount': len(files_in_dir),'process_list':[]}
        with open(os.path.join(in_dir,fname)) as json_file:
            try :
                data = json.load(json_file)
            except UnicodeDecodeError as e :
                print('Error reading {} ({})'.format(fbase,e))

        print('Read file: {} ({}/{}'.format(fbase,attributes['storage.fileIndex'],attributes['storage.fileCount']))
        msg_data = api.Message(attributes=attributes, body=data)

        process(msg_data)


if __name__ == '__main__':
    main()
    # gs.gensolution(os.path.realpath(__file__), config, inports, outports)





