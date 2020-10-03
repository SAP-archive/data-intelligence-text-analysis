import io
import json
import os
import re
import csv
from collections import Counter

import spacy
nlp_g = spacy.load('de_core_news_sm')
nlp_fr = spacy.load('fr_core_news_sm')
nlp_es = spacy.load('es_core_news_sm')

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

language_dict = {'Lefigaro': 'FR', 'Lemonde': 'FR', 'Spiegel': 'DE', 'FAZ': 'DE', 'Sueddeutsche': 'DE', 'Elpais': 'ES',
            'Elmundo': 'ES'}

try:
    api
except NameError:
    class api:
        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes
                
        def send(port,msg) :
            if port == outports[1]['name']  :
                new_filename = os.path.basename(msg.attributes['storage.filename']).split('.')[0] + '-kw.json'
                with open(os.path.join('/Users/Shared/data/onlinemedia/prep_texts', new_filename), mode='w') as f:
                    f.write(json.dumps(msg.body, ensure_ascii=False,indent=4))

        def set_config(config):
            api.config = config
            
        def set_port_callback(port, callback) :
            pass
    
        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils':''}
            version = "0.0.1"
            operator_description = "Search Keywords"
            operator_description_long = "Search Keywords"
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            mode = 'PROPN'
            config_params['mode'] = {'title': 'Mode','description': 'Define the kind of data extraction. \"NOUN\": nouns,'
                                        '\"PNOUN\": proper nouns. Default is removing stopwords.','type': 'string'}

blacklist = dict()
last_msg = None
hash_text_list = list()

# if keyword setting is called the find_word()
# hard-coded DE Language
def setup(msg):
    global blacklist
    kw, langlist = zip(*msg.body)
    setup_data = { l:list() for l in  set(langlist)}
    for kl in msg.body :
        setup_data[kl[1]].append(kl[0])
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
        if len(setup_data) == 0:
            logger.info('No setup message - msg has been saved')
            return None
        else:
            logger.info('Setup is been set. Saved msg retrieved.')
            msg = last_msg
            last_msg = None
            return msg

# get words from articles
def get_words(logger, text,language,mode) :
        # Language settings
        if language == 'DE':
            doc = nlp_g(text)
        elif language == 'FR':
            doc = nlp_fr(text)
        elif language == 'ES':
            doc = nlp_es(text)
        else :
            logger.warning('Language not implemented')
            doc = None
            words = []

        # only when doc has been created - language exists
        if doc :
            if mode == 'NOUN' :
                words = [token.lemma_ for token in doc if token.pos_  in ['PROPN', 'NOUN'] ]
            elif mode == 'PROPN' :
                words = [token.lemma_ for token in doc if token.pos_ == 'PROPN']
            else :
                words = [token.text for token in doc if not token.is_stop]

        return words


def find_word(word):
    return re.compile(r'\b({0})\b'.format(word), flags=re.IGNORECASE).findall

def process(msg) :

    global blacklist
    global last_msg
    global hash_text_list

    operator_name = 'keyword_search'
    logger, log_stream = slog.set_logging(operator_name, loglevel=api.config.debug_mode)
    logger.info("Process started")
    time_monitor = tp.progress()

    # Case: Keywords has been set before
    msg = check_for_setup(logger, msg)
    if not msg:
        api.send(outports[0]['name'], log_stream.flush())
        return 0

    if api.config.debug_mode :
        api.send(outports[0]['name'], log_stream.getvalue())

    article_list = msg.body
    att_dict = msg.attributes
    att_dict['operator'] = operator_name
    kw_index = list()

    word_mode = tfp.read_value(api.config.mode)
    media_set = set()
    for article in article_list :
        media_set.add(article['media'])
        if article['hash_text'] in hash_text_list:
            continue
        hash_text_list.append(article['hash_text'])
        alanguage = language_dict[article['media']]
        if not alanguage in setup_data :
            continue
        words = get_words(logger,article['text'],language=alanguage,mode=word_mode)
        matched_words = [ w for w in words if w in setup_data[alanguage]]
        s_words = [w for w in words if len(w) > 4 and w[-1] == 's']
        s_matched_words = [w[:-1] for w in s_words if w in setup_data[alanguage]]
        matched_words.extend(s_matched_words)
        word_counter = Counter(matched_words)
        for word in word_counter :
            keyword_rec = {'hash_text':article['hash_text'],'keyword':word,'count':word_counter[word]}
            kw_index.append(keyword_rec)
            #logger.debug('Keyword: {}'.format(str(keyword_rec)))
    logger.debug('Process ended, searched media: {} - keywords found {}  - {}'.format(
        str(media_set), len(kw_index), time_monitor.elapsed_time()))

    api.send(outports[0]['name'],log_stream.getvalue())
    api.send(outports[1]['name'], api.Message(attributes=att_dict, body=kw_index))

inports = [{'name': 'keywords', 'type': 'message.table',"description":"Message with keywords to search "},\
           {'name': 'articles', 'type': 'message.dicts',"description":"Message with body as dictionary "}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"}, \
            {'name': 'data', 'type': 'message',"description":"Output List of index words"}]


#api.set_port_callback(inports[0]['name'], setup)
#api.set_port_callback(inports[1]['name'], process)

def main() :
    config = api.config
    config.debug_mode = True
    api.set_config(config)

    kw_filename = '/Users/Shared/data/onlinemedia/repository/keywords.csv'
    languages = list()
    keywords = list()
    with open(kw_filename,mode='r') as csv_file:
        rows = csv.reader(csv_file,delimiter=',')
        next(rows, None)
        for r in rows :
            keywords.append(r[0])
            languages.append(r[1])
    kandl = list(zip(keywords,languages))  # table structure
    kw_msg = api.Message(attributes={'filename': kw_filename}, body=kandl)
    setup(kw_msg)

    in_dir = '/Users/Shared/data/onlinemedia/crawled_texts/'
    files_in_dir = [f for f in os.listdir(in_dir) if os.path.isfile(os.path.join(in_dir, f)) and re.match('Spiegel.*', f)]
    for i, fname in enumerate(files_in_dir):
        fbase = fname.split('.')[0]
        eos = True if len(files_in_dir) == i + 1 else False
        attributes = {'format': 'csv', "storage.filename": fbase, 'storage.endOfSequence': eos, \
                      'storage.fileIndex': i, 'storage.fileCount': len(files_in_dir),'process_list':[]}
        with open(os.path.join(in_dir,fname)) as json_file:
            data = json.load(json_file)
        msg_data = api.Message(attributes=attributes, body=data)
        process(msg_data)


def key_test() :
    data_collector = list()

    def on_input(data):
        global data_collector

        pk = [str(r['hash_text']) + ' ' + r['keyword'] for r in data.body]

        match = [k for k in pk if k in data_collector]
        if len(match) > 0:
            api.send("log", 'Key violation: {}'.format(match))

        data_collector.extend(pk)

        api.send("data", data)

    api.set_port_callback("data", on_input)

if __name__ == '__main__':
    main()
    #gs.gensolution(os.path.realpath(__file__), config, inports, outports)

