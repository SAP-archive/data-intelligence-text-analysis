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


supported_languages = ['DE', 'EN', 'ES', 'FR']
lexicon_languages = {lang: False for lang in supported_languages}

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
                # wi_fn = os.path.join('/Users/Shared/data/onlinemedia/repository',msg.attributes['storage.filename']+'-words.csv')
                # with open(wi_fn, 'w') as f:
                #    writer = csv.writer(f)
                #    cols = [c['name'] for c in msg.attributes['table']['columns']]
                #    writer.writerow(cols)
                #    writer.writerows(msg.body)
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
            operator_name = "word_index_transform"
            operator_description = "word index transformation"
            operator_description_long = "Transforms the index either in place or to new index."
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            language = 'None'
            config_params['language'] = {'title': 'Language', 'description': 'Filter for language of media.',
                                         'type': 'string'}

            type = 'P'
            config_params['type'] = {'title': 'Type',
                                     'description': 'Define the kind of data extraction. P=Proper Nouns, '
                                                    'N=Nouns, X: Removing only stopwords.', 'type': 'string'}

# global articles
blacklist = list()
keywords = list()
lexicon = None
lexicon_stem = None
last_msg = None
id_set = list()
operator_name = 'word_extraction'


def setup_blacklist(msg):
    logger, log_stream = slog.set_logging(operator_name, api.config.debug_mode)
    global blacklist
    logger.info('Set blacklist')
    logger.debug('Attributes: {}'.format(msg.attributes))
    logger.debug('Data: {}'.format(msg.body))
    blacklist = msg.body
    api.send(outports[0]['name'], log_stream.getvalue())
    process(None)


def setup_lexicon(msg):
    logger, log_stream = slog.set_logging(operator_name, api.config.debug_mode)
    global lexicon, lexicon_languages, lexicon_stem
    logger.info('Set lexicon')
    logger.debug('Attributes: {}'.format(msg.attributes))
    logger.debug('Data: {}'.format(msg.body))
    try:
        header = [c["name"] for c in msg.attributes['table']['columns']]
    except Exception as e:
        logger.error(e)
        api.send(outports[0]['name'], log_stream.getvalue())
        return None

    lexicon = {c: dict() for c in header[1:]}
    lexicon_stem = {c: dict() for c in header[1:]}
    for r in msg.body:
        for i, lang in enumerate(header[1:]):
            lang_words = r[i + 1].split()
            lw = list()
            lws = list()
            for w in lang_words :
                if w[-1] == '*' :
                    lws.append(w[:-1])
                else :
                    lw.append(w)
            if lw :
                lw_dict = dict.fromkeys(lw, r[0])
                lexicon[lang].update(lw_dict)
            if lws :
                lws_dict = dict.fromkeys(lws, r[0])
                lexicon_stem[lang].update(lws_dict)

    for lang in header[1:]:
        lexicon_languages[lang] = True
    api.send(outports[0]['name'], log_stream.getvalue())
    process(None)


# Checks for setup
def check_for_setup(logger, msg) :
    global blacklist, lexicon, last_msg

    use_blacklist = True
    use_lexicon = True

    logger.info("Check setup")
    # Case: setupdate, check if all has been set
    if msg == None:
        logger.debug('Setup data received!')
        if last_msg == None:
            logger.info('Prerequisite message has been set, but waiting for data')
            return None
        else:
            if len(blacklist) == 0 or lexicon == None   :
                logger.info("Setup not complete -  blacklist: {}   lexicon: {}".\
                            format(len(blacklist, len(lexicon))))
                return None
            else:
                logger.info("Last msg list has been retrieved")
                return last_msg
    else:
        logger.debug('Processing data received!')
        # saving of data msg
        if last_msg == None:
            last_msg = msg
        else:
            last_msg.attributes = msg.attributes
            last_msg.body.extend(msg.body)

        # check if data msg should be returned or none if setup is not been done
        if (len(blacklist) == 0  and use_blacklist == True) or \
            (lexicon == None and use_lexicon == True):
            len_lex = 0 if lexicon == None else len(lexicon)
            logger.info("Setup not complete -  blacklist: {}  lexicon: {}".\
                        format(len(blacklist), len_lex))
            return None
        else:
            logger.info('Setup is been set. Saved msg retrieved.')
            msg = last_msg
            last_msg = None
            return msg


def process(msg):
    global blacklist
    global last_msg
    global id_set

    logger, log_stream = slog.set_logging(operator_name, api.config.debug_mode)

    # Check if setup complete
    msg = check_for_setup(logger, msg)
    if not msg:
        api.send(outports[0]['name'], log_stream.flush())
        return 0

    logger.info("Main Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()

    att_dict = msg.attributes

    language = tfp.read_value(api.config.language)
    type = tfp.read_value(api.config.type)
    if len(type) > 1 :
        logger.warning('Only one type can be transformed. Take only first one: {}'.format(type[0]))
        type = type[0]

    # DELETE all new type rows
    sql = 'DELETE FROM "WORD_INDEX" WHERE  "TYPE" = \'Q\' '
    # COPY 'TYPE' to 'NEW TYPE'
    sql = 'SELECT * FROM "WORD_INDEX" WHERE "TYPE" = \'' + type + '\' '
    if language :
        sql += '"LANGUAGE" = \'' + language + '\' '


    # REMOVE ALL BLACKLIST

    # REPLACE LEXICON


    api.send(outports[0]['name'], log_stream.getvalue())



inports = [{'name': 'blacklist', 'type': 'message.list', "description": "Message with body as dictionary."},
           {'name': 'lexicon', 'type': 'message.table', "description": "Message with body as lexicon."}, \
           {'name': 'table', 'type': 'message.table', "description": "Message with body as table."}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'data', 'type': 'message', "description": "msg with sql-statement"}]

#api.set_port_callback(inports[0]['name'], setup_blacklist)
#api.set_port_callback(inports[1]['name'], setup_keywords)
#api.set_port_callback(inports[2]['name'], setup_lexicon)
#api.set_port_callback(inports[3]['name'], process)


def test_operator():
    config = api.config
    config.debug_mode = True
    config.type = 'P'
    config.new_type = 'P'
    config.language = 'None'
    api.set_config(config)


    # BLACKLIST
    bl_filename = '/Users/Shared/data/onlinemedia/repository/blacklist_word_frequency.txt'
    blacklist = list()
    with open(bl_filename, mode='r') as csv_file:
        rows = csv.reader(csv_file, delimiter=',')
        for r in rows:
            blacklist.append(r[0])
        # print(csv_file.read())
    bl_msg = api.Message(attributes={'filename': bl_filename}, body=blacklist)
    setup_blacklist(bl_msg)

    # WORD_INDEX
    wi_filename = '/Users/Shared/data/onlinemedia/data/word_frequency.csv'
    wi_table = list()
    with open(wi_filename, 'r') as csv_file:
        rows = csv.reader(csv_file,delimiter=',')
        for i,r in enumerate(rows):
            if i == 10000:
                break
            wi_table.append(r)
    attributes = {'table':{'columns':['HASH_TEXT','LANGUAGE','TYPE','WORD','COUNT']}}
    process(api.Message(attributes=attributes,body = wi_table))

    # LEXICON
    lex_filename = '/Users/Shared/data/onlinemedia/repository/lexicon_word_frequency.csv'
    lexicon_list = list()
    with open(lex_filename, mode='r') as csv_file:
        rows = csv.reader(csv_file, delimiter=',')
        headers = next(rows, None)
        for r in rows:
            #r[3] = r[3].replace('*', '')  # only needed when lexicon in construction
            lexicon_list.append(r)
    attributes = {"table": {"name": lex_filename, "version": 1, "columns": list()}}
    for h in headers:
        attributes["table"]["columns"].append({"name": h})
    lex_msg = api.Message(attributes=attributes, body=lexicon_list)
    setup_lexicon(lex_msg)


if __name__ == '__main__':
    test_operator()

    if False :
        subprocess.run(["rm",'-r','/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name+'_'+api.config.version
        subprocess.run(["vctl", "solution", "bundle", '/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_0.0.18',\
                                  "-t", solution_name])
        subprocess.run(["mv", solution_name+'.zip', '../../../solution/operators'])


