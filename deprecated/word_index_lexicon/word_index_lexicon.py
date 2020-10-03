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
                api.queue.append(msg)
            else:
                # print('{}: {}'.format(port, msg))
                pass

        def set_config(config):
            api.config = config

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': '', 'pandas': ''}
            version = "0.0.18"
            operator_name = "word_index_blacklist"
            operator_description = "Word index, remove blacklisted words"
            operator_description_long = "Word index, remove blacklisted words."
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            languages = 'None'
            config_params['language'] = {'title': 'Language', 'description': 'Filter for language of media.',
                                         'type': 'string'}

            types = 'PROPN'
            config_params['type'] = {'title': 'Type',
                                     'description': 'Define the kind of data extraction. P=Proper Nouns, '
                                                    'N=Nouns, X: Removing only stopwords.', 'type': 'string'}

# global articles
lexicon = None
lexicon_stem = None
last_msg = None
hash_list = list()
operator_name = 'word_index_lexicon'


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
# Checks for setup
def check_for_setup(logger, msg) :
    global blacklist, lexicon, last_msg

    use_lexicon = True

    logger.info("Check setup")
    # Case: setupdate, check if all has been set
    if msg == None:
        logger.debug('Setup data received!')
        if last_msg == None:
            logger.info('Prerequisite message has been set, but waiting for data')
            return None
        else:
            if  lexicon == None  :
                logger.info("Setup not complete -  lexicon: {}".format( len(lexicon)))
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
        if lexicon == None:
            len_lex = 0 if lexicon == None else len(lexicon)
            logger.info("Setup not complete - lexicon: {}".format(len_lex))
            return None
        else:
            logger.info('Setup is been set. Saved msg retrieved.')
            msg = last_msg
            last_msg = None
            return msg


def process(msg):
    global last_msg
    global hash_list
    global lexicon_stem, lexicon

    logger, log_stream = slog.set_logging(operator_name, api.config.debug_mode)

    # Check if setup complete
    msg = check_for_setup(logger, msg)
    if not msg:
        api.send(outports[0]['name'], log_stream.flush())
        return 0

    logger.info("Main Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()

    att_dict = msg.attributes

    # pandas Dataframe and select only values with right word_type
    cols = [c["name"] for c in msg.attributes['table']['columns']]
    df = pd.DataFrame(msg.body, columns=cols)

    # word type
    types = tfp.read_list(api.config.types)
    if not types :
        logger.warning('Word types had to be defined. Default word type : \'PROPN\'')
        types = ['PROPN']

    # Language filter
    languages = tfp.read_list(api.config.languages)
    if not languages :
        logger.warning('Languages had to be defined. Default languages : EN, FR, ES, DE')
        languages = ['EN', 'FR', 'ES', 'DE']

    for lang in lexicon:
        for w in lexicon[lang] :
            df.loc[(df['TYPE'].isin(types)) & (df['LANGUAGE']==lang) & (df['WORD']==w)] = lexicon[lang][w]
        for w in lexicon_stem[lang]:
            df.loc[(df['TYPE'].isin(types)) & (df['LANGUAGE'] == lang) & (df['WORD'] == w)] = lexicon_stem[lang][w]

    api.send(outports[1]['name'], api.Message(attributes=att_dict, body=df.values.tolist()))
    api.send(outports[0]['name'],log_stream.getvalue())


inports = [{'name': 'blacklist', 'type': 'message.list', "description": "Message with body as dictionary."},
           {'name': 'table', 'type': 'message.table', "description": "Message with body as table."}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'data', 'type': 'message.table', "description": "message table after blacklist removals"}]

#api.set_port_callback(inports[0]['name'], setup_blacklist)
#api.set_port_callback(inports[1]['name'], process)


def test_operator():
    config = api.config
    config.debug_mode = True
    config.type = 'P'
    config.new_type = 'P'
    config.language = 'None'
    api.set_config(config)

    # LEXICON
    lex_filename = '/Users/Shared/data/onlinemedia/repository/lexicon_march.csv'
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

    word_index_filename = '/Users/Shared/data/onlinemedia/data/word_extraction_regex.csv'
    word_index = list()
    with open(word_index_filename, mode='r') as csv_file:
        rows = csv.reader(csv_file, delimiter=',')
        r = next(rows)
        for r in rows:
            word_index.append(r)

    hcols = [{"class": "string", "name": "HASH_TEXT", "nullable": True, "size": 10, "type": {"hana": "BIGINT"}},
             {"class": "string", "name": "LANGUAGE", "nullable": True, "size": 2, "type": {"hana": "NVARCHAR"}},
             {"class": "string", "name": "TYPE", "nullable": True, "size": 15, "type": {"hana": "NVARCHAR"}},
             {"class": "string", "name": "WORD", "nullable": True, "size": 80, "type": {"hana": "NVARCHAR"}},
             {"class": "string", "name": "COUNT", "nullable": True, "type": {"hana": "INTEGER"}}]
    attributes = {'table' : {"columns": hcols,"name": "DIPROJECTS.WORD_EXTRACTION", "version": 1}}
    msg = api.Message(attributes=attributes, body=word_index)
    process(msg)

    # saving outcome as word index
    with open('/Users/Shared/data/onlinemedia/data/word_extraction_regex_lexicon.csv', 'w') as f:
        writer = csv.writer(f)
        cols = ['HASH_TEXT', 'LANGUAGE', 'TYPE', 'WORD', 'COUNT']
        writer.writerow(cols)
        for msg in api.queue:
            for rec in msg.body:
                writer.writerow(rec)


if __name__ == '__main__':
    test_operator()

    if True :
        subprocess.run(["rm",'-r','/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name+'_'+api.config.version
        subprocess.run(["vctl", "solution", "bundle", '/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_0.0.18',\
                                  "-t", solution_name])
        subprocess.run(["mv", solution_name+'.zip', '../../../solution/operators'])


