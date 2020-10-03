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
            version = "0.1.0"
            operator_name = "word_blacklist"
            operator_description = "Remove Blacklisted Words"
            operator_description_long = "Remove Blacklisted Words"
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}


            word_types = 'PROPN'
            config_params['word_types'] = {'title': 'Word types',
                                           'description': 'Setting word type selection for delete',
                                           'type': 'string'}

            language_filter = 'None'
            config_params['language_filter'] = {'title': 'Language filter', 'description': 'Filter for languages of media.',
                                         'type': 'string'}

# global articles
blacklist = list()

last_msg = None
id_set = list()
operator_name = 'word_blacklist'


def setup_blacklist(msg):
    logger, log_stream = slog.set_logging(operator_name, api.config.debug_mode)
    global blacklist
    logger.info('Set blacklist')
    logger.debug('Attributes: {}'.format(msg.attributes))
    logger.debug('Data: {}'.format(msg.body))
    blacklist = msg.body
    api.send(outports[0]['name'], log_stream.getvalue())
    process(None)



# Checks for setup
def check_for_setup(logger, msg) :
    global blacklist, last_msg

    use_blacklist = True

    logger.info("Check setup")
    # Case: setupdate, check if all has been set
    if msg == None:
        logger.debug('Setup data received!')
        if last_msg == None:
            logger.info('Prerequisite message has been set, but waiting for data')
            return None
        else:
            if len(blacklist) == 0    :
                logger.info("Setup not complete -  blacklist: {}".format(len(blacklist)))
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
        if len(blacklist) == 0 :
            logger.info("Setup not complete -  blacklist: {}".format(len(blacklist)))
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

    logger.info("Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()

    att_dict = msg.attributes
    df = msg.body

    # word type
    word_types = tfp.read_list(api.config.word_types)
    if not word_types :
        word_types = list(df['type'].unique())

    # Language filter
    language_filter = tfp.read_list(api.config.language_filter)
    if not language_filter :
        language_filter = list(df['language'].unique())

    df = df.loc[~(df['type'].isin(word_types) & df['language'].isin(language_filter) & df['word'].isin(blacklist)) ]

    # test for duplicates
    dup_s = df.duplicated(subset=['text_id','language','type','word']).value_counts()
    num_duplicates = dup_s[True] if True in dup_s  else 0
    logger.info('Duplicates: {} / {}'.format(num_duplicates, df.shape[0]))
    logger.info('End process: {}'.format(time_monitor.elapsed_time()))

    api.send(outports[1]['name'], api.Message(attributes=att_dict, body=df))
    api.send(outports[0]['name'],log_stream.getvalue())


inports = [{'name': 'blacklist', 'type': 'message.list', "description": "Message with body as dictionary."},
           {'name': 'table', 'type': 'message.DataFrame', "description": "Message with body as table."}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'data', 'type': 'message.DataFrame', "description": "Message table after blacklist removals"}]

#api.set_port_callback(inports[0]['name'], setup_blacklist)
#api.set_port_callback(inports[1]['name'], process)


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

    doc_file = '/Users/Shared/data/onlinemedia/data/word_extraction_regex.csv'
    attributes = {'format':'DataFrame'}
    df = pd.read_csv(doc_file,sep=',',nrows=10000000)
    msg = api.Message(attributes={'file': {'path': doc_file},'format':'pandas'}, body=df)
    process(msg)

    # saving outcome as word index
    out_file = '/Users/Shared/data/onlinemedia/data/word_extraction_regex_blacklist.csv'
    df_list = [d.body for d in api.queue]
    pd.concat(df_list).to_csv(out_file, index=False)



if __name__ == '__main__':
    #test_operator()

    if True :
        subprocess.run(["rm",'-r','/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name+'_'+api.config.version
        subprocess.run(["vctl", "solution", "bundle", '/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_'+ api.config.version,\
                                  "-t", solution_name])
        subprocess.run(["mv", solution_name+'.zip', '../../../solution/operators'])


