import json
import os
import csv
import re
import pandas as pd
import pickle
import collections
import subprocess


import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp



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
            if port == outports[1]['name'] :
                api.queue.append(msg)
            if  port == outports[0]['name'] :
                #print(msg)
                pass

        def set_config(config):
            api.config = config

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': '','pandas': ''}
            version = "0.0.18"
            operator_name = "word_index_regex"
            operator_description = "Run regex on word_index"
            operator_description_long = "Run regex on word_index."
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            word_types = 'PROPN'
            config_params['word_type'] = {'title': 'Word types',
                                           'description': 'Setting word type selection for delete',
                                           'type': 'string'}

            language_filter = 'None'
            config_params['language_filter'] = {'title': 'Language filter', 'description': 'Filter for languages of media.',
                                         'type': 'string'}

            patterns = None
            config_params['patterns'] = {'title': 'Regular expression patterns',
                                           'description': 'Regular expression patterns',
                                           'type': 'string'}

def process(msg):

    att_dict = msg.attributes

    logger, log_stream = slog.set_logging('word_index_regex', api.config.debug_mode)
    logger.info("Main Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()

    # regex patterns
    regex_patterns = tfp.read_list(api.config.patterns)

    # word type
    word_types = tfp.read_list(api.config.word_types)
    if not word_types :
        logger.warning('Word types had to be defined. Default word type : \'PROPN\'')
        word_types = ['PROPN']

    # pandas Dataframe and select only values with right word_type
    cols = [c["name"] for c in msg.attributes['table']['columns']]
    df = pd.DataFrame(msg.body, columns=cols)
    df_p = df.loc[df['TYPE'].isin(word_types)]

    # Language filter
    language_filter = tfp.read_list(api.config.language_filter)
    if language_filter :
        df_p = df_p.loc[df['LANGUAGE'].isin(language_filter)]

    # get unique words to get words that comply with regex
    words = df_p['WORD'].unique()
    logger.info('Number of words to test with regex pattern: {}'.format(len(words)))

    for ipat, pat in enumerate(regex_patterns):
        if pat == '' :
            logger.warning('Empty pattern')
            continue
        logger.info('Execute pattern: {} ({}/{})'.format(pat,ipat,len(regex_patterns)))
        cleansing_words = [ w for w in words if re.match(pat, w)]

    df = df.loc[~df['WORD'].isin(cleansing_words)]

    api.send(outports[1]['name'], api.Message(attributes=att_dict, body=df.values.tolist()))
    api.send(outports[0]['name'],log_stream.getvalue())


inports = [{'name': 'words', 'type': 'message.table', "description": "Message table."}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'data', 'type': 'message.table', "description": "Table after regex"}]

#api.set_port_callback(inports[0]['name'], process)


def test_operator():

    config = api.config
    config.debug_mode = True
    config.patterns = '"\d{2}:\d{2}:\d{2}","\d{3}-", "^[-\'\./+]", "[-\./+]$"'
    config.test_mode = False
    config.word_type = 'P'
    api.set_config(config)

    word_index_filename = '/Users/Shared/data/onlinemedia/data/word_extraction.csv'
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
    with open('/Users/Shared/data/onlinemedia/data/word_extraction_regex.csv', 'w') as f:
        writer = csv.writer(f)
        cols = ['HASH_TEXT','LANGUAGE','TYPE','WORD', 'COUNT']
        writer.writerow(cols)
        for msg in api.queue :
            for rec in msg.body:
                writer.writerow(rec)



if __name__ == '__main__':
    test_operator()

    if True :
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name+'_'+api.config.version
        subprocess.run(["vctl", "solution", "bundle", '/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_' + api.config.version,\
                                  "-t", solution_name])
        subprocess.run(["mv", solution_name+'.zip', '../../../solution/operators'])




