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
            version = "0.1.0"
            operator_name = "word_frequency"
            operator_description = "Word Frequency"
            operator_description_long = "Calculates word frequency"
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


def process(msg):

    att_dict = msg.attributes

    logger, log_stream = slog.set_logging('word_regex', api.config.debug_mode)
    logger.info("Main Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()

    df = msg.body

    if not isinstance(df, pd.DataFrame) or df.empty:
        logger.warning('Empty dataframe, no output send!')
        api.send(outports[0]['name'], log_stream.getvalue())
        api.send(outports[2]['name'], api.Message(attributes=att_dict, body=df))
        return 0

    df['count'] = df['count'].astype('int32')

    # word type
    word_types = tfp.read_list(api.config.word_types)
    if word_types :
        df = df.loc[df['type'].isin(word_types)]

    # Language filter
    language_filter = tfp.read_list(api.config.language_filter)
    if language_filter :
        df = df.loc[df['language'].isin(language_filter)]

    df = df.groupby(['language','type','word'])['count'].agg('sum').reset_index()

    api.send(outports[1]['name'], api.Message(attributes=att_dict, body=df))
    api.send(outports[0]['name'],log_stream.getvalue())


inports = [{'name': 'words', 'type': 'message.DataFrame', "description": "Message table."}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'data', 'type': 'message.DataFrame', "description": "Table after regex"}]

#api.set_port_callback(inports[0]['name'], process)


def test_operator():

    config = api.config
    config.debug_mode = True
    config.test_mode = False
    config.language_filter = 'None'
    config.word_types = 'None'
    api.set_config(config)

    doc_file = '/Users/Shared/data/onlinemedia/data/word_extraction.csv'
    df = pd.read_csv(doc_file,sep=',',nrows=10000000)
    msg = api.Message(attributes={'file': {'path': doc_file},'format':'pandas'}, body=df)
    process(msg)

    out_file = '/Users/Shared/data/onlinemedia/data/word_freq_test.csv'
    df_list = [d.body for d in api.queue]
    pd.concat(df_list).to_csv(out_file,index=False)



if __name__ == '__main__':
    #test_operator()

    if True :
        subprocess.run(["rm",'-r','/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name+'_'+api.config.version
        subprocess.run(["vctl", "solution", "bundle", '/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_' + api.config.version,\
                                  "-t", solution_name])
        subprocess.run(["mv", solution_name+'.zip', '../../../solution/operators'])




