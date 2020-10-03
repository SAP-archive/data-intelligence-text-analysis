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




try:
    api
except NameError:
    class api:

        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if port == outports[1]['name']:
                print('Attributes: ')
                print(msg.attributes)
                print('Data')
                print(msg.body)


        def set_config(config):
            api.config = config

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': '', 'spacy': ''}
            version = "0.1.0"
            operator_name = "sql_word_index"
            operator_description = "sql word index"
            operator_description_long = "Creates SQL statement for creating a word _index from text_word."
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            language = 'DE'
            config_params['language'] = {'title': 'Language',
                                               'description': 'Language for which the index should be created.',
                                               'type': 'string'}

            text_id_col = 'TEXT_ID'
            config_params['text_id_col'] = {'title': 'Text_id column',
                                           'description': 'Name of text_id column',
                                           'type': 'string'}

            table_name = 'WORD_TEXT'
            config_params['table_name'] = {'title': 'Table name',
                                           'description': 'Name of table',
                                           'type': 'string'}

            type_limit_map = 'LEX: 1, PROPN: 5, PER:2, ORG:2, LOC:2'
            config_params['type_limit_map'] = {'title': 'Limit of each type',
                                           'description': 'Minimum frequency of words for each type (map)',
                                           'type': 'string'}



def process():

    operator_name = 'sql_word_index'
    logger, log_stream = slog.set_logging(operator_name, api.config.debug_mode)
    logger.info("Main Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()

    language = tfp.read_value(api.config.language)
    type_limit = tfp.read_dict(api.config.type_limit_map)
    table_name = tfp.read_value(api.config.table_name)
    text_id_col = tfp.read_value(api.config.text_id_col)

    for i, [wtype, limit] in enumerate(type_limit.items()) :
        sql_s = "SELECT {tid}, \"{tn}\".LANGUAGE, \"{tn}\".TYPE, \"{tn}\".WORD, COUNT FROM \"{tn}\" INNER JOIN"\
                "(SELECT WORD, TYPE, LANGUAGE, SUM(COUNT) as CUMS FROM \"{tn}\" "\
                "WHERE LANGUAGE = \'{lang}\' AND TYPE = \'{wt}\' "\
                "GROUP BY WORD, TYPE, LANGUAGE) AS CTABLE ON "\
                "\"{tn}\".WORD = CTABLE.WORD AND \"{tn}\".TYPE = CTABLE.TYPE AND \"{tn}\".LANGUAGE = CTABLE.LANGUAGE "\
                "WHERE CUMS >= {lt}".format(tid = text_id_col,tn = table_name,lang=language,wt = wtype,lt=limit)

        lastbatch = True if len(type_limit) == i+1 else False
        att_dict = attributes={'operator':operator_name,'parameter':{'type':wtype,'limit':limit,'language':language},\
                               'message.batchIndex':i,'message.batchSize':len(type_limit),'message.lastBatch':lastbatch}
        msg = api.Message(attributes=att_dict,body = sql_s)
        api.send(outports[1]['name'], msg)

    api.send(outports[0]['name'], log_stream.getvalue())



outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'data', 'type': 'message', "description": "sql statement"}]

#api.add_generator(process)


def test_operator():

    config = api.config
    config.debug_mode = True
    config.type_limit_map = 'LEX: 1, PROPN: 5, PER:2, ORG:2, LOC:2'
    config.language = 'DE'
    api.set_config(config)
    process()

if __name__ == '__main__':
    test_operator()

    if True :
        subprocess.run(["rm",'-r','/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, None, outports)
        solution_name = api.config.operator_name+'_'+api.config.version
        subprocess.run(["vctl", "solution", "bundle", '/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_'+ api.config.version,\
                                  "-t", solution_name])
        subprocess.run(["mv", solution_name+'.zip', '../../../solution/operators'])

