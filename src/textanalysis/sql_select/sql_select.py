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
            tags = {'sdi_utils': ''}
            version = "0.1.0"
            operator_name = "sql_select"
            operator_description = "sql select"
            operator_description_long = "Creates select statement from columns and table."
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}


            columns = 'ID, TEXT_ID'
            config_params['columns'] = {'title': 'Columns to select',
                                           'description': 'Columns to select',
                                           'type': 'string'}

            table_name = 'DOCS'
            config_params['table_name'] = {'title': 'Table name',
                                           'description': 'Name of table',
                                           'type': 'string'}



def process():

    operator_name = 'sql_select'
    logger, log_stream = slog.set_logging(operator_name, api.config.debug_mode)
    logger.info("Main Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()

    columns = api.config.columns
    table_name = api.config.table_name

    sql_statement = 'SELECT {} FROM {}'.format(columns, table_name)


    att_dict = attributes={'operator':operator_name,'parameter':{'columns':columns,'table':table_name},\
                               'message.batchIndex':0,'message.batchSize':1,'message.lastBatch':True}
    msg = api.Message(attributes=att_dict,body = sql_statement)

    api.send(outports[1]['name'], msg)
    api.send(outports[0]['name'], log_stream.getvalue())



outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'data', 'type': 'message', "description": "sql statement"}]

#api.add_generator(process)


def test_operator():

    api.config.columns =  '"ID", "TEXT_AS_NVARCHAR" as "TEXT"'
    api.config.debug_mode = True
    api.config.table_name =  '"${schema}"."bpanceditor.db::news.V_editorInbox_TextAs_nvarchar" where "ARTIFACT_TYPE" not in (\'NEWSTICKER\')'
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

