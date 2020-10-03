import io
import json
import os
import csv
import re
import pickle

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

import subprocess


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
                pass

        def set_config(config):
            api.config = config

        def set_port_callback(port, callback):
            pass

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': ''}
            version = "0.1.0"
            operator_name='word_frequency_sql'
            operator_description = "Word frequency SQL"
            operator_description_long = "Word frequency SQL-statement generator"
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            language = None
            config_params['language'] = {'title': 'Language filter',
                                           'description': 'Language filter',
                                           'type': 'string'}
            type = None
            config_params['type'] = {'title': 'Type filter',
                                           'description': 'Type filter',
                                           'type': 'string'}

            min_count = 1
            config_params['min_count'] = {'title': 'Minimum count of words',
                                     'description': 'Minimum count of words',
                                     'type': 'integer'}


def process(msg):

    logger, log_stream = slog.set_logging('word_frquency_sql', loglevel=api.config.debug_mode)
    logger.info("Process started")
    time_monitor = tp.progress()

    language = tfp.read_value(api.config.language)
    type = tfp.read_value(api.config.type)
    min_count = api.config.min_count

    sql_statement ='INSERT INTO WORD_FREQUENCY ("DATE", "LANGUAGE", "TYPE", "WORD", "COUNT") '\
                   'SELECT * FROM (SELECT AM."DATE", WI."LANGUAGE", "TYPE","WORD", sum("COUNT") ' \
                   'AS "COUNT" FROM "WORD_INDEX"  AS WI INNER JOIN "ARTICLES_METADATA" AS AM ' \
                   'ON WI."HASH_TEXT" = AM."HASH_TEXT"'\

    if type or language :
        sql_statement += ' WHERE'

    if language :
        sql_statement += ' WI."LANGUAGE" = \'' + language + '\''

    if type :
        if language :
            sql_statement += ' AND ('
        for i, t in enumerate(type):
            if i > 0 :
                sql_statement += ' OR'
            sql_statement += ' "TYPE" = \'' + t + '\''
        if language :
            sql_statement += ')'

    sql_statement += ' GROUP BY "DATE", "WORD","TYPE",WI."LANGUAGE") WHERE "COUNT">= ' + str(min_count) + ' ;'

    logger.debug('Process ended, articles processed {}'.format(time_monitor.elapsed_time()))
    api.send(outports[0]['name'], log_stream.getvalue())
    api.send(outports[1]['name'], sql_statement)


outports = [{'name': 'log', 'type': 'string', "description": "Logging data"},
            {'name': 'sql', 'type': 'string', "description": "sql statement"}]
inports = [{'name': 'articles', 'type': 'message.*', "description": "Trigger"}]

#api.set_port_callback(inports[0]['name'], process)


def test_operator():

    config = api.config
    config.debug_mode = True
    config.language = 'DE'
    config.type = 'NLP'
    config.min_count = 1
    api.set_config(config)

    msg = api.Message(attributes={'type':'trigger'},body = 'trigger')
    process(msg)


if __name__ == '__main__':
    #test_operator()
    if True :
        subprocess.run(["rm", '-r',
                        '/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name + '_' + api.config.version
        subprocess.run(["vctl", "solution", "bundle",
                        '/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_' + api.config.version, \
                        "-t", solution_name])
        subprocess.run(["mv", solution_name + '.zip', '../../../solution/operators'])





