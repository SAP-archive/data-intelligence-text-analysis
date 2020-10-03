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

        def set_config(config):
            api.config = config

        def set_port_callback(port, callback):
            pass

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': ''}
            version = "0.0.1"
            operator_description = "Word Indexing"
            operator_description_long = "Word indexing."
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}


def process(msg):

    logger, log_stream = slog.set_logging('word_indexing', loglevel=api.config.debug_mode)
    logger.info("Process started")
    time_monitor = tp.progress()

    articles = msg.body
    word_index = list()
    # as table
    for article in articles :
        word_index.extend([[article[0], article[1], article[2], w[0],w[1]] for w in article[3]])

    att_dict = msg.attributes
    att_dict['table'] = {"columns": [{"class": "string", "name": "HASH_TEXT", "nullable": True, "type": {"hana": "INTEGER"}},
                              {"class": "string", "name": "LANGUAGE", "nullable": True, "size": 2,"type": {"hana": "NVARCHAR"}},
                              {"class": "string", "name": "TYPE", "nullable": True, "size": 1,"type": {"hana": "NVARCHAR"}},
                              {"class": "string", "name": "WORD", "nullable": True, "size": 80,"type": {"hana": "NVARCHAR"}},
                              {"class": "string", "name": "COUNT", "nullable": True, "type": {"hana": "INTEGER"}}],
                         "name": "DIPROJECTS.WORD_INDEX", "version": 1}

    logger.debug('Process ended, articles processed {}  - {}  '.format(len(articles), time_monitor.elapsed_time()))
    api.send(outports[0]['name'], log_stream.getvalue())

    msg = api.Message(attributes=att_dict,body=word_index)
    api.send(outports[1]['name'], msg)


outports = [{'name': 'log', 'type': 'string', "description": "Logging data"},
            {'name': 'words', 'type': 'message.table', "description": "Message table"}]
inports = [{'name': 'articles', 'type': 'message.table', "description": "Message table"}]

#api.set_port_callback(inports[0]['name'], process)


def main():
    with open('/Users/Shared/data/onlinemedia/repository/word_extraction.pickle',mode='rb') as f:
        queue_list = pickle.load(f)

    config = api.config
    config.debug_mode = True
    api.set_config(config)
    for msg in queue_list :
        process(msg)

    # reading queue and saving all msg to one file
    wi_fn = os.path.join('/Users/Shared/data/onlinemedia/repository', 'word_index.csv')
    with open(wi_fn, 'w') as f:
        writer = csv.writer(f)
        for i, m in enumerate(api.queue) :
            if i == 0 :
                cols = [c['name'] for c in m.attributes['table']['columns']]
                writer.writerow(cols)
            writer.writerows(m.body)

if __name__ == '__main__':
    main()
    # gs.gensolution(os.path.realpath(__file__), config, inports, outports)





