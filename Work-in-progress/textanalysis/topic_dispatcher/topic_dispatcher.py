import io
import json
import os
import csv

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp


try:
    api
except NameError:
    class api:
        class Message:
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            pass

        def set_config(config):
            api.config = config

        def set_port_callback(port, callback):
            pass

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': ''}
            version = "0.0.1"
            operator_description = "Topic dispatcher"
            operator_description_long = "Sends input topics to SQL processor and topic frequency operator."
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            topic_table = 'DIPROJECTS.WORD_INDEX2'
            config_params['topic_table'] = {'title': 'Topic table',
                                           'description': 'Topic table on database',
                                           'type': 'string'}
            table_colum = 'WORD'
            config_params['table_colum'] = {'title': 'Table Column',
                                           'description': 'Table column of words',
                                           'type': 'string'}



def process(msg):


    logger, log_stream = slog.set_logging('topic dispatcher', loglevel=api.config.debug_mode)
    logger.info("Process started")
    time_monitor = tp.progress()

    table = tfp.read_value(api.config.topic_table)
    column = tfp.read_value(api.config.table_colum)

    topics = msg.body
    for t in topics :
        topic_keywords = ["'"+t[i]+"'" for i in range(6, len(t)) if not t[i] == '']
        sql = 'SELECT * FROM '+table+' WHERE ' + column + ' IN (' + ','.join(topic_keywords)+')'
        att_dict = {'topic':t[0],'keywords':topic_keywords}
        sql_msg = api.Message(attributes = att_dict,body = sql)
        api.send(outports[1]['name'], sql_msg)
        logger.debug('Send sql: {}'.format(sql))

    logger.debug('Process ended, topics processed {}  - {}  '.format(len(topics), time_monitor.elapsed_time()))
    api.send(outports[0]['name'], log_stream.getvalue())


outports = [{'name': 'log', 'type': 'string', "description": "Logging data"},
            {'name': 'sql', 'type': 'message', "description": "message with body is sql and attributes contains the topic"}]
inports = [{'name': 'topic', 'type': 'message.table', "description": "Message with body as table."}]


#api.set_port_callback(inports[0]['name'], process)


def main():
    topics_filename = '/Users/Shared/data/onlinemedia/repository/topics.csv'
    topics = list()
    with open(topics_filename, mode='r') as csv_file:
        rows = csv.reader(csv_file, delimiter=',')
        next(rows,None)
        for r in rows:
            topics.append(r)
    topics_msg = api.Message(attributes={'filename': topics_filename}, body=topics)
    process(topics_msg)

if __name__ == '__main__':
    main()
    # gs.gensolution(os.path.realpath(__file__), config, inports, outports)





