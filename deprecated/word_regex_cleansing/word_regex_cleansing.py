import json
import os
import csv
import re
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
                #print(msg.attributes)
                print(msg.body)
                pass
            if  port == outports[0]['name'] :
                #print(msg)
                pass


        def set_config(config):
            api.config = config

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': ''}
            version = "0.0.18"
            operator_name = "word_regex_cleansing"
            operator_description = "Word cleansing with regular expressions"
            operator_description_long = "Word cleansing with regular expressions."
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            test_mode = True
            config_params['test_mode'] = {'title': 'Test mode',
                                           'description': 'Checking only on regex but not sending DELETE sql',
                                           'type': 'boolean'}

            word_type = 'None'
            config_params['word_type'] = {'title': 'Word type',
                                           'description': 'Setting word type selection for delete',
                                           'type': 'string'}

            patterns = None
            config_params['patterns'] = {'title': 'Regular expression patterns',
                                           'description': 'Regular expression patterns',
                                           'type': 'string'}

def process(msg):

    words = msg.body
    att_dict = msg.attributes

    logger, log_stream = slog.set_logging('word_regex_cleansing', api.config.debug_mode)
    logger.info("Main Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()

    if isinstance(words[0],list) :
        words = [ w[0] for w in words]

    regex_patterns = tfp.read_list(api.config.patterns)

    logger.info('Test mode: {}'.format(api.config.test_mode))
    logger.info('Number of words to cleanse: {}'.format(len(words)))

    word_type = tfp.read_value(api.config.word_type)
    if len(word_type) > 1 :
        logger.warning('Only one word type can be processed. Take first one only: {}'.format(word_type[0]))

    count = 0
    for ipat, pat in enumerate(regex_patterns):
        if pat == '' :
            logger.warning('Empty pattern')
            continue
        cleansing_words = [ w for w in words if re.match(pat, w)]
        logger.info('Execute pattern: {} ({}/{})'.format(pat,ipat,len(regex_patterns)))
        logger.info('Number of DELETE statements: {}'.format(len(cleansing_words)))
        api.send(outports[0]['name'],log_stream.getvalue())
        log_stream.seek(0)
        log_stream.truncate()
        if not api.config.test_mode :
            for iw,w in enumerate(cleansing_words) :
                if word_type :
                    sql = 'DELETE FROM WORD_INDEX WHERE WORD = \'' + w + '\' AND WORD_TYPE = \''+ word_type + '\';'
                else :
                    sql = 'DELETE FROM WORD_INDEX WHERE WORD = \'' + w + '\';'
                att_dict['message.indexBatch'] = count
                att_dict['message.lastBatch'] = False
                api.send(outports[1]['name'],api.Message(attributes=att_dict,body=sql))
                count+=1

    sql = 'SELECT * FROM DUMMY;'
    att_dict['message.indexBatch'] = count
    att_dict['message.lastBatch'] = True
    api.send(outports[1]['name'], api.Message(attributes=att_dict, body=sql))
    api.send(outports[0]['name'],log_stream.getvalue())


inports = [{'name': 'words', 'type': 'message.table', "description": "Message table."}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'data', 'type': 'message', "description": "sql statement DELETE"}]

#api.set_port_callback(inports[0]['name'], process)


def test_operator():

    config = api.config
    config.debug_mode = True
    config.patterns = '"\d{2}:\d{2}:\d{2}","\d{3}-"'
    config.test_mode = False
    config.word_type = 'P'
    api.set_config(config)

    testdata_file = '/Users/Shared/data/test/data.csv'
    testdata = list()
    with open(testdata_file, mode='r') as csv_file:
        rows = csv.reader(csv_file, delimiter=',')
        r = next(rows)
        for r in rows:
            testdata.append([r[0]])
        # print(csv_file.read())
    msg = api.Message(attributes={'filename': testdata_file}, body=testdata)

    process(msg)



if __name__ == '__main__':
    test_operator()

    if True :
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name+'_'+api.config.version
        subprocess.run(["vctl", "solution", "bundle", '/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_' + api.config.version,\
                                  "-t", solution_name])
        subprocess.run(["mv", solution_name+'.zip', '../../../solution/operators'])




