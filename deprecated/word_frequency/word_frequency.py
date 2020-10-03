import io
import json
import os
import re
from collections import Counter
import pandas as pd

import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp

try:
    api
except NameError:
    class api:
        class Message:
            def __init__(self,body = None,attributes = ""):
                self.body = body
                self.attributes = attributes
                
        def send(port,msg) :
            if port == outports[1]['name']  :
                with open('/Users/Shared/data/onlinemedia/prep_texts/counted_words.csv',mode='w') as f:
                    csv = "\n".join([k + ';' + str(int(v)) for k, v in msg.body.items()])
                    f.write(csv)
            else :
                print('{}: {}'.format(port,msg))
    
        def call(config,msg):
            api.config = config
            return process(msg)
            
        def set_port_callback(port, callback) :
            pass
    
        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils':'','pandas':''}
            version = "0.0.1"
            operator_description = "Word Frequency"
            operator_description_long = "Find word frequency of prepared "
            add_readme = dict()
            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            collect = True
            config_params['collect'] = {'title': 'Collect data', 'description': 'Collect data before sending it to the output port',
                                           'type': 'boolean'}

            num_words = 200
            config_params['num_words'] = {'title': 'Number of Words', 'description': 'Number of words return to the output port',
                                           'type': 'integer'}

word_count = pd.Series()

def create_msg (attributes,body, collect = False) :
    if not collect :
        msg = api.Message(attributes=attributes, body=body)
        progress_str = '<BATCH ENDED><1>'
        return msg, progress_str
    elif ('batch.number' in attributes and 'batch.index' in attributes) or \
        ('storage.fileCount' in attributes and 'storage.fileIndex' in attributes) :

        # just in case the batch attributes are not set
        if 'batch.number' in attributes and 'batch.index' in attributes :
            attributes['batch.index'] = attributes['storage.fileIndex']
            attributes['batch.number'] = attributes['storage.fileCount']

        if attributes['batch.index'] + 1 == attributes['batch.number'] :
            progress_str = '<BATCH ENDED><{}>'.format(attributes['batch.number'])
            msg = api.Message(attributes=attributes, body=body)
            return msg, progress_str
        else:
            progress_str = '<BATCH IN-PROCESS><{}/{}>'.format(attributes['batch.index'] + 1,
                                                              attributes['batch.number'])
            return None, progress_str
    else :
        raise ValueError('For collecting data batch.index or storage.fileIndex is necessary in Message attributes.')

def process(msg) :
    att_dict = msg.attributes

    att_dict['operator'] = 'keyword_search'
    logger, log_stream = slog.set_logging(att_dict['operator'], loglevel=api.config.debug_mode)
    logger.info("Process started")
    time_monitor = tp.progress()

    adict = msg.body

    global word_count

    for a in adict :
        cw = Counter(a['words'])
        word_count = word_count.add(pd.Series(cw),fill_value = 0)

    word_count.sort_values(ascending=False,inplace = True)
    msg, progress_str = create_msg(attributes=att_dict,body=word_count.to_dict(),collect = api.config.collect)
    if msg :
        word_count.sort_values(ascending=False, inplace=True)
        msg.body = word_count.head(api.config.num_words).to_dict()
        api.send(outports[1]['name'], msg)

    logger.debug('Process ended, articles processed {}  - {}  '.format(progress_str, time_monitor.elapsed_time()))
    api.send(outports[0]['name'], log_stream.getvalue())

inports = [{'name': 'articles', 'type': 'message.dicts',"description":"Message with body as list of dictionaries "}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"},
            {'name': 'data', 'type': 'message.dict',"description":"Output words with number of occurances"}]


#api.set_port_callback(inports[0]['name'], process)

def main() :
    config = api.config
    config.debug_mode = True
    config.collect = False

    filename = '/Users/Shared/data/onlinemedia/prep_texts/scrape_2020-02-10-kw.json'
    with open(filename) as json_file:
        data = json.load(json_file)
    msg_data = api.Message(attributes={'filename': filename}, body=data)

    api.call(config, msg_data)


if __name__ == '__main__':
    main()
    #gs.gensolution(os.path.realpath(__file__), config, inports, outports)
        
