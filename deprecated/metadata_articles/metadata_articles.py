import io
import json
import os
import re
import subprocess
from datetime import datetime, timezone

import pandas as pd
import numpy as np

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

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': '', 'nltk': ''}
            version = "0.0.18"
            operator_name = 'metadata_articles'
            operator_description = "Metadata Articles"
            operator_description_long = "Create metadata from articles."
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}



def process(msg):

    logger, log_stream = slog.set_logging('metadata_articles', loglevel=api.config.debug_mode)
    logger.info("Process started")
    time_monitor = tp.progress()

    df = msg.body
    att_dict = msg.attributes

    df['url'] = df['url'][:255]
    df['title'] = df['title'][:255]
    df['num_characters'] = df['text'].str.len()
    df.loc[df['media'].isin(['Spiegel','FAZ','Sueddeutsche']),'language'] = 'DE'
    df.loc[df['media'].isin(['Lefigaro', 'Lemonde']), 'language'] = 'FR'
    df.loc[df['media'].isin(['Elpais', 'Elmundo']), 'language'] = 'ES'
    df['date'] = pd.to_datetime(df['date'],format = '%Y-%m-%d',utc = True)
    df = df[['hash_text','language','date','media','url','rubrics','title','num_characters']]

    #datea = datetime.strptime(article['date'], '%Y-%m-%d').replace(tzinfo=timezone.utc)

    logger.debug('Process ended, articles processed {}  - {}  '.format(df.shape[0], time_monitor.elapsed_time()))

    api.send(outports[1]['name'], msg)
    api.send(outports[1]['name'],api.Message(attributes = att_dict, body = df.values.tolist()))


inports = [{'name': 'articles', 'type': 'message.DataFrame', "description": "Message with body as DataFrame."}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"},
            {'name': 'data', 'type': 'message.DataFrame', "description": "Output metadata of articles"}]

#api.set_port_callback(inports[0]['name'], process)


def test_operator():
    config = api.config
    config.debug_mode = True
    config.num_important_words = 100
    api.set_config(config)

<<<<<<< HEAD
    in_dir = '/Users/Shared/data/onlinemedia/crawled_texts/'
    files_in_dir = [f for f in os.listdir(in_dir) if os.path.isfile(os.path.join(in_dir, f)) and re.match('.*json', f)]

    for i, fname in enumerate(files_in_dir):
        if i > 1 :
            break

        fbase = fname.split('.')[0]
        eos = True if len(files_in_dir) == i + 1 else False
        attributes = {'format': 'csv', "storage.filename": fbase, 'storage.endOfSequence': eos, \
                      'storage.fileIndex': i, 'storage.fileCount': len(files_in_dir),'process_list':[]}
        with open(os.path.join(in_dir,fname)) as json_file:
            data = json.load(json_file)
        msg_data = api.Message(attributes=attributes, body=data)
=======
    doc_file = '/Users/Shared/data/onlinemedia/data/metadata.csv'
    df = pd.read_csv(doc_file,sep=',',nrows=1000000000)
    msg = api.Message(attributes={'file': {'path': doc_file},'format':'pandas'}, body=df)
    process(msg)
>>>>>>> 59fe1471b368a62934ab846ad04dfc3bdcb1c042

    out_file = '/Users/Shared/data/onlinemedia/data/metadata.csv'
    df_list = [pd.DataFrame(d.body) for d in api.queue]
    pd.concat(df_list).to_csv(out_file,index=False)


if __name__ == '__main__':
    #test_operator()

    if True :
        subprocess.run(["rm",'-r','/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name+'_'+api.config.version
        subprocess.run(["vctl", "solution", "bundle", '/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_0.0.18',\
                                  "-t", solution_name])
        subprocess.run(["mv", solution_name+'.zip', '../../../solution/operators'])


