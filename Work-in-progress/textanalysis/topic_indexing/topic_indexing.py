
import json
import os
import csv
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
            def __init__(self, body=None, attributes=""):
                self.body = body
                self.attributes = attributes

        def send(port, msg):
            if port == outports[1]['name']:
                new_filename = 'topic_count.json'
                with open(os.path.join('/Users/Shared/data/onlinemedia/prep_texts', new_filename), mode='w') as f:
                    f.write(json.dumps(msg.body, ensure_ascii=False,indent=4))
            else:
                #print('{}: {}'.format(port, msg))
                pass

        def set_config(config):
            api.config = config

        def set_port_callback(port, callback):
            pass

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': '','pandas':''}
            version = "0.0.1"
            operator_description = "Topic indexing"
            operator_description_long = "Index topics based on word index"
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            tolerance = 1
            config_params['tolerance'] = {'title': 'Tolerance',
                                           'description': 'Factor tolerated to count as has been found',
                                           'type': 'integer'}


def process(db_msg):

    logger, log_stream = slog.set_logging('topic_index', loglevel=api.config.debug_mode)
    logger.info("Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()

    att_dict = dict()
    topic = db_msg.attributes['topic']
    keywords = db_msg.attributes['keywords']
    columns = [c['name'] for c in db_msg.attributes['table']['columns']]
    columns = [ 'KEYWORD' if c == 'WORD' else c  for c in columns ]

    att_dict['topic'] = topic
    att_dict['tolerance']  = api.config.tolerance
    if api.config.tolerance < 1.0 :
        min_keyword_num = len(keywords) - int(api.config.tolerance * len(keywords))
    else :
        min_keyword_num = len(keywords) - api.config.tolerance

    df = pd.DataFrame(db_msg.body,columns = columns)
    logger.debug('Input DataFrame: {} - {}'.format(df.shape[0],df.shape[1]))
    # filter out all indices of not containing keyword - not needed when getting the sql output directly
    #df = df.loc[df['KEYWORD'].isin(keywords)]

    num_keyword_articles = df.shape[0]
    g_df = df.groupby(['HASH_TEXT']).count().reset_index()
    g_df = g_df.loc[g_df['KEYWORD']>=min_keyword_num]
    g_df['count'] = min_keyword_num
    g_df['topic'] = topic
    g_df.rename(columns={'KEYWORD':'count','count':'min_num'},inplace=True)
    g_df = g_df[['HASH_TEXT','topic','count','min_num']]

    #print(g_df)
    topic_index = g_df.to_dict('records')

    topic_index_msg = api.Message(attributes=att_dict,body=topic_index)
    logger.info('Topic found in articles: \"{}\" in {}/{} ({}/{})'.format(topic,g_df.shape[0],num_keyword_articles,\
                                                                          min_keyword_num,len(keywords)))

    logger.debug('Process ended,  {}'.format(time_monitor.elapsed_time()))
    api.send(outports[1]['name'], topic_index_msg)
    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'db', 'type': 'message.table', "description": "Message with body as table."}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"},\
            {'name': 'data', 'type': 'message', "description": "Output List of dicts with word frequency"}]

#api.set_port_callback(inports[0]['name'], process)

def main():
    config = api.config
    config.debug_mode = True
    config.tolerance = 2
    api.set_config(config)

    ki_filename = '/Users/Shared/data/onlinemedia/repository/keyword_index.csv'
    keyword_index = list()
    with open(ki_filename,mode='r') as csv_file:
        rows = csv.reader(csv_file,delimiter=',')
        next(rows,None)
        for r in rows :
            keyword_index.append(r)

    att_dict = {"table":{"columns":[{"class":"integer","name":"HASH_TEXT","nullable":False,"type":{"hana":"BIGINT"}},\
                                     {"class":"string","name":"KEYWORD","nullable":False,"size":200,"type":{"hana":"NVARCHAR"}},\
                                     {"class":"integer","name":"COUNT","nullable":True,"type":{"hana":"INTEGER"}}]}}

    topics_filename = '/Users/Shared/data/onlinemedia/repository/topics.csv'
    topics = list()
    with open(topics_filename, mode='r') as csv_file:
        rows = csv.reader(csv_file, delimiter=',')
        next(rows,None)
        for r in rows:
            keywords = [ r[i] for i in range(6,len(r)) if not r[i]=='']
            topics.append({'topic': r[0], 'keywords':keywords})
    att_dict['topic'] = topics[0]['topic']
    att_dict['keywords'] = topics[0]['keywords']
    db_msg = api.Message(attributes=att_dict,body=keyword_index)
    process(db_msg)


if __name__ == '__main__':
    main()
    # gs.gensolution(os.path.realpath(__file__), config, inports, outports)





