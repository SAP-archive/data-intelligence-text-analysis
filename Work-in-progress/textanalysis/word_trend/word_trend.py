import json
import os
import csv
import re
import pandas as pd
import numpy as np
from sklearn import datasets, linear_model
from sklearn.metrics import mean_squared_error, r2_score
import subprocess


import sdi_utils.gensolution as gs
import sdi_utils.set_logging as slog
import sdi_utils.textfield_parser as tfp
import sdi_utils.tprogress as tp


supported_languages = ['DE', 'EN', 'ES', 'FR']
lexicon_languages = {lang: False for lang in supported_languages}

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
                print(msg.attributes)
                print(msg.body)
            else:
                # print('{}: {}'.format(port, msg))
                pass

        def set_config(config):
            api.config = config

        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils': '','pandas':'','sklearn':''}
            version = "0.0.18"
            operator_name = "word_trend"
            operator_description = "word trend"
            operator_description_long = "Calculates trend of words "
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}

            language = 'None'
            config_params['language'] = {'title': 'Language', 'description': 'Filter for language of media.',
                                         'type': 'string'}

            type = 'P'
            config_params['type'] = {'title': 'Type',
                                     'description': 'Define the kind of data extraction. P=Proper Nouns, '
                                                    'N=Nouns, X: Removing only stopwords.', 'type': 'string'}

            min_count = 5
            config_params['min_count'] = {'title': 'Minimum number of occurances',
                                     'description': 'Minimum number of occurances of words.','type': 'integer'}

            regr_ratio = 0.5
            config_params['regr_ratio'] = {'title': 'Ratio for how many top words to calculate the trend',
                                     'description': 'Ratio for how many top words to calculate the trend','type': 'number'}

            periods_into_past = '7, 17, 21, 28,90'
            config_params['periods_into_past'] = {'title': 'Periods into past',
                                     'description': 'Periods into the past','type': 'string'}

# global articles
blacklist = list()
keywords = list()
lexicon = None
lexicon_stem = None
last_msg = None
<<<<<<< HEAD
hash_list = list()
=======
id_set = list()
>>>>>>> 59fe1471b368a62934ab846ad04dfc3bdcb1c042
operator_name = 'word_extraction'


def setup_blacklist(msg):
    logger, log_stream = slog.set_logging(operator_name, api.config.debug_mode)
    global blacklist
    logger.info('Set blacklist')
    logger.debug('Attributes: {}'.format(msg.attributes))
    logger.debug('Data: {}'.format(msg.body))
    blacklist = msg.body
    api.send(outports[0]['name'], log_stream.getvalue())
    process(None)


def setup_lexicon(msg):
    logger, log_stream = slog.set_logging(operator_name, api.config.debug_mode)
    global lexicon, lexicon_languages, lexicon_stem
    logger.info('Set lexicon')
    logger.debug('Attributes: {}'.format(msg.attributes))
    logger.debug('Data: {}'.format(msg.body))
    try:
        header = [c["name"] for c in msg.attributes['table']['columns']]
    except Exception as e:
        logger.error(e)
        api.send(outports[0]['name'], log_stream.getvalue())
        return None

    lexicon = {c: dict() for c in header[1:]}
    lexicon_stem = {c: dict() for c in header[1:]}
    for r in msg.body:
        for i, lang in enumerate(header[1:]):
            lang_words = r[i + 1].split()
            lw = list()
            lws = list()
            for w in lang_words :
                if w[-1] == '*' :
                    lws.append(w[:-1])
                else :
                    lw.append(w)
            if lw :
                lw_dict = dict.fromkeys(lw, r[0])
                lexicon[lang].update(lw_dict)
            if lws :
                lws_dict = dict.fromkeys(lws, r[0])
                lexicon_stem[lang].update(lws_dict)

    for lang in header[1:]:
        lexicon_languages[lang] = True
    api.send(outports[0]['name'], log_stream.getvalue())
    process(None)


# Checks for setup
def check_for_setup(logger, msg) :
    global blacklist, lexicon, last_msg

    use_blacklist = True
    use_lexicon = True

    logger.info("Check setup")
    # Case: setupdate, check if all has been set
    if msg == None:
        logger.debug('Setup data received!')
        if last_msg == None:
            logger.info('Prerequisite message has been set, but waiting for data')
            return None
        else:
            if len(blacklist) == 0 or lexicon == None   :
                logger.info("Setup not complete -  blacklist: {}   lexicon: {}".\
                            format(len(blacklist, len(lexicon))))
                return None
            else:
                logger.info("Last msg list has been retrieved")
                return last_msg
    else:
        logger.debug('Processing data received!')
        # saving of data msg
        if last_msg == None:
            last_msg = msg
        else:
            last_msg.attributes = msg.attributes
            last_msg.body.extend(msg.body)

        # check if data msg should be returned or none if setup is not been done
        if (len(blacklist) == 0  and use_blacklist == True) or \
            (lexicon == None and use_lexicon == True):
            len_lex = 0 if lexicon == None else len(lexicon)
            logger.info("Setup not complete -  blacklist: {}  lexicon: {}".\
                        format(len(blacklist), len_lex))
            return None
        else:
            logger.info('Setup is been set. Saved msg retrieved.')
            msg = last_msg
            last_msg = None
            return msg


def process(msg):
    global blacklist
    global last_msg
<<<<<<< HEAD
    global hash_list
=======
    global id_set
>>>>>>> 59fe1471b368a62934ab846ad04dfc3bdcb1c042

    logger, log_stream = slog.set_logging(operator_name, api.config.debug_mode)

    # Check if setup complete
    msg = check_for_setup(logger, msg)
    if not msg:
        api.send(outports[0]['name'], log_stream.flush())
        return 0

    logger.info("Main Process started. Logging level: {}".format(logger.level))
    time_monitor = tp.progress()

    att_dict = msg.attributes
    cols = [c["name"] for c in msg.attributes['table']['columns']]
    df = pd.DataFrame(msg.body,columns=cols)
    df['COUNT'] = df['COUNT'].astype('int32')

    type = tfp.read_value(api.config.type)
    if len(type) > 1 :
        logger.warning('Only one type can be transformed. Take only first one: {}'.format(type[0]))
        type = type[0]

    num_rows_before = df.shape[0]
    df = df.loc[df['TYPE']==type]
    logger.info('Word type filter: {} ({}->{})'.format(type,num_rows_before,df.shape[0]))
    num_rows_before = df.shape[0]

    # remove blacklist words
    for blw in blacklist:
        df = df.loc[df['WORD'] != blw]
    logger.info('Use blacklist: {} ({}->{})'.format(len(blacklist), num_rows_before,df.shape[0]))
    num_rows_before = df.shape[0]

    # merge words
    language = tfp.read_value(api.config.language)
    languages = list(df['LANGUAGE'].unique())
    for lang in languages :
        if lexicon_languages[lang]:
            for lw in lexicon_stem[lang] :
                df.loc[df['WORD']==lw,'WORD'] = lexicon_stem[lang][lw]
            for lw in lexicon[lang] :
                df.loc[df['WORD']==lw,'WORD'] = lexicon[lang][lw]

    # set past days
    df['DATE'] = pd.to_datetime(df['DATE'])
    latest_day = df['DATE'].max()
    df['PAST_DAYS'] = (df['DATE'] - latest_day).dt.days

    df = df.groupby(by=['DATE','LANGUAGE','WORD', 'PAST_DAYS'])['COUNT'].sum().reset_index()

    # define words for which to calculate the trend
    min_count = api.config.min_count
    n_df = df.loc[(df['DATE'] == latest_day) & (df['COUNT'] >= min_count),['DATE','LANGUAGE','WORD','COUNT']].copy()
    n_df['RANK'] = n_df['COUNT'].rank(method = 'min',ascending=False).astype('int32')

    top_num = int(api.config.regr_ratio * n_df.shape[0])
    logger.debug('Top num: {}'.format(top_num))
    num_rows_before = n_df.shape[0]
    n_df = n_df.loc[n_df['RANK']<=top_num]
    logger.info('Number of top words for which the trend is going to be calculated: {} ({})'.format(n_df.shape[0],num_rows_before))

    # Create linear regression object
    regr = linear_model.LinearRegression()
    # Train the model using the training sets

    periods_into_past = tfp.read_list(api.config.periods_into_past)
    for pt in periods_into_past:
        r_df = df.loc[df['PAST_DAYS'] > -pt]
        trend_col = 'TREND_' + str(pt)
        for i, w in enumerate(n_df['WORD']):
            x = r_df.loc[df['WORD'] == w, 'PAST_DAYS'].values.reshape(-1, 1)
            y = r_df.loc[df['WORD'] == w, 'COUNT'].values.reshape(-1, 1)
            regr.fit(x, y)
            n_df.loc[n_df['WORD'] == w, trend_col] = regr.coef_
            n_df.loc[n_df['WORD'] == w, trend_col + '_IC'] = regr.intercept_

    n_df.sort_values(by='RANK',inplace = True, ascending=True)
    n_df['DATE'] = n_df['DATE'].dt.strftime('%Y-%m-%d')

    att_dict = msg.attributes
    hcols = [{"class": "string", "name": "DATE", "nullable": True, "size": 10,"type": {"hana": "NVARCHAR"}},
             {"class": "string", "name": "LANGUAGE", "nullable": True, "size": 2,"type": {"hana": "NVARCHAR"}},
             {"class": "string", "name": "WORD", "nullable": True, "size": 80,"type": {"hana": "NVARCHAR"}},
             {"class": "string", "name": "COUNT", "nullable": True, "type": {"hana": "INTEGER"}},
             {"class": "string", "name": "RANK", "nullable": True, "type": {"hana": "INTEGER"}}]
    for ppast in periods_into_past :
        hcols.append({"class": "double", "name": "TREND_"+str(ppast), "nullable": True, "type": {"hana": "DOUBLE"}})
        hcols.append({"class": "double", "name": "TREND_" + str(ppast) +'_IC', "nullable": True, "type": {"hana": "DOUBLE"}})

    body = n_df.values.tolist()
    att_dict['table'] = {"columns": hcols,"name": "DIPROJECTS.WORD_TREND", "version": 1}
    api.send(outports[1]['name'],api.Message(attributes=att_dict,body=n_df.values.tolist()))
    api.send(outports[0]['name'], log_stream.getvalue())
    #print(n_df[['WORD','RANK','COUNT','TREND_7','TREND_14','TREND_21','TREND_365']])






inports = [{'name': 'blacklist', 'type': 'message.list', "description": "Message with body as dictionary."},
           {'name': 'lexicon', 'type': 'message.table', "description": "Message with body as lexicon."}, \
           {'name': 'table', 'type': 'message.table', "description": "Message with body as table."}]
outports = [{'name': 'log', 'type': 'string', "description": "Logging data"}, \
            {'name': 'data', 'type': 'message.table', "description": "Trend_table"}]

#api.set_port_callback(inports[0]['name'], setup_blacklist)
#api.set_port_callback(inports[1]['name'], setup_lexicon)
#api.set_port_callback(inports[2]['name'], process)


def test_operator():
    config = api.config
    config.debug_mode = True
    config.type = 'P'
    config.new_type = 'P'
    config.language = 'None'
    config.min_count = 3
    config.regr_ratio = 0.1
    config.periods_into_past = '7, 14, 21, 28, 90'
    config.periods_into_past = '7'
    api.set_config(config)


    # BLACKLIST
    bl_filename = '/Users/Shared/data/onlinemedia/repository/blacklist_word_frequency.txt'
    blacklist = list()
    with open(bl_filename, mode='r') as csv_file:
        rows = csv.reader(csv_file, delimiter=',')
        for r in rows:
            blacklist.append(r[0])
        # print(csv_file.read())
    bl_msg = api.Message(attributes={'filename': bl_filename}, body=blacklist)
    setup_blacklist(bl_msg)

    # WORD_INDEX
    wf_filename = '/Users/Shared/data/onlinemedia/data/word_frequency.csv'
    wf_table = list()
    with open(wf_filename, 'r') as csv_file:
        rows = csv.reader(csv_file,delimiter=',')
        for i,r in enumerate(rows):
            wf_table.append(r)
    attributes = {'table':{'columns':[{'name':'DATE'},{'name':'LANGUAGE'},{'name':'TYPE'},{'name':'WORD'},{'name':'COUNT'}]}}
    process(api.Message(attributes=attributes,body = wf_table))

    # LEXICON
    lex_filename = '/Users/Shared/data/onlinemedia/repository/lexicon_word_frequency.csv'
    lexicon_list = list()
    with open(lex_filename, mode='r') as csv_file:
        rows = csv.reader(csv_file, delimiter=',')
        headers = next(rows, None)
        for r in rows:
            #r[3] = r[3].replace('*', '')  # only needed when lexicon in construction
            lexicon_list.append(r)
    attributes = {"table": {"name": lex_filename, "version": 1, "columns": list()}}
    for h in headers:
        attributes["table"]["columns"].append({"name": h})
    lex_msg = api.Message(attributes=attributes, body=lexicon_list)
    setup_lexicon(lex_msg)


if __name__ == '__main__':
    test_operator()

    if True :
        subprocess.run(["rm",'-r','/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_' + api.config.version])
        gs.gensolution(os.path.realpath(__file__), api.config, inports, outports)
        solution_name = api.config.operator_name+'_'+api.config.version
        subprocess.run(["vctl", "solution", "bundle", '/Users/d051079/OneDrive - SAP SE/GitHub/di_textanalysis/solution/operators/textanalysis_0.0.18',\
                                  "-t", solution_name])
        subprocess.run(["mv", solution_name+'.zip', '../../../solution/operators'])


