import io
import json
import os
import re
import nltk

from nltk.tokenize import RegexpTokenizer
#from nltk.corpus import stopwords
#nltk.download('stopwords')
#nltk.download('punkt')
#nltk.download('averaged_perceptron_tagger')

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
            if port == outports[2]['name']  :
                new_filename = os.path.basename(msg.attributes['filename']).split('.')[0]+'-kw.json'
                with open(os.path.join('/Users/Shared/data/onlinemedia/prep_texts', new_filename),mode='w') as f:
                    f.write(json.dumps(msg.body, ensure_ascii=False))
            elif port == outports[1]['name'] :
                new_filename = os.path.basename(msg.attributes['filename']).split('.')[0] + '-unique.txt'
                with open(os.path.join('/Users/Shared/data/onlinemedia/prep_texts', new_filename), mode='w') as f:
                    f.write('\n'.join(msg.body))
            else :
                print('{}: {}'.format(port,msg))
    
        def set_config(config):
            api.config = config
            
        def set_port_callback(port, callback) :
            pass
    
        class config:
            ## Meta data
            config_params = dict()
            tags = {'sdi_utils':'','nltk':''}
            version = "0.0.1"
            operator_description = "Text Condensation (German)"
            operator_description_long = "Condense text to significant German words "
            add_readme = dict()

            debug_mode = True
            config_params['debug_mode'] = {'title': 'Debug mode',
                                           'description': 'Sending debug level information to log port',
                                           'type': 'boolean'}
            nouns_only = True
            config_params['nouns_only'] = {'title': 'Nouns only (Capital words)',
                                           'description': 'Nouns only (Capital words)',
                                           'type': 'boolean'}

            capword_tokenizer = RegexpTokenizer('[A-Z]\w+')


blacklist = list()
last_msg = None

# if keyword setting is called the find_word()
def set_disregard(msg):
    global blacklist
    disregarded_words = msg.body
    condense_text(None)


def get_index_words(text,tokenizer,min_occurance=1,min_word_length = 2) :
    tokenized = tokenizer.tokenize(text)
    is_noun = lambda pos: pos[:2] == 'NN'
    iwords = [word for (word, pos) in nltk.pos_tag(tokenized) if is_noun(pos)]
    iwords = [word for word in iwords if iwords.count(word) >= min_occurance]
    iwords = [word for word in iwords if len(word) >= min_word_length]
    return iwords


def condense_text(msg) :
    global blacklist
    global last_msg

    logger, log_stream = slog.set_logging('text_condensation_g', loglevel=api.config.debug_mode)
    logger.info("Process started")
    time_monitor = tp.progress()

    # Case: Keywords has been set before
    if msg == None :
        if last_msg == None :
            logger.info('disregard wordlist have been set, but waiting for data')
            return 0
        else :
            msg = last_msg
            logger.info("Last msg list has been retrieved")
    else :
        if len(disregarded_words) == 0 :
            if last_msg == None :
                last_msg = msg
                logger.info('No disregards word list and no text list - save text list')
            else :
                last_msg.attributes = msg.attributes
                last_msg.body.extend(msg.body)
                logger.info('No disregard word list but text list - extend stored text list')
            return 0

    if api.config.debug_mode :
        api.send(outports[0]['name'], log_stream.getvalue())


    adict = msg.body
    att_dict = msg.attributes

    if api.config.nouns_only :
        tokenizer = RegexpTokenizer('[A-Z]\w+')

    #stop_words = stopwords.words('german')
    result_list = list()
    unique_words = set()
    for article in adict :
        index_words = get_index_words(article['text'],tokenizer,min_occurance=1,min_word_length=2)
        index_words = [ w for w in index_words if not w in disregarded_words]
        result_list.append({'hash_text':article['hash_text'],'num_words':len(index_words),'words':index_words})
        unique_words.update(set(index_words))

    logger.debug('Process ended, articles processed {}  - {}  '.format(len(adict), time_monitor.elapsed_time()))

    att_dict['content'] = 'words in article'
    msg = api.Message(attributes=att_dict,body=result_list)
    api.send(outports[2]['name'], msg)
    att_dict['content'] = 'Unique words'
    msg = api.Message(attributes=att_dict, body=unique_words)
    api.send(outports[1]['name'], msg)
    api.send(outports[0]['name'], log_stream.getvalue())


inports = [{'name': 'articles', 'type': 'message.dicts',"description":"Message with body as dictionary."},\
           {'name': 'disregards', 'type': 'message.list',"description":"Message with list of words to disregard. "}]
outports = [{'name': 'log', 'type': 'string',"description":"Logging data"},
            {'name': 'uniquewords', 'type': 'message.list',"description":"Array of new unique words"},             \
            {'name': 'data', 'type': 'message.dict',"description":"Output List of index words"}]


#api.set_port_callback(inports[0]['name'], condense_text)
#api.set_port_callback(inports[1]['name'], set_disregard)

def main() :
    config = api.config
    config.debug_mode = True
    config.nouns_only = True
    api.set_config(config)

    filename = '/Users/Shared/data/onlinemedia/crawled_texts/scrape_2020-02-10.json'
    with open(filename) as json_file:
        data = json.load(json_file)
    msg_data = api.Message(attributes={'filename': filename}, body=data)

    disregards_filename = '/Users/Shared/data/onlinemedia/repository/disregards.txt'
    with open(disregards_filename) as swords_file:
        disregarded_words = swords_file.read().splitlines()
    msg_disregards = api.Message(attributes={'filename': filename}, body=disregarded_words)




if __name__ == '__main__':
    main()
    #gs.gensolution(os.path.realpath(__file__), config, inports, outports)
        
