import pandas as pd
from os import listdir
from os.path import isfile, join
import json
import io

doc_path = '/Users/Shared/data/onlinemedia/crawled_texts'
new_doc_path = '/Users/Shared/data/onlinemedia/onlinedocs_formated'
doc_files = [f for f in listdir(doc_path) if isfile(join(doc_path, f)) and not '.DS_Store' in f ]
for file in doc_files :
    print(file)

    # byte stream
    bstream = open(join(doc_path, file), mode='rb').read()
    json_io = io.BytesIO(bstream)

    try :
        jdict = json.load(json_io)
        df = pd.DataFrame(jdict)
    except UnicodeDecodeError :
        print("dict UTF Decode Error File: {}".format(file))


    df = pd.read_json(json_io,orient='records')

    df['num_characters'] = df['text'].str.len()
    df.rename(columns={'hash_text':'text_id','rubrics':'rubric'},inplace=True)
    df = df[['media','date','text_id','title','rubric','url','paywall','num_characters','text']]

    df_json = df.to_json(orient='records',force_ascii=False)