import pandas as pd
from os.path import join

# unknown encoding - utf-8 is not decode it correctly
# manually replaced wrong characters

df = pd.read_excel(io='../data_repository/pnas.1411678112.sd01.xlsx',encoding='utf-8',\
                   header = None, nrows = None, names = ['language','word','value',],usecols = [0,1,2])

df = df.loc[df['language'].isin(['spanish', 'portuguese', 'english', 'french', 'german'])]
df['language']=df['language'].replace({'spanish':'ES','portuguese':'PT','english':'EN','german':'DE','french':'FR'})
df['word'] = df['word'].str.strip()
df= df.loc[df['language']=='DE']
print(df.head(50))

#df.to_csv(join('../data_repository','pnas_sentiments.csv'),index=False,encoding='utf-8')
df.to_csv(join('../data_repository','pnas_sentiments_DE.csv'),index=False,encoding='utf-8')

#languages =  df['language'].unique()
#for lang in languages :
#    df.loc[df['language']==lang].to_csv(join('../data_repository','pnas_sentiments_' + lang + '.csv'),index=False)


