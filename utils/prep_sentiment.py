import pandas as pd
from os.path import join

df = pd.read_csv('../data_repository/sentiments_DE.csv')
df.value = (df.value / 4.5 - 1).round(2)

df.drop_duplicates(subset=['word'],inplace = True)
print('Number of words: {}'.format(df.shape[0]))
print('Range of sentiment values: [{} , {}]'.format(df.value.min(),df.value.max()))
df.to_csv('../data_repository/sentiments_DE.csv',index = False)

