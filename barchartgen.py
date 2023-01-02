import pandas as pd
import numpy as np

df = pd.read_csv('2022_current_things.csv')
print(df)


df['date'] = pd.to_datetime(df['date'])
df.set_index('date', inplace=True)
df['meta_code'] = pd.factorize(df['meta'])[0]

print(df)


def cumcounter(x):
    y = [x.loc[d - pd.DateOffset(months=4):d].count() for d in x.index]
    gr = x.groupby('date')
    adjust = gr.rank(method='first') - gr.size() 
    y += adjust
    return y


df['cum_count'] = df.groupby('meta')['meta_code'].transform(cumcounter)


df.reset_index(inplace=True)
print(df.head(100))


#df = df.pivot_table(values='cum_count',index='date', columns='meta')
df = pd.pivot_table(df, values='cum_count', index='date', columns='meta',
                          aggfunc='count')
df.fillna(0, inplace=True)

print(df)
#df.reset_index(inplace=True)


# get cumulative sum for barchart race
cols = ['Abortion', 'China',  'Economy',  'Fbi Trump Raid',  
        'Jan 6',  'Pelosi',  'Russia',  'Shooting',  'Student Debt',  
        'Twitter / Musk',  'UK Politics', 'US Politics',  'Ukraine'
        ]
df.loc[:, cols] = df.loc[:, cols].cumsum(axis=0)



print(df)

df.to_csv('cumsum.csv')

test = df.head()

# barchart
import bar_chart_race as bcr

bcr.bar_chart_race(df = df, 
                   n_bars = 11, 
                   period_length=100, #duration of each row (ms)
                   sort='desc',
                   figsize=(10, 8),
                   title='Current Thing 2022',
                   filename = 'current_thing_2022.mp4')

