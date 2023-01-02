import pandas as pd
import twint
import sqlite3
import time
from sklearn.feature_extraction.text import CountVectorizer
from wordcloud import STOPWORDS
from itertools import islice
from datetime import date, timedelta, datetime
import csv
import pickle

#from nltk.corpus import stopwords

skip_words  = ['@', "https", "co", "RT", 's', 't', 'will', 'say', 'make', 'new', 'know', 'today', 'people', 'says', 'said', 'u', 'one', 'think', 'thing', 'going', 'now'
        'thank', 'see', 'good', 'year', 'time', 'even', 'love', 're', 'first', 'much', 'well', 'mean', 'm', 'may', 'day', 'look', 'want', 'still', 'work', 'amp', 'really',
        'show', 'now', 'great', 'lot', 'a', 'got', 'many', 'way', 'tweet', 'point', 'two', 'one', 'better', 'back', 'Thank', 'every', 'come', 'never', 'things', 'read',
        've', 'i', '\'', 'take', 'Yes', 'yes', 'go', 'part', 'via', 'need', 'right', 'left', 'Right', 'Left', 'something', 'best', 'don', 'Don', 'Thanks', 'us', 'story',
        'years', 'seem', 'guy', 'actually', 'lol', 'important', 'told', 'real', 'always', 'week', 'American', 'far', 'life', 'made', 'man', 'woman', 'last', 'political',
        'world', 'gt', 'free', 'seems', 'true', 'false', 'support', 'high', 'low', 'talk', 'talking', 'big', 'little', 'bad', 'feel', 'hit', 'trying', 'v', 'believe',
        'everyone', 'next', 'call', 'around', 'book', 'user', 'use', 'might', 'watch', 'news', 'old', 'tell', 'someone', 'another', 'Oh', 'oh', 'sure', 'less', 'tonight',
        'night', 'shows', 'long', 'done', 'd', 'll', 'let', 'movie', 'amazing', 'question', 'idea', 'politics', 'win', 'won', 'keep', 'song', 'problem', 'happy', 'saying',
        'please', 'already', 'game', 'wrote', 'public', 'politician', 'help', 'maybe', 'yeah', 'money', 'end', 'getting', 'kind', 'makes', 'Saturday', 'least', 'stop',
        'thought', 'times', 'change', 'piece', 'name', 'though', 'listen', 'pretty', 'view', 'give', 'w', 'sorry', 'making', 'friend', 'nothing', 'enough', 'fine', 'joke',
        'put', 'live', 'looking', 'try', 'morning', 'didn\'', 'stuff', 'watched', 'coming', 'TV', 'agree', 'yet', 'reason', 'word', 'hope', 'month', 'means', 'lie', 'kid',
        'care', 'everything', 'hear', 'heard', 'definitely', 'video', 'event', 'worth', 'wrong', 'report', 'number', 'home', 'photo', 'past', 'write', 'writes', 'writer',
        'used', 'clear', 'top', 'set', 'place', 'given', 'member', 'job', 'city', 'happened', 'went', 'journalism', 'hour', 'understand', 'whether', 'away', 'anti', 'exactly',
        'didn\'', 'government', 'fact', 'find', 'favorite', 'ago', 'later', 'working', 'hate', 'mind', 'position', 'huge', 'either', 'group', 'thinking', 'must', 'asked',
        'including', 'others', 'among', 'different', 'months', 'moment', 'anything', 'post', 'accurate', 'hold', 'likely', 'final', 'didn', 'isn', 'data', 'taking', 'led',
        'al', 'Mr', 'mr', 'Mrs', 'Dr', 'dr', 'weeks', 'B', 'A', 'called', 'full', 'across', 'days', 'thanks', 'thank', 'without', 'person', 'anyone' 'doesn', 'wasn',
        'doesn', 'seen', 'thread', 'haven', 'possible', 'early', 'interesting', 'took', 'telling', 'totally', 'probably', 'found', 'tomorrow', 'reading', 'tells', 
        'congrats', 'almost', 'looks', '000', 'needs', 'open', '20', 'future', 'often', 'ask', 'start', 'wait', '2022'
        ]
    
#print(stopwords.words('english'))
#print(list(STOPWORDS))


stop_words = list(STOPWORDS) + skip_words




def get_top_n_words(corpus, stop_words,  n=None):
    vec = CountVectorizer().fit(corpus)
    bag_of_words = vec.transform(corpus)
    sum_words = bag_of_words.sum(axis=0)
    words = {}
    for word, idx in vec.vocabulary_.items():
        words[word] = sum_words[0, idx]


    # remove stopwords
    for k in stop_words:
        words.pop(k, None)

    words = dict(sorted(words.items(), key=lambda item: item[1], reverse=True))

    #return dict(list(words.items())[-n:])
    return dict(list(words.items())[:n])


    #words_freq = [(word, sum_words[0, idx]) for word, idx in vec.vocabulary_.items()]
    #words_freq =sorted(words_freq, key = lambda x: x[1], reverse=True)
    #return words_freq[:n]
    
def get_tweets_by_date(cur, date):
    q = "select tweet from Tweets where date like '%{}%'".format(date)
    users_in_db = []
    try:
        cur.execute(q)
        rows = cur.fetchall()
        tweets_in_db = [i[0].decode('utf-8') for i in rows]
    except sqlite3.Error as my_error:
        print("error: ",my_error)

    return tweets_in_db


def connect_to_db(db_name):
    #create news database
    conn = sqlite3.connect(db_name)
    conn.text_factory = bytes
    cur = conn.cursor()
    
    return (cur, conn)


def create_table(cur):
    try:
        cur.executescript('''
    CREATE TABLE Terms (
        date TEXT PRIMARY KEY UNIQUE,
        term_dict TEXT
    );
    ''')
    except:
        print("database already exists")

    #print(json.dumps(json_response, indent=4, sort_keys=True))


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

        
# read from tweets db
(cur_rd, conn_rd) = connect_to_db('tweets_copy.sqlite')


# write to terms db
(cur_wr, conn_wr) = connect_to_db('terms.sqlite')
create_table(cur_wr)

#date = '2022-10-10'
start_date = date(2022, 3, 1)
end_date = date(2022, 12, 15)
now = datetime.now()

all_terms = {}
top_five_terms = {}

for date in daterange(start_date, end_date):
    ds = date.strftime("%Y-%m-%d")
    print(ds)
    tweets = get_tweets_by_date(cur_rd, ds)
    #text = " ".join(tweet for tweet in tweets)
    term_dict = get_top_n_words(tweets, stop_words, 150)
    top_words = str(term_dict)
    cur_wr.execute('''INSERT OR REPLACE INTO Terms
                (date, term_dict) 
                VALUES ( ?, ? )''', 
                ( ds, top_words ) )
    conn_wr.commit()

    top_five = dict(list(term_dict.items())[:5]) # current things
    top_five_terms[str(ds)] = top_five

    #print(top_words)
    #print(top_five)
            
    
    # total counts
    for k,v in term_dict.items():
        if k not in all_terms:
            all_terms[k] = v
        else:
            all_terms[k] = all_terms[k] + v

words = dict(sorted(all_terms.items(), key=lambda item: item[1], reverse=True))
#print(words)
#print(top_five_terms)
    

with open('term_dict.pickle', 'wb') as f:
    pickle.dump(words, f, protocol=pickle.HIGHEST_PROTOCOL)

        
with open("top5_terms.csv", "w") as csv_file:
    writer = csv.writer(csv_file, delimiter=',')
    
    for k,v in top_five_terms.items():
        vals = list(v.keys())
        print(vals)
        row = [k] + vals
        writer.writerow(row)
        


