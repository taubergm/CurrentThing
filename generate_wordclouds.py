import sqlite3
from os import path
from PIL import Image
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
import matplotlib.pyplot as plt
import nltk
from datetime import date, timedelta, datetime


#from nltk.corpus import stopwords
#stop_words = stopwords.words('english')

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



conn = sqlite3.connect('tweets.sqlite')
conn.text_factory = bytes
cur = conn.cursor()



def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)
        
skip_words  = ['@', "https", "co", "RT", 's', 't', 'will', 'say', 'make', 'new', 'know', 'today', 'people', 'says', 'said', 'u', 'one', 'think', 'thing', 'going', 'now'
        'thank', 'see', 'good', 'year', 'time', 'even', 'love', 're', 'first', 'much', 'well', 'mean', 'm', 'may', 'day', 'look', 'want', 'still', 'work', 'amp', 'really',
        'show', 'now', 'great', 'lot', 'a', 'got', 'many', 'way', 'tweet', 'point', 'two', 'one', 'better', 'back', 'Thank', 'every', 'come', 'never', 'things', 'read',
        've', 'i', '\'', 'take', 'Yes', 'yes', 'go', 'part', 'via', 'need', 'right', 'left', 'Right', 'Left', 'something', 'best', 'don', 'Don', 'Thanks', 'us', 'story',
        'years', 'seem', 'guy', 'actually', 'lol', 'important', 'told', 'real', 'always', 'week', 'American', 'far', 'life', 'made', 'man', 'woman', 'last', 'political',
        'world', 'gt', 'free', 'seems', 'true', 'false', 'support', 'high', 'low', 'talk', 'talking', 'big', 'little', 'bad', 'feel', 'hit', 'trying', 'v', 'believe',
        'everyone', 'next', 'call', 'around', 'book', 'user', 'use', 'might', 'watch', 'news', 'old', 'tell', 'someone', 'another', 'Oh', 'oh', 'sure', 'less', 'tonight',
        'night', 'shows', 'long', 'done', 'd', 'll', 'let', 'movie', 'amazing', 'question', 'idea', 'politics', 'win', 'won', 'keep', 'song', 'problem', 'happy', 'saying',
        'please', 'already', 'game', 'wrote', 'public', 'politician', 'help', 'Maybe', 'yeah', 'money', 'end', 'getting', 'kind', 'makes', 'Saturday', 'least', 'stop',
        'thought', 'times', 'change', 'piece', 'name', 'though', 'listen', 'pretty', 'view', 'give', 'w', 'sorry', 'making', 'friend', 'nothing', 'enough', 'fine', 'joke',
        'put', 'live', 'looking', 'try', 'morning', 'didn\'', 'stuff', 'watched', 'coming', 'TV', 'agree', 'yet', 'reason', 'word', 'hope', 'month', 'means', 'lie', 'kid',
        'care', 'everything', 'hear', 'heard', 'definitely', 'video', 'event', 'worth', 'wrong', 'report', 'number', 'home', 'photo', 'past', 'write', 'writes', 'writer',
        'used', 'clear', 'top', 'set', 'place', 'given', 'member', 'job', 'city', 'happened', 'went', 'journalism', 'hour', 'understand', 'whether', 'away', 'anti', 'exactly',
        'didn\'', 'government', 'fact', 'find', 'favorite', 'ago', 'later', 'working', 'hate', 'mind', 'position', 'huge', 'either', 'group', 'thinking', 'must', 'asked',
        'including', 'others', 'among', 'different', 'months', 'moment', 'anything', 'post', 'accurate', 'hold', 'likely', 'final', 'didn', 'isn', 'data', 'taking', 'led',
        'al', 'Mr', 'mr', 'Mrs', 'Dr', 'dr', 'weeks', 'B', 'A'
        ]

def get_wordcloud_by_date(cur, date, skip_words):
    tweets = get_tweets_by_date(cur, date)
    text = " ".join(tweet for tweet in tweets)
    
    stop_words = STOPWORDS.update(skip_words)
    wordcloud = WordCloud(background_color='white', colormap='Set2', collocations=False).generate(text)
    
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    #plt.show()
    plt.savefig('cloud_{}.png'.format(date))

    print(wordcloud.words_.keys())
    print(wordcloud.words_.values())


start_date = date(2022, 12, 13)
end_date = date(2022, 12, 18)

now = datetime.now()
#start_date = date(now.year, now.month, now.day) - timedelta(days=10)
#end_date = date(now.year, now.month, now.day)  + timedelta(days=1)


for date in daterange(start_date, end_date):
    ds = date.strftime("%Y-%m-%d")
    print(ds)
    get_wordcloud_by_date(cur, ds, skip_words)

