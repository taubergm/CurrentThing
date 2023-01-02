# for first thousand handles in csv
# twint -u <user>
# parse and put in new database, partitioned by day

# do wordcload on all
# do sentiment on all


import pandas as pd
import twint
import sqlite3
import time

def connect_to_db(db_name):
    #create news database
    conn = sqlite3.connect(db_name)
    conn.text_factory = bytes
    cur = conn.cursor()
    
    return (cur, conn)


def create_table():
    try:
        cur.executescript('''
    CREATE TABLE Tweets (
        id  INTEGER NOT NULL PRIMARY KEY UNIQUE,
        user TEXT,
        date    TEXT, 
        tweet    TEXT UNIQUE
    );
    ''')
    except:
        print("database already exists")

    #print(json.dumps(json_response, indent=4, sort_keys=True))


LIMIT = 1200
DB_NAME = 'tweets.sqlite'
(cur, conn) = connect_to_db(DB_NAME)
create_table()



df = pd.read_csv('Top10000Journos.csv')
users = df['username']

n = 0
for user in users:
    print(user)
    n = n + 1 
    c = twint.Config()
    c.Username = user
    c.Pandas = True
    try:
        twint.run.Search(c)
    except:
        time.sleep(30)
        continue

    #c.Output = "./{}.csv".format(user)

    print(c.Output)

    df = twint.storage.panda.Tweets_df
    print(df)
    if df is None:
        continue
        time.sleep(30)
        
    for index, row in df.iterrows():
        cur.execute('''INSERT OR REPLACE INTO Tweets
                (id, user, date, tweet) 
                VALUES ( ?, ?, ?, ?)''', 
                ( row['id'], user, row['date'], row['tweet'] ) )
        conn.commit()

    
    if n >= LIMIT:
        break

