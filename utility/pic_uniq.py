import sqlite3
import os


# create new db
if os.path.exists('uniq.db'):
    os.remove('uniq.db')
conn2 = sqlite3.connect('uniq.db')
c2 = conn2.cursor()
c2.execute('''CREATE TABLE pubmed
              (pmid INTEGER UNIQ, journal TEXT, pubdate TEXT, page TEXT, volume TEXT, issue TEXT, title TEXT, abstract TEXT, author TEXT, language TEXT)''')


# fetch data from old db
conn1 = sqlite3.connect('pubmed3.db')
c1 = conn1.cursor()
sql = 'select * from pubmed group by pmid'
c1.execute(sql)

# store data to new db
rows = 1
while rows:
    rows = c1.fetchmany(size=100000)
    c2.executemany('''insert into pubmed values (?,?,?,?,?,?,?,?,?,?)''', [tuple(x) for x in rows])
    conn2.commit()

conn1.close()
conn2.close()

    