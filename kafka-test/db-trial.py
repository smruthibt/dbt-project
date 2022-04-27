import psycopg2

conn = psycopg2.connect(dbname='pes1ug19cs018', user='pes1ug19cs018', host='localhost', password='abhiram')

cur = conn.cursor()

tag1 = "Jack"
count1 = 3
present = False
# cur.execute("insert into counters values(%s, %s)",("Jill", 3))
# cur.execute("INSERT INTO counters(tags, count) VALUES (\"{0}\", {1}) ON CONFLICT(tags) DO UPDATE SET count = count + {0} WHERE tag = {0}".format("Jack",4))

cur.execute("select counters.tags, counters.count from counters")
rows = cur.fetchall()

for r in rows:
    if(r[0] == tag1):
        present = True
    # print(r[0], r[1])

if(present):
    cur.execute("UPDATE counters SET count = count+ {1} WHERE tags = \'{0}\'".format(tag1, count1))
else:
    cur.execute("insert into counters values(%s, %s)",(tag1, count1))


cur.execute("select counters.tags, counters.count from counters")
rows = cur.fetchall()

for r in rows:
    print(r[0], r[1])


conn.commit()
cur.close()
conn.close()

