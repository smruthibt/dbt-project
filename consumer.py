from kafka import KafkaConsumer
import psycopg2
import sys
import json


conn = psycopg2.connect(dbname='pes1ug19cs018', user='pes1ug19cs018', host='localhost', password='abhiram')
cur = conn.cursor()

# tracks = sys.argv[1:]
# consumer = KafkaConsumer(TOPIC)
consumer = KafkaConsumer(value_deserializer=lambda m: json.loads(m.decode('utf-8')))
all_topics = list(consumer.topics())

consumer.subscribe(all_topics)
cur.execute("drop table counters")
cur.execute("create table counters(tags varchar primary key, count int)")

for m in consumer:
    # print(int(m.value.decode("utf-8")))
    # print(m.topic, m.value)
    print("Write")
    count_hashtag = m.value
    topic_hashtag = m.topic
    # print("a")
    # print(type(a))
    # print(type(b))

    # print(m)
    
    cur.execute("select counters.tags, counters.count from counters")
    rows = cur.fetchall()
    present = False
    for r in rows:
        if(r[0] == topic_hashtag):
            present = True
        # print(r[0], r[1])

    
    if(present):
        cur.execute("UPDATE counters SET count = count+ {1} WHERE tags = \'{0}\'".format(topic_hashtag, count_hashtag ))
    else:
        cur.execute("insert into counters values(%s, %s)",(topic_hashtag, count_hashtag))


    # cur.execute("select counters.tags, counters.count from counters")
    # rows = cur.fetchall()

    # for r in rows:
    #     print(r[0], r[1])


    conn.commit()