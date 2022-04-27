import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import udf
from pyspark.sql.functions import to_utc_timestamp
from pyspark.sql.functions import from_json
from pyspark.sql.types import *
from pyspark.sql.functions import window
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
from datetime import datetime
import pytz
import json
from kafka import KafkaProducer
KAFKA_SERVER = 'localhost:9092'
TOPIC = 'ElonMusk'


def aggregate_tags_count(new_values, total_sum):
	# return sum(new_values) + (total_sum or 0)
	return sum(new_values)


def extract_tags(word):
	if word.lower().startswith("#"):
		if word in tracks:
			return word
		else:
			return "nonTag"

def get_sql_context_instance(spark_context):
	if ('sqlContextSingletonInstance' not in globals()):
		globals()['sqlContextSingletonInstance'] = SQLContext(spark_context)
	return globals()['sqlContextSingletonInstance']

def process_rdd(time, rdd):
	print("----------- %s -----------" % str(time))
	try:
		# Get spark sql singleton context from the current context
		sql_context = get_sql_context_instance(rdd.context)
		print("1")
		# convert the RDD to Row RDD
		row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
		print("2")
		print(row_rdd)
		print(row_rdd.collect())

		if not row_rdd.isEmpty():
			# create a DF from the Row RDD
			hashtags_df = sql_context.createDataFrame(row_rdd)
			print("3")
			# Register the dataframe as table
			hashtags_df.registerTempTable("hashtags")
			print("4")
			# get the top 10 hashtags from the table using SQL and print them
			hashtag_counts_df = sql_context.sql("select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 10")
			print("5")
			hashtag_counts_df.show()
			print("6")
			# call this method to prepare top 10 hashtags DF and send them
		
	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)


# def serialize_int(data):
# 	return 


def print_rdd(time, rdd):
	print("----------- %s -----------" % str(time))
	try:
		# Get spark sql singleton context from the current context
		sql_context = get_sql_context_instance(rdd.context)
		# print("1")
		# convert the RDD to Row RDD
		# row_rdd = rdd.map(lambda w: Row(hashtag=w[0], hashtag_count=w[1]))
		row_rdd = rdd.map(lambda w: w)

		# print("2")
		#print(row_rdd)
		#print(row_rdd.collect())

		if not row_rdd.isEmpty():
			# create a DF from the Row RDD
			hashtags_df = sql_context.createDataFrame(row_rdd)
			# producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
			producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, value_serializer=str.encode)


			# for i in hashtags_df.collect():
			# 	# display
			# 	print(i["_1"], i["_2"])
				# s = i["_1"]
				# s = s[1:]
				# num = bytes(i["_2"], encoding='utf-8')
				# producer.send(s,  value = num)
				
			# print("3")
			# Register the dataframe as table
			# hashtags_df.registerTempTable("hashtags")
			# print("4")
			# get the top 10 hashtags from the table using SQL and print them
			# hashtag_counts_df = sql_context.sql("select hashtag, hashtag_count from hashtags order by hashtag_count desc limit 10")
			# print("5")
			result = hashtags_df.toJSON().map(lambda j: json.loads(j)).collect()
			for i in result:
				s = i["_1"]
				s = s[1:]
				n = i["_2"]
				n = str(n)
				print(type(n), type(s))
				# num = bytes(n, encoding='utf-8')
				producer.send(s,  value = n)


			# print(result)
			# producer.send(TOPIC, result)
			hashtags_df.show()
			# print("6")
			# call this method to prepare top 10 hashtags DF and send them
		
	except:
		e = sys.exc_info()[0]
		print("Error: %s" % e)

host = sys.argv[1]
port = int(sys.argv[2])
tracks = sys.argv[3:]

for i in range(len(tracks)):
	tracks[i] = '#' + tracks[i]


# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 20)
# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream(host,port)
# split each tweet into words
words = dataStream.flatMap(lambda line: line.split(" "))
# filter the words to get only hashtags, then map each hashtag to be a pair of (hashtag,1)
hashtags = words.filter(lambda w: '#' in w).filter(lambda w: w in tracks).map(lambda x: (x, 1))
# adding the count of each hashtag to its last count
tags_totals = hashtags.updateStateByKey(aggregate_tags_count)
# do processing for each RDD generated in each interval
# tags_totals.foreachRDD(process_rdd)
tags_totals.foreachRDD(print_rdd)

# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()
