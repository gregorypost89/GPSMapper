# sparkSQL.py
#
# This program creates a spark session and use SparkSQL to query locations

from pyspark.sql import SparkSession
from pyspark.sql import Row

import collections

spark = SparkSession.builder.appname("SparkSQL").getOrCreate()


def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), lat=float(fields[2]), long=float(fields[3]))


lines = spark.sparkContext.textFile("data/zipcodes.csv")
places = lines.map(mapper)

schemaPlaces = spark.createDataFrame(places).cache
schemaPlaces.createOrReplaceTempView("places")



