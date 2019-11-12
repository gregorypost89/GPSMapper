from pyspark.sql import SparkSession
from pyspark.sql import Row
import sqlGenerator

import collections

# Input the origin's coordinates here:

latitudeCoordinate = 40
longitudeCoordinate = -80

spark = SparkSession.builder.config("spark.sql.warehouse.dir",
                                    "file:///C:/temp").appName("SparkSQL").\
                                    getOrCreate()


def mapper(line):
    fields = line.split(',')
    return Row(id=int(fields[0]), zipcode=int(fields[1]),
               city=str(fields[2].encode("utf-8")),
               state=str(fields[3].encode("utf-8")),
               latitude=float(fields[4]), longitude=float(fields[5]),
               gridX=int(fields[6]), gridY=int(fields[7])),


pointsLines, zipcodesLines = spark.sparkContext.textFile("data/points.csv"), \
                             spark.sparkContext.textFile("data/zipcodes.csv")

points, zipcodes = pointsLines.map(mapper), zipcodesLines.map(mapper)

#schemaPoints, schemaZipcodes = spark.createDataFrame(points), \
#                               spark.createDataFrame(zipcodes)

#schemaPoints.createOrReplaceTempView("points")
#schemaZipcodes.createOrReplaceTempView("zipcodes")

#sqlQuery = sqlGenerator.radius100(latitudeCoordinate, longitudeCoordinate)


locations = spark.sql("""
SELECT 
city, 
state, 
latitude, 
longitude, 
COUNT(*) 
JOIN points P
ON P.GridX IN 
(SELECT GridX - 5, GridX - 4, GridX - 3, GridX - 2, GridX - 1,
GridX, GridX + 1, GridX + 2, GridX + 3, GridX + 4, GridX + 5 FROM zipcode ZX
WHERE Z.id = ZX.id) AND P.GridY IN (SELECT GridY - 5, GridY - 4, GridY - 3,
GridY - 2, GridY - 1, GridY, GridY + 1, GridY + 2, GridY + 3, GridY + 4, GridY +
 5 FROM zipcode ZY WHERE Z.id = ZY.id) WHERE P.Status = A AND((Z.latitude - P
 .latitude) * 40 ^ 2 + ((Z.longitude - P.longitude) * -80 ^ 2 <
 (100^2) GROUP BY city, state, latitude, longitude;""")

for location in locations:
    print(location)


