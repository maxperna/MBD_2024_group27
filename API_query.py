import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

"""Convert a list into a str"""
def data_formulator(list):
    return str(list).strip('[').strip(']').replace("'","")


lichess_api_url = "https://lichess.org/api/users"

input_path = "hdfs:///user/s3262642/stats/analysis/cheaters"

spark = SparkSession.builder.appName("Lichess Cheat Analysis").getOrCreate()
cheaters = spark.read.parquet(input_path)

#Get the cheaters as a list 
cheaters_list = cheaters.select("Player").rdd.flatMap(lambda x: x).collect()


#QUERYING (maximum allowed users per time =300)
actual_cheaters = []
for i in range(len(cheaters_list)%300):
    query_list = cheaters_list[i*300:(i+1)*300]

    data = data_formulator(query_list)
    query_reply = requests.post(lichess_api_url,data=data).json()

    for element in query_reply:
        try:
            if element["tosViolation"]:
                actual_cheaters.append(element["username"])
        except:
            pass


#Creation of DF
schema = StructType([StructField("Player", StringType(), True)])
df = spark.createDataFrame([(player,) for player in actual_cheaters], schema)
df.write.mode("overwrite").parquet("hdfs:///user/s3262642/stats/analysis/actual_cheaters")




