import re
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Initialize Spark Session
spark = SparkSession.builder.appName("Lichess Cheat Analysis").getOrCreate()

# Define the path to the Parquet file on HDFS
output_path_player = "hdfs:///user/s3262642/stats/all_players_games_db"
output_path_games = "hdfs:///user/s3262642/stats/games_db"
input_path = f"hdfs:///user/s3262642/lichess_db_standard_rated_2023-05.pgn"

data = spark.read.option("delimiter", "[Event").text(input_path)


def parse_match(partition,schema: dict):
    #Creat a list to append the game
    for row in partition:
        value = row.value
        text = value.split('"')
        column = text[0][1:].replace(' ', '')

        if column in ("White","Black"):
            schema[column] = text[1]
        
        if column == "TimeControl":
            schema["TC"] = text[1].split("+")[0]

        if column == "Termination":
            schema["Termination"] = text[1]

        if column == "ECO":
            schema["ECO"] = text[1]

        if column == "Result":
            result = text[0]

            if result == '1-0':
                schema["WonBy"] = "White"
            elif result == '1-1':
                schema["WonBy"] = "Tie"
            else:
                schema["WonBy"] = "Black"
        
        if '1. ' in value:
            schema["Game"] = value[:-4]
            
            yield(list(schema.values()))

            
schema = {
    "White":'',
    "Black":'',
    "TC":'',
    "ECO":'',
    "Termination":'',
    "Game": '',
    "WonBy":''
}

#DF containing all the games and relative information
games_df = data.rdd.mapPartitions(lambda partition: parse_match(partition, schema)).toDF((list(schema.keys())))
games_df = games_df.filter("TC > 300").withColumn("ID",F.monotonically_increasing_id())

#ENCODING 1 for win, 0 for lost and 2 for tie

white_df = games_df.withColumnRenamed("White", "Player") \
             .withColumnRenamed("Black", "Opponent") \
             .withColumn("WonBy", F.when(F.col("WonBy") == "White", 1)
                         .when(F.col("WonBy") == "Black", 0).otherwise(2))\
             .withColumnRenamed("WonBy","Result")


black_df = games_df.withColumnRenamed("Black", "Player") \
             .withColumnRenamed("White", "Opponent")\
             .withColumn("WonBy", F.when(F.col("WonBy") == "White", 0)
                         .when(F.col("WonBy") == "Black", 1).otherwise(2))\
             .withColumnRenamed("WonBy","Result")

combined_df = white_df.unionByName(black_df)

#DF containing player and all matches played related info
player_games = combined_df.groupBy("Player").agg(
    F.collect_list("ID").alias("Games_ID"),
    F.collect_list("Opponent").alias("Opponents"),
    F.collect_list("Result").alias("Results")
)


# Write the DataFrame to Parquet
games_df.write.mode("overwrite").parquet(output_path_games)
#DF containing all the played games (by bot and humans)
player_games.write.mode("overwrite").parquet(output_path_player)

spark.stop()