from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Initialize Spark Session
spark = SparkSession.builder.appName("Lichess Cheat Analysis").getOrCreate()

#PATH
input_path = f"hdfs:///user/s3262642/lichess_db_standard_rated_2023-05.pgn"
output_path = f"hdfs:///user/s3262642/stats/player_stats"
debug_path =  f"hdfs:///user/s3262642/debug/"

#UNCOMMENT TO DEBUG
#hdfs_pgn_file = f"hdfs:///user/s3262642/pgn_test"

data = spark.read.option("delimiter", "[Event").text(input_path)



def reformat(partition, schema: dict):
    for row in partition:
        value = row.value
        if '"' in value:
            text = value.split('"')
            column = text[0][1:].replace(' ', '')
            if column in schema.keys():
                record = text[1]
                if column in ('WhiteElo', 'BlackElo', 'WhiteRatingDiff', 'BlackRatingDiff'):
                    try:
                        record = int(record)
                    except ValueError:
                        pass
                elif column == 'TimeControl':
                    try:
                        record = int(record.split('+')[0])
                    except ValueError:
                        pass
                schema[column] = record
            elif column == "WhiteTitle":
                    if text[1] == "BOT":
                        schema["IsBotW"] = True
            elif column == "BlackTitle":
                    if text[1] == "BOT":
                        schema["IsBotB"] = True

        if '1. ' in value:
            yield list(schema.values())
            #Reset Values
            schema["IsBotB"] = False
            schema["IsBotW"] = False


schema = {
        'White': '',
        'Black': '',
        'Result': '',
        'IsBotW': False,
        'IsBotB' : False,
        'WhiteElo': 0,
        'BlackElo': 0,
        'WhiteRatingDiff': 0,
        'BlackRatingDiff': 0,
        'TimeControl': 0,
    }

df = data.rdd.mapPartitions(lambda partition: reformat(partition, schema)).toDF(list(schema.keys()))

# print(df.select("White").distinct().count())

white_df = df.withColumnRenamed("White", "Player") \
             .withColumnRenamed("Black", "Opponent") \
             .withColumnRenamed("WhiteElo", "PlayerElo") \
             .withColumnRenamed("BlackElo", "OpponentElo") \
             .withColumnRenamed("WhiteRatingDiff", "RatingDiff") \
             .withColumnRenamed("IsBotW", "IsBot") \
             .withColumnRenamed("BlackRatingDiff", "OpponentRatingDiff").drop("IsBotB")   #Invert result and drop black title 


black_df = df.withColumnRenamed("Black", "Player") \
             .withColumnRenamed("White", "Opponent") \
             .withColumnRenamed("BlackElo", "PlayerElo") \
             .withColumnRenamed("WhiteElo", "OpponentElo") \
             .withColumnRenamed("BlackRatingDiff", "RatingDiff") \
             .withColumnRenamed("WhiteRatingDiff", "OpponentRatingDiff") \
             .withColumnRenamed("IsBotB", "IsBot") \
             .withColumn("Result", F.when(F.col("Result") == "1-0", "0-1")
                         .when(F.col("Result") == "1-1", "1-1").otherwise("1-0")).drop("IsBotW") 
                         

white_df = white_df.filter("TimeControl > 300")
black_df = black_df.filter("TimeControl > 300")

# Merge white and black
combined_df = white_df.unionByName(black_df)

#Separe bot from player
bot_df = combined_df.filter("IsBot = True").select("Player").distinct()
player_df = combined_df.filter("IsBot = False").drop("IsBot")

#Output stats (uncomment if necessary )
#combined_df.write.mode("overwrite").parquet(debug_path + "debug_combined")
#player_df.write.mode("overwrite").parquet(debug_path + "debug_player")

bot_df.write.mode("overwrite").parquet(output_path + "bot_df")


# New schema for players
player_metrics = player_df.groupBy("Player").agg(
    F.count("Player").alias("games_played"),
    F.round(F.avg(F.when(F.col("Result") == "1-0", 1).otherwise(0)),2).alias("win_rate"),
    F.round(F.avg(F.col("PlayerElo")),2).alias("avg_elo"),
    F.round(F.avg(F.col("OpponentElo")),2).alias("avg_opponent_elo"),
    F.sum("RatingDiff").alias("EloIncrement")
)

player_metrics.write.mode("overwrite").parquet(output_path)

# Stop the Spark session
spark.stop()
