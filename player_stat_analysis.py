"""Script to analyze player statistics from player_stats dataset"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Initialize Spark Session
spark = SparkSession.builder.appName("Lichess Cheat Analysis").getOrCreate()
input_path = f"hdfs:///user/s3262642/stats/player_stats"
output_path = f"hdfs:///user/s3262642/stats/analysis/"

player_stats = spark.read.parquet(input_path)

#Divide players into three skills band and get statistics
band_limit1 = 1100
band_limit2 = 1900

beginners = player_stats.where(player_stats.avg_elo <= band_limit1)\
    .select("win_rate","EloIncrement").summary("50%","75%", "max","mean","stddev")

intermediate = player_stats.where((player_stats.avg_elo <= band_limit2) & (player_stats.avg_elo > band_limit1))\
    .select("win_rate","EloIncrement").summary("50%","75%", "max","mean","stddev")

advanced = player_stats.where(player_stats.avg_elo > band_limit2)\
    .select("win_rate","EloIncrement").summary("50%","75%", "max","mean","stddev")


beginners.write.mode("overwrite").parquet(output_path +"beginners_stat")
intermediate.write.mode("overwrite").parquet(output_path +"intermediate_stat")
advanced.write.mode("overwrite").parquet(output_path +"advanced_stat")


#CHEATER RECOGNITION
games_db_path = "hdfs:///user/s3262642/stats/games_db"

games = spark.read.parquet(games_db_path)

cheaters = games.where(games.Termination == 'Rules infraction')\
    .withColumn("Player", F.when(F.col("WonBy") == "Black",F.col("White"))\
    .otherwise(F.col("Black"))).select("Player").distinct()
                                                                        


#GET LIMITS
#win_rate threshold
win_rate_limit = advanced.filter("summary = '75%'").collect()[0].win_rate
#EloIncrement threshold
elo_incr_limit = advanced.filter("summary = '75%'").collect()[0].win_rate + advanced.filter("summary = 'stddev'").collect()[0].EloIncrement
flagged_player = player_stats.where((player_stats.win_rate>= win_rate_limit) & (player_stats.games_played>10))\
    .orderBy(F.desc("win_rate"))\
    .filter((F.col("EloIncrement") < F.col("games_played")*10) & (F.col("EloIncrement") > F.col("games_played")*6))

flagged_player.write.mode("overwrite").parquet(output_path + "flagged")

cheaters = cheaters.unionByName(flagged_player.select("Player")).distinct()
cheaters.write.mode("overwrite").parquet(output_path+"cheaters")

