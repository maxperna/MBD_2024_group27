from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import FloatType, StructType, StructField
import re
from statistics import stdev,mean, StatisticsError
from datetime import datetime

"""Function to parse a clock instant string into a datetime object
@type time_str: str
@param time_str: textual clock instant to parse
@return: a datetime object
"""
def parse_clock(time_str):
    return datetime.strptime(time_str, '%H:%M:%S')

"""Return the differnce in seconds of two consecutives time instants
@type clock_list: list
@param clock_list: list of clock instance
@returns: a list of delta in seconds
"""
def sec_difference(clock_list):
    delta_list = []
    
    for i in range(1,len(clock_list)):
        difference = abs(clock_list[i] - clock_list[i-1])
        delta_list.append(difference.total_seconds())
    
    return delta_list 

"""Function to calculate the variance
@type time_list: list
@param time_list: contains a list of time of whom calculate the variance
@returns: a tuple containing mean and variacne
"""
def get_summary(time_list):
    try:
        mean_ = mean(time_list)
        var = stdev(time_list)
    except StatisticsError:
        var = 0
        mean_ = 0
    
    return (mean_,var)

"""Function to analize a game
@type game: str
@param game: textual file containing the annotated game
@return: a tuple containing both players summary tuple (mean,variance)
"""
def time_exctractor(game):
    clock_pattern = re.compile(r'%clk (\d+:\d+:\d+)')

    matches = re.finditer(clock_pattern, game)

    #Creating time list, adding a 0 at the beginning to perfrom subtraction after
    times = [parse_clock(match.group(1)) for match in matches]
    delta_list_white = sec_difference(times[::2])
    delta_list_black = sec_difference(times[1::2])

    print(delta_list_black)
    print(delta_list_white)

    white_data = get_summary(delta_list_white)
    black_data = get_summary(delta_list_black)

    return (white_data,black_data)


spark = SparkSession.builder.appName("Lichess Cheat Analysis").getOrCreate()

input_path = f"hdfs:///user/s3262642/stats/"
output_path = f("hdfs:///user/s3262642/stats/games/")

#Dataframe
bot_df = spark.read.parquet(input_path + "bot_df")
player_games = spark.read.parquet(input_path + "all_players_games_db")

#Join the df to get all the games played by bot 
bot_games = bot_df.join(player_games, on="Player")
#Remove all the games played by bot using ANTI-JOIN
player_games = player_games.join(bot_df,on="Player",how="anti")

bot_games.write.mode("overwrite").parquet(output_path + "bot_games_db")
player_games.write.mode("overwrite").parquet(output_path + "human_games_db")

#39040 games played by bot
#15649042 played by human

#Schema
schema = StructType([
    StructField("white_data", StructType([
        StructField("white_mean", FloatType(), False),  # Non-nullable field
        StructField("white_stddev", FloatType(), False)   # Non-nullable field
    ]), False),  # Non-nullable struct field
    StructField("black_data", StructType([
        StructField("black_mean", FloatType(), False),  # Non-nullable field
        StructField("black_stddev", FloatType(), False)   # Non-nullable field
    ]), False)  # Non-nullable struct field
])

variance_extractor = F.udf(time_exctractor,schema)

games_df = spark.read.parquet("hdfs:///user/s3262642/stats/games_db")
games_df = games_df.filter("TC = 600")

exploded_bot_df = bot_games.drop("Opponents","Results").withColumn("Games_ID", F.explode("Games_ID"))
result_df = exploded_bot_df.join(games_df, exploded_bot_df.Games_ID == games_df.ID)

df_with_stddev = result_df.withColumn("data", variance_extractor(result_df["Game"]))\
    .selectExpr("Games_ID", 
                "TC", 
                "data.white_data.white_mean AS white_mean",
                "data.white_data.white_stddev AS white_stddev",
                "data.black_data.black_mean AS black_mean",
                "data.black_data.black_stddev AS black_stddev")


df_with_stddev.show(5)
df_with_stddev.write.mode("overwrite").parquet(output_path + "bot_variance")


#Flagged players
flagged = spark.read.parquet("hdfs:///user/s3262642/stats/analysis/actual_cheaters")


flagged_games_ID = flagged.join(player_games,on="Player").drop("Opponents","Results").withColumn("Games_ID", F.explode("Games_ID"))
not_flagged_games_ID = player_games.join(flagged,on="Player",how="anti").drop("Opponents","Results").withColumn("Games_ID", F.explode("Games_ID"))


flagged_games = flagged_games_ID.join(games_df,flagged_games_ID.Games_ID == games_df.ID)
not_flagged_games = not_flagged_games_ID.join(games_df,not_flagged_games_ID.Games_ID == games_df.ID)

flagged_with_stddev = flagged_games.withColumn("data", variance_extractor(result_df["Game"]))\
    .withColumn("mean",F.when(F.col("White")==F.col("Player"),F.col("data.white_data.white_mean")).otherwise(F.col("data.black_data.black_mean")))\
    .withColumn("stddev",F.when(F.col("White")==F.col("Player"),F.col("data.white_data.white_stddev")).otherwise(F.col("data.black_data.black_stddev")))\
    .select("Games_ID","mean","stddev")
                

not_flagged_games= not_flagged_games.withColumn("data", variance_extractor(result_df["Game"]))

not_flagged_games = not_flagged_games.withColumn("data", variance_extractor(result_df["Game"]))\
    .withColumn("mean",F.when(F.col("White")==F.col("Player"),F.col("data.white_data.white_mean")).otherwise(F.col("data.black_data.black_mean")))\
    .withColumn("stddev",F.when(F.col("White")==F.col("Player"),F.col("data.white_data.white_stddev")).otherwise(F.col("data.black_data.black_stddev")))\
    .select("Games_ID","mean","stddev")


flagged_with_stddev.write.mode("overwrite").parquet(output_path + "flagged_variance")
not_flagged_games.write.mode("overwrite").parquet(output_path + "not_flagged_variance")

#Stat differientation
input_path = "hdfs:///user/s3262642/stats/player_stats"
players_stats_path = "hdfs:///user/s3262642/stats/player_stats"

player_stats = spark.read.parquet(players_stats_path).filter("games_played > 10")


cheaters_stat = flagged.join(player_stats,on="Player").drop("games_played","avg_opponent_elo")
normal_stat = player_stats.join(flagged,on="Player",how="anti").drop("games_played","avg_opponent_elo")

print(f"The number of cheaters is {cheaters_stat.count()}")
print(f"The number of non cheaeters is {normal_stat.count()}")

cheaters_stat = cheaters_stat.select("win_rate","avg_elo","EloIncrement").summary("25%","50%","75%","mean","stddev")
normal_stat = normal_stat.select("win_rate","avg_elo","EloIncrement").summary("25%","50%","75%","mean","stddev")

normal_stat.write.mode("overwrite").parquet("hdfs:///user/s3262642/analysis/normal_stats")
cheaters_stat.write.mode("overwrite").parquet("hdfs:///user/s3262642/analysis/cheaters_stats")

normal_stat.show()
cheaters_stat.show()

