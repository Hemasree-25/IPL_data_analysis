# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("IPL Data Analysis").getOrCreate()

# COMMAND ----------

spark

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.window import *

# COMMAND ----------

from pyspark.sql.types import * 

ball_by_ball_schema = StructType([
    StructField("match_id", IntegerType(), True),
    StructField("over_id", IntegerType(), True),
    StructField("ball_id", IntegerType(), True),
    StructField("innings_no", IntegerType(), True),
    StructField("team_batting", StringType(), True),
    StructField("team_bowling", StringType(), True),
    StructField("striker_batting_position", IntegerType(), True),
    StructField("extra_type", StringType(), True),
    StructField("runs_scored", IntegerType(), True),
    StructField("extra_runs", IntegerType(), True),
    StructField("wides", IntegerType(), True),
    StructField("legbyes", IntegerType(), True),
    StructField("byes", IntegerType(), True),
    StructField("noballs", IntegerType(), True),
    StructField("penalty", IntegerType(), True),
    StructField("bowler_extras", IntegerType(), True),
    StructField("out_type", StringType(), True),
    StructField("caught", BooleanType(), True),
    StructField("bowled", BooleanType(), True),
    StructField("run_out", BooleanType(), True),
    StructField("lbw", BooleanType(), True),
    StructField("retired_hurt", BooleanType(), True),
    StructField("stumped", BooleanType(), True),
    StructField("caught_and_bowled", BooleanType(), True),
    StructField("hit_wicket", BooleanType(), True),
    StructField("obstructingfeild", BooleanType(), True),
    StructField("bowler_wicket", BooleanType(), True),
    StructField("match_date", DateType(), True),
    StructField("season", IntegerType(), True),
    StructField("striker", IntegerType(), True),
    StructField("non_striker", IntegerType(), True),
    StructField("bowler", IntegerType(), True),
    StructField("player_out", IntegerType(), True),
    StructField("fielders", IntegerType(), True),
    StructField("striker_match_sk", IntegerType(), True),
    StructField("strikersk", IntegerType(), True),
    StructField("nonstriker_match_sk", IntegerType(), True),
    StructField("nonstriker_sk", IntegerType(), True),
    StructField("fielder_match_sk", IntegerType(), True),
    StructField("fielder_sk", IntegerType(), True),
    StructField("bowler_match_sk", IntegerType(), True),
    StructField("bowler_sk", IntegerType(), True),
    StructField("playerout_match_sk", IntegerType(), True),
    StructField("battingteam_sk", IntegerType(), True),
    StructField("bowlingteam_sk", IntegerType(), True),
    StructField("keeper_catch", BooleanType(), True),
    StructField("player_out_sk", IntegerType(), True),
    StructField("matchdatesk", DateType(), True)
])

# COMMAND ----------

ball_by_ball_df = spark.read.schema(ball_by_ball_schema).option("header", True).csv("s3://ipl-data-analysis-project/Ball_By_Ball.csv")
     

# COMMAND ----------

match_schema = StructType([
    StructField("match_sk", IntegerType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("team1", StringType(), True),
    StructField("team2", StringType(), True),
    StructField("match_date", DateType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("venue_name", StringType(), True),
    StructField("city_name", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("toss_winner", StringType(), True),
    StructField("match_winner", StringType(), True),
    StructField("toss_name", StringType(), True),
    StructField("win_type", StringType(), True),
    StructField("outcome_type", StringType(), True),
    StructField("manofmach", StringType(), True),
    StructField("win_margin", IntegerType(), True),
    StructField("country_id", IntegerType(), True)
])

match_df = spark.read.schema(match_schema).option("header", True).csv("s3://ipl-data-analysis-project/Match.csv")

# COMMAND ----------

player_schema = StructType([
    StructField("player_sk", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True)
])

player_df = spark.read.schema(player_schema).csv("s3://ipl-data-analysis-project/Player.csv", header = True)

# COMMAND ----------

player_match_schema = StructType([
    StructField("player_match_sk", IntegerType(), True),
    StructField("playermatch_key", DecimalType(), True),
    StructField("match_id", IntegerType(), True),
    StructField("player_id", IntegerType(), True),
    StructField("player_name", StringType(), True),
    StructField("dob", DateType(), True),
    StructField("batting_hand", StringType(), True),
    StructField("bowling_skill", StringType(), True),
    StructField("country_name", StringType(), True),
    StructField("role_desc", StringType(), True),
    StructField("player_team", StringType(), True),
    StructField("opposit_team", StringType(), True),
    StructField("season_year", IntegerType(), True),
    StructField("is_manofthematch", BooleanType(), True),
    StructField("age_as_on_match", IntegerType(), True),
    StructField("isplayers_team_won", BooleanType(), True),
    StructField("batting_status", StringType(), True),
    StructField("bowling_status", StringType(), True),
    StructField("player_captain", StringType(), True),
    StructField("opposit_captain", StringType(), True),
    StructField("player_keeper", StringType(), True),
    StructField("opposit_keeper", StringType(), True)
])

player_match_df = spark.read.schema(player_match_schema).csv("s3://ipl-data-analysis-project/Player_match.csv", header = True)

# COMMAND ----------

team_schema = StructType([
    StructField("team_sk", IntegerType(), True),
    StructField("team_id", IntegerType(), True),
    StructField("team_name", StringType(), True)
])

team_df = spark.read.schema(team_schema).csv("s3://ipl-data-analysis-project/Team.csv", header = True)

# COMMAND ----------

#filter out no balls and wides
ball_by_ball_df = ball_by_ball_df.filter((col("wides")==0) & (col("noballs")==0))

#total and avg runs scored in each match and inning
total_and_avg_runs = ball_by_ball_df.groupBy("match_id", "innings_no")\
    .agg(sum("runs_scored").alias("total_runs"),
         avg("runs_scored").alias("avg_runs"))\
             .orderBy("match_id", "innings_no")
    
total_and_avg_runs.show()

# COMMAND ----------

#Window Function: calculating running total of runs in each match for each over
windowspec = Window.partitionBy("match_id", "innings_no").orderBy("over_id")

ball_by_ball_df = ball_by_ball_df.withColumn("running_total_runs", sum("runs_scored").over(windowspec))

# COMMAND ----------

#Conditional column: flag for high impact balls (either a wicket or more than 6 runs)
ball_by_ball_df = ball_by_ball_df.withColumn(
    "high_impact",
    when((col("runs_scored")+col("extra_runs") > 6) | (col("bowler_wicket") == True), True).otherwise(False)
)

# COMMAND ----------

ball_by_ball_df.show(5)

# COMMAND ----------

from pyspark.sql.functions import *

#extracting year, month, day
match_df = match_df.withColumn("year", year("match_date"))\
    .withColumn("month", month("match_date"))\
        .withColumn("day", dayofmonth("match_date")) 

#categorizing through win margin
match_df = match_df.withColumn(
    "win_margin_category",
    when(col("win_margin")>=100, "high")
    .when((col("win_margin")>=50) & (col("win_margin")<100), "medium")
    .otherwise("low")
)

# impact of toss
match_df = match_df.withColumn(
    "toss_match_winner",
    when(col("toss_winner") == col("match_winner"), "Yes").otherwise("No"))

match_df.show(2)

# COMMAND ----------

# Normalize and clean player names
player_df = player_df.withColumn("player_name", lower(regexp_replace("player_name", "[^a-zA-Z0-9 ]", "")))

# Handle missing values in 'batting_hand' and 'bowling_skill' with a default 'unknown'
player_df = player_df.na.fill({"batting_hand": "unknown", "bowling_skill": "unknown"})

# Categorizing players based on batting hand
player_df = player_df.withColumn(
    "batting_style",
    when(col("batting_hand").contains("left"), "Left-Handed").otherwise("Right-Handed")
)

# Show the modified player DataFrame
player_df.show(2)

# COMMAND ----------

# Add a 'veteran_status' column based on player age
player_match_df = player_match_df.withColumn(
    "veteran_status",
    when(col("age_as_on_match") >= 35, "Veteran").otherwise("Non-Veteran")
)

# Dynamic column to calculate years since debut
player_match_df = player_match_df.withColumn(
    "years_since_debut",
    (year(current_date()) - col("season_year"))
)

# Show the enriched DataFrame
display(player_match_df)


# COMMAND ----------

ball_by_ball_df.createOrReplaceTempView("ball_by_ball")
match_df.createOrReplaceTempView("match")
player_df.createOrReplaceTempView("player")
player_match_df.createOrReplaceTempView("player_match")
team_df.createOrReplaceTempView("team")

# COMMAND ----------

player_match_df.columns

# COMMAND ----------

ball_by_ball_df.columns

# COMMAND ----------

# MAGIC %sql
# MAGIC --top_scoring_batsmen_per_season
# MAGIC SELECT 
# MAGIC p.player_name,
# MAGIC m.season_year,
# MAGIC SUM(b.runs_scored) AS total_runs 
# MAGIC FROM ball_by_ball b
# MAGIC INNER JOIN match m ON b.match_id = m.match_id   
# MAGIC INNER JOIN player_match pm ON m.match_id = pm.match_id AND b.striker = pm.player_id     
# MAGIC INNER JOIN player p ON p.player_id = pm.player_id
# MAGIC GROUP BY p.player_name, m.season_year
# MAGIC ORDER BY m.season_year, total_runs DESC
# MAGIC LIMIT 10
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --economical_bowlers_powerplay 
# MAGIC SELECT 
# MAGIC p.player_name, 
# MAGIC AVG(b.runs_scored) AS avg_runs_per_ball, 
# MAGIC COUNT(b.bowler_wicket) AS total_wickets
# MAGIC FROM ball_by_ball b
# MAGIC JOIN player_match pm ON b.match_id = pm.match_id AND b.bowler = pm.player_id
# MAGIC JOIN player p ON pm.player_id = p.player_id
# MAGIC WHERE b.over_id <= 6
# MAGIC GROUP BY p.player_name
# MAGIC HAVING COUNT(*) >= 1
# MAGIC ORDER BY avg_runs_per_ball, total_wickets DESC
# MAGIC LIMIT 10
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --toss_impact_individual_matches
# MAGIC SELECT m.match_id, m.toss_winner, m.toss_name, m.match_winner,
# MAGIC        CASE WHEN m.toss_winner = m.match_winner THEN 'Won' ELSE 'Lost' END AS match_outcome
# MAGIC FROM match m
# MAGIC WHERE m.toss_name IS NOT NULL
# MAGIC ORDER BY m.match_id

# COMMAND ----------

# MAGIC %sql
# MAGIC --average_runs_in_wins
# MAGIC SELECT p.player_name, AVG(b.runs_scored) AS avg_runs_in_wins, COUNT(*) AS innings_played
# MAGIC FROM ball_by_ball b
# MAGIC JOIN player_match pm ON b.match_id = pm.match_id AND b.striker = pm.player_id
# MAGIC JOIN player p ON pm.player_id = p.player_id
# MAGIC JOIN match m ON pm.match_id = m.match_id
# MAGIC WHERE m.match_winner = pm.player_team
# MAGIC GROUP BY p.player_name
# MAGIC ORDER BY avg_runs_in_wins ASC
# MAGIC LIMIT 10
# MAGIC

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


