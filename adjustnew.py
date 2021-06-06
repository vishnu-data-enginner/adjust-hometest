# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
spark = SparkSession.builder.appName('Basics').getOrCreate()

# COMMAND ----------

commits=spark.read.format("csv").load("dbfs:/FileStore/tables/commits-1.csv", header = True)
repos=spark.read.format("csv").load("dbfs:/FileStore/tables/repos-1.csv", header = True)
events=spark.read.format("csv").load("dbfs:/FileStore/tables/events-1.csv", header = True)
actors=spark.read.format("csv").load("dbfs:/FileStore/tables/actors-1.csv", header = True)

# COMMAND ----------

eventrenamed = events.withColumnRenamed("id", "eventid").filter('type == "PushEvent"')
pushevent = events.withColumnRenamed("id", "eventid").filter('type == "PushEvent"')
actorjoinevents = actors.join(eventrenamed, actors.id == eventrenamed.actor_id , "inner").drop(eventrenamed.actor_id)

eventsjoincommits = eventrenamed.join(commits, eventrenamed.eventid == commits.event_id, "fullouter")

repojoincommits = repos.join(pushevent, repos.id == pushevent.repo_id,"inner").drop(pushevent.repo_id)


# COMMAND ----------

##Top 10 active users by amount of PRs created and commits pushed (username, PRs count, commits count)
from pyspark.sql.functions import col,when,count
df3 = actors.join(events, actors.id == events.actor_id, "inner").drop(events.id)
df3.select("username","type").groupBy("username").agg(count(when(col('type')=="PullRequestEvent",True)).alias('PRs count'),count(when(col('type')=="PushEvent",True)).alias('commits count')).orderBy(col('PRs Count').desc(),col('commits count').desc()).limit(10).show()


# COMMAND ----------

##Top 10 repositories by amount of commits pushed (repo name, commits count)
win = Window.partitionBy("name").orderBy(repojoincommits['type'].desc())
df = repojoincommits.select("name","type", rank().over(win).alias('commitscount')).filter(col('commitscount') <= 20).groupBy(("name")).agg(count("type").alias("commits count"))
df.orderBy(df['commits count'].desc()).show(10)

# COMMAND ----------

##Top 10 repositories by amount of watch events (repo name, watch events count)
win = Window.partitionBy("name").orderBy(repojoincommits['type'].desc())
df = repojoincommits.select("name","type", rank().over(win).alias('commitscount')).filter(col('commitscount') <= 20).groupBy(("name")).agg(count("type").alias("watch events count"))
df.orderBy(df['watch events count'].desc()).show(10)
