# Using the Github events dataset
# Please provide the code for the following tasks

# Q1:
# Write an Apache Spark application in Python/Scala that reads the Github events json,
# extracts every events that has as type==PullRequestEvent

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import *
import logging

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger('Spark Exercise - Main File')

spark = SparkSession.builder\
	.appName("Spark Exercise")\
	.config("spark.some.config.option", "some-value")\
	.enableHiveSupport()\
	.getOrCreate()


df = spark.read.json("/Users/oguzhanyediel/Desktop/air_desktop/CS_tazi.io/Pyspark-SmallExercises/2017-10-01-10.json")

logger.info("Let's take a look at the general schema.")
df.printSchema()

logger.info("Initial dataframe:")
df.show(10)

df.createOrReplaceTempView("rawTable")
q1_sql_statement = """SELECT * FROM rawTable WHERE type = 'PullRequestEvent'"""
q1_df = spark.sql(q1_sql_statement)
q1_df.show(10)

# Q2:
# Clean the dataset, selecting only these fields for each event: 
# created_at as created_at, repo.name as repo_name, actor.login as username, 
# payload.pull_request.user.login as pr_username
# payload.pull_request.created_at as pr_created_at, 
# payload.pull_request.head.repo.language as pr_repo_language

q2_df = q1_df.select(
	col("created_at"),
	col("repo.name").alias("repo_name"), 
	col("actor.login").alias("username"), 
	col("payload.pull_request.user.login").alias("pr_username"), 
	col("payload.pull_request.created_at").alias("pr_created_at"), 
	col("payload.pull_request.head.repo.language").alias("pr_repo_language"))

q2_df.show(20)