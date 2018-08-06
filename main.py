# Using the Github events dataset
# Please provide the code for the following tasks

# Q1:
# Write an Apache Spark application in Python/Scala that reads the Github events json,
# extracts every events that has as type==PullRequestEvent

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import logging

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger('Spark Exercise - Main File')

spark = SparkSession.builder\
	.appName("Spark Exercise")\
	.config("spark.some.config.option", "some-value")\
	.enableHiveSupport()\
	.getOrCreate()


df = spark.read.json("~/Pyspark-SmallExercises/2017-10-01-10.json")

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
	F.col("created_at"),
	F.col("repo.name").alias("repo_name"), 
	F.col("actor.login").alias("username"), 
	F.col("payload.pull_request.user.login").alias("pr_username"), 
	F.col("payload.pull_request.created_at").alias("pr_created_at"), 
	F.col("payload.pull_request.head.repo.language").alias("pr_repo_language"))

q2_df.show(20)

# Q3:
# For each event, add another field, called pr_repo_language_type, based on the following criteria:
# Procedural -> Basic, C
# Object Oriented -> C#, C++, Java, Python,
# Functional -> Lisp, Haskell, Scala
# Data Science -> R, Jupyter Notebook, Julia
# Others -> contains all the other languages that are not mention above

q3_df = q2_df.withColumn('pr_repo_language_type',
                         F.when((F.col('pr_repo_language') == 'Basic') | (F.col('pr_repo_language') == 'C'),
                                F.lit('Procedural')).otherwise(F.when(
                             (F.col('pr_repo_language') == 'C#') | (F.col('pr_repo_language') == 'C++') | (
                             F.col('pr_repo_language') == 'Java') | (F.col('pr_repo_language') == 'Python'),
                             F.lit('Object Oriented')).otherwise(F.when(
                             (F.col('pr_repo_language') == 'Lisp') | (F.col('pr_repo_language') == 'Haskell') | (
                             F.col('pr_repo_language') == 'Scala'), F.lit('Functional')).otherwise(F.when(
                             (F.col('pr_repo_language') == 'R') | (F.col('pr_repo_language') == 'Jupyter Notebook') | (
                             F.col('pr_repo_language') == 'Julia'), F.lit('Data Science')).otherwise(F.lit(None))))))

q3_df.show(30)