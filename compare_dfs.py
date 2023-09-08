
from pyspark.sql import SparkSession
from gresearch.spark.diff import *

#https://github.com/G-Research/spark-extension/blob/master/DIFF.md
spark = SparkSession.builder.appName('grawp').getOrCreate()
df1 = spark.read.format("csv").option("header", "true").load("location_details.csv")
df2 = spark.read.format("csv").option("header", "true").load("location_details1.csv")

map = [('yoe_yrs','yoe')]

options = DiffOptions().with_change_column('changes')
df1.diff_with_options(df2, options, 'id').show()
