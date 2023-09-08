
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from functools import reduce
from pyspark.sql.functions import lit
import pyspark.sql.functions as F

# check TODO_spark.todo

spark = SparkSession.builder.appName('grawp').getOrCreate()
df1 = spark.read.format("csv").option("header", "true").load("location_details.csv")
df2 = spark.read.format("csv").option("header", "true").load("location_details1.csv")

map = [('yoe_yrs','yoe')]

def applymap(col):
  col1 = col2 = col
  if any(v[0] == col or v[1] == col for v in map):
    difcol = [x for x in (set(map) - set(col))][0]
    col1 = difcol[0]
    col2 = difcol[1]

  return col1, col2

def find_mismatch(df1, df2, key_list):

  def is_mismatch(col):
    col1, col2 = applymap(col)  
    values = (df1[col1] != df2[col2])
           # if( abs(df1[col] - df2[col] > EPSILON))
    return values | (df1[col1].isNull() & df2[col2].isNotNull()) | (df1[col1].isNotNull() & df2[col2].isNull())
  
  def answer1(col):
    col1, col2 = applymap(col)
    return df1[col1]
  
  def answer2(col):
    col1, col2 = applymap(col)
    return df2[col2]

  preresult = df1.join(df2, key_list, "outer")
  preresult.show()
  result = preresult.select(*[preresult[i] for i in key_list], *[is_mismatch(i).alias(i + '_mismatch')
                                                          for i in df1.columns if i not in key_list])
  result.show() #for debug purposes only
  mismatched_cols = result.where(reduce(lambda acc, e:acc | e, [result[c+ '_mismatch'] for c in df1.columns if c not in key_list], lit(False)))
  
  mismatched_cols.show()

  #final_result = mismatched_cols.select(*[mismatched_cols[i] for i in key_list], *[answer1(i)  
  #                                                        for i in df1.columns if i not in key_list])

  #final_result.show()

  mismatch_count = mismatched_cols.count()
  return mismatch_count, mismatched_cols

mismatch_count, mismatches = find_mismatch(df1, df2, ["id"])
print(mismatch_count)
mismatches.show()