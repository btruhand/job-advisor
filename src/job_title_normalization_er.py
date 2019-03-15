from pyspark.sql import SparkSession
from pyspark.sql import types
import sys

def get_resume_schema():
	return types.StructType([
		types.StructField('id', types.StringType(), nullable=False),
		types.StructField('summary', types.ArrayType(types.StringType()), nullable=True)
		types.StructField('additional', types.ArrayType(types.StringType())), nullable=True)
		types.StructField('skill', types.ArrayType(types.StringType()))
	])

def main(spark, resume_path, titles_path):
	resume_df = spark.read.json(resume_path)

if __name__ == "__main__":
	resume_path = sys.argv[1]
	titles_path = sys.argv[2]

	spark = SparkSession.builder.appName('job title normalize er').getOrCreate()