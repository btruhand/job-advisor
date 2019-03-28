from pyspark.sql import SparkSession
from pyspark.sql import types
from pyspark.sql import functions as f
from lib.schemas import SchemaDoc2VecTokenize

import re
import sys

# _punctuation_list = list(punctuation)
# _punctuation_list.remove('+') # remove because of things like C++ (probably + is not in normal text
# _eng_stopwords = stopwords.words('english')

# # this is a copy and paste from lib/cleaners because can't get that whole module running
# # on pyspark, so resort to something simpler
# @f.udf(returnType=types.ArrayType(types.StringType()))
# def remove_punctuations_and_stopwords(s):
# 	# split sentences further to individual sentences, then word tokenize
# 	no_bullets_sentences = [sent for sent in sent_tokenize(s)]
# 	keepwords = [w for s in no_bullets_sentences for w in word_tokenize(s) if w not in _eng_stopwords]
# 	keepwords = [word for word in keepwords if word not in _punctuation_list]
# 	return keepwords

# @f.udf(returnType=types.ArrayType(types.StringType()))
# def tokenize_job_titles(title):
#         return title.apply(remove_punctuations_and_stopwords)

_bullets = r'\s*[\u2022|\u25cf|\u27a2|-|*|]\s*'

def main(spark, resume_path, titles_path):
	resume_df = spark.read.json(resume_path, schema=SchemaDoc2VecTokenize.get_doc2vec_tokenize_schema())
	resume_df = resume_df.select('id', f.explode('jobs').alias('indiv_jobs'))
	resume_df = resume_df.select(
		'id',
		resume_df.indiv_jobs.getField('title').alias('job_title'),
		resume_df.indiv_jobs.getField('details').alias('job_details')
	)
	resume_df = resume_df.withColumnRenamed('id', 'resume_id')
	resume_df = resume_df.withColumn('id', f.monotonically_increasing_id())
	exploded_details_df = resume_df.select(
		'id',
		f.explode('job_details').alias('exp_details')
	)
	remove_bullets = exploded_details_df.select('id', f.regexp_replace(f.col('exp_details'), _bullets, ''))

	exploded_details_df.show(truncate=False)
	remove_bullets.show(truncate=False)
	# resume_df.show(truncate=False)

if __name__ == "__main__":
	resume_path = sys.argv[1]
	titles_path = sys.argv[2]

	spark = SparkSession.builder.appName('job title normalize er').getOrCreate()
	spark.sparkContext.setLogLevel('WARN')

	main(spark, resume_path, titles_path)