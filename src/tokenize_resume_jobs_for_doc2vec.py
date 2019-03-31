import spacy
import en_core_web_md
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

# check https://en.wikipedia.org/wiki/Bullet_(typography) for some bullet unicodes
_bullets = r'\s*[\u2022|\u25cf|\u27a2|\u2023|\u25e6|\u2043|-|*|]\s*'

nlp = None

def tokenize_string_details(s):
	global nlp
	if nlp is None:
		# not doing it here causes some errors about en_models.vector not being able to be loaded
		# essentially this causes the models to be loaded at most once in the executors
		# not very good, but it will do for this sie
		nlp = en_core_web_md.load()
	doc = nlp(s)
	tokens = []
	for token in doc:
		if token.ent_type_ in ['GPE', 'LOC', 'DATE', 'TIME']:
			tokens.append(f'-{token.ent_type}-')
		elif token.like_url:
			tokens.append(f'-URL-')
		else:
			tokens.append(token.lower_)
	return tokens

def tokenize_task_statements(task_statement_path):
	task_statement_df = pd.read_csv(task_statement_path)
	task_statement_df = task_statement_df.rename({'O*NET-SOC Code': 'onet_job_id', 'Title': 'onet_job_title', 'Task': 'task'}, axis='columns')
	task_statement_df = task_statement_df.groupby(['onet_job_id', 'onet_job_title']).task.apply(lambda s: ' '.join(s)).to_frame()
	task_statement_df.task = task_statement_df.task.apply(tokenize_string_details)
	task_statement_df.to_csv('onet/tokenized_onet_occupation_task_statements.csv')

def tokenize_resume_jobs(spark, resume_path):
	resume_df = spark.read.json(resume_path, schema=SchemaDoc2VecTokenize.get_doc2vec_tokenize_resume_schema())
	resume_df = resume_df.select('id', f.explode('jobs').alias('indiv_jobs'))
	resume_df = resume_df.select(
		'id',
		resume_df.indiv_jobs.getField('title').alias('job_title'),
		resume_df.indiv_jobs.getField('details').alias('job_details')
	)
	resume_df.show(truncate=False)
	resume_df = resume_df.withColumnRenamed('id', 'resume_id')
	joined_details_df = resume_df.select(
		'resume_id',
		'job_title',
		f.concat_ws(' ', f.col('job_details')).alias('joined_details')
	)
	removed_bullets_df = joined_details_df.select('resume_id', 'job_title', f.regexp_replace(f.col('joined_details'), _bullets, ' ').alias('non_bulleted_details'))
	removed_bullets_df.show(truncate=False)
	tokenizer = f.udf(tokenize_string_details, returnType=types.ArrayType(types.StringType()))
	tokenized_details_df = removed_bullets_df.select('resume_id', 'job_title', tokenizer('non_bulleted_details').alias('tokenized_details'))
	# keep only those that have more than 5 tokens (arbitrarily chosen breakpoint roughly 10% of the average job tokens)
	tokenized_details_df = tokenized_details_df.where(f.size('tokenized_details') > 5)
	tokenized_details_df.write.json('tokenized_job_details_for_doc2vec', compression='gzip', mode='overwrite')
	# resume_df.show(truncate=False)

if __name__ == "__main__":
	data_path = sys.argv[1]
	type = sys.argv[2]


	if type == "resume_jobs":
		print("Tokenizing Resume Jobs (Spark Job)")
		from pyspark.sql import SparkSession
		from pyspark.sql import types
		from pyspark.sql import functions as f
		from lib.schemas import SchemaDoc2VecTokenize

		spark = SparkSession.builder.appName('job details and tasks tokenization').getOrCreate()
		spark.sparkContext.setLogLevel('WARN')
		tokenize_resume_jobs(spark, data_path)
		spark.stop()
		print("Finished")
	elif type == "task_statements":
		import pandas as pd
		print("Tokenizing Task Statements (Pandas)")
		tokenize_task_statements(data_path)
		print("Finished")
	else:
		sys.stderr.write("Not a recognizable task type\n")
		sys.exit(1)		