from pyspark.sql import types
	
class SchemaER:
	@staticmethod
	def get_resume_schema():
		return types.StructType([
			types.StructField('id', types.StringType(), nullable=False),
			types.StructField('title', types.ArrayType(types.StringType()), nullable=False)
		])

	@staticmethod
	def get_title_schema():
		return types.StructType([
			types.StructField('O*NET-SOC Code', types.StringType(), nullable=False),
			types.StructField('Title', types.StringType(), nullable=False),
			types.StructField('Alternate Title', types.ArrayType(types.StringType()), nullable=False),
		])
	
class SchemaCheckingER:
	@staticmethod
	def get_er_schema():
		return types.StructType([
			types.StructField('id1', types.LongType(), nullable=False),
			types.StructField('id2', types.LongType(), nullable=False),
			types.StructField('jaccard', types.FloatType(), nullable=False)
		])
	
	@staticmethod
	def get_resume_er_schema():
		return types.StructType([
			types.StructField('id', types.LongType(), nullable=False),
			types.StructField('resume_id', types.StringType(), nullable=False),
			types.StructField('job_titles', types.StringType(), nullable=False)
		])
	
	@staticmethod
	def get_title_er_schema():
		return types.StructType([
			types.StructField('id', types.LongType(), nullable=False),
			types.StructField('O*NET-SOC Code', types.StringType(), nullable=False),
			types.StructField('Title', types.StringType(), nullable=False),
			types.StructField('Alternate Title', types.ArrayType(types.StringType()), nullable=False),
		])
