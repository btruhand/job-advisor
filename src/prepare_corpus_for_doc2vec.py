"""Preparing corpus for Doc2Vec with Gensim

There were difficulties to run Gensim by loading the data directly as a corpus, so instead here
we prepared a TaggedLineDocument for Gensim.

This requires the preprocessed files from ER step which you can find here: https://drive.google.com/drive/folders/1Doh6YKgAri9OV8xhEVTKJzNQ_-Kl5S03
The files you need are:
- er_resume_preprocessed

Note that cleaning dataset cleaning and tokenization is/was done with NLTK stopwords and punctuations.

We may consider using ScaPy in the near future

Usage:
	python -m src.prepare_corpus_for_doc2vec <directory path to er_resume_preprocessed>

	iPython is suggested for speed
"""

from lib.load_data import read_json_from_directory
from lib.cleaners import sentence_tokens_without_punctuations
import pandas as pd
import numpy as np
import sys
import os

def main(preprocessed_resume_dir):
	occupation_data = pd.read_csv('onet/onet_occupation_data.csv')
	preprocessed_resumes = read_json_from_directory(preprocessed_resume_dir, lines=True, compression='gzip')

	for idx, df in enumerate(preprocessed_resumes):
		print("Number of entries in preprocessed resumes #%s is %d" % (idx, len(df)))
		df = df[df['job_details'].str.len() > 0]
		print("Number of entries after empty job details is %d" % len(df))
		preprocessed_resumes[idx] = df

	corpus = occupation_data['Description'].apply(lambda s: sentence_tokens_without_punctuations(s.lower()))
	id_mapping = occupation_data['O*NET-SOC Code'].rename('id')

	job_details = [df['job_details'] for df in preprocessed_resumes]
	id_maps = [df['id'] for df in preprocessed_resumes]
	
	corpus = corpus.append(job_details).reset_index(drop=True)
	corpus = corpus.str.join(' ')

	id_mapping = id_mapping.append(id_maps).reset_index(drop=True)
	
	if not os.path.isdir('doc2vec_data'):
		os.mkdir('doc2vec_data')
	np.savetxt('doc2vec_data/corpus_continuing_er.cor', corpus.values, fmt='%s')
	id_mapping.to_csv('doc2vec_data/corpus_id_mapping.csv')

if __name__ == "__main__":
	preprocessed_resume_dir = sys.argv[1]

	main(preprocessed_resume_dir)