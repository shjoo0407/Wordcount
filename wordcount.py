import sys
from mrjob.job import MRJob

class MRWordCount(MRJob):
	def mapper(self, _, line):
		for word in line.split():
			yield(word,1)
	def combiner(self,word,counts):
		yield(word,sum(counts))
	def reducer(self,word,counts):
		yield(word,sum(counts))

if __name__ == '__main__':
	MRWordCount.run()


