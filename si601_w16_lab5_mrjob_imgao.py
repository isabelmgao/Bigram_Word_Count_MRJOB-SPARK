#!/usr/bin/python

import mrjob
from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"\b[\w']+\b")

class BigramCount(MRJob):
  OUTPUT_PROTOCOL = mrjob.protocol.RawProtocol

  def mapper(self, _, line):
    words = WORD_RE.findall(line)
    for i in range(len(words)-1):
        yield(words[i].lower() + ' ' + words[i+1].lower(), 1)


  def combiner(self, bigram, counts):   #bigram & counts are from yield returns from mapper
    yield (bigram, sum(counts))

  def reducer(self, bigram, counts): #bigram & counts are from yield returns from combiner
    yield (bigram, str(sum(counts)))

if __name__ == '__main__':
    BigramCount.run()
