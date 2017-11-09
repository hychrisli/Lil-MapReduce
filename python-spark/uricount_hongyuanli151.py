
import re
import sys
from pyspark import SparkConf, SparkContext


class UriCount:


    def __init__(self, input, output):
        conf = SparkConf().setMaster('local').setAppName('URI-MapReduce')
        sc = SparkContext(conf=conf)
        sc.setLogLevel("WARN")
        self.input_rdd = sc.textFile(input)
        self.output = output

    def mapreduce(self):
        pair_rdd = self.input_rdd.map(UriCount.__parser__)
        reduce_rdd = pair_rdd.reduceByKey(lambda x, y: x + y)
        UriCount.__writer__(reduce_rdd.collect(), self.output)
        UriCount.__writer__(reduce_rdd.sortBy(lambda  x: (x[1], x[0])).collect(), self.output + '_sorted')

    @staticmethod
    def __parser__(x):

        apicalls = re.findall('"([^"]*)"', x.replace(u'\u201c', '"').replace(u'\u201d', '"').encode("utf-8").lower())
        uri = apicalls[0]
        uri = uri[uri.index('/'):]
        rbound = uri.rfind(' http/')
        if rbound > 0:
            uri = uri[:rbound]
        return (uri, 1)

    @staticmethod
    def __writer__(res, file):

        f = open(file, 'w')
        for (key, value) in res:
            f.write(key + '\t' + str(value) + '\n')
        f.close()

if __name__ == '__main__':
    task = UriCount(sys.argv[1], sys.argv[2])
    task.mapreduce()