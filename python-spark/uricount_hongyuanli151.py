
import re
import sys
from pyspark import SparkConf, SparkContext


class UriCount_HongyuanLi151:


    def __init__(self, input, output):
        conf = SparkConf().setMaster('local').setAppName('URI-MapReduce')
        sc = SparkContext(conf=conf)
        sc.setLogLevel("WARN")
        self.input_rdd = sc.textFile(input)
        self.output = output

    def mapreduce(self):
        pair_rdd = self.input_rdd.map(self.__parser__)
        reduce_rdd = pair_rdd.reduceByKey(lambda x, y: x + y)
        sorted_rdd = reduce_rdd.sortBy(lambda x: (x[1], x[0]))
        sorted_rdd\
            .map(lambda x: x[0] + '\t' + str(x[1]))\
            .coalesce(1)\
            .saveAsTextFile(self.output)
        # self.__writer__(reduce_rdd.collect(), self.output)
        # self.__writer__(reduce_rdd.sortBy(lambda  x: (x[1], x[0])).collect(), self.output)

    @staticmethod
    def __parser__(x):

        apicalls = re.findall('"([^"]*)"', x.replace(u'\u201c', '"').replace(u'\u201d', '"').encode("utf-8").lower())
        uri = apicalls[0]
        uri = uri[uri.index('/'):]
        rbound = uri.rfind(' http/')
        if rbound > 0:
            uri = uri[:rbound]
        return (uri, 1)

if __name__ == '__main__':
    task = UriCount_HongyuanLi151(sys.argv[1], sys.argv[2])
    task.mapreduce()