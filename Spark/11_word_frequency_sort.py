# -*- coding: utf-8 -*-

from pyspark import SparkContext
import sys
import string
import time


def replace(word):
    for a in string.punctuation+'¿¡«»':
        word = word.replace(a, '')
    return word


def main(sc, filename):
    data = sc.textFile(filename)
    t0 = time.time()
    datanp = data.map(replace)
    words_rdd = datanp.flatMap(lambda x: x.split())
    # print(words_rdd.take(10))

    appearances_rdd = words_rdd.map(lambda x: (x.lower(), 1))
    # print(appearances_rdd.take(10))

    result_rdd = appearances_rdd.reduceByKey(lambda x, y: x+y)
    # print('RESULTS------------------')
    #print('words frequency', result_rdd.take(10))

    sorted_rdd = result_rdd.sortBy(lambda x: x[1], ascending=False)

    # print('RESULTS------------------')
    #print('words frequency sorted', sorted_rdd.take(10))
    sorted_rdd.collect()
    print('Tiempo transcurrido:', time.time()-t0)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))
    else:
        with SparkContext() as sc:
            sc.setLogLevel("ERROR")
            main(sc, sys.argv[1])
