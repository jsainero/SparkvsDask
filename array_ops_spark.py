from pyspark import SparkContext
import sys
import string
import numpy as np
import time


def main(sc, filename):
    """
    data = sc.parallelize(np.random.uniform(
        low=0.0, high=100.0, size=(250000000,)))
    """
    data = sc.textFile(filename)
    data = data.map(lambda x: float(x))
    print('RESULTS ---------------------')
    t0 = time.time()
    print('Sum:', data.sum())
    print('Mean:', data.mean())
    data_sort = data.map(lambda x: (x, x)).sortByKey(ascending=True).values()
    print('Sort data:', data_sort.take(3))
    print('Tiempo:', time.time()-t0)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))
    else:
        with SparkContext() as sc:
            sc.setLogLevel("ERROR")
            main(sc, sys.argv[1])
