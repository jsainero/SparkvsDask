from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import col
import sys
import string
import numpy as np
import pandas as pd
import time


def main(sc, filename):
    t0 = time.time()
    sqlCtx = SQLContext(sc)
    df = sqlCtx.read.options(header='false', delimiter=',').csv(filename)
    df.groupBy("_c0").count().show()
    print(df.filter(col('_c0') == 30050).count())
    print('Tiempo transcurrido:', time.time()-t0)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))
    else:
        with SparkContext() as sc:
            sc.setLogLevel("ERROR")
            main(sc, sys.argv[1])
