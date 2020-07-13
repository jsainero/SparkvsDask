from __future__ import print_function
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import StringIndexer
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
import sys
import time

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("LinearRegressionWithElasticNet")\
        .getOrCreate()

    t0 = time.time()
    training = spark.read.format("csv").options(header='false', inferSchema='true', delimiter=',')\
        .load(sys.argv[1])
    assembler = VectorAssembler(inputCols=['_c0', '_c1', '_c2', '_c4', '_c5', '_c6', '_c7', '_c8', '_c9', '_c10',
                                           '_c11', '_c12', '_c13', '_c14', '_c15', '_c16', '_c17', '_c18', '_c20', '_c21'], outputCol="features")
    training2 = assembler.transform(training)
    lr = LinearRegression(featuresCol="features", labelCol='_c3')

    lrModel = lr.fit(training2)

    print("Coefficients: %s" % str(lrModel.coefficients))
    print("Intercept: %s" % str(lrModel.intercept))
    print(time.time()-t0)
    spark.stop()
