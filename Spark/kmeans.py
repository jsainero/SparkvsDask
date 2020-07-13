from __future__ import print_function
import sys
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
import time

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("KMeansExample")\
        .getOrCreate()

    dataset = spark.read.format("csv").options(
        header='false', inferSchema='true', delimiter=',').load(sys.argv[1])
    t0 = time.time()
    assembler = VectorAssembler(inputCols=['_c0', '_c1', '_c2', '_c3', '_c4', '_c5', '_c6', '_c7', '_c8', '_c9', '_c10',
                                           '_c11', '_c12', '_c13', '_c14', '_c15', '_c16', '_c17', '_c18', '_c20', '_c21'], outputCol="features")
    dataset = assembler.transform(dataset)
    # Trains a k-means model.
    kmeans = KMeans().setK(5).setTol(0.0001)
    model = kmeans.fit(dataset)
    # Make predictions
    predictions = model.transform(dataset)

    centers = model.clusterCenters()
    print("Cluster Centers: ")
    for center in centers:
        print(center)

    print(time.time()-t0)
    spark.stop()
