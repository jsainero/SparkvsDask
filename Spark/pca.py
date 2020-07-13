from __future__ import print_function
from pyspark.ml import Pipeline
from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import VectorAssembler
import sys
import time
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("PCAExample")\
        .getOrCreate()

    dataset = spark.read.format("csv").options(
        header='false', inferSchema='true', delimiter=',').load(sys.argv[1])
    t0 = time.time()
    assembler = VectorAssembler(inputCols=['_c0', '_c1', '_c2', '_c3', '_c4', '_c5', '_c6', '_c7', '_c8', '_c9', '_c10',
                                           '_c11', '_c12', '_c13', '_c14', '_c15', '_c16', '_c17', '_c18', '_c20', '_c21'], outputCol="features")
    df = assembler.transform(dataset)
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures",
                            withStd=False, withMean=True)
    pca = PCA(k=1, inputCol=scaler.getOutputCol(), outputCol="pcaFeatures")
    pipeline = Pipeline(stages=[scaler, pca])
    model = pipeline.fit(df)
    result = model.transform(df).select("pcaFeatures")
    print(time.time()-t0)
    spark.stop()
