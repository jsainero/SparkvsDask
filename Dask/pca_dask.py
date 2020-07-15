from dask.distributed import Client
import time
import sys
from dask_ml.decomposition import PCA
import dask.dataframe as dd

client = Client(n_workers=4)
t0 = time.time()
data = dd.read_csv(sys.argv[1], header=None)
pca = PCA(n_components=1, svd_solver='randomized')
pca.fit(data[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11,
              12, 13, 14, 15, 16, 17, 18, 20, 21]].values)
data_trans = pca.transform(
    data[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 20, 21]].values)
data_trans.compute()
print('Tiempo transcurrido', time.time()-t0)
