from dask.distributed import Client
import time
import sys
from dask_ml.cluster import KMeans
import dask.dataframe as dd

client = Client(n_workers=4)
t0 = time.time()
dataset = dd.read_csv(sys.argv[1], header=None)
dataset = dataset[[0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
                   10, 11, 12, 13, 14, 15, 16, 17, 18, 20, 21]]
clf = KMeans(n_clusters=5, tol=0.0001)
clf.fit(dataset)
a = clf.transform(dataset)
a.compute()
print(clf.cluster_centers_)
print('Tiempo transcurrido:', time.time()-t0)
client.close()
