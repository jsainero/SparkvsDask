from dask.distributed import Client
import dask
import dask.dataframe as dd
import time
import sys

client = Client(n_workers=4)
t0 = time.time()
df = dd.read_csv(sys.argv[1], header=None)
df.groupby(0).count().compute()
print(len(df[df[0] == 30050]))
print('Tiempo transcurrido:', time.time()-t0)
client.close()
