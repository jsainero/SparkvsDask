import dask.array as da
import numpy as np
from dask.distributed import Client
import time
import sys

client = Client(n_workers=4)
data = da.from_array(np.loadtxt(sys.argv[1], delimiter='\n'))
t0 = time.time()
print('RESULTS ---------------------')
print('Sum:', data.sum().compute())
print('Mean:', data.mean().compute())
print('Sort:', da.map_blocks(np.sort, data).compute()[:3])
print('Tiempo transcurrido:', time.time()-t0)
client.close()
