from dask.distributed import Client
import time
import sys
from dask_ml.linear_model import LinearRegression
import dask.dataframe as dd


client = Client(n_workers=4)
t0 = time.time()
data = dd.read_csv(sys.argv[1], header=None)
model = LinearRegression(fit_intercept=False)
reg = model.fit(data[[0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12,
                      13, 14, 15, 16, 17, 18, 20, 21]].values, data[3].values)
print(reg.coef_)
print('Tiempo transcurrido:', time.time()-t0)
client.close()
