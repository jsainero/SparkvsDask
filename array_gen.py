import numpy as np
import random
output = open('vector100M.txt', 'w')

for i in range(100000000):
    aux = random.uniform(0.0, 100.0)
    output.write(str(aux)+'\n')

output.close()
