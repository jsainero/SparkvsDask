from dask.distributed import Client
import dask.bag as db
import string
import sys
import time


def replace(word):
    for a in string.punctuation+'¿¡«»':
        word = word.replace(a, '')
    return word


def main(filename):
    client = Client(n_workers=4)
    t0 = time.time()
    data = db.read_text(sys.argv[1], encoding='latin-1')
    datanp = data.map(replace)
    words_bag = datanp.map(lambda x: x.split()).flatten()
    appearances_bag = words_bag.map(lambda x: (x.lower(), 1))
    result_bag = words_bag.map(lambda x: x.lower()).frequencies()
    sorted_bag = words_bag.map(lambda x: x.lower()).frequencies(sort=True)
    print('RESULTS------------------')
    print('words frequency sorted', sorted_bag.compute()[:10])
    print('Tiempo transcurrido:', time.time()-t0)
    client.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python3 {0} <file>".format(sys.argv[0]))
    else:
        main(sys.argv[1])
