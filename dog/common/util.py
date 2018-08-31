import psutil
import pandas as pd
import numpy as np
import random
import multiprocessing

path = """R:\scu\\frm_secure\HD Graphics Lab\\"""
psutil.virtual_memory()
class dog:
    """ dog object

        Example:
        pupper = dog('/dir/smallfile.csv')
        doggo = dog('/dir/bigfile.csv')
    """
    def __init__(self, path, tmpPath = None, chunkSize = None):
        if path is None:
            raise ValueError("Must specify a path as an argument.")
        else:
            self.path = path
        if tmpPath is not None:
            self.tmpPath = tmpPath
        else:
            self.tmpPath = '/'.join(path.split('/')[:-1]) + '/'
        if chunkSize is None:
            self.chunkSize = int(psutil.virtual_memory().free*0.75/multiprocessing.cpu_count()/self._averageMemoryUsagePerRow()) + 1
        else:
            self.chunkSize = chunkSize
    def _averageMemoryUsagePerRow(self, sample=1000):
        df = pd.read_csv(self.path, nrows = sample)
        return df.memory_usage(True).values.sum()/sample

    def mergesort(self, columnName, ascending = True):
        """ External Merge Sort on column specified using pandas
        """
        return [self.chunkSize * i for i in range(multiprocessing.cpu_count())]

spawn = multiprocessing.get_context('spawn')

doggo = dog("""R:\mfa\common\paysys\protected\SimulationFactory\lvts_1109_to_1612_csv\\trans.csv""")

doggo.mergesort('lol')

df = pd.read_csv(doggo.path, nrows = 1, skiprows=999999999)
