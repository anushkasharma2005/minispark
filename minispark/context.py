from minispark.core import RDD
from multiprocessing import cpu_count
from typing import List, Any

# For people who dont know: RRD stands for Resilient Distributed Dataset. It is a fundamental data structure of MiniSpark.

# okay so this class has the functions that are used to create RDDs and perform transformations on them. 
 
class MiniSparkContext:

    # this fxn parallelizes a list of data into an RDD with the specified number of partitions.
    # If num_partitions is not specified, it defaults to the number of CPU cores available
    def parallelize(self, data: List[Any], num_partitions: int = None) -> RDD:
        
        if num_partitions is None:
            num_partitions = cpu_count()
        n = len(data)
        
        if n == 0:
            partitions = [[] for _ in range(num_partitions)]
            return RDD(partitions=partitions)

        size = (n + num_partitions - 1) // num_partitions
        partitions = []
        
        for i in range(num_partitions):
            start = i * size
            end = min(start + size, n)
            partitions.append(data[start:end])
        
        
        return RDD(partitions=partitions)
