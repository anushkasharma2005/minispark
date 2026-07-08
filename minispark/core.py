try:
    import cloudpickle as _serializer
except Exception:
    import pickle as _serializer

from multiprocessing import Pool, get_context
from typing import List, Any, Tuple

# just so that my future self is not clueless:
# RDD stands for Resilient Distributed Dataset. Its just a datastructure like a struct in c and it basically holds the data and the lineage of transformations that have been applied to it.
# lineage is a list of transformations that have been applied to the RDD. It is used to compute the result of the RDD by applying the transformations in order.
# transformations are functions that take an RDD and return a new RDD. They are used to transform the data in the RDD. Examples of transformations are map, filter, and flatMap.




# this function is a private helper function that computes the result of a partition of an RDD by applying the lineage of transformations to the data in that partition.
def _compute_partition(args):
    
    partition_data, lineage_pickled = args
    lineage = _serializer.loads(lineage_pickled)
    data = partition_data
    
    
    for op, fn in lineage:
        if op == 'map':
            data = [fn(x) for x in data]
        elif op == 'filter':
            data = [x for x in data if fn(x)]
        elif op == 'flatMap':
            new = []
            for x in data:
                res = fn(x)
                if res is None:
                    continue
                for y in res:
                    new.append(y)
            data = new
        else:
            raise ValueError(f"Unknown op: {op}")
    return data

# this reduces a list of values using a binary function. It is used to compute the result of an RDD by reducing the values in the partitions.
def _reduce_values(values, fn):
    
    if not values:
        return None
    result = values[0]
    
    for value in values[1:]:
        result = fn(result, value)
    
    return result

# this parallelizes the computation of partitions using multiprocessing. It is used to compute the result of an RDD by applying the lineage of transformations to the data in the partitions in parallel.
def _parallel_map(args):
    
    try:
        with get_context('fork').Pool() as pool:
            return pool.map(_compute_partition, args)
    
    except Exception:
        with Pool() as pool:
            return pool.map(_compute_partition, args)



# this class is the main entry point for the MiniSpark API. 
# It has functions that are used to create RDDs and perform transformations on them.
class RDD:


    def __init__(self, partitions: List[List[Any]] = None, parent=None, transform: Tuple = None):
        self.partitions = partitions  # partition here are just lists of data. Each partition is a list of data that is processed in parallel.
        self.parent = parent          # parent is the RDD that this RDD was derived from. It is used to build the lineage of transformations that have been applied to this RDD.
        self.transform = transform    # transform is a tuple that describes the transformation that was applied to the parent RDD to create this RDD.



    # parallelizes a list of data into an RDD with the specified number of partitions.
    def map(self, fn):
        return RDD(parent=self, transform=('map', fn))

    # filters the elements of the RDD based on a predicate function.
    def filter(self, fn):
        return RDD(parent=self, transform=('filter', fn))

    # flatMaps the elements of the RDD based on a function.
    def flatMap(self, fn):
        return RDD(parent=self, transform=('flatMap', fn))

    # checks if the RDD is a base RDD (i.e., it has no parent and has partitions).
    def _is_base(self):
        return self.parent is None and self.partitions is not None

    # returns the base RDD of the current RDD by traversing up the lineage until it finds a base RDD.
    def _get_base(self):
        
        node = self
        
        while node and not node._is_base():
            node = node.parent
        if node is None:
            raise ValueError('No base RDD found')
        
        return node


    # builds the lineage of transformations from the base RDD to the current 
    # RDD by traversing up the parent chain and collecting the transformations.
    def _build_lineage(self):
        
        lineage = []
        node = self
        
        while node and node.transform is not None:
            lineage.append(node.transform)
            node = node.parent
        lineage.reverse()
        
        return lineage

    # collects the elements of the RDD into a list. 
    # It applies the lineage of transformations to the base RDD and returns the result as a list. 
    # It can run in parallel or sequentially based on the parallel argument.
    
    def collect(self, parallel=True):
        
        base = self._get_base()
        base_partitions = base.partitions
        lineage = self._build_lineage()
        
        if not lineage:
            # nothing to do
            return [x for p in base_partitions for x in p]

        lineage_pickled = _serializer.dumps(lineage)
        args = [(p, lineage_pickled) for p in base_partitions]
        
        if parallel:
            results = _parallel_map(args)
        else:
            results = list(map(_compute_partition, args))

        # flatten preserving partition order coz the order of partitions is important for some operations like sortByKey
        return [x for p in results for x in p]

    # counts the number of elements in the RDD
    def count(self):
    
        base = self._get_base()
        lineage = self._build_lineage()

        if not lineage:
            return sum(len(partition) for partition in base.partitions)

        lineage_pickled = _serializer.dumps(lineage)
        args = [(p, lineage_pickled) for p in base.partitions]

        results = _parallel_map(args)

        return sum(len(partition) for partition in results)

    # reduces the elements of the RDD using a binary function
    def reduce(self, fn):
    
        base = self._get_base()
        lineage = self._build_lineage()

        if not lineage:
            partitions = base.partitions
        else:
            lineage_pickled = _serializer.dumps(lineage)
            args = [(p, lineage_pickled) for p in base.partitions]
            partitions = _parallel_map(args)

        flat_values = [value for partition in partitions for value in partition]
        result = _reduce_values(flat_values, fn)
    
        if result is None:
            raise ValueError('Cannot reduce an empty RDD')
    
        return result








# 

# In easy words whats happening rn in this project is:  
# the RDD class is a representation of a distributed dataset. It allows you to perform transformations like map, filter, and flatMap on the data. 
# Each transformation creates a new RDD that keeps track of its parent and the transformation applied. 
# When you call collect(), it computes the final result by applying all the transformations in order to the base RDD's partitions, either in parallel or sequentially.
# 

# now we added the count() and reduce() methods to the RDD class. which basically allow us to count the number of elements in the RDD and reduce the elements using a binary function.
# binary function here means a function that takes two arguments and returns a single value. For example, a function that adds two numbers together is a binary function.
# the smoketest is basically checking if the RDD is being created correctly and if the transformations are being applied correctly. 
# It creates an RDD from a list of numbers, applies a map transformation to double the numbers, and then collects the results. 
# It also tests the count and reduce methods of the RDD class. 



# The use of this is that we can create a pipeline of transformations on a dataset and then execute them all at once, which is more efficient than executing each transformation separately.
# the use of the count() and reduce() methods is for aggregating data. 