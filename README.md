# MiniSpark: single-node Spark-like prototype

This project is a single-machine reimplementation of key Spark concepts: RDDs, lazy transformations, partitioning, and parallel execution (via `multiprocessing`). It's intended demo project to explain DAGs, lineage, narrow vs wide operations, and fault-tolerance concepts.


## What it does

- `parallelize` splits input into partitions
- `map`, `filter`, `flatMap` build a lazy lineage
- `collect`, `count`, `reduce` execute the lineage
- `reduceByKey` performs a simple shuffle and per-key aggregation
- `visualize_dag` renders the transformation graph with `matplotlib`
- `lose_partition` simulates a lost partition and recomputes it from lineage and base data


## Steps to run it
 
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt
pip install -e .

python examples/wordcount.py data/sample.txt

# if the above command doesnt work try this 
PYTHONPATH=. python3 examples/wordcount.py data/sample.txt

pytest -q

```

Docker

```bash
docker build -t minispark .
# docker run --rm -v "$(pwd)/data:/data" minispark python examples/wordcount.py /data/sample.txt
docker run --rm minispark


```

Full showcase with DAG visualization and simulated partition loss

```py

    python examples/showcase.py data/sample.txt

```

What's implemented so far
- Core `RDD` class with `map`, `filter`, `flatMap` (lazy)
- `MiniSparkContext.parallelize` to create partitioned base RDDs
- `collect` and `count` actions that compute in parallel across partitions
- `reduce` action that aggregates results across partitions
- `reduceByKey` transformation that performs a simple shuffle and per-key aggregation
- `visualize_dag` method to render the lineage of transformations as a DAG



### For begins like me 
---


 In easy words whats happening rn in this project is:  
 the RDD class is a representation of a distributed dataset. It allows you to perform transformations like map, filter, and flatMap on the data. 
 Each transformation creates a new RDD that keeps track of its parent and the transformation applied. 
 When you call collect(), it computes the final result by applying all the transformations in order to the base RDD's partitions, either in parallel or sequentially.



 now we added the count() and reduce() methods to the RDD class. which basically allow us to count the number of elements in the RDD and reduce the elements using a binary function.
 binary function here means a function that takes two arguments and returns a single value. For example, a function that adds two numbers together is a binary function.
 the smoketest is basically checking if the RDD is being created correctly and if the transformations are being applied correctly. 
 It creates an RDD from a list of numbers, applies a map transformation to double the numbers, and then collects the results. 
 It also tests the count and reduce methods of the RDD class. 



 The use of this is that we can create a pipeline of transformations on a dataset and then execute them all at once, which is more efficient than executing each transformation separately.
 the use of the count() and reduce() methods is for aggregating data. 


Alright so now we have added the reduceByKey() method to the RDD class. This method allows us to perform a reduce operation on key-value pairs in the RDD.
The reduceByKey() method takes a binary function as an argument and applies it to the values of each key in the RDD, producing a new RDD with the reduced values.

if this doesnt make any sense to you, think of it like this: if you have a list of key-value pairs, and you want to combine the values for each key using a specific operation (like summing them up), you can use reduceByKey() to do that. Clear? yayyy 

Also added the visualize_dag() method to the RDD class. This method allows us to visualize the lineage of transformations applied to the RDD as a directed acyclic graph (DAG).

lose_partition() method to the RDD class. This method simulates a lost partition in the RDD, which can be useful for testing fault-tolerance and recovery mechanisms.

---

ya so thats it for now ig. I know i lost the challenge of making this project in 3 hrs but hey, at least we have a working prototype now. HAHAHA. 
In future i can extend this to include more features like caching, checkpointing, and more complex transformations.I can also make it distributed across multiple machines instead of just a single node. 

But for now Adio!!
THanks for stopping by!

Note: This is not vibe codede. Its build with the aid of AI tho :))
source .venv/bin/activate && PYTHONPATH=. python tests/test_core.py

