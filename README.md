# MiniSpark — single-node Spark-like prototype

This project is a single-machine reimplementation of key Spark concepts: RDDs, lazy transformations, partitioning, and parallel execution (via `multiprocessing`). It's intended demo project to explain DAGs, lineage, narrow vs wide operations, and fault-tolerance concepts.


## Steps to run it
 
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements.txt


python examples/wordcount.py data/sample.txt
# if the above command doesnt work try this 
PYTHONPATH=. python3 examples/wordcount.py data/sample.txt


# run the smoketest 
source .venv/bin/activate && PYTHONPATH=. python tests/test_core.py


```

Docker

```bash
docker build -t minispark .
docker run --rm -v "$(pwd)/data:/data" minispark python examples/wordcount.py /data/sample.txt
```

What's implemented so far
- Core `RDD` class with `map`, `filter`, `flatMap` (lazy)
- `MiniSparkContext.parallelize` to create partitioned base RDDs
- `collect` and `count` actions that compute in parallel across partitions



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


---

Next steps: add `reduceByKey` (shuffle), DAG visualization, and fault-tolerance demo.



Note: This is not vibe codede. Its build with the aid of AI tho :))