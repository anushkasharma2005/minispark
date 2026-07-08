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

The use of this is that we can create a pipeline of transformations on a dataset and then execute them all at once, which is more efficient than executing each transformation separately.


---

Next steps: add `reduceByKey` (shuffle), DAG visualization, and fault-tolerance demo.



Note: This is not vibe codede. Its build with the aid of AI tho :))