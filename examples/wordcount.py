
#!/usr/bin/env python3

# the purpose of this file is to provide a simple example of how to use MiniSparkContext to perform a word count on a text file. 
# It reads the file, splits the lines into words, converts them to lowercase, and counts the occurrences of each word. 
# Finally, it prints the top 50 most frequent words along with their counts.

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from minispark.context import MiniSparkContext


def main():



    if len(sys.argv) < 2:
        print("Usage: examples/wordcount.py <file>")
        sys.exit(1)
    
    
    path = sys.argv[1]

    with open(path, 'r', encoding='utf-8') as f:
        lines = f.read().splitlines()

    # create a MiniSparkContext and parallelize the lines of the file into an RDD with 4 partitions
    sc = MiniSparkContext()

    # parallelize the lines of the file into an RDD with 4 partitions
    rdd = sc.parallelize(lines, num_partitions=4)

    # split each line into words, convert them to lowercase, and flatten the result into a single RDD of words
    words = rdd.flatMap(lambda line: line.split()).map(lambda w: w.lower())



    # run sequentially for a local smoke test (set parallel=True when running in venv with cloudpickle)
    all_words = words.collect(parallel=False)

    counts = {}
    for w in all_words:
        counts[w] = counts.get(w, 0) + 1


    # return the top 50 most frequent words sorted by count in descending order
    for word, cnt in sorted(counts.items(), key=lambda x: -x[1])[:50]:
        print(word, cnt)


if __name__ == '__main__':
    main()
