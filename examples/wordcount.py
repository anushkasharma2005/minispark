
#!/usr/bin/env python3

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from minispark.context import MiniSparkContext
from collections import Counter


def main():



    if len(sys.argv) < 2:
        print("Usage: examples/wordcount.py <file>")
        sys.exit(1)
    
    
    path = sys.argv[1]

    with open(path, 'r', encoding='utf-8') as f:
        lines = f.read().splitlines()

    sc = MiniSparkContext()
    rdd = sc.parallelize(lines, num_partitions=4)

    words = rdd.flatMap(lambda line: line.split())
    counts = words.map(lambda word: (word.lower(), 1)).reduceByKey(lambda a, b: a + b)
    results = counts.collect(parallel=True)

    for word, cnt in sorted(results, key=lambda item: (-item[1], item[0]))[:50]:
        print(word, cnt)


if __name__ == '__main__':
    main()
