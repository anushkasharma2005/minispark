#!/usr/bin/env python3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from minispark.context import MiniSparkContext


def main():
    if len(sys.argv) < 2:
        print('Usage: examples/showcase.py <file>')
        sys.exit(1)

    path = sys.argv[1]
    with open(path, 'r', encoding='utf-8') as handle:
        lines = handle.read().splitlines()

    sc = MiniSparkContext()
    text = sc.parallelize(lines, num_partitions=4)
    words = text.flatMap(lambda line: line.split())
    counts = words.map(lambda word: (word.lower(), 1)).reduceByKey(lambda a, b: a + b)

    print('Word count before fault injection:')
    for word, count in sorted(counts.collect(), key=lambda item: (-item[1], item[0])):
        print(word, count)

    dag_path = Path('dag.png')
    counts.visualize_dag(output_path=str(dag_path))
    print(f'Wrote DAG visualization to {dag_path}')

    counts.lose_partition(0)
    print('Word count after simulating a lost partition:')
    for word, count in sorted(counts.collect(), key=lambda item: (-item[1], item[0])):
        print(word, count)


if __name__ == '__main__':
    main()
