import pytest

from minispark.context import MiniSparkContext


def main():
    sc = MiniSparkContext()
    rdd = sc.parallelize([1, 2, 3, 4], 2).map(lambda x: x * 2)
    assert rdd.collect() == [2, 4, 6, 8]
    assert rdd.count() == 4
    assert rdd.reduce(lambda a, b: a + b) == 20


def test_reduce_by_key_shuffle():
    sc = MiniSparkContext()
    words = sc.parallelize(['a a', 'b c'], 2).flatMap(lambda line: line.split())
    counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    assert sorted(counts.collect()) == [('a', 2), ('b', 1), ('c', 1)]


def test_fault_tolerance_partition_recovery():
    sc = MiniSparkContext()
    words = sc.parallelize(['a a', 'b c'], 2).flatMap(lambda line: line.split())
    counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    baseline = sorted(counts.collect())

    counts.lose_partition(0)
    recovered = sorted(counts.collect())
    assert recovered == baseline


def test_dag_visualization_writes_file(tmp_path):
    sc = MiniSparkContext()
    counts = (
        sc.parallelize(['hello world', 'hello spark'], 2)
        .flatMap(lambda line: line.split())
        .map(lambda word: (word, 1))
        .reduceByKey(lambda a, b: a + b)
    )
    output_path = tmp_path / 'dag.png'

    result = counts.visualize_dag(output_path=str(output_path))
    assert result == str(output_path)
    assert output_path.exists()


def test_reduce_empty_rdd_raises():
    sc = MiniSparkContext()
    empty = sc.parallelize([], 2)
    with pytest.raises(ValueError, match='Cannot reduce an empty RDD'):
        empty.reduce(lambda a, b: a + b)
