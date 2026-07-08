from minispark.context import MiniSparkContext

# This file is a simple smoke test for the MiniSparkContext and RDD classes. 
# It creates an RDD from a list of numbers, applies a map transformation to double the numbers, and then collects the results. 
# It also tests the count and reduce methods of the RDD class.

def main():
    sc = MiniSparkContext()
    rdd = sc.parallelize([1, 2, 3, 4], 2).map(lambda x: x * 2)
    assert rdd.collect() == [2, 4, 6, 8]
    assert rdd.count() == 4
    assert rdd.reduce(lambda a, b: a + b) == 20
    print('ok')


if __name__ == '__main__':
    main()
