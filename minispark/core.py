try:
    import cloudpickle as _serializer
except Exception:
    import pickle as _serializer

from multiprocessing import Pool, get_context
from typing import Any, Callable, List, Optional, Tuple


def _compute_narrow_partition(args):
    partition_data, lineage_pickled = args
    lineage = _serializer.loads(lineage_pickled)
    data = list(partition_data)

    for op, fn in lineage:
        if op == 'map':
            data = [fn(x) for x in data]
        elif op == 'filter':
            data = [x for x in data if fn(x)]
        elif op == 'flatMap':
            flattened = []
            for item in data:
                produced = fn(item)
                if produced is None:
                    continue
                flattened.extend(produced)
            data = flattened
        else:
            raise ValueError(f'Unknown narrow op: {op}')

    return data


def _reduce_values(values, fn):
    if not values:
        return None
    result = values[0]
    for value in values[1:]:
        result = fn(result, value)
    return result


def _parallel_map(args):
    try:
        with get_context('fork').Pool() as pool:
            return pool.map(_compute_narrow_partition, args)
    except Exception:
        with Pool() as pool:
            return pool.map(_compute_narrow_partition, args)


def _partition_evenly(items, num_partitions):
    if num_partitions <= 0:
        return [items]
    if not items:
        return [[] for _ in range(num_partitions)]

    size = (len(items) + num_partitions - 1) // num_partitions
    partitions = []
    for index in range(num_partitions):
        start = index * size
        end = min(start + size, len(items))
        partitions.append(items[start:end])
    return partitions


def _partition_pairs_by_hash(pairs, num_partitions):
    if num_partitions <= 0:
        return [pairs]

    partitions = [[] for _ in range(num_partitions)]
    for key, value in pairs:
        partitions[hash(key) % num_partitions].append((key, value))
    return partitions


class RDD:
    def __init__(
        self,
        partitions: Optional[List[Optional[List[Any]]]] = None,
        data: Optional[List[Any]] = None,
        partition_bounds: Optional[List[Tuple[int, int]]] = None,
        parent=None,
        transform: Tuple = None,
    ):
        self.partitions = partitions
        self.data = data
        self.partition_bounds = partition_bounds
        self.parent = parent
        self.transform = transform

    def map(self, fn):
        return RDD(parent=self, transform=('map', fn))

    def filter(self, fn):
        return RDD(parent=self, transform=('filter', fn))

    def flatMap(self, fn):
        return RDD(parent=self, transform=('flatMap', fn))

    def reduceByKey(self, fn):
        return RDD(parent=self, transform=('reduceByKey', fn))

    def _is_base(self):
        return self.parent is None and self.partitions is not None

    def _get_base(self):
        node = self
        while node and not node._is_base():
            node = node.parent
        if node is None:
            raise ValueError('No base RDD found')
        return node

    def _build_lineage(self):
        lineage = []
        node = self
        while node and node.transform is not None:
            lineage.append(node.transform)
            node = node.parent
        lineage.reverse()
        return lineage

    def _split_lineage(self):
        lineage = self._build_lineage()
        for index, (op, fn) in enumerate(lineage):
            if op == 'reduceByKey':
                return lineage[:index], (op, fn), lineage[index + 1 :]
        return lineage, None, []

    def _recover_base_partition(self, index):
        base = self._get_base()
        partition = base.partitions[index]
        if partition is not None:
            return partition

        if base.data is None or base.partition_bounds is None:
            raise ValueError('Cannot recover lost partition without base data')

        start, end = base.partition_bounds[index]
        partition = list(base.data[start:end])
        base.partitions[index] = partition
        return partition

    def lose_partition(self, index):
        base = self._get_base()
        if index < 0 or index >= len(base.partitions):
            raise IndexError('partition index out of range')
        base.partitions[index] = None

    def _materialize_narrow(self, lineage, parallel=True):
        base = self._get_base()
        if not lineage:
            partitions = []
            for index in range(len(base.partitions)):
                partitions.append(list(self._recover_base_partition(index)))
            return partitions

        lineage_pickled = _serializer.dumps(lineage)
        args = [(self._recover_base_partition(index), lineage_pickled) for index in range(len(base.partitions))]

        if parallel:
            return _parallel_map(args)
        return list(map(_compute_narrow_partition, args))

    def _execute_reduce_by_key(self, prefix_lineage, reducer, suffix_lineage, parallel=True):
        base = self._get_base()
        prefix_partitions = self._materialize_narrow(prefix_lineage, parallel=parallel)

        shuffled_pairs = []
        for partition in prefix_partitions:
            for item in partition:
                if not isinstance(item, tuple) or len(item) != 2:
                    raise ValueError('reduceByKey expects (key, value) pairs')
                shuffled_pairs.append(item)

        grouped = {}
        for key, value in shuffled_pairs:
            grouped.setdefault(key, []).append(value)

        reduced_pairs = [(key, _reduce_values(values, reducer)) for key, values in grouped.items()]
        reduced_partitions = _partition_pairs_by_hash(reduced_pairs, len(base.partitions))

        if not suffix_lineage:
            return reduced_partitions

        suffix_pickled = _serializer.dumps(suffix_lineage)
        args = [(partition, suffix_pickled) for partition in reduced_partitions]
        if parallel:
            return _parallel_map(args)
        return list(map(_compute_narrow_partition, args))

    def collect(self, parallel=True):
        prefix_lineage, reduce_by_key_op, suffix_lineage = self._split_lineage()

        if reduce_by_key_op is not None:
            partitions = self._execute_reduce_by_key(prefix_lineage, reduce_by_key_op[1], suffix_lineage, parallel=parallel)
            return [item for partition in partitions for item in partition]

        partitions = self._materialize_narrow(prefix_lineage, parallel=parallel)
        return [item for partition in partitions for item in partition]

    def count(self):
        return len(self.collect())

    def reduce(self, fn):
        values = self.collect(parallel=True)
        result = _reduce_values(values, fn)
        if result is None:
            raise ValueError('Cannot reduce an empty RDD')
        return result

    def visualize_dag(self, output_path=None, show=False):
        lineage = self._build_lineage()
        labels = ['Base RDD'] + [f'{op}' for op, _ in lineage]

        try:
            import matplotlib

            matplotlib.use('Agg', force=True)
            import matplotlib.pyplot as plt
        except Exception as exc:
            raise RuntimeError('matplotlib is required for DAG visualization') from exc

        fig, ax = plt.subplots(figsize=(max(6, len(labels) * 1.6), 3.2))
        ax.axis('off')

        x_positions = list(range(len(labels)))
        for index, (label, x_pos) in enumerate(zip(labels, x_positions)):
            ax.scatter([x_pos], [0], s=1200, color='#2d6cdf')
            ax.text(x_pos, 0, label, ha='center', va='center', color='white', fontsize=10, weight='bold')
            if index > 0:
                ax.annotate('', xy=(x_pos - 0.45, 0), xytext=(x_pos - 0.55, 0), arrowprops=dict(arrowstyle='->', lw=2))

        ax.set_xlim(-0.75, len(labels) - 0.25)
        ax.set_ylim(-1, 1)
        ax.set_title('MiniSpark DAG')

        if output_path:
            fig.savefig(output_path, bbox_inches='tight')
        if show:
            plt.show()
        plt.close(fig)
        return output_path




