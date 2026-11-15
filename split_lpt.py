import heapq

__all__ = [
    "partition_lpt",
    "with_totals",
]
def partition_lpt(items: list[tuple[str, int]], k: int = 2, resort: bool = True) -> list[list[tuple[str, int]]]:
    if resort:
        items = sorted(items, key=lambda x: x[1], reverse=True)
        
    if k < 3 or len(items) <= k:
        return _special_case(items, k)
    else:
        return _core_lpt(items, k)

def with_totals(groups: list[list[tuple[str, int]]]) -> list[tuple[tuple[str, ...], int]]:
    return [(tuple(label for label, _ in group), sum(weight for _, weight in group)) for group in groups]

def _special_case(items: list[tuple[str, int]], k: int) -> list[list[tuple[str, int]]]:
    if k < 1:
        raise ValueError("k must be at least 1")
    elif k == 1:
        return [items]   
    elif k == 2: 
        return _two_groups_lpt(items)
    elif len(items) <= k:
        return [[item] for item in items] + [[] for _ in range(k - len(items))]
    else:
        raise ValueError(f"k must be 1, 2, or greater than the number of items - wrong case for this function k={k}, len(items)={len(items)}")

def _two_groups_lpt(items: list[tuple[str, int]]) -> list[list[tuple[str, int]]]:
    group_a, group_b = [], []
    sum_a = sum_b = 0
    for item in items:
        if sum_a <= sum_b:
            group_a.append(item)
            sum_a += item[1]
        else:
            group_b.append(item)
            sum_b += item[1]
    return [group_a, group_b]

def _core_lpt(items: list[tuple[str, int]], k: int) -> list[list[tuple[str, int]]]:
    heap = [(0, i) for i in range(k)]
    groups = [[] for _ in range(k)]

    for item in items:
        total, idx = heap[0]
        groups[idx].append(item)
        heapq.heapreplace(heap, (total + item[1], idx))

    return groups

