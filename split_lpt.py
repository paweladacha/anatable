def split_lpt(items: list[tuple[str, int]]) -> tuple[list, list]:
    # Jeśli lista nie jest wcześniej posortowana:
    items = sorted(items, key=lambda x: x[1], reverse=True)

    group_a, group_b = [], []
    sum_a = sum_b = 0

    for label,weight in items:
        # item[1] to waga
        if sum_a <= sum_b:
            group_a.append(label)
            sum_a += weight
        else:
            group_b.append(label)
            sum_b += weight

    return (tuple(group_a),sum_a), (tuple(group_b),sum_b)