from typing import List

def argmin(values: List):
    return min([i for i, _ in enumerate(values)], key=lambda i: values[i])