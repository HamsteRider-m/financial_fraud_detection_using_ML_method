import time
from functools import wraps
import math


def timethis(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.process_time()
        r = func(*args, **kwargs)
        end = time.process_time()
        print('{}.{} : {}'.format(func.__module__, func.__name__, end - start))
        return r
    return wrapper


def split_list(target_list, num_elements=200):
    """将一个列表分成多个固定长度的列表。

    Arguments:
        target_list {list} -- 需要且分的列表
        num_elements {int} -- 每个列表中的元素

    Returns:
        list -- 包含列表的列表
    """
    return [target_list[i*num_elements: (i+1)*num_elements] for i in range(math.ceil(len(target_list)/num_elements))]
