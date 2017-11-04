import time
import os
import signal
from safe_pool import SafePool
from threading import Thread
# import multiprocessing
# import logging
# logger = multiprocessing.log_to_stderr()
# logger.setLevel(logging.DEBUG)

F_TIMEOUT = 0.2


def kill_process_thread(arg):
    time.sleep(F_TIMEOUT / 2)
    os.kill(arg, signal.SIGKILL)


def kill_child(pool, max_processes=4):
    if not max_processes or max_processes <= 2:
        raise ValueError('kill child expects at least 2 processes')
    child_index = (max_processes / 2) + 1
    pid = pool._pool[child_index].pid  # kill one of the workers (about the half of the list)
    thread = Thread(target = kill_process_thread, args=(pid, ))
    thread.start() # notice we don't join to avoid blocking the main thread


def f(x):
    time.sleep(F_TIMEOUT)
    return x*x


def test_no_retry():
    """
    Checks that one task could not give result but the pool finishes if a worker is killed
    """
    process_nr = 4
    pool = SafePool(processes=process_nr)
    res = pool.map_async(f, range(10))  # using map_async instead of map to help test async killing a subprocess
    kill_child(pool, max_processes=process_nr)
    pool.close()
    pool.join()
    results = res._value
    empty_results = [x for x in results if x is None]
    assert(len(empty_results) == 1)
    assert(len(pool.get_killed_tasks()) == 1)


def test_retry():
    """
    Check that all the tasks are executed when retry is enabled and a worker is killed
    """
    process_nr = 4
    pool = SafePool(processes=process_nr, retry_killed_tasks=True)
    res = pool.map_async(f, range(10))
    kill_child(pool, max_processes=process_nr)
    pool.close()
    pool.join()
    results = res._value
    empty_results = [x for x in results if x is None] 
    assert(not empty_results)
    assert (len(pool.get_killed_tasks()) == 1)


if __name__ == '__main__':
    test_no_retry()
    test_retry()
