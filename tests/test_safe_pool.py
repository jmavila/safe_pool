import time
import os
import signal
from threading import Thread
import sys
from safe_pool import SafePool, SubProcessKilledException


# import multiprocessing
# import logging
# logger = multiprocessing.log_to_stderr()
# logger.setLevel(logging.DEBUG)

F_TIMEOUT = 0.2

# TODO: make faster tests: use force_exit instead of kill_child for all the tests

def kill_process_thread(arg):
    time.sleep(F_TIMEOUT / 2)
    os.kill(arg, signal.SIGKILL)


def kill_child(pool, max_processes=4, worker_nr=None):
    if not max_processes or max_processes <= 2:
        raise ValueError('kill child expects at least 2 processes')
    if not worker_nr:
        worker_nr = (max_processes / 2) + 1
    pid = pool._pool[worker_nr].pid  # kill one of the workers (about the half of the list)
    thread = Thread(target=kill_process_thread, args=(pid,))
    thread.start()  # notice we don't join to avoid blocking the main thread


def f(x):
    time.sleep(F_TIMEOUT)
    return x * x


def test_no_retry():
    """
   Check that all the tasks are executed when retry is disabled and a worker is killed
    """
    process_nr = 4
    pool = SafePool(processes=process_nr)
    res = pool.map_async(f, range(10))  # using map_async instead of map to help test async killing a subprocess
    kill_child(pool, max_processes=process_nr)
    pool.close()
    pool.join()
    results = res._value
    empty_results = [x for x in results if x is None]
    assert (len(empty_results) == 1)
    assert (len(pool.get_killed_tasks()) == 1)


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
    assert (not empty_results)
    assert (len(pool.get_killed_tasks()) == 1)


def test_multi_kill_with_retry():
    """
    Check that all the tasks are executed when retry is enabled and a worker is killed
    """
    process_nr = 4
    pool = SafePool(processes=process_nr, retry_killed_tasks=True)
    res = pool.map_async(f, range(10))
    kill_child(pool, max_processes=process_nr, worker_nr=3)
    kill_child(pool, max_processes=process_nr, worker_nr=2)
    pool.close()
    pool.join()
    results = res._value
    empty_results = [x for x in results if x is None]
    assert (len(empty_results) == 0)
    assert (len(pool.get_killed_tasks()) == 2)


def test_multi_kill_no_retry():
    """
    Check that all the tasks are executed when retry is disabled and a worker is killed
    """
    process_nr = 4
    pool = SafePool(processes=process_nr)
    res = pool.map_async(f, range(10))
    kill_child(pool, max_processes=process_nr, worker_nr=3)
    kill_child(pool, max_processes=process_nr, worker_nr=2)
    pool.close()
    pool.join()
    results = res._value
    empty_results = [x for x in results if x is None]
    assert (len(empty_results) == 2)
    assert (len(pool.get_killed_tasks()) == 2)


def force_exit(value):
    if not value:
        sys.exit(15)
    return value * value


def test_exit_no_retry():
    process_nr = 4
    pool = SafePool(processes=process_nr)
    cases = [0, 20, 0, 40, 50, 0, 60, 0, 80, 0, 100, 0]
    res = pool.map_async(force_exit, cases)
    pool.close()
    pool.join()
    results = res._value
    empty_results = [x for x in results if x is None]
    assert (len(empty_results) == 6)
    assert (len(pool.get_killed_tasks()) == 6)


def test_abort():
    process_nr = 4
    pool = SafePool(processes=process_nr, abort_when_killed=True)
    cases = [0, 20, 0, 40, 50, 0, 60, 0, 80, 0, 100, 0]
    res = pool.map_async(force_exit, cases)
    pool.close()
    pool.join()
    result = res._value
    assert (isinstance(result, SubProcessKilledException))


# TODO: to fix the next commented test, we need to add support for a maximum number of task retries (force_exit always
# generates a kill signal). Possible solution to implement:
# - add a new SafePool param: min_memory
# - add a new SafePool param: memory_timeout
# - add a while loop in  _join_exited_workers() checking available memory until available mem> min_memory or
#   the timeout expires
# def test_exit_with_retry():
#     process_nr = 4
#     pool = SafePool(processes=process_nr, retry_killed_tasks=True)
#     cases = [0, 20, 0, 40, 50, 0, 60, 0, 80, 0, 100, 0]
#     res = pool.map_async(force_exit, cases)
#     pool.close()
#     pool.join()
#     results = res._value
#     print 'RESULTS: ', results
#     empty_results = [x for x in results if x is None]
#     assert (len(empty_results) == 6)
#     assert (len(pool.get_killed_tasks()) == 6)

