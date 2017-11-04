import multiprocessing
import logging
import time
import os
import signal
from safe_pool import SafePool
from threading import Thread

#logger = multiprocessing.log_to_stderr()
#logger.setLevel(logging.DEBUG)

F_TIMEOUT = 0.2

def kill_process_thread(arg):
    time.sleep(F_TIMEOUT / 2)
    os.kill(arg, signal.SIGKILL)

def kill_child(pool, max_processes=4):
    if not max_processes or max_processes <= 2:
        raise ValueError('kill child expects at least 2 processes')
    child_index = (max_processes / 2) + 1
    pid = pool._pool[child_index].pid  # kill one of the workers
    thread = Thread(target = kill_process_thread, args = (pid, ))
    thread.start() # notice we don't join to not block main thread

def f(x):
    try:
        # print 'f ', x
        time.sleep(F_TIMEOUT)
        return x*x
    except:
        print 'Exception catched! '

def test_no_retry():
    process_nr = 4
    pool = SafePool(processes=process_nr)
    res = pool.map_async(f, range(10))  # using map_async instead of map to help test async killing a subprocess
    kill_child(pool, max_processes=process_nr)
    pool.close()
    pool.join()
    results = res._value
    empty_results = [x for x in results if x is None]
    assert(empty_results)

def test_retry():
    process_nr = 4
    pool = SafePool(processes=process_nr, retry_killed_tasks=True)
    res = pool.map_async(f, range(10))  # using map_async instead of map to help test async killing a subprocess
    kill_child(pool, max_processes=process_nr)
    pool.close()
    pool.join()
    results = res._value
    empty_results = [x for x in results if x is None] 
    assert(not empty_results)

if __name__ == '__main__':
    test_no_retry()
    test_retry()
