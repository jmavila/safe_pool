import signal
from collections import defaultdict
import sys
from safe_pool import SafePool, SubProcessKilledException


class ExitCallback(object):
    """
        Utility class used to simulate a task receiving a parameter 
        that will exit with `exit_code` if value is equal to `fail_value` 
        if the attempt nr is less than attempts_failing
    """
    def __init__(self, exit_code=0, fail_value=None, attempts_failing=1):
        self.attempts_cache = defaultdict(int)
        self.fail_value = fail_value
        self.attempts_failing = attempts_failing
        self.signal_code = exit_code

    def run(self, value):
        self.attempts_cache[value] += 1
        if value == self.fail_value and self.attempts_failing and self.attempts_cache[value] <= self.attempts_failing:
            sys.exit(self.signal_code)
        return value


def test_no_retry():
    """
        Checks that all the tasks are executed when retry is disabled and a worker is killed
    """
    process_nr = 4
    pool = SafePool(processes=process_nr)
    exit_cb = ExitCallback(exit_code=signal.SIGKILL, fail_value=0, attempts_failing=1)

    cases = [0, 20, 0, 40, 50, 0, 60, 0, 80, 0, 100, 0]
    res = pool.map_async(exit_cb.run, cases, retry_killed_tasks=False)

    pool.close()
    pool.join()
    results = res._value
    empty_results = [x for x in results if x is None]
    assert (len(empty_results) == 6)
    assert (len(pool.get_killed_tasks()) == 6)


def test_retry():
    """
        Checks that all the tasks are executed when retry is enabled and a worker is killed
    """
    process_nr = 4
    pool = SafePool(processes=process_nr, retry_killed_tasks=True)
    exit_cb = ExitCallback(exit_code=signal.SIGKILL, fail_value=0, attempts_failing=1)

    cases = [0, 20, 0, 40, 50, 0, 60, 0, 80, 0, 100, 0]
    res = pool.map_async(exit_cb.run, cases)

    pool.close()
    pool.join()
    results = res._value
    empty_results = [x for x in results if x is None]
    assert (not empty_results)
    assert (len(pool.get_killed_tasks()) == 6)


def test_multi_retry():
    """
        Checks that when the workers fail several times processing the same task
        If retry is enabled we should get all the results
    """
    process_nr = 4
    pool = SafePool(processes=process_nr, retry_killed_tasks=True)
    exit_cb = ExitCallback(exit_code=signal.SIGKILL, fail_value=0, attempts_failing=2)

    cases = [0, 20, 0, 40, 50, 0, 60, 0, 80, 0, 100, 0]
    res = pool.map_async(exit_cb.run, cases)

    pool.close()
    pool.join()
    results = res._value
    empty_results = [x for x in results if x is None]
    assert (not empty_results)
    assert (len(pool.get_killed_tasks()) == 6)  # each task will fail twice


def test_abort():
    """
        Checks the pool stops and raise an Exception when `abort_when_killed` is enabled
    """
    process_nr = 4
    pool = SafePool(processes=process_nr, abort_when_killed=True)
    exit_cb = ExitCallback(exit_code=signal.SIGKILL, fail_value=0, attempts_failing=1)

    cases = [0, 20, 0, 40, 50, 0, 60, 0, 80, 0, 100, 0]
    res = pool.map_async(exit_cb.run, cases)
    pool.close()
    pool.join()
    result = res._value
    assert (isinstance(result, SubProcessKilledException))



