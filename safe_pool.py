# -*- coding: utf8 -*-
#
# Module providing the `SafePool` class for managing a process pool ensuring
# it will not hangs when OS kill any of its subprocesses
#
# safepool.py
#
# Copyright (c) 2017, Juan Ávila
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
# 1. Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
# 2. Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in the
#    documentation and/or other materials provided with the distribution.
# 3. Neither the name of author nor the names of any contributors may be
#    used to endorse or promote products derived from this software
#    without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS "AS IS" AND
# ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED.  IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
# OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
# HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
# OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
# SUCH DAMAGE.
#

__all__ = ['SafePool']

#
# Imports
#

from multiprocessing import Process, Manager
from multiprocessing.util import debug
from multiprocessing.pool import Pool, MapResult, RUN, mapstar
from uuid import uuid4
from signal import SIGKILL, SIGTERM, SIGSEGV, SIGINT, SIGPIPE

HANDLED_SIGNALS = [SIGKILL, SIGTERM, SIGSEGV, SIGINT, SIGPIPE]


#
# Code run by worker processes
#

def worker(inqueue, outqueue, initializer=None, initargs=(), maxtasks=None,
           worker_tasks_log=None, worker_name=None):
    """
        Extends multiprocessing.pool.worker adding worker log registry, that can be used to determine
        which task was running when a worker was killed 
    """
    assert maxtasks is None or (type(maxtasks) == int and maxtasks > 0)
    put = outqueue.put
    get = inqueue.get
    if hasattr(inqueue, '_writer'):
        inqueue._writer.close()
        outqueue._reader.close()

    if initializer is not None:
        initializer(*initargs)

    completed = 0
    while maxtasks is None or (maxtasks and completed < maxtasks):
        try:
            task = get()
        except (EOFError, IOError):
            debug('worker got EOFError or IOError -- exiting')
            break

        if task is None:
            debug('worker got sentinel -- exiting')
            break

        job, i, func, args, kwds = task

        if worker_tasks_log is not None and worker_name:
            worker_tasks_log[worker_name] = task
        try:
            result = (True, func(*args, **kwds))
        except Exception, e:
            result = (False, e)
        if worker_tasks_log is not None and worker_name and worker_name in worker_tasks_log:
            del worker_tasks_log[worker_name]
        put((job, i, result))
        completed += 1
    debug('worker exiting after %d tasks' % completed)


#
# Class representing a process pool
#
class SafePool(Pool):
    """
    Extends multiprocessing.pool.Pool:
    - new pool param: retry_killed_tasks: allow to retry killed tasks
    - new pool attr: worker_tasks_log a Manager.dict() to keep track the latest task executed by each worker
    - code added to '_join_exited_workers' to handle 2 situations:
    -  a) worker exited with kill error: decrement cache counter to prevent an infinite loop (infamous handling bug)
    -  b) if 'retry_killed_tasks' is enabled, the worker task killed is reappended to the in-queue so it will be picked 
          up by one of the active workers.
    """
    Process = Process

    def __init__(self, processes=None, initializer=None, initargs=(),
                 maxtasksperchild=None, retry_killed_tasks=False, abort_when_killed=False):
        """Extends Pool with a shared dict 'worker_tasks_log', used to log tasks executed by workers
        """
        self._worker_tasks_log = Manager().dict()
        self._retry_killed_tasks = retry_killed_tasks
        self._abort_when_killed = abort_when_killed
        super(SafePool, self).__init__(processes, initializer, initargs, maxtasksperchild)

    def _join_exited_workers(self):
        """Extends pool _join_exited_workers. 
        Detects When a worker is killed by either SIGKILL, SIGTERM, SIGSEGV, SIGNIT or SIGPIPE
        If the pool is set to retry it will put in the input queue the pending task to re-execute it
        Else, the left counter is decremented so the pool can finish its execution in a normal way
        """
        cleaned = False
        for i in reversed(range(len(self._pool))):
            worker = self._pool[i]
            debug('Worker {} exitcode: {}'.format(worker.name, worker.exitcode))
            if worker.exitcode is not None:
                debug('cleaning up worker %d' % i)
                worker.join()
                if abs(worker.exitcode) in HANDLED_SIGNALS:
                    debug('*'*10)
                    debug(str(HANDLED_SIGNALS))
                    debug('Worker died: ' + str(worker.name))
                    pending_task = self._worker_tasks_log.get(worker.name)
                    if self._retry_killed_tasks:
                        debug('It was assigned to task: ' + str(pending_task))
                        if pending_task:
                            debug('Retrying task: ' + str(pending_task))
                            try:
                                self._quick_put(pending_task)
                            except IOError:
                                debug('could not put task on queue')
                                break

                    elif self._cache and len(self._cache) == 1:  # just skip the result
                            cache_index = list(self._cache)[0]
                            debug('Skipping result of faulty worker: ' + str(worker.name))
                            if self._abort_when_killed:
                                self._cache[cache_index].abort_workers()
                            else:
                                self._cache[cache_index].handle_killed_worker()
                cleaned = True
                del self._pool[i]
        return cleaned

    def map_async(self, func, iterable, chunksize=None, callback=None):
        '''
        Asynchronous equivalent of `map()` builtin
        '''
        assert self._state == RUN
        if not hasattr(iterable, '__len__'):
            iterable = list(iterable)

        if chunksize is None:
            chunksize, extra = divmod(len(iterable), len(self._pool) * 4)
            if extra:
                chunksize += 1
        if len(iterable) == 0:
            chunksize = 0

        task_batches = Pool._get_tasks(func, iterable, chunksize)
        result = SafeMapResult(self._cache, chunksize, len(iterable), callback)
        self._taskqueue.put((((result._job, i, mapstar, (x,), {})
                              for i, x in enumerate(task_batches)), None))
        return result

    def _repopulate_pool(self):
        """Bring the number of pool processes up to the specified number,
        for use after reaping workers which have exited.
        """
        for i in range(self._processes - len(self._pool)):
            worker_name = uuid4().hex
            w = self.Process(target=worker,
                             args=(self._inqueue, self._outqueue,
                                   self._initializer,
                                   self._initargs, self._maxtasksperchild,
                                   self._worker_tasks_log,
                                   worker_name)
                             )
            self._pool.append(w)
            w.name = worker_name
            w.daemon = True
            w.start()
            debug('added worker' + str(w.name))

    def get_killed_tasks(self):
        """
        Returns the number of failing tasks (because of workers died)        
        :return: list of tuples with task info. Each task looks as a tuple with tuple with task function 
                    at 1st position and arguments as 2nd one:
            (<function __main__.f>, (30,)),)                    
        """
        return [task[3][0] for task in self._worker_tasks_log.values()]


class SubProcessKilledException(Exception):
    pass


class SafeMapResult(MapResult):
    """
    Extends MapResult with the method `handle_killed_worker` that is called from the Worker thread
    when it detects that a worker has died unexpectedly
    """

    def __init__(self, cache, chunksize, length, callback):
        super(SafeMapResult, self).__init__(cache, chunksize, length, callback)

    def handle_killed_worker(self):
        """
        Based on the code of MapResult._set, it decrements the number of pending tasks and
        checks if there's something else to do. If nothing remains, it deletes the cache (this will
        stop the workers thread and notify the rest of the threads (results, tasks) to finish.
        """
        self._number_left -= 1
        if self._number_left == 0:
            if self._callback:
                self._callback(self._value)
            del self._cache[self._job]
            self._cond.acquire()
            try:
                self._ready = True
                self._cond.notify()
            finally:
                self._cond.release()

    def abort_workers(self):
        """
        Stop execution of all the workers and set a `SubProcessKilledException` as the result
        """
        self._success = False
        self._value = SubProcessKilledException('Process received a kill signal. It may be related to a lack of OS Memory')
        del self._cache[self._job]
        self._cond.acquire()
        try:
            self._ready = True
            self._cond.notify()
        finally:
            self._cond.release()

