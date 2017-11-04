# -*- coding: utf8 -*-
#
# Module providing the `SafePool` class for managing a process pool ensuring
# it will not hangs when OS kill any of its subprocesses
#
# safepool.py
#
# Copyright (c) 2017, J Ávila
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
from multiprocessing.pool import Pool

SIG_KILL = -9

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
        put((job, i, result))
        completed += 1
    debug('worker exiting after %d tasks' % completed)

#
# Class representing a process pool
#
class SafePool(Pool):
    '''
    Extends multiprocessing.pool.Pool:
    - new pool param: retry_killed_tasks: allow to retry killed tasks
    - new pool attr: worker_tasks_log a Manager.dict() to keep track the latest task executed by each worker
    - code added to '_join_exited_workers' to handle 2 situations:
    -  a) worker exited with kill error: decrement cache counter to prevent an infinite loop (infamous handling bug)
    -  b) if 'retry_killed_tasks' is enabled, the worker task killed is reappended to the in-queue so it will be picked 
          up by one of the active workers.
    '''
    Process = Process

    def __init__(self, processes=None, initializer=None, initargs=(),
                 maxtasksperchild=None, retry_killed_tasks=False):
        """Extends Pool with a shared dict 'worker_tasks_log', used to log tasks executed by workers
        """
        self.worker_tasks_log = Manager().dict()
        self.retry_killed_tasks = retry_killed_tasks
        super(SafePool, self).__init__(processes, initializer, initargs, maxtasksperchild)

        
    def _join_exited_workers(self):
        """Extends pool _join_exited_workers. When a worker is killed (for now only SIG_KILL) is considered,
            it will 
        """
        cleaned = False
        for i in reversed(range(len(self._pool))):
            worker = self._pool[i]
            if worker.exitcode is not None:
                debug('cleaning up worker %d' % i)
                worker.join()
                if worker.exitcode == SIG_KILL:
                    debug('Worker died: ' + str(worker.name))  
                    if self.retry_killed_tasks:                  
                        missing_task = self.worker_tasks_log.get(worker.name)
                        debug('It was assigned to task: ' + str(missing_task))
                        if missing_task:
                            debug('RETRYING TASK: ' + str(missing_task))
                            try:
                                self._quick_put(missing_task)
                            except IOError:
                                debug('could not put task on queue')
                                break                                            
                    elif self._cache and len(self._cache) == 1:  # just skip the result
                        self._cache[0]._number_left -= 1
                        debug('Skipping result of faulty worker: ' + str(worker.name))
                cleaned = True
                del self._pool[i]
        return cleaned

    def _repopulate_pool(self):
        """Bring the number of pool processes up to the specified number,
        for use after reaping workers which have exited.
        """
        for i in range(self._processes - len(self._pool)):
            worker_name = 'PoolWorker-{}'.format(i)
            w = self.Process(target=worker,
                             args=(self._inqueue, self._outqueue,
                                   self._initializer,
                                   self._initargs, self._maxtasksperchild, 
                                   self.worker_tasks_log, 
                                   worker_name)
                            )
            self._pool.append(w)
            w.name = worker_name
            w.daemon = True
            w.start()
            debug('added worker' + str(w.name))
