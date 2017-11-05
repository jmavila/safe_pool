# Safe Pool - A Python Multiprocessing Pool Extension

Extension for the standard Python Multiprocessing Pool class, helps a better handling of unusual exceptions,
more specifically it handles the situation when a subprocess of the pool receives the SIGKILL signal (and other similar ones)
from other processes (e.g.:OS).

See the official Python2.7 multiprocessing.Pool code [here](https://github.com/python/cpython/blob/2.7/Lib/multiprocessing/pool.py)

# Why you may need SafePool?
If your system load is high, and memory can be low sometimes, when you start a multiprocessing pool, the OS could kills some
of the pool sub-processes.
This makes sense as the OS needs the kernel processes up and running, so when it needs
more memory to keep them up, it will kill other processes running. Unfortunately, one of that processes can be one of your
pool sub-processes. The OS job in charge of this is called `the OOM killer`. Learn more in the next few links:

- https://plumbr.io/blog/memory-leaks/out-of-memory-kill-process-or-sacrifice-child
- http://www.win.tue.nl/~aeb/linux/lk/lk-9.html#ss9.6
- https://www.hskupin.info/2010/06/17/how-to-fix-the-oom-killer-crashe-under-linux/

What any programmer would expect is that the Python2.7 multiprocessing pool is smart enough to handle this situation,
but it actually is not the case. The result is that your pool gets **hung**or **stuck** in
an infinite loop, so the pool execution never finishes. This can bring you lots of headaches and hours of debugging to
notice the exact reason why this happens and how to fix it. This is the reason I decided to fix this, and hopefully this
effort will help many other people as frustrated as me :)

There's a bug in the current Pool implementation. Some people already noticed it, and they created bugs in
the Python Issue tracker:

- https://bugs.python.org/issue24927
- https://bugs.python.org/issue22393

Basically, the workers thread keeps running when one sub-process is killed and they don't report the MapResult class
that it needs to terminate the Pool execution.

Unfortunately those fixes are 1) not merged, and b) not enough to really fix the problem. What we need (and `SafePool`) will provide is:

- 1) the pool needs to finish even if subprocesses are killed and die, missing the results of the killed workers, but
with the rest of results.
- 2) the option to abort the whole pool if a sub-process die.
- 3) the option to retry faulty tasks.
- 4) the option to know which are the faulty tasks.


# How is this fixed?

Firstly, I'd like to clarity this will not fix the "Out Of Memory" (OOM) error at all. For that, you need either optimizing
your code, buy more memory or both of them. Even, you could think about switching to distribute your tasks using some
of the nice libraries out there as Celery or ZeroMQ.

Main steps to fix the issue were:

- 1) Finding out the core reason of the bug and fix it: as explained before, the current thread maintaining the workers (`Pool._handle_workers`)
enters in an infinite loop because the `MapResult` instance is not aware of this situation.
The default behaviour of MapResult is decrementing its attribute `_number_left` when a new result coming from the workers
is received, what is done through the Results Thread (`Pool._handle_results`), it will get every new worker from the
pool workers (`_outqueue`) by the next sentence: (`cache[job]._set(i, obj)`).

So, only when `number_left` is zero or the result coming from the worker is an exception, it terminates the pool.
But this is the only way that the `Pool` class is able to terminate its own execution.

The fix, therefore, is as simple as notifying `MapResult` whenever a worker dies from the workers handling pool.
When this happens two options are available:

- a) skip the result for that worker and continue
- b) terminate the pool and finish.

Both are available in the current implementation of the `SafePool` class.


- 2) Subclassing the original `Pool` class with a new `SafePool` class that will not hang even if children processes are sacrified.
In this way, we use the standard multiprocess code patched with the bug fixes and improvements in the `SafePool` class.


# Basic Usage

You just need to instantiate the `SafePool` class and use it in the same way as the Pool class:

```
    pool = SafePool(processes=process_nr)
    res = pool.map(f, range(10))
```
The mapping results will contain all the expected results, except the ones corresponding to the killed task(s). They will be None.

Example of result for a multiprocessing the square of the items in a list:

```
[0, 1, 4, None, 16, 25, 36, 49, 64, 81]
```

In this example, a worker in assigned to execute the square of '3' was killed. It could not provide the result to the result
list. This is way better than having no result at all and a stalled process running forever!


# Extra options

## Abort when subprocess is killed

By default, SafePool will try to continue when a sub-process dies. But you can force the Pool to stop when the first
sub-process is killed using the `abort_when_killed` option. This is recommended in most of the cases, because
it's likely that when a worker is killed, other ones will be killed as well, so the pool will stop giving a exception with
the reason:
```
    pool = SafePool(processes=process_nr, abort_when_killed=True)
    res = pool.map_async(f, range(10))  # res is an instance of SubProcessKilledException
```


## Retry killed tasks

Be careful with this. If your tasks were killed, it usually means your OS is overloaded and has not enough memory.
Knowing that, you can use the `retry_killed_tasks` when instantiating the SafePool object. It will force to retry
the task.
In future releases  it will be smarter: checking the actual memory availability, and if it is a
reasonable amount, retry the task or skip if a waiting timeout expires.

```
    pool = SafePool(processes=process_nr, retry_killed_tasks=True)
    res = pool.map_async(f, range(10))  # using map_async instead of map to help test async killing a subprocess
```

In this case, the result list (from the previous example) would be:

```
[0, 1, 4, None, 16, 25, 36, 49, 64, 81]
```

## Get killed tasks:

You can get the list of tasks that were killed using the method:

```
    pool.get_killed_tasks() # list with tuples with function and arguments: (<function __main__.f>, (30,)),)
```


# Future work

- smarter retry option: check OS memory, retry only in case it is safe. Consider max attempts and waiting timeouts.
- add option to prevent a sub-process exception to stop the pool. Use the same approach as when workers are killed
and restarted with the `retry_killed_tasks` option.
- job stats, when each task started and finished
