# safe_pool

Extension for the standard Python Multiprocessing Pool class, helps a better handling of unusual exceptions,
more specifically it handles the situation when a subprocess of the pool receives the SIGKILL signal from other
processes (e.g.:OS).

See https://github.com/python/cpython/blob/master/Lib/multiprocessing/pool.py

# Why?
If your system memory is low and you start a multiprocessing pool, it can in some moment in the future your OS kills
some of the pool subprocesses. This makes sense as the OS needs the kernel processes up and running, so when it needs
more memory to keep them up, it will kill other processes running. Unfortunately, one of that processes can be one of your
pool subprocesses. The OS job in charge of this is called `the OOM killer`. Learn more in the next few links:

- https://plumbr.io/blog/memory-leaks/out-of-memory-kill-process-or-sacrifice-child
- http://www.win.tue.nl/~aeb/linux/lk/lk-9.html#ss9.6
- https://www.hskupin.info/2010/06/17/how-to-fix-the-oom-killer-crashe-under-linux/
- http://www.win.tue.nl/~aeb/linux/lk/lk-9.html#ss9.6

What any programmer would expect is that the pool is smart enough to handle this situation, but I can confirm this is
not the case in the multiprocessing Pool running in Python 2.7. The actual result is that your pool gets **hung** in
a loop forever, so your process hangs and never finishes. This can bring lots of headaches and hours of debugging to
notice the exact reason why this happens, so I decided to fix this, and hopefully this effort will help many other
people as frustrated as me :)

There's a bug in the current Pool implementation. Some people already noticed it, and they created bugs in
the Python Issue tracker:

- https://bugs.python.org/issue24927
- https://bugs.python.org/issue22393

Unfortunately this fixes are 1) not merged, and b) not enough to really fix the problem. What we need is:

- 1) the pool needs to finish even if subprocesses are killed and die.
- 2) the option to retry faulty tasks.
- 3) the option to know which are the faulty tasks.


# How is this fixed?

Firstly, I'd like to clarity this will not fix the "Out Of Memory" (OOM) error at all. For that, you need either optimizing
your code, buy more memory or both of them. Even, you could think about switching to paralelize your tasks using some
of the nice libraries out there as Celery or ZeroMQ.

So, this were my mains goals with this patch:

- 1) Locate the core reason of the hung. The current thread responsible of updating the results gets frozen because there's a
critical section around a shared result variable `number_left` (See the `MapResult` class in the original multiprocessing
code). This threads expect this number to be zero in order to notify the workers to finish and join to the main thread.
The fix is as simple as decrementing that variable whenever a worker dies from the workers handling pool.
This makes sense because the results threads has no other way to know some woker was killed.

- 2) Subclassing the original `Pool` class with a new `SafePool` that will not hang even if children processes are sacrified.
 (FYI: subclassing allowed me to write less than 10 lines of real changes comparing with the former Pool class!).


# Usage

You just need to instantiate the `SafePool` class and use it as the Pool class:

```
    pool = SafePool(processes=process_nr)
    res = pool.map(f, range(10))
```
The mapping results will contain all your results, except the ones corresponding to the killed task(s). They will be None.

Example of result for a multiprocessing the square of the items in a list:

```
[0, 1, 4, None, 16, 25, 36, 49, 64, 81]
```

In this case, a worker in charge to execute the square of '3' was killed. It could not provide the result to the result
list. This is way better than having no result at all and a stalled process running forever!


# Extra features

## Retry killed tasks:

Be careful with this. If your tasks were killed, it usually means your OS is overloaded and has not enough memory.
In future releases I have thought in making this smarter: checking the actual memory availability, and if it is
a reasonable amount, retry the task.

```
    pool = SafePool(processes=process_nr, retry_killed_tasks=True)
    res = pool.map_async(f, range(10))  # using map_async instead of map to help test async killing a subprocess
```

In this case, the result list (see example above) would be:

```
[0, 1, 4, None, 16, 25, 36, 49, 64, 81]
```

# Future work

- smarter retry option: check OS memory, rety only in case it is safe.
- capture more signals, for now only SIGKILL is handled, but it's easy to add more like SIGTERM.
