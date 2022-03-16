from contextlib import contextmanager
from cProfile import Profile
from pstats import Stats


@contextmanager
def profiler(enable, outfile):
    try:
        profiler = Profile()
        profiler.enable()

        yield
    finally:
        profiler.disable()
        stats = Stats(profiler)
        stats.sort_stats("tottime")
        stats.dump_stats(outfile)
