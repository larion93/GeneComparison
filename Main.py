import time
from Parser import Parser
from ComparingClass import ComparingClass

try:
    from line_profiler import LineProfiler
    def do_profile(follow=[]):
        def inner(func):
            def profiled_func(*args, **kwargs):
                try:
                    profiler = LineProfiler()
                    profiler.add_function(func)
                    for f in follow:
                        profiler.add_function(f)
                    profiler.enable_by_count()
                    return func(*args, **kwargs)
                finally:
                    profiler.print_stats()
            return profiled_func
        return inner

except ImportError:
    def do_profile(follow=[]):
        "Helpful if you accidentally leave in production!"
        def inner(func):
            def nothing(*args, **kwargs):
                return func(*args, **kwargs)
            return nothing
        return inner

###########################################
Parser.set_connection(username= 'admin', password= 'Password123456789', host='127.0.0.1', database='gene')
start_time = time.time()
Parser.parse_ftdna(open('E:/DNA/4585.ftdna-illumina.3187'), "snptable1")
print("Time parsing first file:", time.time()-start_time)
start_time = time.time()
Parser.parse_ftdna(open('E:/DNA/4630.ftdna-illumina.3229'), "snptable2")
print("Time parsing second file:", time.time()-start_time)

ComparingClass.load_genetic_map_files('E:/DNA/genetic_map_HapMapII_GRCh37/')
ComparingClass.set_connection(username= 'admin', password= 'Password123456789', host='127.0.0.1', database='gene')

obj1 = ComparingClass("snptable23andme1", "snptable23andme2", 700, 7, 150)
obj1.compare()
'''obj2 = ComparingClass("snptable23andme1", "snptable1", 700, 7, 150)
obj2.compare()
obj3 = ComparingClass("snptable23andme1", "snptable2", 700, 7, 150)
obj3.compare()'''
