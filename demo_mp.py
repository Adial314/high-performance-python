# Ansyncio testing
# Austin Dial
# Jul 19, 2020
#
#   Contains functions for testing multiprocessing.
#
# REF: https://www.machinelearningplus.com/python/parallel-processing-python/#:~:text=In%20python%2C%20the%20multiprocessing%20module,in%20completely%20separate%20memory%20locations.
#

# ---------------- IMPORT LIBRARIES ---------------- #

import multiprocessing as mp
import numpy as np
import config
import time



# ---------------- GLOBAL VARIABLES ---------------- #

async_results = list([])



# ----------------- SETUP DATASETS ----------------- #

# Prepare a single dataset for analysis
def create_data(rows, cols, range_min, range_max):
    """Creates a 2D-Array with `rows` rows and 'cols' columns."""
    
    np.random.RandomState(100)
    data = np.random.randint(range_min, range_max, size=[rows, cols])
    
    return data.tolist()
    

# Prepare a series of datasets for analysis
def create_datasets(rows, cols, range_min, range_max, length):
    """Creates an array of 2D-Arrays with the specified dimensions."""
    
    datasets = list([])
    for _ in range(0, length):
        datasets.append(create_data(rows=rows, cols=cols, range_min=range_min, range_max=range_max))
        
    return datasets



# --------------- PROBLEM SOLUTIONS --------------- #


# Define the dummy task
def count_values_within_range(iteration, row, minimum, maximum):
    """Returns how many numbers lie within `maximum` and `minimum` in a given `row`."""
    
    count = 0
    for n in row:
        if minimum <= n <= maximum:
            count = count + 1
    
    return iteration, count


# Define serial solution
def serial_solution(data, minimum, maximum):
    """Analyzes the dataset using standard processing."""
    
    results = list([])
    for row in data:
        _, result = count_values_within_range(iteration=0, row=row, minimum=minimum, maximum=maximum)
        results.append(result)
        
    return results
    
    
# Define synchronous parallel solution
def sync_parallel_solution(data, minimum, maximum):
    """Analyzes the dataset using synchronous parallel processing."""
    
    # Initialize the pool with the available processors
    pool = mp.Pool(mp.cpu_count())

    # Pass the function and arguments to the multiprocessing pool
    results = [pool.apply(count_values_within_range, args=(0, row, minimum, maximum)) for row in data]
    
    # Close the pool to release resources
    pool.close()
    
    # Return analysis results
    return results
    
    
# Define callback function
def collect_result(result):
    """Aggregates the results of an asynchronous parallel computing task."""
    
    global async_results
    # async_results.append(result)

    
# Define asynchronous parallel solution
def async_parallel_solution(data, minimum, maximum):
    """Analyzes the dataset using asynchronous parallel processing."""
    
    # Initialize the pool with the available processors
    pool = mp.Pool(mp.cpu_count())
    
    for i, row in enumerate(data):
        pool.apply_async(count_values_within_range, args=(i, row, minimum, maximum), callback=collect_result)
    
    # Close the pool to release resources
    pool.close()
    
    # Postpone the execution of next line of code until all processes in the queue are done
    pool.join()



# ----------------- MAIN ANALYSIS ----------------- #

# Analyze serial solution
def analyze_serial(rows, cols, range_min, range_max, length, search_min, search_max):
    
    print("LOG: Instantiating datasets...\n")
    
    # Instantiate datasets
    datasets = create_datasets(rows=rows, cols=cols, range_min=range_min, range_max=range_max, length=length)
        
    print("LOG: Analyzing serial solution...\n")
    
    # ----- SERIAL ----- #
    
    # Analyze serial solution
    serial_performance = list([])
    for dataset in datasets:
        start = time.time()
        serial_solution(data=dataset, minimum=search_min, maximum=search_max);
        stop = time.time()
        serial_performance.append(stop - start)
    
    # Print performance results
    precision = 3
    minimum = round(np.min(serial_performance), precision)
    maximum = round(np.max(serial_performance), precision)
    average = round(np.average(serial_performance), precision)
    
    print("# " + 10*"-" + " PERFORMANCE " + 10*"-" + " #\n")
    print("MIN: {} [s]\nMAX: {} [s]\nAVE: {} [s]\n".format(minimum, maximum, average))
    
    # Return statistics
    return minimum, maximum, average
    

# Analyze synchronous parallel solution
def analyze_sync_parallel(rows, cols, range_min, range_max, length, search_min, search_max):
    
    print("LOG: Instantiating datasets...\n")
    
    # Instantiate datasets
    datasets = create_datasets(rows=rows, cols=cols, range_min=range_min, range_max=range_max, length=length)
    
    print("LOG: Analyzing synchronous parallel solution...\n")
    
    # ----- SYNCHRONOUS ----- #
    
    # Analyze synchronous parallel solution
    sync_parallel_performance = list([])
    for dataset in datasets:
        start = time.time()
        sync_parallel_solution(data=dataset, minimum=search_min, maximum=search_max);
        stop = time.time()
        sync_parallel_performance.append(stop - start)
    
    # Print synchronous parallel results
    precision = 3
    minimum = round(np.min(sync_parallel_performance), precision)
    maximum = round(np.max(sync_parallel_performance), precision)
    average = round(np.average(sync_parallel_performance), precision)
    
    print("# " + 10*"-" + " PERFORMANCE " + 10*"-" + " #\n")
    print("MIN: {} [s]\nMAX: {} [s]\nAVE: {} [s]\n".format(minimum, maximum, average))
    
    # Return statistics
    return minimum, maximum, average
    
    
# Analyze asynchronous parallel solution
def analyze_async_parallel(rows, cols, range_min, range_max, length, search_min, search_max):
    
    print("LOG: Instantiating datasets...\n")
    
    # Instantiate datasets
    datasets = create_datasets(rows=rows, cols=cols, range_min=range_min, range_max=range_max, length=length)
    
    print("LOG: Analyzing asynchronous parallel solution...\n")
    
    # ----- ASYNCHRONOUS ----- #
    
    # Analyze asynchronous parallel solution
    async_parallel_performance = list([])
    for dataset in datasets:
        start = time.time()
        async_parallel_solution(data=dataset, minimum=search_min, maximum=search_max);
        stop = time.time()
        async_parallel_performance.append(stop - start)
    
    # Print asynchronous parallel results
    precision = 3
    minimum = round(np.min(async_parallel_performance), precision)
    maximum = round(np.max(async_parallel_performance), precision)
    average = round(np.average(async_parallel_performance), precision)
    
    print("# " + 10*"-" + " PERFORMANCE " + 10*"-" + " #\n")
    print("MIN: {} [s]\nMAX: {} [s]\nAVE: {} [s]\n".format(minimum, maximum, average))
    
    # Optionally sort async results by iteration
    # async_results.sort(key=lambda x: x[0])
    # async_results_final = [r for i, r in async_results]
    
    # Return statistics
    return minimum, maximum, average
    


# ------------------ MAIN PROGRAM ------------------ #

# Script procedures
if __name__ == "__main__":
    
    # Quantify available resources
    cpus = mp.cpu_count()
    print("CPUs: {}".format(cpus))
    if cpus == 1:
        print("!!! WARNING !!! MULTIPROCESSING REQUIRES MORE THAN ONE PROCESSOR.\n")
    
    # Call analysis functions
    
    print("# " + 15*"-" + " SERIALIZED " + 15*"-" + " #\n")
    
    ser_mini, ser_maxi, ser_ave = analyze_serial(rows=config.rows, cols=config.cols, range_min=config.range_min,
                                                range_max=config.range_max, length=config.length, search_min=config.search_min,
                                                search_max=config.search_max)
                                                
    print("# " + 15*"-" + " SYNCHRONOUS " + 15*"-" + " #\n")
            
    syn_mini, syn_maxi, syn_ave = analyze_sync_parallel(rows=config.rows, cols=config.cols, range_min=config.range_min,
                                                range_max=config.range_max, length=config.length, search_min=config.search_min,
                                                search_max=config.search_max)
                                                
    print("# " + 14*"-" + " ASYNCHRONOUS " + 14*"-" + " #\n")
            
    asyn_mini, asyn_maxi, asyn_ave = analyze_async_parallel(rows=config.rows, cols=config.cols, range_min=config.range_min,
                                                range_max=config.range_max, length=config.length, search_min=config.search_min,
                                                search_max=config.search_max)
                                                
    print("LOG: Analysis completed.\n")

