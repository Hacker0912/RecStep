import psutil
import time

MEMORY_NORMALIZE_FACTOR = 1024.00**3


class MemoryMonitor(object):

    def __init__(self):
        self.memory = dict()
        self.memory['total'] = 0.00
        self.memory['available'] = 0.00
        self.memory['percent'] = 0.00
        self.memory['used'] = 0.00
        self.memory['free'] = 0.00
        self.memory['active'] = 0.00
        self.memory['inactive'] = 0.00
        self.memory['actual_usage_percent'] = 0.00

    def update(self):
        current_memory_raw_stats = psutil.virtual_memory()
        self.memory['total'] = current_memory_raw_stats[0]/MEMORY_NORMALIZE_FACTOR
        self.memory['available'] = current_memory_raw_stats[1]/MEMORY_NORMALIZE_FACTOR
        self.memory['percent'] = current_memory_raw_stats[2]
        self.memory['used'] = current_memory_raw_stats[3]/MEMORY_NORMALIZE_FACTOR
        self.memory['free'] = current_memory_raw_stats[4]/MEMORY_NORMALIZE_FACTOR
        self.memory['active'] = current_memory_raw_stats[5]/MEMORY_NORMALIZE_FACTOR
        self.memory['inactive'] = current_memory_raw_stats[6]/MEMORY_NORMALIZE_FACTOR
        self.memory['actual_usage_percent'] = (self.memory['total'] - self.memory['available'])/self.memory['total']


class CpuMonitor(object):

    def __init__(self):
        self.cpu_percent = 0.00

    def update(self):
        self.cpu_percent = psutil.cpu_percent(percpu=True)


class TimeMonitor(object):

    def __init__(self):
        self.global_start_time = time.time()
        self.local_start_time = time.time()

    def update(self):
        self.local_start_time = time.time()

    def local_elapse_time(self):
        current_time = time.time()
        return current_time - self.local_start_time

    def global_elapse_time(self):
        current_time = time.time()
        return current_time - self.global_start_time


