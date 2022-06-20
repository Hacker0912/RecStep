#! /usr/bin/python3
from monitoring import MemoryMonitor
from monitoring import CpuMonitor
import time
import sys

log_dir = sys.argv[1]
tracking_time = int(sys.argv[2])
memory_log = open("{}/memory_log".format(log_dir), "w")
cpu_log = open("{}/cpu_log".format(log_dir), "w")

interval = 0.2

mm = MemoryMonitor()
cm = CpuMonitor()
start_time = time.time()
while tracking_time > time.time() - start_time:
    mm.update()
    cm.update()
    memory_log.write("{}\n".format(mm.memory["actual_usage_percent"]))
    cpu_log_str = ", ".join([str(cpu) for cpu in cm.cpu_percent])
    cpu_log.write("{}\n".format(cpu_log_str))
    time.sleep(interval)
