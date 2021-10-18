from utility.monitoring import MemoryMonitor
from utility.monitoring import CpuMonitor
import time

memory_log = open("./log/memory_log", "w")
cpu_log = open("./log/cpu_log", "w")

interval = 1

mm = MemoryMonitor()
cm = CpuMonitor()

while True:
    mm.update()
    cm.update()
    memory_log.write("{}\n".format(mm.memory["actual_usage_percent"]))
    cpu_log_str = ", ".join([str(cpu) for cpu in cm.cpu_percent])
    cpu_log.write("{}\n".format(cpu_log_str))
    time.sleep(interval)
