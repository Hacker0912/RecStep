from utility.monitoring import MemoryMonitor
from utility.monitoring import CpuMonitor
import time

memory_log = open('./log/memory_log', 'w')
cpu_log = open('./log/cpu_log', 'w')

interval = 1

mm = MemoryMonitor()
cm = CpuMonitor()

while True:
    mm.update()
    cm.update()
    memory_log.write(str(mm.memory['actual_usage_percent']) + '\n')
    for cpu in cm.cpu_percent:
        cpu_log.write(str(cpu) + ', ') 
    cpu_log.write('\n')
    time.sleep(interval)




