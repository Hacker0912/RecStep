import os
import shutil
import psutil
from execution.config import QUICKSTEP_SHELL_DIR, THREADS_NUM


def start_quickstep_instance():
    quickstep_shell_arguments = list()
    quickstep_shell_arguments.append("-use_eliminate_empty_node=true")
    quickstep_shell_arguments.append("-enable_compactkey_hashjoin_op=true")
    quickstep_shell_arguments.append("-mode=network")
    quickstep_shell_arguments.append("-initialize_db")
    quickstep_shell_arguments.append("-force_save_dirty_blocks=false")
    quickstep_shell_arguments.append("-num_workers {}".format(THREADS_NUM))

    cmd = "{}/quickstep_cli_shell {} &".format(
        QUICKSTEP_SHELL_DIR, " ".join(quickstep_shell_arguments)
    )

    # Kill all running quickstep instances
    for proc in psutil.process_iter():
        if proc.status() == psutil.STATUS_ZOMBIE:
            continue
        if proc.name().find("quickstep") != -1:
            proc.kill()

    if os.path.exists("./qsstor"):
        shutil.rmtree("./qsstor")

    os.system(cmd)
