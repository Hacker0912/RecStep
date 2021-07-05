import json
import os

from optparse import OptionParser

config_json_file_name = 'Config.json'
with open(config_json_file_name) as config_json_file:
    config = json.load(config_json_file)

quickstep_shell_dir = config['QuickStep_Shell_Dir']

parser = OptionParser()

parser.add_option('--mode', type='string', dest='mode',
                  help='network or interactive')
parser.add_option('--initialize', action='store_true', dest='initialize', default=False,
                  help='initialize quickstep db instance')

(options, args) = parser.parse_args()

if options.mode is None:
    raise Exception("The quickstep_shell mode must be specified")

mode = options.mode
initialize = options.initialize

cmd = ''
if mode == 'network':
    cmd = quickstep_shell_dir + \
                               '/quickstep_cli_shell ' + \
                               '-use_eliminate_empty_node=true ' + \
                               '-enable_compactkey_hashjoin_op=true ' + \
                               '-mode=network ' + '-force_save_dirty_blocks=false ' + \
                               '-num_workers {}'.format(config['Parameters']['threads_num'])
elif mode == 'interactive':
    cmd = quickstep_shell_dir + '/quickstep_cli_shell -force_save_dirty_blocks=true '
elif mode == 'clean':
    cmd = "pgrep quickstep | xargs kill"

if initialize and mode != 'clean':
    cmd += ' -initialize_db'

os.system(cmd)


