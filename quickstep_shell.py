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

(options, args) = parser.parse_args()

if options.mode is None:
    raise Exception("The quickstep_shell mode must be specified")

mode = options.mode

cmd = ''
if mode == 'network':
    initialize_quickstep_cmd = quickstep_shell_dir + \
                               '/quickstep_cli_shell ' + \
                               '-initialize_db ' + \
                               '-use_eliminate_empty_node=true ' + \
                               '-enable_compactkey_hashjoin_op=true ' + \
                               '-mode=network ' + '-force_save_dirty_blocks=false ' + \
                               '-num_workers {}'.format(config['Parameters']['threads_num'])
    cmd = initialize_quickstep_cmd

elif mode == 'interactive':
    interactive_quickstep_shell_cmd = quickstep_shell_dir + '/quickstep_cli_shell -force_save_dirty_blocks=true '
    cmd = interactive_quickstep_shell_cmd


os.system(cmd)


