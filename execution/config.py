import json

config_json_file_name = "Config.json"
with open(config_json_file_name) as config_json_file:
    config = json.load(config_json_file)

###############
# Debug Flags #
###############
LOG_ON = config["Logging"]["log"]
STATIC_DEBUG = config["Debug"]["static_debug"]
DYNAMIC_DEBUG = config["Debug"]["dynamic_debug"]
DYNAMIC_DEBUG_ITER_NUM = config["Debug"]["dynamic_debug_iter_num"]
COST_MODEL_CHECK = config["Debug"]["cost_model_check"]
INTERPRET = config["Debug"]["interpret"]
##################
# Output Configs #
##################
WRITE_TO_CSV = config["Output"]["write_to_csv"]
RETAIN_FINAL_OUTPUT_ONLY = config["Output"]["retain_final_output_only"]
FINAL_OUTPUT_RELATIONS = config["Output"]["final_output_relations"]
################
# Input Config #
################
INPUT_DIR = config["Input_Dir"]
PRE_LOAD = config["Input"]["preload"]
CSV_DELIMITER = config["QuickStep"]["csv_delimiter"]
######################
# Optimization Flags #
######################
DEFAULT_SET_DIFF_ALG = config["Optimization"]["default_set_diff_alg"]
SET_DIFF_OP = config["Optimization"]["dynamic_set_diff"]
ANALYZER_OP = config["Optimization"]["analyzer_level"]
ANALYZE_ALL_TABLES = config["Optimization"]["analyze_all_tables"]
SELECTIVE_DEDUP = config["Optimization"]["selective_dedup"]
DEDUP_RELATION_LIST = config["Optimization"]["dedup_relation_list"]
UNIFIED_IDB_EVALUATION = config["Optimization"]["unified_idb_evaluation"]
#############
# Execution #
#############
QUICKSTEP_SHELL_DIR = config["QuickStep_Shell_Dir"]
BACK_END = config["Execution"]["backend"]
SINGLE_QUERY_EVALUATION = config["Execution"]["single_query_evaluation"]
INTERMEDIATE_DISTINCT = config["Execution"][
    "single_query_evaluation_intermediate_distinct"
]
FINAL_OUTPUT_STORE = config["Execution"]["single_query_evaluation_final_output_store"]
##############################
# Consistent Query Answering #
##############################
CQA_REWRITING = config["CQA"]["rewriting"]
CQA_ALGO = config["CQA"]["algorithm"]
PAIR_PRUNING_RULES = config["CQA"]["pair_pruning_rules"]
######################
#  System Parameters #
######################
# Actual threads available for computation
THREADS_NUM = config["Parameters"]["threads_num"]
# Block is the minimal parallelism unit
# This number only considers tables with 2 attributes
TUPLE_NUM_PER_BLOCK = config["Parameters"]["block_size"]
###########
# Logging #
###########
LOG_DIR = config["Logging"]["logging_directory"]
STDOUT = bool(config["Logging"]["logging_level"]["stdout"])
LOG = bool(config["Logging"]["logging_level"]["info_log"])
