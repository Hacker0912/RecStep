import sys
import time
import collections
from copy import deepcopy

from execution.config import *
from execution.executor import Executor

from parser.datalog_program import DatalogProgram


def interpret(datalog_program_file_path):
    """
    Before iterative process starts:
        1. Read the file specifying the datalog program (ended with .datalog) and construct the datalog program object
        2. Create EDBs and load data (data should be put under the directory named './Input' by default)
        3. Create IDBs
        4. Analyze all the tables => build catalog including analytical stats of all tables
        5. Evaluate rules following the stratification
        6. Initialize all deltas => analyze all delta tables (for recursive rules if any)

    Iterative evaluation process (For recursive rules if any):
    """
    # Get the data structures storing the information of the input datalog program
    datalog_program = DatalogProgram(datalog_program_file_path)
    rules = datalog_program.rules
    edb_decl = datalog_program.edb_decl
    idb_decl = datalog_program.idb_decl
    rule_groups = datalog_program.rule_groups

    if not INTERPRET:
        sys.exit()

    # Build a mapping between relation name and the corresponding relation definition
    relation_def_map = dict()
    for relation in edb_decl:
        relation_def_map[relation["name"]] = {"relation": relation, "type": "edb"}
    for relation in idb_decl:
        relation_def_map[relation["name"]] = {"relation": relation, "type": "idb"}

    # Configure and initialize the executor
    executor = Executor()
    executor.log("Start creating IDB and EDB tables and populating facts")

    # Catalog to keep track of all the objects and stats
    catalog = dict()
    catalog["tables"] = dict()
    catalog["optimization"] = dict()
    # Create edb tables
    for relation in edb_decl:
        if not PRE_LOAD:
            catalog["tables"][relation["name"]] = executor.create_table_from_relation(
                relation
            )
        catalog["optimization"][relation["name"]] = dict()
        catalog["optimization"][relation["name"]]["size"] = 0

    # Create idb tables
    for relation in idb_decl:
        catalog["tables"][relation["name"]] = executor.create_table_from_relation(
            relation
        )
        catalog["optimization"][relation["name"]] = dict()
        catalog["optimization"][relation["name"]]["size"] = 0

    # Populate facts into edbs
    for relation in edb_decl:
        if not PRE_LOAD:
            executor.populate_data_into_edb(relation)
        catalog["optimization"][relation["name"]] = executor.count_rows(
            relation["name"]
        )

    # Analyze all tables
    executor.analyze([], count=True)
    executor.log_local_time()

    if STATIC_DEBUG:
        print("----------DEBUGGING SECTION----------")

    # Start generating code to evaluate rule groups
    rule_group_index = 0
    non_dedup_relations = set()
    # all rules are non-recursive
    for rule_group in rule_groups["rule_groups"]:
        for rule_index in rule_group:
            relation_name = rules[rule_index]["head"]["name"]
            if relation_name not in non_dedup_relations:
                continue
            non_dedup_relations.add(relation_name)

    for rule_group in rule_groups["rule_groups"]:
        executor.log(
            "-----Start evaluating rule group[{}]-----".format(rule_group_index),
        )
        evaluated_rules = [rules[rule_index] for rule_index in rule_group]
        if not rule_groups["rule_group_bitmap"][rule_group_index]:
            executor.log(">>>>Evaluating Non-Recursive Rule<<<<<")
            idb_relation_name = evaluated_rules[0]["head"]["name"]
            executor.non_recursive_rule_eval(
                idb_relation_name,
                catalog,
                evaluated_rules,
                relation_def_map
            )
            executor.log_local_time(
                descrip="Rule Evaluation Time",
            )
            executor.update_local_time()
            executor.log("-----SEPERATOR-----\n")

        else:
            executor.log(">>>>Evaluating Recursive Rules<<<<<")
            executor.recursive_rules_eval(
                catalog,
                evaluated_rules,
                relation_def_map,
            )
        rule_group_index += 1

    executor.log_global_time(descrip="Total Evaluation Time")
    if REMOVE_IDBS:
        for relation in idb_decl:
            executor.drop_table(relation["name"])
    
    if not REMOVE_IDBS and WRITE_TO_CSV:
        for relation in idb_decl:
            executor.output_data_from_table_to_csv(relation["name"])

    executor.stop()


def main():
    try:
        input_datalog_program_file_path = sys.argv[1]
    except Exception as e:
        print(e)
        raise Exception("The file specifying the datalog program is missing")

    interpret(input_datalog_program_file_path)


if __name__ == "__main__":
    main()
