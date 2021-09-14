import sys
import time
import collections
import json
import os
from copy import deepcopy

from parser import datalog_program
import rule_analyzer.translator
import query_generator.sql_query_generator

from quickstep_api import quickstep
from quickstep_api.quickstep import Table

config_json_file_name = 'Config.json'
with open(config_json_file_name) as config_json_file:
    config = json.load(config_json_file)

######################
#     Debug Flags    #
######################
LOG_ON = config['Logging']['log']
STATIC_DEBUG = config['Debug']['static_debug']
DYNAMIC_DEBUG = config['Debug']['dynamic_debug']
DYNAMIC_DEBUG_ITER_NUM = config['Debug']['dynamic_debug_iter_num']
COST_MODEL_CHECK = config['Debug']['cost_model_check']
INTERPRET = config['Debug']['interpret']
##################
# Output Configs #
##################
WRITE_TO_CSV = config['Output']['write_to_csv']
###########################
# File Parsing Parameters #
###########################
CSV_DELIMITER = config['QuickStep']['csv_delimiter']
######################
# Optimization Flags #
######################
DEFAULT_SET_DIFF_ALG = config['Optimization']['default_set_diff_alg']
SET_DIFF_OP = config['Optimization']['dynamic_set_diff']
CQA_OP = config['Optimization']['cqa']
CQA_DELAY_DEDUP_RELATION_LIST = config['Optimization']['cqa_delay_dedup_relation_list']
######################
#  System Parameters #
######################
# Actual threads available for computation
THREADS_NUM = config['Parameters']['threads_num']
# Block is the minimal parallelism unit
# This number only considers tables with 2 attributes
TUPLE_NUM_PER_BLOCK = config['Parameters']['block_size']
# Frequent Used Global Variables
COMMON_TABLE_NAME = 'COMMON_TABLE'


def log_info(logger, log_str):
    if LOG_ON:
        logger.info(log_str)


def log_info_time(logger, log_time, time_descrip='Time'):
    if LOG_ON:
        logger.info("{}: {}".format(time_descrip, log_time))


def update_time(time_monitor):
    if LOG_ON:
        time_monitor.update()


def count_row(quickstep_shell_instance, logger, table_name):
    row_num = quickstep_shell_instance.count_rows(table_name)
    if LOG_ON:
        logger.info("Number of tuples in {}: {}".format(table_name, row_num))

    return row_num


def is_trivial_scc(scc, dependency_graph):
    """
    A given scc is called trivial if it only contains a single rule and there is no
    self-loop on that rule in the dependency graph - such scc evaluation requires no iteration
    """
    if len(scc) == 1:
        rule_index = scc[0]
        dependent_rules = dependency_graph[rule_index]
        for dependent_rule_index in dependent_rules:
            if dependent_rule_index == rule_index:
                return False
        return True

    return False


def create_table_from_relation(quickstep_shell_instance, relation, table_name=""):
    if len(table_name) == 0:
        table_name = relation['name']

    attributes = relation['attributes']

    table = Table(table_name)

    for attribute in attributes:
        table.add_attribute(attribute.name, attribute.type)

    quickstep_shell_instance.create_table(table)
    return table


def populate_data_into_edb(quickstep_shell_instance, relation, delimiter=CSV_DELIMITER):
    """
    Given the data structure storing the information of the relation, load the data into the
    created table from the file given under the specified path ./Input/relation.tbl
    """
    table_name = relation['name']
    input_file_name = "{}/{}.csv".format(config['Input_Dir'], table_name)
    quickstep_shell_instance.load_data_from_file(
        table_name, input_file_name, delimiter)


def load_data_from_table(quickstep_shell_instance, src_table, dest_table):
    src_table_attributes = src_table.attributes
    dest_table_attributes = dest_table.attributes

    src_table_attribute_list = [
        attribute for attribute in src_table_attributes]
    dest_table_attribte_list = [
        attribute for attribute in dest_table_attributes]

    quickstep_shell_instance.load_data_from_table(src_table, src_table_attribute_list,
                                                  dest_table, dest_table_attribte_list)


def non_recursive_rule_eval(quickstep_shell_instance, logger, catalog, datalog_rule, relation_def_map,
                            delay_dedup_relation_counter={}):
    """
    Example:
        Schema: A(a,b), B(a,b), C(a,b), D(a,b)
        Rule: A(x,y) :- B(z,x), C(z,w), D(w,y)
    1. Map the attributes in the head to the attributes in the body -> attributes_map (for 'select' and 'from')
       key-value pair:  <head_atom_arg_index, [body_atom_index, body_atom_arg_index]>
    2. Construct the map summarizing the join operations
       key-value pairs: <body_atom_arg_index, [<body_atom_index, body_atom_arg_indices>]>
    """

    # Map attributes to be projected
    head = datalog_rule['head']
    head_name = head['name']
    body = datalog_rule['body']

    if body is None:
        # insertion - example rule: R(1, 2) :-
        quickstep_shell_instance.sql_command(
            query_generator.sql_query_generator.generate_insertion_str(head) + ';')
        return

    original_body_atom_list = body['atoms']
    body_atom_list = deepcopy(body['atoms'])
    negation_atom_list = body['negations']

    if LOG_ON:
        count_row(quickstep_shell_instance, logger, head_name)

    # select_info = [attributes_map, attributes_type_map, aggregation_map]
    select_info = rule_analyzer.translator.extract_selection_info(datalog_rule)

    # join_info = join_map
    join_info = rule_analyzer.translator.extract_join_info(datalog_rule)

    # negation_map
    negation_info = rule_analyzer.translator.extract_negation_map(body)

    # comparison_map
    comparison_map = rule_analyzer.translator.extract_comparison_map(
        body, body_atom_list)

    # constant constraint map
    constant_constraint_map = rule_analyzer.translator.extract_constant_constraint_map(
        body)

    body_atom_alias_list = rule_analyzer.translator.build_atom_aliases(
        body_atom_list)

    negation_atom_alias_list = rule_analyzer.translator.build_negation_atom_aliases(
        negation_atom_list)

    # select
    select_str = query_generator.sql_query_generator.generate_select(datalog_rule, select_info,
                                                                     relation_def_map, body_atom_alias_list)

    # from
    from_str = query_generator.sql_query_generator.generate_from(
        body_atom_list, body_atom_alias_list)

    # where::join
    join_str = query_generator.sql_query_generator.generate_join_str(join_info, original_body_atom_list,
                                                                     body_atom_alias_list, relation_def_map)

    # where::comparison
    compare_str = query_generator.sql_query_generator.generate_compare_str(comparison_map, original_body_atom_list,
                                                                           body_atom_alias_list, relation_def_map)

    # where::constant_constraint
    constant_constraint_str = query_generator.sql_query_generator.generate_constant_constraint_str(
        constant_constraint_map, body,
        body_atom_alias_list,
        relation_def_map)

    # where::negation
    negation_str = query_generator.sql_query_generator.generate_negation_str(negation_info, original_body_atom_list,
                                                                             negation_atom_list,
                                                                             body_atom_alias_list,
                                                                             negation_atom_alias_list,
                                                                             relation_def_map)

    non_recursive_rule_eval_str = select_str + ' ' + \
        from_str

    if len(join_str) > 0 or len(compare_str) > 0 or len(constant_constraint_str) > 0 or len(negation_str) > 0:
        non_recursive_rule_eval_str += ' ' + \
                                       'WHERE' + ' '

        condition = False
        if len(join_str) > 0:
            non_recursive_rule_eval_str += join_str
            condition = True
        if len(compare_str) > 0:
            if condition:
                non_recursive_rule_eval_str += ' AND '
            non_recursive_rule_eval_str += compare_str
            condition = True
        if len(constant_constraint_str) > 0:
            if condition:
                non_recursive_rule_eval_str += ' AND '
            non_recursive_rule_eval_str += constant_constraint_str
            condition = True
        if len(negation_str) > 0:
            if condition:
                non_recursive_rule_eval_str += ' AND '
            non_recursive_rule_eval_str += negation_str

    # aggregation (group by)
    aggregation_map = select_info[2]
    if len(aggregation_map) > 0:
        group_by_str = query_generator.sql_query_generator.generate_group_by_str(
            relation_def_map[head_name][0]['attributes'],
            aggregation_map)
        non_recursive_rule_eval_str += ' ' + group_by_str

    if STATIC_DEBUG:
        print('##### NON-RECURSIVE RULE #####')
        print(datalog_program.iterate_datalog_rule(datalog_rule))
        print('##### NON-RECURSIVE RULE EVAL SQL######')
        print(non_recursive_rule_eval_str)

    head_relation_name = head['name']
    if not CQA_OP:
        head_relation_table = catalog['tables'][head_relation_name]
        # Create tmp table to store the evaluation results
        head_relation = relation_def_map[head_relation_name][0]
        if catalog['optimization'][head_relation_name]['size'] == 0 and len(aggregation_map) == 0:
            # 1st time populating the relation
            quickstep_shell_instance.load_data_from_eval_query_str(
                head_relation_table, non_recursive_rule_eval_str, dedup=True)

            catalog['optimization'][head_relation_name]['size'] = count_row(
                quickstep_shell_instance, logger, head_name)
        else:
            tmp_relation = deepcopy(head_relation)
            tmp_relation['name'] = 'tmp_res_table'
            catalog['tables']['tmp_res_table'] = create_table_from_relation(
                quickstep_shell_instance, tmp_relation)
            # Insert the evaluation results into tmp table
            quickstep_shell_instance.load_data_from_eval_query_str(catalog['tables']['tmp_res_table'], non_recursive_rule_eval_str)
            # Load data from tmp table into the table corresponding to the head atom
            tmp_relation_table = catalog['tables']['tmp_res_table']
            load_data_from_table(quickstep_shell_instance,
                                 tmp_relation_table, head_relation_table)
            quickstep_shell_instance.drop_table('tmp_res_table')
            if LOG_ON:
                count_row(
                quickstep_shell_instance, logger, head_name)
        quickstep_shell_instance.analyze([head_relation_name], count=True)
    else:
        # delay deduplication here
        quickstep_shell_instance.sql_command("INSERT INTO {} {}".format(
            head_relation_name, non_recursive_rule_eval_str))
        if head_relation_name in delay_dedup_relation_counter and delay_dedup_relation_counter[head_relation_name] == 1:
            quickstep_shell_instance.dedup_table(
                catalog['tables'][head_relation_name])
        else:
            quickstep_shell_instance.analyze([head_relation_name], count=True)
        if LOG_ON:
            count_row(
                quickstep_shell_instance, logger, head_name)


def recursive_rule_eval_sql_str_gen(datalog_rule, relation_def_map, eval_idbs, iter_num):
    # map attributes to be projected
    head = datalog_rule['head']
    head_name = head['name']
    body = datalog_rule['body']
    original_body_atom_list = body['atoms']
    body_atom_list = deepcopy(body['atoms'])
    negation_atom_list = body['negations']

    #  one recursive rule could be evaluated by
    # 'multiple sub-queries' (delta & non-delta combination in nonlinear recursive rule)
    recursive_rule_eval_strs = list()

    # select_info = [attributes_map, attributes_type_map, aggregation_map]
    select_info = rule_analyzer.translator.extract_selection_info(datalog_rule)

    # join_info = join_map
    join_info = rule_analyzer.translator.extract_join_info(datalog_rule)

    # comparison_map
    comparison_map = rule_analyzer.translator.extract_comparison_map(
        body, body_atom_list)

    # negation_map
    negation_info = rule_analyzer.translator.extract_negation_map(body)

    # constant constraint map
    constant_constraint_map = rule_analyzer.translator.extract_constant_constraint_map(
        body)

    body_atom_alias_list = rule_analyzer.translator.build_atom_aliases(
        body_atom_list)

    negation_atom_alias_list = rule_analyzer.translator.build_negation_atom_aliases(
        negation_atom_list)

    body_atom_eval_names, idb_num = rule_analyzer.translator.build_recursive_atom_aliases(body_atom_list, eval_idbs,
                                                                                          iter_num)

    if STATIC_DEBUG:
        print('#####BODY ATOM EVALUATION NAMES#####')
        print(body_atom_eval_names)

    atom_eval_name_list = rule_analyzer.translator.build_recursive_atom_alias_combinations(body_atom_list,
                                                                                           body_atom_eval_names,
                                                                                           eval_idbs, idb_num)

    if STATIC_DEBUG:
        print('#####RECURSIVE ATOM NAME LIST IN RECURSIVE RULE BODY#####')
        print(atom_eval_name_list)

    # select
    select_str = query_generator.sql_query_generator.generate_select(datalog_rule, select_info, relation_def_map,
                                                                     body_atom_alias_list)

    # from
    from_strs = query_generator.sql_query_generator.generate_from_recursive(
        body_atom_alias_list, atom_eval_name_list)

    # where::join
    join_str = query_generator.sql_query_generator.generate_join_str(join_info, original_body_atom_list,
                                                                     body_atom_alias_list, relation_def_map)

    # where::comparison
    compare_str = query_generator.sql_query_generator.generate_compare_str(comparison_map, original_body_atom_list,
                                                                           body_atom_alias_list, relation_def_map)

    # where::constant_constraint
    constant_constraint_str = query_generator.sql_query_generator.generate_constant_constraint_str(
        constant_constraint_map, body,
        body_atom_alias_list,
        relation_def_map)

    # where::negation
    negation_str = query_generator.sql_query_generator.generate_negation_str(negation_info, original_body_atom_list,
                                                                             negation_atom_list,
                                                                             body_atom_alias_list,
                                                                             negation_atom_alias_list,
                                                                             relation_def_map)

    recursive_rule_num = len(from_strs)

    for rule_index in range(recursive_rule_num):
        recursive_rule_eval_strs.append(
            select_str + ' ' + from_strs[rule_index])

    if len(join_str) > 0 or len(compare_str) > 0 or len(constant_constraint_str) > 0 or len(negation_str) > 0:

        for rule_index in range(recursive_rule_num):
            recursive_rule_eval_strs[rule_index] += ' ' + 'where' + ' '

            condition = False
            if len(join_str) > 0:
                recursive_rule_eval_strs[rule_index] += join_str
                condition = True
            if len(compare_str) > 0:
                if condition:
                    recursive_rule_eval_strs[rule_index] += ' AND '
                recursive_rule_eval_strs[rule_index] += compare_str
                condition = True
            if len(constant_constraint_str) > 0:
                if condition:
                    recursive_rule_eval_strs[rule_index] += ' AND '
                recursive_rule_eval_strs[rule_index] += constant_constraint_str
                condition = True
            if len(negation_str) > 0:
                if condition:
                    recursive_rule_eval_strs[rule_index] += ' AND '
                recursive_rule_eval_strs[rule_index] += negation_str

    # aggregation (group by)
    aggregation_map = select_info[2]
    if len(aggregation_map) > 0:
        group_by_str = query_generator.sql_query_generator.generate_group_by_str(
            relation_def_map[head_name][0]['attributes'],
            aggregation_map)

        for rule_index in range(recursive_rule_num):
            recursive_rule_eval_strs[rule_index] += ' ' + group_by_str

    return recursive_rule_eval_strs, aggregation_map


def initialize_delta_tables(quickstep_shell_instance, catalog, relation_set, relation_def_map):
    """
    Create 'detla0' table and 'common-delta'(for set-difference and recursive aggregation)
    for each recursive relation to be evaluated
    """
    for relation_name in relation_set:
        relation = relation_def_map[relation_name][0]
        delta_relation_name = relation['name'] + '_Delta'
        if delta_relation_name not in catalog['tables']:
            delta_relation = deepcopy(relation)
            delta_relation['name'] = delta_relation_name + '0'
            catalog['tables'][delta_relation_name] = \
                create_table_from_relation(
                    quickstep_shell_instance, delta_relation)
        else:
            delta_table = catalog['table'][delta_relation_name]
            delta_table.rename(delta_relation_name + '0')
            quickstep_shell_instance.create_table(delta_table)

        quickstep_shell_instance.analyze(table_list=[delta_relation_name + '0'],
                                         count=True)

        # Initialize/Update the optimization catalog
        if delta_relation_name not in catalog['optimization']:
            catalog['optimization'][delta_relation_name] = {}
        catalog['optimization'][delta_relation_name]['size'] = 0

        common_delta_name = relation['name'] + '_Common_Delta'
        if common_delta_name not in catalog['optimization']:
            catalog['optimization'][common_delta_name] = {}
        catalog['optimization'][common_delta_name]['size'] = 0

        mDelta_name = relation['name'] + '_mDelta'
        if mDelta_name not in catalog['optimization']:
            catalog['optimization'][mDelta_name] = {}
        catalog['optimization'][mDelta_name]['size'] = 0


def initialize_prev_tables(quickstep_shell_instance, recursive_rules, catalog, relation_set, relation_def_map):
    # if the rule body contains 'more than one' (>=2) idb atoms (count repeated ones) in the current scc,
    # then 'prev' table needs to be created for these idb atoms for non-linear rule evaluation
    pre_table_set = set([])
    rule_num = len(recursive_rules)
    for rule_index in range(rule_num):
        idb_atom_counter = 0
        rule = recursive_rules[rule_index]
        rule_body_atoms = rule['body']['atoms']
        idb_atom_candidates = []
        for atom in rule_body_atoms:
            if atom['name'] in relation_set:
                idb_atom_candidates.append(atom['name'])
                idb_atom_counter += 1
        if idb_atom_counter >= 2:
            for idb_atom_candidate in idb_atom_candidates:
                pre_table_set.add(idb_atom_candidate)

    # Create 'prev' tables
    for relation_name in pre_table_set:
        relation = relation_def_map[relation_name][0]
        prev_relation_name = relation['name'] + 'Prev'
        if prev_relation_name not in catalog['tables']:
            prev_relation = deepcopy(relation)
            prev_relation['name'] = prev_relation_name
            catalog['tables'][prev_relation_name] = \
                create_table_from_relation(
                    quickstep_shell_instance, prev_relation)
        else:
            prev_table = catalog['tables'][prev_relation_name]
            quickstep_shell_instance.create_table(prev_table)

        quickstep_shell_instance.analyze(
            table_list=[prev_relation_name], count=True)

    return pre_table_set


def load_data_into_delta(quickstep_shell_instance, catalog, relation_set, relation_def_map):
    for table in relation_set:
        src_relation = relation_def_map[table][0]
        delta_relation_name = src_relation['name'] + '_Delta'
        src_relation_table = catalog['tables'][src_relation['name']]
        delta_relation_table = catalog['tables'][delta_relation_name]
        load_data_from_table(quickstep_shell_instance,
                             src_relation_table, delta_relation_table)
        quickstep_shell_instance.analyze(
            table_list=[delta_relation_table.table_name], count=True)


def create_delta_tables(quickstep_shell_instance, catalog, relation_set, iter_num):
    for table in relation_set:
        cur_delta_table = catalog['tables'][table + '_Delta']
        cur_delta_table.rename(table + '_Delta' + str(iter_num))
        quickstep_shell_instance.create_table(cur_delta_table)


def one_phase_diff(quickstep_shell_instance, l_table, r_table, dest_table, aggregation_map):
    """ The one-phase algorithm computing the set difference between two tables
        and insert the results into the destination table (IDB relation table)

        l_table, r_table and dest_table have the same schema

    Args:
        quickstep_shell_instance:
        l_table:
        r_table:
        dest_table:

    Returns:
    """
    one_phase_diff_str = \
        query_generator.sql_query_generator.generate_set_diff_str(
            l_table, r_table, dest_table, aggregation_map) + ';'
    quickstep_shell_instance.sql_command(one_phase_diff_str)
    quickstep_shell_instance.analyze(
        table_list=[dest_table.table_name], count=True)


def two_phase_diff(quickstep_shell_instance, l_table, r_table, dest_table, aggregation_map):
    """ The two-phase algorithm computing the set difference between two tables
        and insert the results into the destination table (IDB relation table)

        Mathematical formulation:
            intersection = l_table INTERSECT r_table  (first phase)
              dest_table = l_table - intersection    (second phase)

    Args:
        Same as defined in the function "one_phase_diff"

    Returns:
    """

    ##### First Phase #####
    common_table = deepcopy(dest_table)
    common_table.rename(COMMON_TABLE_NAME)
    quickstep_shell_instance.create_table(common_table)
    intersection_sql_str = \
        query_generator.sql_query_generator.generate_intersect_str(
            l_table, r_table, aggregation_map)
    quickstep_shell_instance.sql_command('insert into ' + COMMON_TABLE_NAME + ' ' +
                                         intersection_sql_str + ';')
    quickstep_shell_instance.analyze(
        table_list=[COMMON_TABLE_NAME], count=True)

    ##### Second Phase ######
    set_diff_str = \
        query_generator.sql_query_generator.generate_set_diff_str(
            l_table, common_table, dest_table, None) + ';'
    quickstep_shell_instance.sql_command(set_diff_str)
    quickstep_shell_instance.analyze(
        table_list=[dest_table.table_name], count=True)
    quickstep_shell_instance.drop_table(COMMON_TABLE_NAME)


def set_diff(quickstep_shell_instance, logger, l_table, r_table, dest_table, aggregation_map,
             alpha=1.38, beta=10, prev_mu=5):
    """ Returns the string to compute set-difference between two tables

    Generate string to perform sef-difference between table S and R (R - S)

    Args:
        quickstep_shell_instance: the quickstep_shell instance used to communicate with the quickstep backend
                         l_table: R table
                         r_table: S table
                      dest_table: the table into which the results of (l_table - r_table) are inserted
                           alpha: parameter described in the cost model
                            beta: parameter described in the cost model
                         prev_mu: parameter described in the cost model

    Parameters used in the cost model:
        |S|: the size of the IDB relation
        |R|: the size of the relation storing the evaluated results
        |r|: the size of (S intersect R)
         Cb: building hash table cost per tuple
         Cp: probing hash table cost per tuple
      alpha:  Cb/Cp
       beta: |S|/|R|
         mu: |R|/|r|
       alg1: delta_S = R - S
       alg2: r = S intersect R, delta_S = R - r

        Cost Model based algorithm selection:
            Choice of algorithm depends on the *beta* interval:
                [ 0, alpha/(alpha-1) ]:
                    Choose alg1
                ( alpha/(alpha-1), 2*alpha/(alpha-1) ):
                    approximate Cost_Diff ~= Cost(alg1) - Cost(alg2) = beta(alpha-1) - (alpha + (prev_mu/alpha))
                        if Cost_Diff < 0:
                            Choose alg1
                        else:
                            Choose alg2
                [2*alpha/(alpha-1), +infinity]:
                    Choose alg2

        Returns:
    """
    SET_DIFF_ALG = 0
    if COST_MODEL_CHECK:
        test_delta_table = deepcopy(dest_table)
        test_delta_table.table_name = 'test_delta'
        quickstep_shell_instance.create_table(test_delta_table)
        start = time.time()

    if SET_DIFF_OP:
        if beta <= (alpha / (alpha - 1)):
            log_info(logger, 'Confidence interval: beta <= alpha/(alpha-1)')
            log_info(
                logger, 'Compute set-difference via *ONE PHASE DIFFERENCE ALGORITHM*')
            one_phase_diff(quickstep_shell_instance, l_table,
                           r_table, dest_table, aggregation_map)
            SET_DIFF_ALG = 1
        elif (alpha / (alpha - 1)) < beta < (2 * alpha / (alpha - 1)):
            log_info(logger, 'Previous mu is ' + str(prev_mu))
            # |r| = 0
            if prev_mu == -1:
                cost_diff = alpha * beta - alpha - beta
                if cost_diff < 0:
                    log_info(
                        logger, 'Compute set-difference via *ONE PHASE DIFFERENCE ALGORITHM*')
                    one_phase_diff(quickstep_shell_instance, l_table,
                                   r_table, dest_table, aggregation_map)
                    SET_DIFF_ALG = 1
                else:
                    log_info(
                        logger, 'Compute set-difference via *TWO PHASE DIFFERENCE ALGORITHM*')
                    two_phase_diff(quickstep_shell_instance, l_table,
                                   r_table, dest_table, aggregation_map)
                    SET_DIFF_ALG = 2
            else:
                cost_diff = beta * (alpha - 1) - (alpha + alpha / prev_mu)
                log_info(
                    logger, 'Approximated cost difference factor ' + str(cost_diff))
                log_info(
                    logger, 'Uncertain interval: beta in (alpha/(alpha-1), 2*alpha/(alpha-1))')
                if cost_diff < 0:
                    log_info(
                        logger, 'Compute set-difference via *ONE PHASE DIFFERENCE ALGORITHM*')
                    one_phase_diff(quickstep_shell_instance, l_table,
                                   r_table, dest_table, aggregation_map)
                    SET_DIFF_ALG = 1
                else:
                    log_info(
                        logger, 'Compute set-difference via *TWO PHASE DIFFERENCE ALGORITHM*')
                    two_phase_diff(quickstep_shell_instance, l_table,
                                   r_table, dest_table, aggregation_map)
                    SET_DIFF_ALG = 2
        else:
            log_info(logger, 'Confidence interval: beta >= 2 * alpha/(alpha-1)')
            log_info(
                logger, 'Compute set-difference via *TWO PHASE DIFFERENCE ALGORITHM*')
            two_phase_diff(quickstep_shell_instance, l_table,
                           r_table, dest_table, aggregation_map)
            SET_DIFF_ALG = 2

    else:
        log_info(logger, 'Set difference opimizier is not turned on')
        if DEFAULT_SET_DIFF_ALG == 1:
            log_info(
                logger, 'Compute set-difference via *ONE PHASE DIFFERENCE ALGORITHM*')
            one_phase_diff(quickstep_shell_instance, l_table,
                           r_table, dest_table, aggregation_map)
            SET_DIFF_ALG = 1
        if DEFAULT_SET_DIFF_ALG == 2:
            log_info(
                logger, 'Compute set-difference via *TWO PHASE DIFFERENCE ALGORITHM*')
            two_phase_diff(quickstep_shell_instance, l_table,
                           r_table, dest_table, aggregation_map)
            SET_DIFF_ALG = 2

    if COST_MODEL_CHECK:
        end = time.time()
        chosen_set_diff_alg_time = end - start
        start = time.time()
        if SET_DIFF_ALG == 1:
            two_phase_diff(quickstep_shell_instance, l_table,
                           r_table, test_delta_table, aggregation_map)
        else:
            one_phase_diff(quickstep_shell_instance, l_table,
                           r_table, test_delta_table, aggregation_map)
        end = time.time()
        other_set_diff_alg_time = end - start
        time_diff = chosen_set_diff_alg_time - other_set_diff_alg_time
        quickstep_shell_instance.drop_table('test_delta')
        if time_diff < 0:
            log_info(logger, 'Set-difference algorithm selection: CORRECT')
            log_info(logger, 'Time Off: ' + str(time_diff) + '(' +
                     str(time_diff * 100 / chosen_set_diff_alg_time) + '%)')
        else:
            log_info(logger, 'Set-difference algorithm selection: WRONG')
            log_info(logger, 'Time Off: ' + str(time_diff) + '(' +
                     str(time_diff * 100 / chosen_set_diff_alg_time) + '%)')


def check_empty_delta(quickstep_shell_instance, catalog, relation_set):
    is_delta_empty = True
    for relation_name in relation_set:
        delta_relation_name = relation_name + '_Delta'
        delta_table_name = catalog['tables'][delta_relation_name].table_name
        m_delta_table_name = relation_name + '_mDelta'
        common_table_name = relation_name + '_Common_Delta'
        [empty, row_num] = quickstep_shell_instance.is_table_empty(
            delta_table_name)
        is_delta_empty = is_delta_empty and empty
        catalog['optimization'][delta_relation_name]['size'] = row_num
        catalog['optimization'][relation_name]['size'] += row_num
        catalog['optimization'][common_table_name]['size'] = \
            catalog['optimization'][m_delta_table_name]['size'] - row_num

    return is_delta_empty


def recursive_rules_eval(quickstep_shell_instance, logger, time_monitor, catalog, recursive_rules, relation_def_map):
    # construct relation set to store all the unique relations (appear in the head of certain rules) to be evaluated
    # rules evaluating the same relation are grouped together and will be evaluated together later
    relation_set = collections.OrderedDict({})
    for rule in recursive_rules:
        eval_relation_name = rule['head']['name']
        if eval_relation_name not in relation_set:
            relation_set[eval_relation_name] = []
        relation_set[eval_relation_name].append(rule)

    log_info(logger, 'Start creating delta, prev tables for semi-naive evaluation')
    update_time(time_monitor)
    initialize_delta_tables(quickstep_shell_instance,
                            catalog, relation_set, relation_def_map)
    pre_table_set = initialize_prev_tables(quickstep_shell_instance, recursive_rules,
                                           catalog, relation_set, relation_def_map)

    # load data from previous evaluated results into 'deltas'
    load_data_into_delta(quickstep_shell_instance, catalog,
                         relation_set, relation_def_map)

    if LOG_ON:
        log_info_time(logger, time_monitor.local_elapse_time())
    log_info(logger, 'Start Semi-Naive Evaluation\n\n')

    # generate string to check whether all "delta" relations are empty *before* the recursive evaluation
    is_delta_empty = check_empty_delta(
        quickstep_shell_instance, catalog, relation_set)

    # iterative evaluation (semi-naive)
    iter_num = 0
    while not is_delta_empty:
        iter_start_log_str = '#####Start Iteration ' + \
            str(iter_num + 1) + '#####'
        log_info(logger, iter_start_log_str)
        update_time(time_monitor)
        if LOG_ON:
            cur_iter_start_time = time_monitor.local_start_time

        iter_num += 1

        # Update the delta table names in the catalog and create delta tables for the current iteration
        create_delta_tables(quickstep_shell_instance,
                            catalog, relation_set, iter_num)

        # Evaluate rules grouped by 'evaluated relation'
        for idb in relation_set:
            # Create mDelta (results evaluated without duplication)
            original_relation = relation_def_map[idb][0]
            delta_relation_name = original_relation['name'] + '_Delta'
            delta_relation_table_name = catalog['tables'][delta_relation_name].table_name
            common_delta_relation_name = original_relation['name'] + \
                '_Common_Delta'

            # Used for deduplication later
            eval_relation_attributes = original_relation['attributes']

            m_delta_relation_name = original_relation['name'] + '_mDelta'
            if m_delta_relation_name not in catalog['tables']:
                mDelta_relation = deepcopy(original_relation)
                mDelta_relation['name'] = m_delta_relation_name
                catalog['tables'][m_delta_relation_name] = \
                    create_table_from_relation(
                        quickstep_shell_instance, mDelta_relation)
            else:
                quickstep_shell_instance.create_table(
                    catalog['tables'][m_delta_relation_name])

            # Update the catalog
            if m_delta_relation_name not in catalog['optimization']:
                catalog['optimization'][m_delta_relation_name] = {}
            catalog['optimization'][m_delta_relation_name]['size'] = 0

            # If there is more than one rule to evaluate the idb, multiple mDelta will be generated (merged into one)
            eval_rules = relation_set[idb]
            if STATIC_DEBUG:
                eval_rule_num = len(eval_rules)
                print('#####Total number of evaluation rules#####: ' +
                      str(eval_rule_num))

            log_info(logger, 'Evaluate ' + original_relation['name'])
            sub_query_list = []

            aggregation_map = None
            for eval_rule in eval_rules:
                if STATIC_DEBUG:
                    datalog_program.iterate_datalog_rule(eval_rule)

                log_info(logger, datalog_program.iterate_datalog_rule(eval_rule))
                # there might be different combinations of 'delta' and 'non-delta' joins
                delta_atom_num = 0
                eval_rule_body = eval_rule['body']['atoms']
                for atom in eval_rule_body:
                    if atom['name'] in relation_set:
                        delta_atom_num += 1

                sub_queries, aggregation_map = \
                    recursive_rule_eval_sql_str_gen(
                        eval_rule, relation_def_map, relation_set, iter_num)

                if len(aggregation_map) > 0:
                    if len(eval_rules) > 1:
                        raise Exception(
                            "RecStep currently only supports resursive single-rule aggregation evaluation")

                sub_query_num = len(sub_queries)
                if STATIC_DEBUG:
                    print('######Number of subqueries#####: ' + str(sub_query_num))

                for sub_query_index in range(sub_query_num):
                    sub_query_str = sub_queries[sub_query_index]
                    sub_query_list.append(sub_query_str)
                    if STATIC_DEBUG:
                        print(sub_query_str)

            # Create a temporary table to store the results before deduplication
            tmp_m_delta_relation_name = original_relation['name'] + \
                '_tmp_mDelta'
            if tmp_m_delta_relation_name not in catalog['tables']:
                tmp_m_delta_relation = deepcopy(original_relation)
                tmp_m_delta_relation['name'] = tmp_m_delta_relation_name
                catalog['tables'][tmp_m_delta_relation_name] = \
                    create_table_from_relation(
                        quickstep_shell_instance, tmp_m_delta_relation)
            else:
                quickstep_shell_instance.create_table(
                    catalog['tables'][tmp_m_delta_relation_name])

            eval_m_delta_str = 'insert into ' + tmp_m_delta_relation_name + \
                               ' select * from ('

            eval_sub_query_num = len(sub_query_list)

            for eval_sub_query_index in range(eval_sub_query_num):
                eval_m_delta_str += sub_query_list[eval_sub_query_index]
                if eval_sub_query_index != eval_sub_query_num - 1:
                    eval_m_delta_str += ' union all '
            eval_m_delta_str += ') t;'

            if STATIC_DEBUG:
                print(eval_m_delta_str)

            quickstep_shell_instance.sql_command(eval_m_delta_str)
            if LOG_ON:
                count_row(quickstep_shell_instance, logger,
                          tmp_m_delta_relation_name)
            quickstep_shell_instance.analyze(
                table_list=[tmp_m_delta_relation_name], count=True)

            # Deduplication
            deduplication_str = 'insert into ' + m_delta_relation_name + \
                                ' select * from ' + tmp_m_delta_relation_name + ' group by '

            for attribute in eval_relation_attributes:
                deduplication_str += attribute.name + ','
            deduplication_str = deduplication_str[:len(deduplication_str) - 1]
            deduplication_str += ';'

            if STATIC_DEBUG:
                print('######DEDUPLICATION STRING#####:')
                print(deduplication_str)

            quickstep_shell_instance.sql_command(deduplication_str)
            prev_R_size = catalog['optimization'][m_delta_relation_name]['size']
            catalog['optimization'][m_delta_relation_name]['size'] = \
                count_row(quickstep_shell_instance,
                          logger, m_delta_relation_name)

            # Drop the tmp table
            quickstep_shell_instance.drop_table(tmp_m_delta_relation_name)
            quickstep_shell_instance.analyze(
                [m_delta_relation_name], count=True)
            if LOG_ON:
                log_info_time(logger, time_monitor.local_elapse_time())

            # Perform set difference
            S_size = catalog['optimization'][original_relation['name']]['size']
            R_size = catalog['optimization'][m_delta_relation_name]['size']

            delta_relation_table = catalog['tables'][delta_relation_name]

            if R_size == 0:
                log_info(logger, 'The size of mDelta relation table is zero; ' +
                         'no need to perform set-difference computation')
            else:
                if S_size == 0:
                    log_info(logger, 'The size of IDB relation table is zero; ' +
                             'no need to perform set-difference computation')
                    quickstep_shell_instance.sql_command('insert into ' + delta_relation_table_name +
                                                         ' select * from ' + m_delta_relation_name + ';')
                else:
                    normalized_S_size = int(
                        (S_size + TUPLE_NUM_PER_BLOCK - 1) / TUPLE_NUM_PER_BLOCK)
                    log_info(logger, 'Normalized size of S: ' +
                             str(normalized_S_size))
                    normalized_R_size = int(
                        (R_size + TUPLE_NUM_PER_BLOCK - 1) / TUPLE_NUM_PER_BLOCK)
                    log_info(logger, 'Normalized size of R: ' +
                             str(normalized_R_size))
                    beta = float(normalized_S_size) / float(normalized_R_size)
                    if catalog['optimization'][common_delta_relation_name]['size'] == 0:
                        prev_mu = -1
                    else:
                        normalized_prev_R_size = int(
                            (prev_R_size + TUPLE_NUM_PER_BLOCK - 1 / TUPLE_NUM_PER_BLOCK))
                        normalized_prev_R_size = int(
                            (normalized_prev_R_size + THREADS_NUM - 1) / THREADS_NUM)
                        normalize_common_delta_size = catalog['optimization'][common_delta_relation_name]['size']
                        normalize_common_delta_size = (normalize_common_delta_size + TUPLE_NUM_PER_BLOCK - 1) / \
                            TUPLE_NUM_PER_BLOCK
                        normalize_common_delta_size = (normalize_common_delta_size + THREADS_NUM - 1) / \
                            THREADS_NUM
                        prev_mu = float(normalized_prev_R_size) / \
                            float(normalize_common_delta_size)

                    m_delta_relation_table = catalog['tables'][m_delta_relation_name]
                    original_relation_table = catalog['tables'][original_relation['name']]
                    update_time(time_monitor)
                    set_diff(quickstep_shell_instance, logger,
                             m_delta_relation_table, original_relation_table, delta_relation_table, aggregation_map,
                             beta=beta, prev_mu=prev_mu)

            if LOG_ON:
                count_row(quickstep_shell_instance, logger,
                          delta_relation_table.table_name)

            # Drop mDelta table
            quickstep_shell_instance.drop_table(
                catalog['tables'][m_delta_relation_name].table_name)

            # Save the current idb
            if original_relation['name'] in pre_table_set:
                if R_size != 0:
                    log_info(logger, 'Save the current IDB')
                    update_time(time_monitor)
                    prev_table_name = original_relation['name'] + 'Prev'
                    quickstep_shell_instance.drop_table(prev_table_name)
                    quickstep_shell_instance.create_table(
                        catalog['tables'][prev_table_name])
                    quickstep_shell_instance.sql_command('insert into ' + prev_table_name +
                                                         ' select * from ' + original_relation['name'] + ';')
                    quickstep_shell_instance.analyze(
                        table_list=[prev_table_name], count=True)
                    if LOG_ON:
                        log_info_time(logger, time_monitor.local_elapse_time())

            if R_size != 0:
                # update evaluated idb tables
                log_info(logger, 'Update IDB (union delta)')
                update_time(time_monitor)
                quickstep_shell_instance.sql_command('insert into ' + original_relation['name'] +
                                                     ' select * from ' + delta_relation_table_name + ';')
                quickstep_shell_instance.analyze(
                    table_list=[original_relation['name']], count=True)
                if LOG_ON:
                    log_info_time(logger, time_monitor.local_elapse_time())

        # Drop all old deltas
        for idb in relation_set:
            old_idb_delta_name = idb + '_Delta' + str(iter_num - 1)
            if STATIC_DEBUG:
                print('#####OLD IDB DELTA NAME#####')
                print(old_idb_delta_name)
            quickstep_shell_instance.drop_table(old_idb_delta_name)

        # Check whether the evaluation reaches the 'fix-point'
        is_delta_empty = check_empty_delta(
            quickstep_shell_instance, catalog, relation_set)

        # Log the number of tuples in delta tables evaluated in the current iteration
        if LOG_ON:
            for idb in relation_set:
                if STATIC_DEBUG:
                    print('#####DELTA TABLE NAMES IN CURRENT ITERATION######')
                    print(catalog['tables'][idb + '_Delta'].table_name)
                count_row(quickstep_shell_instance, logger,
                          catalog['tables'][idb + '_Delta'].table_name)

        # Log the number of tuples in the idb tables after the evaluation in the current iteration
        if LOG_ON:
            for idb in relation_set:
                count_row(quickstep_shell_instance, logger, idb)

            update_time(time_monitor)
            cur_iter_time = time_monitor.local_start_time - cur_iter_start_time
            log_info_time(logger, cur_iter_time, 'Iteration Time')
            log_info(logger, '#####SEPERATOR######\n\n')

        if STATIC_DEBUG:
            break

        if DYNAMIC_DEBUG:
            if iter_num == DYNAMIC_DEBUG_ITER_NUM:
                quickstep_shell_instance.stop()
                break

    # Clear all the intermediate tables after the iterative evaluation finishes
    for table in pre_table_set:
        prev_table_name = table + 'Prev'
        quickstep_shell_instance.drop_table(prev_table_name)

    for idb in relation_set:
        idb_delta_table_name = catalog['tables'][idb + '_Delta'].table_name
        quickstep_shell_instance.drop_table(idb_delta_table_name)


def interpret(input_datalog_program_file):
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
    datalog_program_instance = datalog_program.construct_datalog_program_instance(
        input_datalog_program_file)

    rules = datalog_program_instance.datalog_program
    edb_decl = datalog_program_instance.edb_decl
    idb_decl = datalog_program_instance.idb_decl
    dependency_graph = datalog_program_instance.dependency_graph
    sccs = datalog_program_instance.scc

    if not INTERPRET:
        sys.exit()

    # Build a mapping between relation name and the corresponding relation definition
    relation_def_map = {}
    for relation in edb_decl:
        relation_def_map[relation['name']] = [relation, 'edb']
    for relation in idb_decl:
        relation_def_map[relation['name']] = [relation, 'idb']

    lpa_logger = None
    time_monitor = None
    if LOG_ON:
        from utility.monitoring import TimeMonitor
        from utility.lpalogging import LpaLogger
        lpa_logger = LpaLogger()
        time_monitor = TimeMonitor()

    # Configure and initialize quickstep instance
    quickstep_shell_dir = config['QuickStep_Shell_Dir']
    quickstep_shell_instance = quickstep.Database(quickstep_shell_dir)
    if LOG_ON:
        log_info(lpa_logger, 'Start creating IDB and EDB tables and populating facts')

    # Catalog to keep track of all the objects and stats
    catalog = dict()
    catalog['tables'] = dict()
    catalog['optimization'] = dict()
    # Create edb tables
    for relation in edb_decl:
        catalog['tables'][relation['name']] = create_table_from_relation(
            quickstep_shell_instance, relation)

    # Create idb tables
    for relation in idb_decl:
        catalog['tables'][relation['name']] = create_table_from_relation(
            quickstep_shell_instance, relation)
        catalog['optimization'][relation['name']] = {}
        catalog['optimization'][relation['name']]['size'] = 0

    # Populate facts into edbs
    for relation in edb_decl:
        populate_data_into_edb(quickstep_shell_instance, relation)

    # Analyze all tables
    quickstep_shell_instance.analyze([], count=True)

    if LOG_ON:
        log_info_time(lpa_logger, time_monitor.local_elapse_time())

    if STATIC_DEBUG:
        print('##########DEBUGGING SECTION##########')
    # Start generating code to evaluate sccs
    stratum_count = 0

    relation_counter = dict()
    if CQA_OP:
        # all rules are non-recursive
        for scc in sccs:
            relation_name = rules[scc]['head']['name']
            if relation_name not in CQA_DELAY_DEDUP_RELATION_LIST:
                continue
            if relation_name not in relation_counter:
                relation_counter[relation_name] = 0
            relation_counter[relation_name] += 1

    for scc in sccs:
        log_info(
            lpa_logger, '-----Start evaluating stratum[' + str(stratum_count) + ']-----')
        if is_trivial_scc(sccs[scc], dependency_graph):
            log_info(lpa_logger, '>>>>Evaluating Non-Recursive Rule<<<<<')
            datalog_rule = rules[scc]
            if LOG_ON:
                rule_str = datalog_program.iterate_datalog_rule(datalog_rule)
                log_info(lpa_logger, rule_str)

            non_recursive_rule_eval(quickstep_shell_instance, lpa_logger,
                                    catalog, datalog_rule, relation_def_map,
                                    delay_dedup_relation_counter=relation_counter)

            head_relation_name = datalog_rule['head']['name']
            if CQA_OP and head_relation_name in relation_counter:
                relation_counter[head_relation_name] -= 1

            if LOG_ON:
                log_info_time(lpa_logger, time_monitor.local_elapse_time(
                ), time_descrip='Rule Evaluation Time')
                update_time(time_monitor)
                log_info(lpa_logger, '#####SEPERATOR#####\n')
        else:
            log_info(lpa_logger, '>>>>Evaluating Recursive Rules<<<<<')
            recursive_rules_indices = sccs[scc]
            recursive_rules = []
            for rule_index in recursive_rules_indices:
                recursive_rules.append(rules[rule_index])
            recursive_rules_eval(quickstep_shell_instance, lpa_logger, time_monitor,
                                 catalog, recursive_rules, relation_def_map)
        stratum_count += 1

    if LOG_ON:
        log_info_time(lpa_logger, time_monitor.global_elapse_time(),
                      time_descrip='Total Evaluation Time')

    if WRITE_TO_CSV:
        for relation_name in relation_def_map:
            relation_type = relation_def_map[relation_name][1]
            if relation_type == 'idb':
                quickstep_shell_instance.output_data_from_table_to_csv(
                    relation_name, delimiter=CSV_DELIMITER)

    quickstep_shell_instance.stop()


def main():
    try:
        input_datalog_program_file_name = sys.argv[1]
    except Exception as e:
        print(e)
        raise Exception('The file specifying the datalog program is missing')

    interpret(input_datalog_program_file_name)


if __name__ == '__main__':
    main()
