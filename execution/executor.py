from parser.datalog_program import DatalogProgram
from quickstep_api.quickstep import Table
from quickstep_api.quickstep import QuickStep
from utility.lpalogging import LpaLogger
from utility.monitoring import TimeMonitor
from execution.config import *

from query_generator.sql_query_generator import *

import collections
import time
from copy import deepcopy


class Executor(object):
    def __init__(self):
        if BACK_END == "quickstep":
            self.__quickstep_shell_instance = QuickStep()
        if LOG_ON:
            self.__logger = LpaLogger()
            self.__time_monitor = TimeMonitor()

    def update_local_time(self):
        if LOG_ON:
            self.__time_monitor.update_local_time()

    def log(self, log_str):
        if LOG_ON:
            self.__logger.info(log_str)

    def log_time(self, log_time, descrip="Time"):
        if LOG_ON:
            self.__logger.info("{}: {}".format(descrip, log_time))

    def log_local_time(self, descrip="Time"):
        if LOG_ON:
            self.log_time(self.__time_monitor.local_elapse_time(), descrip=descrip)

    def log_global_time(self, descrip="Time"):
        if LOG_ON:
            self.log_time(self.__time_monitor.global_elapse_time(), descrip=descrip)

    def drop_table(self, table_name):
        self.__quickstep_shell_instance.drop_table(table_name)

    def execute(self, sql_command):
        if BACK_END != "quickstep":
            raise Exception(
                "The back end is {}! Only quickstep can be used for actual execution".format(
                    BACK_END
                )
            )
        self.__quickstep_shell_instance.sql_command(sql_command)

    def create_table_from_relation(
        self, relation, table_name="", create_physical_table=True
    ):
        if len(table_name) == 0:
            table_name = relation["name"]

        attributes = relation["attributes"]
        table = Table(table_name)
        for attribute in attributes:
            table.add_attribute(attribute.name, attribute.type)

        if create_physical_table:
            self.__quickstep_shell_instance.create_table(table)
        return table

    def populate_data_into_edb(self, relation, delimiter=CSV_DELIMITER):
        """
        Given the data structure storing the information of the relation, load the data into the
        created table from the file given under the specified path ./Input/relation.tbl
        """
        table_name = relation["name"]
        input_file_name = "{}/{}.csv".format(INPUT_DIR, table_name)
        self.__quickstep_shell_instance.load_data_from_file(
            table_name, input_file_name, delimiter
        )

    def count_rows(self, table_name):
        row_num = self.__quickstep_shell_instance.count_rows(table_name)
        self.log("Number of tuples in {}: {}".format(table_name, row_num))
        return row_num

    def analyze(self, table_list=[], count=False, range_analyze=False):
        self.__quickstep_shell_instance.analyze(
            table_list=table_list, count=count, range_analyze=range_analyze
        )

    @staticmethod
    def update_catalog_table_size(catalog, relation_key, new_size):
        if BACK_END != "quickstep":
            return
        if relation_key not in catalog["optimization"]:
            catalog["optimization"][relation_key] = dict()
        catalog["optimization"][relation_key]["size"] = new_size

    @staticmethod
    def get_table_size(catalog, relation_key):
        if relation_key not in catalog["optimization"]:
            raise Exception("{} not found in optimization catalog")
        return catalog["optimization"][relation_key]["size"]

    def initialize_delta_tables(self, catalog, relation_set, relation_def_map):
        """
        Create 'detla0' table and 'common-delta'(for set-difference and recursive aggregation)
        for each recursive relation to be evaluated
        """
        for relation_name in relation_set:
            relation = relation_def_map[relation_name]["relation"]
            delta_relation_key = "{}_delta".format(relation_name)
            delta_table_name = "{}_0".format(delta_relation_key)
            catalog["tables"][delta_relation_key] = self.create_table_from_relation(
                relation, table_name=delta_table_name
            )
            self.update_catalog_table_size(catalog, delta_relation_key, 0)

            common_delta_table_name = "{}_common_delta".format(relation["name"])
            catalog["tables"][
                common_delta_table_name
            ] = self.create_table_from_relation(
                relation,
                table_name=common_delta_table_name,
                create_physical_table=False,
            )
            self.update_catalog_table_size(catalog, common_delta_table_name, 0)

            m_delta_table_name = "{}_m_delta".format(relation["name"])
            catalog["tables"][m_delta_table_name] = self.create_table_from_relation(
                relation,
                table_name=m_delta_table_name,
                create_physical_table=False,
            )
            self.update_catalog_table_size(catalog, m_delta_table_name, 0)

            tmp_m_delta_relation_name = "{}_tmp_m_delta".format(relation["name"])
            catalog["tables"][
                tmp_m_delta_relation_name
            ] = self.create_table_from_relation(
                relation,
                table_name=tmp_m_delta_relation_name,
                create_physical_table=False,
            )
            self.update_catalog_table_size(catalog, tmp_m_delta_relation_name, 0)

    def initialize_prev_tables(
        self,
        recursive_rules,
        catalog,
        relation_set,
        relation_def_map,
    ):
        # if the rule body contains more than one idb atoms (count repeated ones) in the same rule,
        # then 'prev' table needs to be created for these idb atoms for non-linear rule evaluation
        pre_table_set = set()
        for rule in recursive_rules:
            idb_atom_counter = 0
            rule_body_atoms = rule["body"]["atoms"]
            idb_atom_candidates = list()
            for atom in rule_body_atoms:
                if atom["name"] in relation_set:
                    idb_atom_candidates.append(atom["name"])
                    idb_atom_counter += 1
            if idb_atom_counter >= 2:
                for idb_atom_candidate in idb_atom_candidates:
                    pre_table_set.add(idb_atom_candidate)

        # Create 'prev' tables
        prev_tables = list()
        for relation_name in pre_table_set:
            relation = relation_def_map[relation_name]["relation"]
            prev_relation_name = "{}_prev".format(relation["name"])
            prev_relation = deepcopy(relation)
            prev_relation["name"] = prev_relation_name
            prev_tables.append(prev_relation_name)
            catalog["tables"][prev_relation_name] = self.create_table_from_relation(
                prev_relation
            )
            catalog["optimization"][prev_relation_name] = dict()
            catalog["optimization"][prev_relation_name]["size"] = 0
        if len(prev_tables) > 0:
            self.analyze(table_list=prev_tables, count=True)
        return pre_table_set

    def load_data_into_delta(self, catalog, relation_set, relation_def_map):
        delta_tables = list()
        for table in relation_set:
            src_relation = relation_def_map[table]["relation"]
            src_relation_table = catalog["tables"][src_relation["name"]]
            # if no fact in the table yet, then there is no need to copy
            if catalog["optimization"][src_relation["name"]]["size"] == 0:
                continue
            delta_relation_key = "{}_delta".format(src_relation["name"])
            delta_relation_table = catalog["tables"][delta_relation_key]
            catalog["optimization"][delta_relation_key]["size"] = catalog[
                "optimization"
            ][src_relation["name"]]["size"]
            self.__quickstep_shell_instance.load_data_from_table(
                src_relation_table, delta_relation_table, deduplication=False
            )
            delta_tables.append(delta_relation_table.table_name)

        if len(delta_tables) > 0:
            self.analyze(table_list=delta_tables, count=True)

    def check_empty_delta(
        self,
        catalog,
        relation_set,
        before_recursive_evaluation_check=False,
    ):
        if STATIC_DEBUG:
            return False
        is_delta_empty = True
        if before_recursive_evaluation_check:
            for relation_name in relation_set:
                delta_relation_key = "{}_delta".format(relation_name)
                delta_size = self.get_table_size(delta_relation_key)
                is_delta_empty = is_delta_empty and delta_size == 0
            return is_delta_empty

        for relation_name in relation_set:
            delta_relation_key = "{}_delta".format(relation_name)
            m_delta_table_name = "{}_m_delta".format(relation_name)
            common_table_name = "{}_common_delta".format(relation_name)
            delta_size = self.get_table_size(delta_relation_key)
            is_delta_empty = is_delta_empty and (delta_size == 0)
            catalog["optimization"][delta_relation_key]["size"] = delta_size
            catalog["optimization"][relation_name]["size"] += delta_size
            catalog["optimization"][common_table_name]["size"] = (
                catalog["optimization"][m_delta_table_name]["size"] - delta_size
            )

        return is_delta_empty

    def create_delta_tables(self, catalog, relation_set, iter_num):
        for table in relation_set:
            cur_delta_table = catalog["tables"]["{}_delta".format(table)]
            cur_delta_table.rename("{}_delta_{}".format(table, iter_num))
            self.__quickstep_shell_instance.create_table(cur_delta_table)

    def one_phase_diff(self, l_table, r_table, dest_table, aggregation_map):
        """The one-phase algorithm computing the set difference between two tables
            and insert the results into the destination table (IDB relation table)

            l_table, r_table and dest_table have the same schema

        Args:
            quickstep_shell_instance:
            l_table:
            r_table:
            dest_table:

        Returns:
        """
        one_phase_diff_str = "{};".format(
            generate_set_diff_str(l_table, r_table, dest_table, aggregation_map)
        )
        self.__quickstep_shell_instance.sql_command(one_phase_diff_str)

    def two_phase_diff(self, l_table, r_table, dest_table, aggregation_map):
        """The two-phase algorithm computing the set difference between two tables
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
        common_table.rename("COMMON_TABLE")
        self.__quickstep_shell_instance.create_table(common_table)
        intersection_sql_str = generate_intersect_str(l_table, r_table, aggregation_map)
        self.__quickstep_shell_instance.sql_command(
            "INSERT INTO COMMON_TABLE {};".format(intersection_sql_str)
        )
        self.analyze(table_list=["COMMON_TABLE"], count=True)
        ##### Second Phase ######
        set_diff_str = "{};".format(
            generate_set_diff_str(l_table, common_table, dest_table, None)
        )
        self.__quickstep_shell_instance.sql_command(set_diff_str)
        self.__quickstep_shell_instance.drop_table("COMMON_TABLE")

    def set_diff(
        self,
        l_table,
        r_table,
        dest_table,
        aggregation_map,
        alpha=1.38,
        beta=10,
        prev_mu=5,
    ):
        """Returns the string to compute set-difference between two tables
        Generate string to perform sef-difference between table S and R (R - S)

        Args:
            l_table: R table
            r_table: S table
            dest_table: the table into which the results of (l_table - r_table) are inserted
            alpha: parameter described in the cost model
            beta: parameter described in the cost model
            prev_mu: parameter described in the cost model

        Parameters used in the cost model:
            |S|: the size of the IDB relation
            |R|: the size of the relation storing the evaluated results
            |r|: the size of the intersection between S and R
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
            test_delta_table.table_name = "test_delta"
            self.__quickstep_shell_instance.create_table(test_delta_table)
            start = time.time()

        if SET_DIFF_OP:
            if beta <= (alpha / (alpha - 1)):
                self.log("Confidence Interval: beta <= alpha/(alpha-1)")
                self.log("Compute set-difference via 'one-phase difference algorithm'")
                self.one_phase_diff(
                    l_table,
                    r_table,
                    dest_table,
                    aggregation_map,
                )
                SET_DIFF_ALG = 1
            elif (alpha / (alpha - 1)) < beta < (2 * alpha / (alpha - 1)):
                self.log("Previous mu is ".format(prev_mu))
                # |r| = 0
                if prev_mu == -1:
                    cost_diff = alpha * beta - alpha - beta
                    if cost_diff < 0:
                        self.log(
                            "Compute set-difference via 'one-phase difference algorithm'"
                        )
                        self.one_phase_diff(
                            l_table,
                            r_table,
                            dest_table,
                            aggregation_map,
                        )
                        SET_DIFF_ALG = 1
                    else:
                        self.log(
                            "Compute set-difference via 'two-phase difference algorithm'"
                        )
                        self.two_phase_diff(
                            l_table,
                            r_table,
                            dest_table,
                            aggregation_map,
                        )
                        SET_DIFF_ALG = 2
                else:
                    cost_diff = beta * (alpha - 1) - (alpha + alpha / prev_mu)
                    self.log(
                        "Approximated cost difference factor: {}".format(cost_diff)
                    )
                    self.log(
                        "Uncertain interval: beta in (alpha/(alpha-1), 2*alpha/(alpha-1))",
                    )
                    if cost_diff < 0:
                        self.log(
                            "Compute set-difference via 'one-phase difference algorithm'"
                        )
                        self.one_phase_diff(
                            l_table,
                            r_table,
                            dest_table,
                            aggregation_map,
                        )
                        SET_DIFF_ALG = 1
                    else:
                        self.log(
                            "Compute set-difference via 'two-phase difference algorithm'",
                        )
                        self.two_phase_diff(
                            l_table,
                            r_table,
                            dest_table,
                            aggregation_map,
                        )
                        SET_DIFF_ALG = 2
            else:
                self.log("Confidence interval: beta >= 2 * alpha/(alpha-1)")
                self.log(
                    "Compute set-difference via 'two-phase difference algorithm'",
                )
                self.two_phase_diff(
                    l_table,
                    r_table,
                    dest_table,
                    aggregation_map,
                )
                SET_DIFF_ALG = 2

        else:
            self.log("Set difference opimizier is not turned on")
            if DEFAULT_SET_DIFF_ALG == 1:
                self.log(
                    "Compute set-difference via 'one-phase difference algorithm'",
                )
                self.one_phase_diff(
                    l_table,
                    r_table,
                    dest_table,
                    aggregation_map,
                )
                SET_DIFF_ALG = 1
            if DEFAULT_SET_DIFF_ALG == 2:
                self.log(
                    "Compute set-difference via 'two-phase difference algorithm'",
                )
                self.two_phase_diff(
                    l_table,
                    r_table,
                    dest_table,
                    aggregation_map,
                )
                SET_DIFF_ALG = 2

        if COST_MODEL_CHECK:
            end = time.time()
            chosen_set_diff_alg_time = end - start
            start = time.time()
            if SET_DIFF_ALG == 1:
                self.two_phase_diff(
                    l_table,
                    r_table,
                    test_delta_table,
                    aggregation_map,
                )
            else:
                self.one_phase_diff(
                    l_table,
                    r_table,
                    test_delta_table,
                    aggregation_map,
                )
            end = time.time()
            other_set_diff_alg_time = end - start
            time_diff = chosen_set_diff_alg_time - other_set_diff_alg_time
            self.__quickstep_shell_instance.drop_table("test_delta")
            if time_diff < 0:
                self.log("Set-difference algorithm selection: CORRECT")
                self.log(
                    "Time Saving: {} ({} %)".format(
                        -time_diff, -time_diff * 100 / other_set_diff_alg_time
                    )
                )
            else:
                self.log("Set-difference algorithm selection: WRONG")
                self.log(
                    "Time Cost: {} ({} %)".format(
                        time_diff, time_diff * 100 / other_set_diff_alg_time
                    )
                )

    def recursive_rules_eval(
        self,
        catalog,
        recursive_rules,
        relation_def_map,
    ):
        # Construct idb to rule maps to store all the unique idbs (appear in the head of certain rules) to be evaluated
        # rules evaluating the same idb are grouped together and will be evaluated together later
        eval_idb_to_rule_maps = collections.OrderedDict()
        for rule in recursive_rules:
            eval_idb_name = rule["head"]["name"]
            if eval_idb_name not in eval_idb_to_rule_maps:
                eval_idb_to_rule_maps[eval_idb_name] = list()
            eval_idb_to_rule_maps[eval_idb_name].append(rule)

        self.log("Start creating delta, prev tables for semi-naive evaluation")
        self.update_local_time()
        self.initialize_delta_tables(catalog, eval_idb_to_rule_maps, relation_def_map)
        pre_table_set = self.initialize_prev_tables(
            recursive_rules,
            catalog,
            eval_idb_to_rule_maps,
            relation_def_map,
        )
        # load data from previous evaluated results into 'delta tables' for nonlinear recursive rules
        self.load_data_into_delta(catalog, eval_idb_to_rule_maps, relation_def_map)
        self.log_local_time()
        self.log("Start Semi-Naive Evaluation\n\n")
        # Generate string to check whether all "delta" relations are empty *before* the recursive evaluation
        is_delta_empty = self.check_empty_delta(
            catalog,
            eval_idb_to_rule_maps,
            before_recursive_evaluation_check=True,
        )
        # Iterative Evaluation (Semi-Naive)
        iter_num = 0
        while not is_delta_empty:
            iter_num += 1
            self.log("-----Start Iteration {}-----".format(iter_num))
            self.update_local_time()
            if LOG_ON:
                cur_iter_start_time = self.__time_monitor.local_start_time
            # Update the delta table names in the catalog and create delta tables for the current iteration
            # (to be used in the next iteration)
            self.create_delta_tables(catalog, eval_idb_to_rule_maps, iter_num)
            # Evaluate rules grouped by IDB (rules evaluating the same IDB will run together)
            for idb in eval_idb_to_rule_maps:
                # Create m_delta tables (results evaluated without duplication)
                idb_relation = relation_def_map[idb]["relation"]
                delta_relation_key = "{}_delta".format(idb_relation["name"])
                delta_table = catalog["tables"][delta_relation_key]
                # Used for deduplication later
                m_delta_table_name = "{}_m_delta".format(idb_relation["name"])
                m_delta_table = catalog["tables"][m_delta_table_name]
                self.__quickstep_shell_instance.create_table(m_delta_table)
                # If there is more than one rule to evaluate the idb, multiple m_delta tables will be generated - merged into one
                eval_rules = eval_idb_to_rule_maps[idb]
                if STATIC_DEBUG:
                    eval_rule_num = len(eval_rules)
                    print(
                        "-----Total number of evaluation rules-----: {}".format(
                            eval_rule_num
                        )
                    )
                self.log("Evaluate {}".format(idb_relation["name"]))

                sub_query_list = list()
                aggregation_map = None
                for eval_rule in eval_rules:
                    if STATIC_DEBUG:
                        DatalogProgram.iterate_datalog_rule(eval_rule)
                    self.log(DatalogProgram.iterate_datalog_rule(eval_rule))
                    # there might be different combinations of 'delta' and 'non-delta' joins
                    delta_atom_num = 0
                    eval_rule_body = eval_rule["body"]["atoms"]
                    for atom in eval_rule_body:
                        if atom["name"] in eval_idb_to_rule_maps:
                            delta_atom_num += 1

                    sub_queries, aggregation_map = gen_rule_eval_sql_str(
                        eval_rule,
                        relation_def_map,
                        eval_idb_to_rule_maps,
                        iter_num,
                        recursive=True,
                    )

                    if STATIC_DEBUG:
                        sub_query_num = len(sub_queries)
                        print(
                            "------Number of subqueries------: {}".format(sub_query_num)
                        )

                    for sub_query_str in sub_queries:
                        sub_query_list.append(sub_query_str)

                # create a temporary table to store the results before deduplication
                tmp_m_delta_table_name = "{}_tmp_m_delta".format(idb_relation["name"])
                tmp_m_delta_table = catalog["tables"][tmp_m_delta_table_name]
                self.__quickstep_shell_instance.create_table(tmp_m_delta_table)
                eval_m_delta_str = generate_unified_idb_evaluation_str(
                    tmp_m_delta_table_name, sub_query_list
                )
                if STATIC_DEBUG:
                    print(eval_m_delta_str)
                self.execute(eval_m_delta_str)
                # update catalog
                cur_row_num = self.count_rows(tmp_m_delta_table_name)
                self.update_catalog_table_size(
                    catalog, tmp_m_delta_table_name, cur_row_num
                )
                tmp_m_delta_table = catalog["tables"][tmp_m_delta_table_name]
                self.__quickstep_shell_instance.dedup_table(
                    tmp_m_delta_table, dest_table_name=m_delta_table_name
                )

                prev_R_size = self.get_table_size(catalog, m_delta_table_name)
                m_delta_table_size = self.count_rows(m_delta_table_name)
                self.update_catalog_table_size(
                    catalog, m_delta_table_name, m_delta_table_size
                )
                self.analyze([m_delta_table_name], count=True)
                self.log_local_time()
                # perform set difference
                S_size = self.get_table_size(catalog, idb_relation["name"])
                R_size = self.get_table_size(catalog, m_delta_table_name)
                delta_relation_table = catalog["tables"][delta_relation_key]
                if R_size == 0:
                    self.log(
                        """The size of m_delta relation table is zero 
                        - no need to perform set-difference computation
                        """
                    )
                else:
                    if S_size == 0:
                        self.log(
                            """ The size of IDB relation table is zero
                            - no need to perform set-difference computation
                            """
                        )
                        self.__quickstep_shell_instance.load_data_from_table(
                            m_delta_table, delta_table, deduplication=False
                        )
                    else:
                        common_delta_relation_name = "{}_common_delta".format(
                            idb_relation["name"]
                        )
                        normalized_S_size = int(
                            (S_size + TUPLE_NUM_PER_BLOCK - 1) / TUPLE_NUM_PER_BLOCK
                        )
                        self.log("Normalized size of S: {}".format(normalized_S_size))
                        normalized_R_size = int(
                            (R_size + TUPLE_NUM_PER_BLOCK - 1) / TUPLE_NUM_PER_BLOCK
                        )
                        self.log("Normalized size of R: {}".format(normalized_R_size))
                        beta = float(normalized_S_size) / float(normalized_R_size)
                        if (
                            catalog["optimization"][common_delta_relation_name]["size"]
                            == 0
                        ):
                            prev_mu = -1
                        else:
                            normalized_prev_R_size = (
                                prev_R_size + TUPLE_NUM_PER_BLOCK - 1
                            ) / TUPLE_NUM_PER_BLOCK
                            normalized_prev_R_size = (
                                normalized_prev_R_size + THREADS_NUM - 1
                            ) / THREADS_NUM
                            normalize_common_delta_size = catalog["optimization"][
                                common_delta_relation_name
                            ]["size"]
                            normalize_common_delta_size = (
                                normalize_common_delta_size + TUPLE_NUM_PER_BLOCK - 1
                            ) / TUPLE_NUM_PER_BLOCK
                            normalize_common_delta_size = (
                                normalize_common_delta_size + THREADS_NUM - 1
                            ) / THREADS_NUM
                            prev_mu = float(normalized_prev_R_size) / float(
                                normalize_common_delta_size
                            )
                        m_delta_relation_table = catalog["tables"][m_delta_table_name]
                        original_relation_table = catalog["tables"][
                            idb_relation["name"]
                        ]
                        self.update_local_time()
                        self.analyze([m_delta_relation_table], count=True)
                        self.set_diff(
                            m_delta_relation_table,
                            original_relation_table,
                            delta_relation_table,
                            aggregation_map,
                            beta=beta,
                            prev_mu=prev_mu,
                        )

                self.analyze([delta_relation_table.table_name], count=True)
                delta_relation_size = self.count_rows(delta_relation_table.table_name)
                self.update_catalog_table_size(
                    catalog, delta_relation_key, delta_relation_size
                )
                # drop m_delta table
                self.__quickstep_shell_instance.drop_table(m_delta_table_name)
                # save the current idb
                idb_table = catalog["tables"][idb_relation["name"]]
                if idb_relation["name"] in pre_table_set:
                    if R_size != 0:
                        self.log("Save the current IDB")
                        self.update_local_time()
                        prev_table_name = "{}_prev".format(idb_relation["name"])
                        prev_table = catalog["tables"][prev_table_name]
                        self.__quickstep_shell_instance.drop_table(prev_table_name)
                        self.__quickstep_shell_instance.create_table(prev_table)
                        self.__quickstep_shell_instance.load_data_from_table(
                            idb_table, prev_table
                        )
                        self.analyze(table_list=[prev_table_name], count=True)
                        self.log_local_time()

                if R_size != 0:
                    # update evaluated idb tables
                    self.log("Update IDB (union delta)")
                    self.update_local_time()
                    self.__quickstep_shell_instance.load_data_from_table(
                        delta_table, idb_table
                    )
                    self.analyze(table_list=[idb_table.table_name], count=True)
                    self.log_local_time()

            # drop all old deltas
            for idb in eval_idb_to_rule_maps:
                old_idb_delta_name = "{}_delta_{}".format(idb, iter_num - 1)
                if STATIC_DEBUG:
                    print("-----Old IDB Delta Table Name-----")
                    print(old_idb_delta_name)
                self.__quickstep_shell_instance.drop_table(old_idb_delta_name)

            # check whether the evaluation reaches the 'fix-point'
            is_delta_empty = self.check_empty_delta(catalog, eval_idb_to_rule_maps)
            # log the number of tuples in delta tables evaluated in the current iteration
            for idb in eval_idb_to_rule_maps:
                if STATIC_DEBUG:
                    print("-----Delta Table Names in Current Iteration-----")
                    print(catalog["tables"]["{}_delta".format(idb)].table_name)

            # Log the number of tuples in the idb tables after the evaluation in the current iteration
            if LOG_ON:
                for idb in eval_idb_to_rule_maps:
                    self.count_rows(idb)

                cur_iter_time = time.time() - cur_iter_start_time
                self.log_time(cur_iter_time, "Iteration Time")
                self.log("-----SEPERATOR-----\n\n")

            if STATIC_DEBUG:
                break

            if DYNAMIC_DEBUG:
                if iter_num == DYNAMIC_DEBUG_ITER_NUM:
                    self.stop()
                    break

        # Clear all the intermediate tables after the iterative evaluation finishes
        for table_name in pre_table_set:
            prev_table_name = "{}_prev".format(table_name)
            self.__quickstep_shell_instance.drop_table(prev_table_name)

        for idb in eval_idb_to_rule_maps:
            idb_delta_table_name = catalog["tables"]["{}_delta".format(idb)].table_name
            self.__quickstep_shell_instance.drop_table(idb_delta_table_name)

        if RETAIN_FINAL_OUTPUT_ONLY:
            for idb in eval_idb_to_rule_maps:
                if idb not in FINAL_OUTPUT_RELATIONS:
                    self.__quickstep_shell_instance.drop_table(idb)

    def non_recursive_rule_eval(
        self, idb_relation_name, catalog, non_recursive_rules, relation_def_map
    ):

        sub_queries = list()
        for eval_rule in non_recursive_rules:
            self.log(DatalogProgram.iterate_datalog_rule(eval_rule))
            sub_query = gen_rule_eval_sql_str(
                eval_rule,
                relation_def_map,
                None,
                iter_num=0,
                recursive=False,
            )
            sub_queries.append(sub_query)

        if SELECTIVE_DEDUP and idb_relation_name not in DEDUP_RELATION_LIST:
            target_table_name = idb_relation_name
        else:
            target_table_name = "{}_tmp".format(idb_relation_name)

        if UNIFIED_IDB_EVALUATION:
            eval_str = generate_unified_idb_evaluation_str(
                target_table_name, sub_queries
            )
            if STATIC_DEBUG:
                print("-----nonrecursive unified-idb evaluation str-----")
                print(eval_str)

            if SELECTIVE_DEDUP and idb_relation_name not in DEDUP_RELATION_LIST:
                self.execute(eval_str)
            else:
                # create tmp table
                idb_relation = relation_def_map[idb_relation_name]["relation"]
                tmp_table = self.create_table_from_relation(
                    idb_relation, table_name=target_table_name
                )
                self.execute(eval_str)
                self.__quickstep_shell_instance.dedup_table(
                    tmp_table, dest_table_name=idb_relation_name
                )
        else:
            for sub_query in sub_queries:
                eval_str = generate_insertion_evaluation_str(
                    target_table_name, sub_query
                )
                if STATIC_DEBUG:
                    print("-----nonrecursive evaluation str-----")
                    print(eval_str)

            if SELECTIVE_DEDUP and idb_relation_name not in DEDUP_RELATION_LIST:
                self.execute(eval_str)
            else:
                # create tmp table
                idb_relation = relation_def_map[idb_relation_name]["relation"]
                tmp_table = self.create_table_from_relation(
                    idb_relation, table_name=target_table_name
                )
                self.execute(eval_str)
                self.__quickstep_shell_instance.dedup_table(
                    tmp_table, dest_table_name=idb_relation_name
                )

        self.analyze([idb_relation_name], count=True)
        row_num = self.count_rows(idb_relation_name)
        self.update_catalog_table_size(catalog, idb_relation_name, row_num)

    def non_recursive_single_query_evaluation(
        self, rule_groups, rules, relation_def_map
    ):
        tmp_table_queries = list()
        rule_group_num = len(rule_groups["rule_groups"])
        final_result_eval_str = ""
        for group_index in range(rule_group_num):
            rule_group = rule_groups["rule_groups"][group_index]
            evaluated_rules = [rules[rule_index] for rule_index in rule_group]
            idb_relation_name = evaluated_rules[0]["head"]["name"]
            sub_queries = list()
            for eval_rule in evaluated_rules:
                sub_query = gen_rule_eval_sql_str(
                    eval_rule,
                    relation_def_map,
                    None,
                    iter_num=0,
                    recursive=False,
                )
                sub_queries.append(sub_query)

            if group_index != (rule_group_num - 1):
                # query evaluating the intermediate result into the temporary tables (i.e. with t as)
                eval_str = generate_unified_idb_evaluation_str(
                    idb_relation_name, sub_queries, with_subquery=True
                )
                tmp_table_queries.append(eval_str)
            else:
                final_result_eval_str = generate_unified_idb_evaluation_str(
                    idb_relation_name,
                    sub_queries,
                    with_subquery=False,
                    select_into=True,
                )

        single_query_str = "WITH {} {}".format(
            ", ".join(tmp_table_queries), final_result_eval_str
        )
        print(single_query_str)

    def output_data_from_table_to_csv(self, relation_name):
        self.__quickstep_shell_instance(relation_name, delimiter=CSV_DELIMITER)

    def stop(self):
        self.__quickstep_shell_instance.stop()
