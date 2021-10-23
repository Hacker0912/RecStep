from antlr4 import *
from parser.datalog_parser import DatalogParser
from parser.datalog_lexer import DatalogLexer

import collections
from typing import List

from rule_analyzer.analyzer import *


class DatalogProgram(object):
    def __init__(self, datalog_file_path, print_datalog_program=True):
        self.__print_datalog_program = print_datalog_program

        try:
            input_file = FileStream(datalog_file_path)
        except IOError:
            raise Exception(
                "Error encountered when trying to read file '{}'".format(
                    datalog_file_path
                )
            )

        lexer = DatalogLexer(input_file)
        stream = CommonTokenStream(lexer)
        parser = DatalogParser(stream)
        parser.buildParseTrees = False
        # AST of the given datalog program
        self.edb_decl = edb_decl_tree = parser.datalog_edb_declare().r
        self.idb_decl = idb_decl_tree = parser.datalog_idb_declare().r
        self.rules = rules_tree = parser.datalog_rule_declare().r

        if self.__print_datalog_program:
            print("EDB_DECL:")
            self.iterate_datalog_edb_idb_decl(edb_decl_tree)
            print()
            print("IDB_DECL:")
            self.iterate_datalog_edb_idb_decl(idb_decl_tree)
            print()
            print("RULE_DECL:")
            self.iterate_datalog_program(rules_tree)
            print()
            print("DEPENDENCY_GRAPH: ")
            print()

        (
            self.dependency_graph,
            self.negation_dependency_map,
        ) = construct_dependency_graph(rules_tree)

        self.sccs = compute_rule_sccs(self.dependency_graph)
        if self.__print_datalog_program:
            print(
                "Number of the rule strongly connected components (rscc): {}".format(
                    len(self.sccs)
                )
            )
            print("\nrule strongly connected components in reverse evaluation order: ")
            print("<rscc index>: <rscc key> - <indices of rules>\n")
            scc_index = 0
            for scc_key in self.sccs:
                print("rscc {}: {}-{}".format(scc_index, scc_key, self.sccs[scc_key]))
                scc_index += 1
            print()

        # Detect negation cycle
        negation_cycle = check_negation_cycle(self.sccs, self.negation_dependency_map)
        if negation_cycle:
            raise Exception(
                "Directed cycle with a negative edge detected:\n \
                            The Datalog program is NOT stratifiable"
            )
        else:
            print("No directed cycle with a negative edge being detected:")
            print("The Datalog program is stratifiable")

        self.rule_groups = group_rules(
            construct_rule_atom_map(rules_tree), self.sccs, self.dependency_graph
        )

        if self.__print_datalog_program:
            print()
            print("Rule Groups (in evaluation order):")
            print()

            for rule_group in self.rule_groups["rule_groups"]:
                print("rule indices: {}".format(rule_group))
                for rule_index in rule_group:
                    print(self.iterate_datalog_rule(rules_tree[rule_index]))
                print()

    @staticmethod
    def iterate_datalog_edb_idb_decl(relations):
        """
        Iterate each specific component in the edb/idb declaration and print out the complete declaration
        """
        for relation in relations:
            attribute_name_type_pairs = [
                "{} {}".format(attribute.name, attribute.type)
                for attribute in relation["attributes"]
            ]
            relation_str = "{}({})".format(
                relation["name"], ", ".join(attribute_name_type_pairs)
            )
            print(relation_str)

    @staticmethod
    def iterate_datalog_rule(datalog_rule):
        """
        Iterate each specific component in a datalog rule (head and body)
        """
        head = datalog_rule["head"]
        body = datalog_rule["body"]
        head_arg_list = head["arg_list"]

        try:
            body_atoms = body["atoms"]
            body_comparisons = body["compares"]
            body_assignments = body["assigns"]
            body_negations = body["negations"]
        except TypeError:
            # if the rule of the body is empty, then the body will be 'NoneType'
            body_atoms = list()
            body_comparisons = list()
            body_assignments = list()
            body_negations = list()

        head_args_strs = list()
        for arg in head_arg_list:
            if arg.type == "aggregation":
                if arg.name["agg_arg"]["type"] == "attribute":
                    head_arg_str = "{}({})".format(
                        arg.name["agg_op"], arg.name["agg_arg"]["content"]
                    )
                if arg.name["agg_arg"]["type"] == "math_expr":
                    head_arg_str = "{}({}{}{})".format(
                        arg.name["agg_op"],
                        arg.name["agg_arg"]["content"]["lhs"],
                        arg.name["agg_arg"]["content"]["op"],
                        arg.name["agg_arg"]["content"]["rhs"],
                    )
            elif arg.type == "math_expr":
                head_arg_str = "{}{}{}".format(
                    arg.name["lhs"], arg.name["op"], arg.name["rhs"]
                )
            else:
                head_arg_str = arg.name
            head_args_strs.append(head_arg_str)
        head_str = "{}({})".format(head["name"], ", ".join(head_args_strs))

        body_item_strs = list()
        for atom in body_atoms:
            body_item_strs.append(
                "{}({})".format(
                    atom["name"], ", ".join([arg.name for arg in atom["arg_list"]])
                )
            )
        for compare in body_comparisons:
            body_item_strs.append(
                "{} {} {}".format(compare["lhs"][0], compare["op"], compare["rhs"][0])
            )
        for negation in body_negations:
            body_item_strs.append(
                "!{}({})".format(
                    negation["name"],
                    ", ".join([arg.name for arg in negation["arg_list"]]),
                )
            )
        for assign in body_assignments:
            body_item_strs.append(
                "{} = {} {} {}".format(
                    assign["lhs"],
                    assign["rhs"]["lhs"],
                    assign["rhs"]["op"],
                    assign["rhs"]["rhs"],
                )
            )
        body_str = ", ".join(body_item_strs)
        return "{} :- {}.".format(head_str, body_str)

    def iterate_datalog_program(self, datalog_program):
        """
        Iterate each rule in the AST generated from the datalog program and print the complete datalog program
        """
        rule_count = 0
        for datalog_rule in datalog_program:
            print("{}: {}".format(rule_count, self.iterate_datalog_rule(datalog_rule)))
            rule_count += 1
