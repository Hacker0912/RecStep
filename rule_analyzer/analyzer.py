"""
The analyzer is responsible for:

1. Identify IDB and EDB relations and their mapping to input and output tables
2. Verify the syntactic correctness of the program
3. Construct the dependency graph and perform stratification
"""

import collections

from execution.config import BACK_END


def construct_atom_rule_map(datalog_program):
    """create a dictionary storing <head_atom, rule_index_list> key-value pair"""
    rule_number = len(datalog_program)
    head_rule_map = collections.OrderedDict()
    for rule_index in range(rule_number):
        cur_rule = datalog_program[rule_index]
        head_atom_name = cur_rule["head"]["name"]
        if head_atom_name not in head_rule_map:
            head_rule_map[head_atom_name] = list()
        head_rule_map[head_atom_name].append(rule_index)

    return head_rule_map


def construct_rule_atom_map(datalog_program):
    """create a list rule_atom_map: rule_atom_map[rule_index] = head_atom_name"""
    rule_atom_map = [rule["head"]["name"] for rule in datalog_program]
    return rule_atom_map


def construct_dependency_graph(datalog_program, verbose=False):
    rule_number = len(datalog_program)
    dependency_map = collections.OrderedDict({})
    negation_dependency_map = collections.OrderedDict({})
    # Pre-processing: create a dictionary storing <head_atom, rule_index_list> key-value pair
    head_rule_map = construct_atom_rule_map(datalog_program)

    # Construct dependency graph
    for rule_index in range(rule_number):
        dependency_map[rule_index] = set()
        negation_dependency_map[rule_index] = set()
        cur_rule = datalog_program[rule_index]
        try:
            body_atoms = cur_rule["body"]["atoms"]
            body_negations = cur_rule["body"]["negations"]
        except TypeError:
            # empty rule body - the rule does not depend on any other rules
            continue

        for atom in body_atoms:
            # Check whether the atom is in the head of any IDB rules
            if atom["name"] in head_rule_map:
                dependent_rule_list = head_rule_map[atom["name"]]
                for dependent_rule_index in dependent_rule_list:
                    dependency_map[rule_index].add(dependent_rule_index)

        # For stratification of Datalog rules including negation
        for atom in body_negations:
            if atom["name"] in head_rule_map:
                dependent_rule_list = head_rule_map[atom["name"]]
                for dependent_rule_index in dependent_rule_list:
                    dependency_map[rule_index].add(dependent_rule_index)
                    negation_dependency_map[rule_index].add(dependent_rule_index)

    if verbose:
        for rule_index in dependency_map:
            if len(dependency_map[rule_index]) == 0:
                continue
            dependent_rule_indices = sorted(list(dependency_map[rule_index]))
            dependent_rules = ", ".join(
                ["rule_{}".format(i) for i in dependent_rule_indices]
            )
            print("rule_{}: {}".format(rule_index, dependent_rules))

    if verbose:
        print()
        print("NEGATION_DEPENDENCY_GRAPH:")
        for rule_index in negation_dependency_map:
            if len(negation_dependency_map[rule_index]) == 0:
                continue
            negation_dependent_rule_indices = sorted(
                list(negation_dependency_map[rule_index])
            )
            negation_dedpendent_rules = ", ".join(
                ["rule_{}".format(i) for i in negation_dependent_rule_indices]
            )
            print("rule_{}: {}".format(rule_index, negation_dedpendent_rules))

    return dependency_map, negation_dependency_map


def compute_rule_sccs(dependency_map):
    # compute strongly connected components by Kosaraju's algorithm
    rule_visited_map = [0] * len(dependency_map)
    # map to check whether a rule has already been assigned to one of strongly connected components
    rule_assigned_map = [0] * len(dependency_map)
    dfs_order_list = list()
    # strongly connected components
    sccs = collections.OrderedDict()

    # depth-first search to compute the transponse graph order
    def visit(r):
        """
        Args:
            r: r represents "rule" here
        """
        if rule_visited_map[r] == 0:
            rule_visited_map[r] = 1
            dependent_rs = dependency_map[r]
            for dependent_r in dependent_rs:
                visit(dependent_r)
            dfs_order_list.append(r)

    for rule in dependency_map:
        visit(rule)

    dfs_order_list.reverse()

    # construct transpose dependency graph
    transpose_dependency_graph = collections.OrderedDict({})

    for rule in dependency_map:
        dependent_rules = dependency_map[rule]
        for dependent_rule in dependent_rules:
            if dependent_rule not in transpose_dependency_graph:
                transpose_dependency_graph[dependent_rule] = set([])
            transpose_dependency_graph[dependent_rule].add(rule)

    def assign(r, root):
        """
        Args:
            r: r represents "rule" here
            root: the first rule discovered in the corresponding strongly connected component instance
        """
        if rule_assigned_map[r] == 0:
            if root not in sccs:
                sccs[root] = []
            sccs[root].append(r)
            rule_assigned_map[r] = 1
            # check if the rule is depended on by any other rules
            # corner case: a rule could be totally independent
            if r in transpose_dependency_graph:
                reverse_dependent_rules = transpose_dependency_graph[r]
                for reverse_dependent_rule in reverse_dependent_rules:
                    assign(reverse_dependent_rule, root)

    for rule in dfs_order_list:
        assign(rule, rule)

    # rule evaluation order: the reverse order of sccs
    return sccs


def check_negation_cycle(sccs, negation_dependency_map):
    negation_cycle = False
    cur_negation_cycle = False
    for scc_key in sccs:
        cur_negation_cycle = False
        scc = sccs[scc_key]
        if len(scc) > 1:
            find_negation_cycle = False
            for rule_index in scc:
                if find_negation_cycle:
                    break
                if rule_index in negation_dependency_map:
                    for dependent_rule_index in negation_dependency_map[rule_index]:
                        if dependent_rule_index in scc:
                            find_negation_cycle = True
                            cur_negation_cycle = True
                            negation_cycle = True
                            break
        if cur_negation_cycle:
            print("Negation cycle detected in scc[" + str(scc_key) + "]")

    return negation_cycle


def is_recursive_scc(sccs, scc_key, dependency_graph):
    """check if the rules in the strongly connected component (scc) indexed by scc_key in sccs are recursive or not"""
    scc = sccs[scc_key]
    # non-recursive rule itself must be a strongly connected component of size 1
    if len(scc) > 1:
        return True

    rule = scc[0]
    if rule not in dependency_graph:
        # the recursive rule must depend on some other rule or itself
        return False
    if rule not in dependency_graph[rule]:
        # the recursive rule must depend on itself if it is in a strongly connected component of size 1
        return False

    return True


def group_rules(rule_atom_map, sccs, dependency_graph):
    """put the rule strongly connected components in which each contains only a single
    non-recursive rule with the same idb name in the same group
    """
    rule_groups = list()
    non_recursive_rule_group_map = collections.OrderedDict()
    rule_group_bitmap = list()
    for scc_key in sccs:
        if is_recursive_scc(sccs, scc_key, dependency_graph):
            if BACK_END != "quickstep":
                raise Exception(
                    "Recursive rules can only be executed using quickstep as backend"
                )
            # do nothing if the scc is recursive
            rule_groups.append(sccs[scc_key])
            rule_group_bitmap.append(True)
        else:
            # merge non-recursive rules evaluating the same idb
            head_atom = rule_atom_map[scc_key]
            if head_atom not in non_recursive_rule_group_map:
                rule_groups.append(sccs[scc_key])
                non_recursive_rule_group_map[head_atom] = rule_groups[-1]
                rule_group_bitmap.append(False)
            else:
                non_recursive_rule_group_map[head_atom].extend(sccs[scc_key])

    rule_groups.reverse()
    rule_group_bitmap.reverse()
    return {"rule_groups": rule_groups, "rule_group_bitmap": rule_group_bitmap}
