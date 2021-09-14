"""
The analyzer is responsible for:

1. Identify IDB and EDB relations and their mapping to input and output tables
2. Verify the syntactic correctness of the program
3. Construct the dependency graph and perform stratification
"""

import collections


def construct_dependency_graph(datalog_program):
    rule_number = len(datalog_program)
    dependency_map = collections.OrderedDict({})
    negation_dependency_map = collections.OrderedDict({})
    head_rule_map = collections.OrderedDict({})
    # Pre-processing: create a dictionary storing <head_atom, rule_index_list> key-value pair
    for rule_index in range(rule_number):
        cur_rule = datalog_program[rule_index]
        head_atom_name = cur_rule['head']['name']
        if head_atom_name not in head_rule_map:
            head_rule_map[head_atom_name] = set([])
            head_rule_map[head_atom_name].add(rule_index)
        else:
            head_rule_map[head_atom_name].add(rule_index)

    # Construct dependency graph
    for rule_index in range(rule_number):
        dependency_map[rule_index] = set()
        cur_rule = datalog_program[rule_index]
        try:
            body_atoms = cur_rule['body']['atoms']
            body_negations = cur_rule['body']['negations']
        except TypeError:
            # if the body of the rule is empty, then the body will be 'NoneType'
            body_atoms = list()
            body_negations = list()

        for atom in body_atoms:
            # Check whether the atom is in head of any IDB rules
            if atom['name'] in head_rule_map:
                dependent_rule_list = head_rule_map[atom['name']]
                for dependent_rule_index in dependent_rule_list:
                    dependency_map[rule_index].add(dependent_rule_index)

        # For stratification of Datalog rules including negation
        for atom in body_negations:
            negation_dependency_map[rule_index] = set([])
            if atom['name'] in head_rule_map:
                dependent_rule_list = head_rule_map[atom['name']]
                for dependent_rule_index in dependent_rule_list:
                    dependency_map[rule_index].add(dependent_rule_index)
                    negation_dependency_map[rule_index].add(dependent_rule_index)

    for rule_index in dependency_map:
        dependency_str = 'rule_' + str(rule_index) + ': '
        dependent_rule_indices = dependency_map[rule_index]
        for dependent_rule_index in dependent_rule_indices:
            dependency_str += 'rule_' + str(dependent_rule_index) + ', '
        print(dependency_str[:len(dependency_str)-2])

    print()
    print('NEGATION_DEPENDENCY_GRAPH:')
    for rule_index in negation_dependency_map:
        dependency_str = 'rule_' + str(rule_index) + ': '
        negation_dependent_rule_indices = negation_dependency_map[rule_index]
        for dependent_rule_index in negation_dependent_rule_indices:
            dependency_str += 'rule_' + str(dependent_rule_index) + ', '
        print(dependency_str[:len(dependency_str)-2])

    return [dependency_map, negation_dependency_map]


def group_rules(dependency_map):
    # compute strongly connected components by Kosaraju's algorithm
    rule_visited_map = [0] * len(dependency_map)
    # map to check whether a rule has already been assigned to one of strongly connected components
    rule_assigned_map = [0] * len(dependency_map)
    dfs_order_list = list()
    # strongly connected components
    sccs = collections.OrderedDict({})

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
            print('Negation cycle detected in scc[' + str(scc_key) + ']')

    return negation_cycle
