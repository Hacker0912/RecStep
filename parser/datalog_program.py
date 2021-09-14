from antlr4 import *
from parser.datalog_parser import DatalogParser
from parser.datalog_lexer import DatalogLexer
import collections

from rule_analyzer.analyzer import construct_dependency_graph, group_rules, check_negation_cycle


class DatalogProgram(object):
    def __init__(self, datalog_program, edb_decl, idb_decl, dependency_graph, scc):
        self.datalog_program = datalog_program
        self.edb_decl = edb_decl
        self.idb_decl = idb_decl
        self.dependency_graph = dependency_graph
        self.scc = scc


def iterate_datalog_edb_idb_decl(relations):
    """
    Iterate each specific component in the edb/idb declaration and print out the complete declaration
    """
    for relation in relations:
        relation_str = relation['name'] + '('
        attributes = relation['attributes']
        for attribute in attributes:
            relation_str += attribute.name + ' ' + attribute.type + ', '
        relation_str = relation_str[:len(relation_str)-2]
        relation_str += ')'
        print(relation_str)


def iterate_datalog_rule(datalog_rule):
    """
    Iterate each specific component in a datalog rule (head and body)
    """
    head = datalog_rule['head']
    body = datalog_rule['body']

    head_name = head['name']
    head_arg_list = head['arg_list']

    try:
        body_atom_list = body['atoms']
        body_compare_list = body['compares']
        body_assign_list = body['assigns']
        body_negation_list = body['negations']
    except TypeError:
        # if the rule of the body is empty, then the body will be 'NoneType'
        body_atom_list = list()
        body_compare_list = list()
        body_assign_list = list()
        body_negation_list = list()

    head_str = ""
    if 'non-dedup' in datalog_rule:
        head_str += '[!dedup]'
    if 'non-set-diff' in datalog_rule:
        head_str += '[!set-diff]'
    if 'dedup-only' in datalog_rule:
        head_str += '[dedup_only]'
    head_str += head_name + '('
    for arg in head_arg_list:
        if arg.type == 'aggregation':
            head_str += arg.name['agg_op'] + '('
            if arg.name['agg_arg']['type'] == 'attribute':
                head_str += arg.name['agg_arg']['content']
            if arg.name['agg_arg']['type'] == 'math_expr':
                head_str += arg.name['agg_arg']['content']['lhs'] +\
                            arg.name['agg_arg']['content']['op'] + arg.name['agg_arg']['content']['rhs']
            head_str += ')' + ', '
        elif arg.type == 'math_expr':
            head_str += arg.name['lhs'] + arg.name['op'] + arg.name['rhs'] + ', '
        else:
            head_str += arg.name + ', '

    head_str = head_str[:len(head_str)-2] + ')'

    body_str = ''
    for atom in body_atom_list:
        body_str += atom['name'] + '('
        arg_list = atom['arg_list']
        for arg in arg_list:
            body_str += arg.name + ', '
        body_str = body_str[:len(body_str)-2] + '), '

    for compare in body_compare_list:
        body_str += compare['lhs'][0] + ' ' + compare['op'] + ' ' + compare['rhs'][0] + ', '

    for negation in body_negation_list:
        body_str += '!' + negation['name'] + '('
        arg_list = negation['arg_list']
        for arg in arg_list:
            body_str += arg.name + ', '
        body_str = body_str[:len(body_str)-2] + '), '

    for assign in body_assign_list:
        body_str += assign['lhs'] + ' ' + ' = ' + assign['rhs']['lhs'] + ' ' +  \
                    assign['rhs']['op'] + ' ' + assign['rhs']['rhs'] + ', '

    body_str = body_str[:len(body_str)-2] + '.'

    return head_str + ' :- ' + body_str


def iterate_datalog_program(datalog_program):
    """
    Iterate each rule in the AST generated from the datalog program and print the complete datalog program
    """
    rule_count = 0
    for datalog_rule in datalog_program:
        print(str(rule_count) + ':')
        print(iterate_datalog_rule(datalog_rule))
        rule_count += 1


def construct_datalog_program_instance(input_file_name):
    """
    try:
        input_file_name = sys.argv[1]
    except:
        raise Exception('No input file is given')
    """

    try:
        input_file = FileStream(input_file_name)
    except IOError:
        raise Exception('Error encountered when trying to read file \'' + input_file_name + '\'')

    lexer = DatalogLexer(input_file)
    stream = CommonTokenStream(lexer)
    parser = DatalogParser(stream)
    parser.buildParseTrees = False
    # AST of the given datalog program
    edb_decl_tree = parser.datalog_edb_declare().r
    idb_decl_tree = parser.datalog_idb_declare().r
    rules_tree = parser.datalog_rule_declare().r

    print('EDB_DECL:')
    iterate_datalog_edb_idb_decl(edb_decl_tree)
    print('\n')
    print('IDB_DECL:')
    iterate_datalog_edb_idb_decl(idb_decl_tree)
    print('\n')
    print('RULE_DECL:')
    iterate_datalog_program(rules_tree)
    print('\n\n')
    print('DEPENDENCY_GRAPH: ')
    [dependency_graph, negation_dependency_map] = construct_dependency_graph(rules_tree)
    print('\n\n')
    print('SCC: ')

    sccs = group_rules(dependency_graph)
    # Reverse scc ordered dictionary and the new order of sccs would be the scc evaluation order
    sccs = collections.OrderedDict(reversed(list(sccs.items())))
    print('Number of strata: ' + str(len(sccs)))
    stratum_count = 0
    for scc_key in sccs:
        print('Stratum' + str(stratum_count) + ': ' + str(len(sccs[scc_key])) + ' rules')
        stratum_count += 1
    print(sccs)
    # Detect negation cycle
    negation_cycle = check_negation_cycle(sccs, negation_dependency_map)
    if negation_cycle:
        raise Exception('Directed cycle with a negative edge detected:\n \
                        The Datalog program is NOT stratifiable')
    else:
        print('No directed cycle with a negative edge being detected:\n \
              The Datalog program is stratifiable')
    for root in sccs:
        print('size: ' + str(len(sccs[root])))

    datalog_program_instance = DatalogProgram(rules_tree, edb_decl_tree, idb_decl_tree, dependency_graph, sccs)
    return datalog_program_instance

