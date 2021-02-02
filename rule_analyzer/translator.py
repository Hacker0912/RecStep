"""
Translate the given datalog rules into sql query mapping info -
which can later be used to generate the corresponding sql queries
"""
import collections
from copy import deepcopy


def extract_comparison_map(body, body_atom_list):
    """ Extract and store the information of comparison in the body

        Args:
            body: the rule body map
            body_atom_list: the list containing the atoms in the rule body

        Return:
            comparison_map: {<key,value>}:
                key - atom index in the body
                value - {<key,[value]>}
                        key - attribute index of the atom in the body
                        value - [base_side, op, type, value]
                                - [base_side, op, 'num', numerical value]
                                - [base_side, op, 'var', [atom_index, attribute_index]]
    """
    body_compare_list = body['compares']

    comparison_map = {}

    body_atom_num = len(body_atom_list)

    for comparison in body_compare_list:

        lhs = comparison['lhs']
        rhs = comparison['rhs']
        lhs_attribute = lhs[0]
        rhs_attribute = rhs[0]
        lhs_attri_type = lhs[1]
        rhs_attri_type = rhs[1]

        if lhs_attri_type != 'var' and rhs_attri_type != 'var':
            raise Exception('At least one side of the comparison has to be attribute in the body relations')

        base_side = 'l'

        if lhs_attri_type == 'var':
            base_attribute = lhs_attribute
            compare_attribute = rhs_attribute
            compare_attri_type = rhs_attri_type
        else:
            base_attribute = rhs_attribute
            compare_attribute = lhs_attribute
            compare_attri_type = lhs_attri_type
            base_side = 'r'

        find_mapping = False
        for atom_index in range(body_atom_num):
            if find_mapping:
                break

            cur_body_atom = body_atom_list[atom_index]
            cur_body_atom_arg_list = cur_body_atom['arg_list']
            cur_body_atom_arg_num = len(cur_body_atom_arg_list)

            for cur_atom_arg_index in range(cur_body_atom_arg_num):

                if base_attribute == cur_body_atom_arg_list[cur_atom_arg_index].name:
                    if atom_index not in comparison_map:
                        comparison_map[atom_index] = {}
                    if cur_atom_arg_index not in comparison_map[atom_index]:
                        comparison_map[atom_index][cur_atom_arg_index] = []
                        base_atom_index = atom_index
                        base_atom_arg_index = cur_atom_arg_index
                    if compare_attri_type == 'num':
                        comparison_map[atom_index][cur_atom_arg_index].append([base_side, comparison['op'],
                                                                               'num', float(compare_attribute)])
                        find_mapping = True
                        break

                if compare_attri_type == 'var':
                    if compare_attribute == cur_body_atom_arg_list[cur_atom_arg_index].name:
                        compare_side = [base_side, comparison['op'],
                                        'var', [atom_index, cur_atom_arg_index]]

        if compare_attri_type == 'var':
            comparison_map[base_atom_index][base_atom_arg_index].append(compare_side)

    return comparison_map


def extract_negation_map(body):
    """Extract the negation in the rule body (e.g., T(x,y) :- A(x,w), B(w,y), !C(x,y).
       negation map can be considered as the "anti-join" map

        Args:
            body: the rule body map

        Return:
            negation_map: {<key, value>}
                key - negation atom index in the body
                value - [type, [list]]
                    e.g., ['constant', 1], ['var', [{'a', atom args index}]]
    """
    body_atom_list = body['atoms']
    body_atom_num = len(body_atom_list)
    body_negation_list = body['negations']
    body_negation_num = len(body_negation_list)

    negation_map = dict()
    # <key, value>: <negation atom index, <arg index, <body atom index, atom arg index> > >
    anti_join_map = dict()
    for negation_index in range(body_negation_num):
        cur_body_negation = body_negation_list[negation_index]
        cur_body_negation_name = cur_body_negation['name']
        cur_body_negation_arg_list = cur_body_negation['arg_list']
        cur_body_negation_arg_num = len(cur_body_negation_arg_list)
        negation_map[negation_index] = {}
        cur_negation_atom_map = negation_map[negation_index]
        for arg_index in range(cur_body_negation_arg_num):
            cur_body_negation_arg = cur_body_negation_arg_list[arg_index]
            cur_body_negation_arg_name = cur_body_negation_arg.name
            cur_body_negation_arg_type = cur_body_negation_arg.type
            # when every argument position is 'any', set difference operation is needed
            if cur_body_negation_arg_type == 'any':
                continue
            elif cur_body_negation_arg_type == 'constant':
                if 'constant' not in cur_negation_atom_map:
                    # <arg index, value>
                    cur_negation_atom_map['constant'] = {}
                cur_negation_atom_map['constant'][arg_index] = cur_body_negation_arg.name
            elif cur_body_negation_arg_type == 'variable':
                if 'var' not in cur_negation_atom_map:
                    # <arg name, [negation atom arg indices]>
                    cur_negation_atom_map['var'] = {}
                if cur_body_negation_arg_name not in cur_negation_atom_map['var']:
                    cur_negation_atom_map['var'][cur_body_negation_arg_name] = []
                cur_negation_atom_map['var'][cur_body_negation_arg_name].append(arg_index)
            else:
                raise Exception('Unrecognized argument type in negation atom: ' +
                                cur_body_negation_name + '(' + cur_body_negation_arg_name + ')')

    for negation_atom_index in negation_map:
        negation_atom_map = negation_map[negation_atom_index]
        negation_atom_var_map = negation_atom_map['var']
        for negation_atom_arg_name in negation_atom_var_map:
            find = False
            for atom_index in range(body_atom_num):
                body_atom = body_atom_list[atom_index]
                body_atom_arg_list = body_atom['arg_list']
                arg_num = len(body_atom_arg_list)
                for arg_index in range(arg_num):
                    cur_arg = body_atom_arg_list[arg_index]
                    cur_arg_type = cur_arg.type
                    cur_arg_name = cur_arg.name
                    if cur_arg_type != 'variable':
                        continue
                    if cur_arg_name == negation_atom_arg_name:
                        negation_atom_arg_index = negation_atom_var_map[negation_atom_arg_name][0]
                        if negation_atom_index not in anti_join_map:
                            anti_join_map[negation_atom_index] = {}
                        anti_join_map[negation_atom_index][negation_atom_arg_index] = {}
                        anti_join_map[negation_atom_index][negation_atom_arg_index][atom_index] = arg_index
                        find = True
                        break
                if find:
                    break

    return [negation_map, anti_join_map]


def extract_selection_info(datalog_rule):
    """ Non-Recursive datalog rule
    Extract and store the information for attributes selected (computed) from the query

        Args:
            datalog_rule:
                data structure storing the information of the datalog rule

        Return:
             attributes-map: {<key, value>}:
                    key - attribute index in the head
                    value
                        - [atom_index, attribute_index] in the body if the attribute is selected from the atoms
                          in the rule body
                        - constant if the attribute is some specific value

             attributes-type-map (atm):
                    atm[i] represents the type of attribute indexed at i(e.g., variable, constant, aggregation)
             aggregation-map: {<key, value>}:
                    key - attribute index in the head involving aggregation,
                    value - corresponding aggregation operator
    """
    head = datalog_rule['head']
    head_arg_list = head['arg_list']
    head_arg_num = len(head_arg_list)

    body = datalog_rule['body']
    body_atom_list = body['atoms']
    body_atom_num = len(body_atom_list)

    # map attributes of the head to the position of the corresponding attribute in the body
    attributes_map = collections.OrderedDict({})
    # map attributes of the head to the specific type (e.g., variable, aggregation)
    attributes_type_map = list()
    # map the aggregation attributes of the head to the specific aggregation operator
    aggregation_map = dict()

    def search_attribute_mapping_in_body_atoms(arg_name):
        for atom_index in range(body_atom_num):
            cur_body_atom = body_atom_list[atom_index]
            cur_body_atom_arg_list = cur_body_atom['arg_list']
            cur_body_atom_arg_num = len(cur_body_atom_arg_list)
            for arg_index in range(cur_body_atom_arg_num):
                if arg_name == cur_body_atom_arg_list[arg_index].name:
                    return [atom_index, arg_index]

    for head_arg_index in range(head_arg_num):
        cur_head_arg = head_arg_list[head_arg_index]

        if cur_head_arg.type == 'variable':
            cur_head_arg_name = cur_head_arg.name
            attributes_type_map.append('var')
            attributes_map[head_arg_index] = search_attribute_mapping_in_body_atoms(cur_head_arg_name)

        elif cur_head_arg.type == 'aggregation':
            cur_head_arg_name = cur_head_arg.name['agg_arg']
            attributes_type_map.append('agg')
            aggregation_map[head_arg_index] = cur_head_arg.name['agg_op']
            if cur_head_arg_name['type'] == 'attribute':
                attribute_mapping = search_attribute_mapping_in_body_atoms(cur_head_arg_name['content'])
                attributes_map[head_arg_index] = \
                    {'type': 'attribute', 'map': attribute_mapping}
            elif cur_head_arg_name['type'] == 'math_expr':
                math_expr = cur_head_arg_name['content']
                lhs_attri_name = math_expr['lhs']
                rhs_attri_name = math_expr['rhs']
                math_op = math_expr['op']
                lhs_attribute_mapping = search_attribute_mapping_in_body_atoms(lhs_attri_name)
                rhs_attribute_mapping = search_attribute_mapping_in_body_atoms(rhs_attri_name)
                attributes_map[head_arg_index] = \
                    {'type': 'math_expr', 'lhs_map': lhs_attribute_mapping,
                     'rhs_map': rhs_attribute_mapping, 'math_op': math_op}

        elif cur_head_arg.type == 'math_expr':
            attributes_type_map.append('math_expr')
            math_expr = cur_head_arg.name
            lhs_attri_name = math_expr['lhs']
            rhs_attri_name = math_expr['rhs']
            math_op = math_expr['op']
            lhs_attribute_mapping = search_attribute_mapping_in_body_atoms(lhs_attri_name)
            rhs_attribute_mapping = search_attribute_mapping_in_body_atoms(rhs_attri_name)
            attributes_map[head_arg_index] = \
                {'type': 'math_expr', 'lhs_map': lhs_attribute_mapping,
                 'rhs_map': rhs_attribute_mapping, 'math_op': math_op}

        elif cur_head_arg.type == 'constant':
            attributes_type_map.append('constant')
            attributes_map[head_arg_index] = cur_head_arg.name

    return [attributes_map, attributes_type_map, aggregation_map]


def extract_join_info(datalog_rule):
    """
        Args:
            datalog_rule:
                data structure storing the information of the datalog rule
        Return:
            join-map: [{<key, value>}] (array of maps)
                join-map[i]: join-map of atom at position i
                    key - the index of the atom argument
                    value - {key,value}
                                key - atom index
                                value - [atom arguments indices]

            *join-map: <key, value>
                    key - atom argument name
                    value - {<key, value>}
                        key: relation name
                        value: set of relation argument positions

   """
    body = datalog_rule['body']
    body_atom_list = body['atoms']
    body_atom_num = len(body_atom_list)

    arg_relation_map = {}
    for atom_index in range(body_atom_num):
        cur_body_atom = body_atom_list[atom_index]
        cur_body_atom_name = cur_body_atom['name']
        cur_body_atom_arg_list = cur_body_atom['arg_list']
        cur_body_atom_arg_num = len(cur_body_atom_arg_list)
        rest_atom_num = body_atom_num - (atom_index + 1)

        for cur_atom_arg_index in range(cur_body_atom_arg_num):
            cur_atom_arg_type = cur_body_atom_arg_list[cur_atom_arg_index].type
            cur_atom_arg_name = cur_body_atom_arg_list[cur_atom_arg_index].name
            if cur_atom_arg_type == 'any':
                continue
            if cur_atom_arg_type == 'constant':
                continue
            # check all arguments inside the current atom
            rest_cur_atom_arg_num = cur_body_atom_arg_num - (cur_atom_arg_index + 1)
            for rest_cur_atom_arg_index in range(rest_cur_atom_arg_num):
                arg_index_tobe_checked = cur_atom_arg_index + rest_cur_atom_arg_index + 1
                arg_tobe_checked_type = cur_body_atom_arg_list[arg_index_tobe_checked].type
                arg_tobe_checked_name = cur_body_atom_arg_list[arg_index_tobe_checked].name
                if arg_tobe_checked_type == 'any':
                    continue
                if arg_tobe_checked_type == 'constant':
                    continue
                if cur_atom_arg_name == arg_tobe_checked_name:
                    if cur_atom_arg_name not in arg_relation_map:
                        arg_relation_map[cur_atom_arg_name] = {}
                    if atom_index not in arg_relation_map[cur_atom_arg_name]:
                        arg_relation_map[cur_atom_arg_name][atom_index] = set([])
                    arg_relation_map[cur_atom_arg_name][atom_index].add(cur_atom_arg_index)
                    arg_relation_map[cur_atom_arg_name][atom_index].add(arg_index_tobe_checked)

            # check all atoms after the current atom
            # for each of the rest atoms to be checked with the 'outer' atom
            for index in range(rest_atom_num):
                atom_index_tobe_checked = (atom_index + 1) + index
                atom_tobe_checked = body_atom_list[atom_index_tobe_checked]
                atom_tobe_checked_name = atom_tobe_checked['name']
                atom_tobe_checked_arg_list = atom_tobe_checked['arg_list']
                atom_tobe_checked_arg_num = len(atom_tobe_checked_arg_list)
                # for each arg of the atom to be checked with the 'outer' atom
                for atom_arg_tobe_checked_index in range(atom_tobe_checked_arg_num):
                    atom_arg_tobe_checked_name = atom_tobe_checked_arg_list[atom_arg_tobe_checked_index].name
                    if cur_atom_arg_name == atom_arg_tobe_checked_name:
                        # if current atom argument name is not in the dictionary, add it
                        if cur_atom_arg_name not in arg_relation_map:
                            arg_relation_map[cur_atom_arg_name] = {}
                        # if current atom has not been matched with the current argument before, add it
                        if atom_index not in arg_relation_map[cur_atom_arg_name]:
                            arg_relation_map[cur_atom_arg_name][atom_index] = set([])
                        arg_relation_map[cur_atom_arg_name][atom_index].add(cur_atom_arg_index)
                        if atom_index_tobe_checked not in arg_relation_map[cur_atom_arg_name]:
                            arg_relation_map[cur_atom_arg_name][atom_index_tobe_checked] = set([])
                        arg_relation_map[cur_atom_arg_name][atom_index_tobe_checked].add(atom_arg_tobe_checked_index)

    return arg_relation_map


def extract_constant_constraint_map(body):
    """ Extract constant specification in the rule body (T(x,y) :- X(x, 1), Y(x, 2))
    TODO: now only consider integer constant, type transformation (e.g., string) support to be added later

        Args:
            body: the rule body map

        Return:
            constant_constraint_map: <key, value>
                key - body_atom_index
                value - <key, value>
                    key - body_atom_arg_index
                    value - body_atom_arg_constant_specification
    """
    body_atom_list = body['atoms']
    body_atom_num = len(body_atom_list)

    constant_constraint_map = {}
    for atom_index in range(body_atom_num):
        cur_body_atom = body_atom_list[atom_index]
        cur_body_atom_arg_list = cur_body_atom['arg_list']
        cur_body_atom_arg_num = len(cur_body_atom_arg_list)

        for cur_atom_arg_index in range(cur_body_atom_arg_num):
            cur_atom_arg_type = cur_body_atom_arg_list[cur_atom_arg_index].type
            cur_atom_arg_name = cur_body_atom_arg_list[cur_atom_arg_index].name

            if cur_atom_arg_type == 'constant':
                if atom_index not in constant_constraint_map:
                    constant_constraint_map[atom_index] = {}

                # check the type of the attribute of the relation
                constant_constraint_map[atom_index][cur_atom_arg_index] = cur_atom_arg_name

    return constant_constraint_map


def build_negation_atom_aliases(negation_atoms):
    """ Name each negation atom in the body with "aliases" and return the alias list

        Args:
            negation_atoms:
                the list of negation atoms of the rule body
        Return:
            negation_atom_alias_list:
                the list of aliases of negation atoms
    """
    negation_atom_alias_list = []
    negation_atom_naming_index = 0
    for negation_atom in negation_atoms:
        alias = 'neg_' + negation_atom['name'][0].lower() + str(negation_atom_naming_index)
        negation_atom_alias_list.append(alias)
        negation_atom_naming_index += 1

    return negation_atom_alias_list


def build_atom_aliases(body_atoms):
    """ Name each atom in the body with "aliases" and return the alias list

        Args:
            body_atoms:
                the list of atoms of the rule body
        Return:
            body_atom_alias_list:
                the list of aliases of the body atoms
    """
    body_atom_alias_list = []
    body_atom_naming_index = 0
    for atom in body_atoms:
        alias = atom['name'][0].lower() + str(body_atom_naming_index)
        body_atom_alias_list.append(alias)
        body_atom_naming_index += 1

    return body_atom_alias_list


def build_recursive_atom_aliases(body_atom_list, eval_idbs, iter_num):
    """ Each idb atom in the body may have different 'aliases' in terms of 'delta', 'prev' tables,
        the function build the list storing idb aliases [atom_aliases]
    """
    body_atom_num = len(body_atom_list)
    body_atom_eval_names = []
    idb_num = 0

    for atom in body_atom_list:
        if atom['name'] in eval_idbs:
            idb_num += 1
        cur_atom_alias_list = []
        body_atom_eval_names.append(cur_atom_alias_list)

    for i in range(body_atom_num):
        cur_atom_name = body_atom_list[i]['name']
        cur_atom_alias_list = body_atom_eval_names[i]
        if cur_atom_name not in eval_idbs:
            cur_atom_alias_list.append(cur_atom_name)
        else:
            prev_idb_name = cur_atom_name + 'Prev'
            delta_idb_name = cur_atom_name + '_Delta' + str(iter_num - 1)
            cur_atom_alias_list.append(prev_idb_name)
            cur_atom_alias_list.append(delta_idb_name)

    return [body_atom_eval_names, idb_num]


def build_recursive_atom_alias_combinations(body_atom_list, body_atom_eval_names, eval_idbs, idb_num):
    """ Datalog rules having multiple recursive idbs are evaluated by "multiple" subqueries, each of which
        has different combination of recursive atom aliases ('delta', 'prev')

        The function builds a list of combinations of idb aliases considering a single recursive datalog rule

        Args:
            body_atom_list: the list containing the atoms in the rule body
            body_atom_eval_names: the list containing the names of atoms to be evaluated
            eval_idbs:
                the list containing names of *all* idbs in the whole datalog program
            idb_num:
                the number of idbs in the current datalog rule being considered

        Return:
            atom_eval_name_list:
                list containing combinations of idb aliases corresponding to a single recursive datalog rule
    """
    body_atom_num = len(body_atom_list)
    # the length of final list is equal to the number of subqueries
    atom_eval_name_list = [[]]
    # the number of 'prev' should be strictly less than the number of idbs appearing in the rule body
    atom_eval_name_idb_prev_count = [0]

    for atom_index in range(body_atom_num):
        cur_atom_original_name = body_atom_list[atom_index]['name']
        cur_atom_eval_names = body_atom_eval_names[atom_index]
        cur_atom_eval_names_num = len(cur_atom_eval_names)
        atom_eval_name_list_num = len(atom_eval_name_list)
        new_atom_eval_name_list = []
        new_atom_eval_name_idb_prev_count = []
        for sub_list_index in range(atom_eval_name_list_num):
            sub_list = atom_eval_name_list[sub_list_index]
            for i in range(cur_atom_eval_names_num):
                copy_sub_list = deepcopy(sub_list)
                copy_sub_list.append(cur_atom_eval_names[i])
                new_atom_eval_name_list.append(copy_sub_list)
                # update and record the 'prev' table number
                if cur_atom_original_name in eval_idbs and i == 0:
                    new_atom_eval_name_idb_prev_count.append(atom_eval_name_idb_prev_count[sub_list_index] + 1)
                else:
                    new_atom_eval_name_idb_prev_count.append(atom_eval_name_idb_prev_count[sub_list_index])
        atom_eval_name_list = new_atom_eval_name_list
        atom_eval_name_idb_prev_count = new_atom_eval_name_idb_prev_count

    # remove combinations in which aliases of all recursive atoms are 'prev'
    subquery_num = len(atom_eval_name_list)
    for i in range(subquery_num):
        if atom_eval_name_idb_prev_count[i] == idb_num:
            atom_eval_name_list.remove(atom_eval_name_list[i])

    return atom_eval_name_list
