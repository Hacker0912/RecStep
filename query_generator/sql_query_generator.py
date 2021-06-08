"""
Generate the string of SQL query components (e.g., select, from, where) given the query info data structures
"""


def generate_insertion_str(head):
    """
    Generate code to populate some specified fact into the corresponding relation:
        Example: T(0,1) :- (insert tuple (0,1) into the relation 'T')
    """
    insertion_str = 'insert into'

    relation_name = head['name']
    arg_list = head['arg_list']
    attri_num = len(arg_list)
    tuple_instance = []
    for i in range(attri_num):
        arg = arg_list[i]
        arg_type = arg.type
        if arg_type != 'constant':
            raise Exception('Only specific values could be populated into relation!')
        tuple_instance.append(arg.name)

    insertion_str += ' ' + relation_name + ' values('
    tuple_num = len(tuple_instance)
    for k in range(tuple_num):
        if k != tuple_num - 1:
            insertion_str += tuple_instance[k] + ','
        else:
            insertion_str += tuple_instance[k] + ')'

    return insertion_str


def generate_select(datalog_rule, select_info, relation_def_map, body_atom_alias_list):
    select_str = 'select '

    head_name = datalog_rule['head']['name']
    head_relation_attributes = relation_def_map[head_name][0]['attributes']
    body_atom_list = datalog_rule['body']['atoms']

    attributes_map = select_info[0]
    attributes_type_map = select_info[1]
    aggregation_map = select_info[2]

    head_attributes_num = len(head_relation_attributes)

    selection_index = 0
    for attribute_index in range(head_attributes_num):
        if attributes_type_map[attribute_index] == 'var':
            body_atom_attribute_index = attributes_map[attribute_index][1]
            body_atom_name_index = attributes_map[attribute_index][0]
            select_str += body_atom_alias_list[body_atom_name_index] + '.' + \
                          relation_def_map[body_atom_list[body_atom_name_index]['name']][0]['attributes'] \
                              [body_atom_attribute_index].name

        if attributes_type_map[attribute_index] == 'agg':
            if aggregation_map[attribute_index] == 'COUNT_DISTINCT':
                select_str += 'COUNT(DISTINCT('
            else:
                select_str += aggregation_map[attribute_index] + '('
            if attributes_map[attribute_index]['type'] == 'attribute':
                body_atom_attribute_index = attributes_map[attribute_index]['map'][1]
                body_atom_name_index = attributes_map[attribute_index]['map'][0]
                select_str += body_atom_alias_list[body_atom_name_index] + '.' + \
                              relation_def_map[body_atom_list[body_atom_name_index]['name']][0]['attributes'] \
                                  [body_atom_attribute_index].name
            elif attributes_map[attribute_index]['type'] == 'math_expr':
                lhs_body_atom_attribute_index = attributes_map[attribute_index]['lhs_map'][1]
                lhs_body_atom_name_index = attributes_map[attribute_index]['lhs_map'][0]
                rhs_body_atom_attribute_index = attributes_map[attribute_index]['rhs_map'][1]
                rhs_body_atom_name_index = attributes_map[attribute_index]['rhs_map'][0]
                math_op = attributes_map[attribute_index]['math_op']
                select_str += body_atom_alias_list[lhs_body_atom_name_index] + '.' + \
                              relation_def_map[body_atom_list[lhs_body_atom_name_index]['name']][0]['attributes'] \
                                  [lhs_body_atom_attribute_index].name + math_op + \
                              body_atom_alias_list[rhs_body_atom_name_index] + '.' + \
                              relation_def_map[body_atom_list[rhs_body_atom_name_index]['name']][0]['attributes'] \
                                  [rhs_body_atom_attribute_index].name

            select_str += ')'
            if aggregation_map[attribute_index] == 'COUNT_DISTINCT':
                select_str += ')'

        if attributes_type_map[attribute_index] == 'math_expr':
            lhs_body_atom_attribute_index = attributes_map[attribute_index]['lhs_map'][1]
            lhs_body_atom_name_index = attributes_map[attribute_index]['lhs_map'][0]
            rhs_body_atom_attribute_index = attributes_map[attribute_index]['rhs_map'][1]
            rhs_body_atom_name_index = attributes_map[attribute_index]['rhs_map'][0]
            math_op = attributes_map[attribute_index]['math_op']
            select_str += body_atom_alias_list[lhs_body_atom_name_index] + '.' + \
                          relation_def_map[body_atom_list[lhs_body_atom_name_index]['name']][0]['attributes'] \
                              [lhs_body_atom_attribute_index].name + math_op + \
                          body_atom_alias_list[rhs_body_atom_name_index] + '.' + \
                          relation_def_map[body_atom_list[rhs_body_atom_name_index]['name']][0]['attributes'] \
                              [rhs_body_atom_attribute_index].name

        if attributes_type_map[attribute_index] == 'constant':
            select_str += attributes_map[attribute_index]

        select_str += ' as ' + head_relation_attributes[selection_index].name + ', '
        selection_index += 1

    select_str = select_str[:len(select_str) - 2]

    return select_str


def generate_from(body_atom_list, body_atom_alias_list):
    from_str = 'from '

    body_atom_num = len(body_atom_list)
    for atom_index in range(body_atom_num):
        from_str += body_atom_list[atom_index]['name'] + ' ' + body_atom_alias_list[atom_index] + ', '

    from_str = from_str[:len(from_str) - 2]

    return from_str


def generate_from_recursive(body_atom_alias_list, atom_eval_name_list):
    from_strs = []
    from_str = 'from '

    body_atom_num = len(body_atom_alias_list)
    for combinations in atom_eval_name_list:
        cur_recursive_from_str = from_str
        for atom_index in range(body_atom_num):
            cur_recursive_from_str += combinations[atom_index] + ' ' + body_atom_alias_list[atom_index] + ', '
        cur_recursive_from_str = cur_recursive_from_str[:len(cur_recursive_from_str) - 2]
        from_strs.append(cur_recursive_from_str)

    return from_strs


def generate_compare_str(comparison_map, original_body_atom_list, body_atom_alias_list, relation_def_map):
    compare_str = ''

    for atom_index in comparison_map:
        atom_alias = body_atom_alias_list[atom_index]
        atom_name = original_body_atom_list[atom_index]['name']
        atom_attributes = relation_def_map[atom_name][0]['attributes']
        attris_to_compare = comparison_map[atom_index]

        for attri_index in attris_to_compare:
            atom_attribute = atom_attributes[attri_index].name
            comparison_list = attris_to_compare[attri_index]

            for comparison in comparison_list:
                base_side = comparison[0]
                compare_op = comparison[1]
                compare_side_type = comparison[2]
                if compare_side_type == 'num':
                    compare_value = comparison[3]

                    if base_side == 'l':
                        compare_str += atom_alias + '.' + atom_attribute + ' ' + \
                                       compare_op + ' ' + compare_value + \
                                       ' AND '
                    else:
                        compare_str += compare_value + ' ' + \
                                       compare_op + ' ' + atom_alias + '.' + atom_attribute + ' AND '

                elif compare_side_type == 'var':
                    compare_atom_index = comparison[3][0]
                    compare_atom_arg_index = comparison[3][1]
                    compare_atom_alias = body_atom_alias_list[compare_atom_index]
                    compare_atom_name = original_body_atom_list[compare_atom_index]['name']
                    compare_atom_attributes = relation_def_map[compare_atom_name][0]['attributes']
                    compare_atom_attribute = compare_atom_attributes[compare_atom_arg_index].name

                    if base_side == 'l':
                        compare_str += atom_alias + '.' + atom_attribute + ' ' + \
                                       compare_op + ' ' + compare_atom_alias + '.' + compare_atom_attribute + \
                                       ' AND '
                    else:
                        compare_str += compare_atom_alias + '.' + compare_atom_attribute + ' ' + \
                                       compare_op + ' ' + atom_alias + '.' + atom_attribute + \
                                       ' AND '

    compare_str = compare_str[:len(compare_str) - 5]
    return compare_str


def generate_join_str(join_info, original_body_atom_list, body_atom_alias_list, relation_def_map):
    join_str = ''

    arg_relation_map = join_info

    for arg in arg_relation_map:
        prev_atom_arg = None
        join_atom_indices = arg_relation_map[arg]
        for join_atom_index in join_atom_indices:
            cur_atom_name = original_body_atom_list[join_atom_index]['name']
            cur_atom_alias = body_atom_alias_list[join_atom_index]
            cur_atom_attributes = relation_def_map[cur_atom_name][0]['attributes']
            join_args_indices = join_atom_indices[join_atom_index]
            for join_arg_index in join_args_indices:
                attribute_name = cur_atom_attributes[join_arg_index].name
                cur_atom_arg = cur_atom_alias + '.' + attribute_name
                if prev_atom_arg:
                    join_str += prev_atom_arg + ' = ' + cur_atom_arg + ' AND '
                prev_atom_arg = cur_atom_arg

    join_str = join_str[:len(join_str) - 5]

    return join_str


def generate_constant_constraint_str(constant_constraint_map, body, body_atom_alias_list, relation_def_map):
    body_atom_list = body['atoms']
    constant_constraint_str = ''

    for atom_index in constant_constraint_map:
        cur_body_atom = body_atom_list[atom_index]
        body_atom_name = cur_body_atom['name']
        body_atom_alias = body_atom_alias_list[atom_index]
        body_atom_constant_constraints = constant_constraint_map[atom_index]

        for body_atom_arg_index in body_atom_constant_constraints:
            body_atom_arg_name = relation_def_map[body_atom_name][0]['attributes'][body_atom_arg_index].name
            body_atom_arg_type = relation_def_map[body_atom_name][0]['attributes'][body_atom_arg_index].type
            body_atom_arg_constant_constraint = body_atom_constant_constraints[body_atom_arg_index]

            constant_constraint_str += body_atom_alias + '.' + body_atom_arg_name + ' = '
            if body_atom_arg_type == 'int':
                constant_constraint_str += body_atom_arg_constant_constraint
            elif body_atom_arg_type == 'str':
                constant_constraint_str += '\'' + body_atom_arg_constant_constraint + '\''

            constant_constraint_str += ' AND '

    constant_constraint_str = constant_constraint_str[:len(constant_constraint_str) - 5]

    return constant_constraint_str


def generate_negation_str(negation_info, original_body_atom_list, negation_atom_list,
                          body_atom_alias_list, negation_atom_alias_list, relation_def_map):
    negation_map = negation_info[0]
    anti_join_map = negation_info[1]

    negation_str = ''

    for negation_atom_index in negation_map:
        negation_atom = negation_atom_list[negation_atom_index]
        negation_atom_name = negation_atom['name']
        negation_atom_alias = negation_atom_alias_list[negation_atom_index]
        negation_atom_map = negation_map[negation_atom_index]
        negation_atom_constant_map = {}
        negation_atom_variable_map = {}
        if 'constant' in negation_atom_map:
            negation_atom_constant_map = negation_atom_map['constant']
        if 'var' in negation_atom_map:
            negation_atom_variable_map = negation_atom_map['var']
        if len(negation_atom_constant_map) > 0 or len(negation_atom_variable_map) > 0:
            negation_str += 'NOT EXISTS (SELECT * FROM ' + \
                            negation_atom_name + ' ' + negation_atom_alias + \
                            ' WHERE '
        else:
            continue
        for arg_index in negation_atom_constant_map:
            negation_atom_arg_name = relation_def_map[negation_atom_name][0]['attributes'][arg_index].name
            negation_atom_arg_type = relation_def_map[negation_atom_name][0]['attributes'][arg_index].type
            negation_atom_constant_val = negation_atom_constant_map[arg_index]
            negation_str += negation_atom_alias + '.' + negation_atom_arg_name + ' = '
            if negation_atom_arg_type == 'int':
                negation_str += negation_atom_constant_val
            elif negation_atom_arg_type == 'str':
                negation_str += '\'' + 'negation_atom_constant_val' + '\''

            negation_str += ' AND '

        if negation_atom_index in anti_join_map:
            negation_atom_anti_join_map = anti_join_map[negation_atom_index]
            for arg_index in negation_atom_anti_join_map:
                negation_atom_arg_name = relation_def_map[negation_atom_name][0]['attributes'][arg_index].name
                body_atoms = negation_atom_anti_join_map[arg_index]
                for body_atom_index in body_atoms:
                    body_atom_name = original_body_atom_list[body_atom_index]['name']
                    body_atom_alias = body_atom_alias_list[body_atom_index]
                    body_arg_index = body_atoms[body_atom_index]
                    body_atom_arg_name = relation_def_map[body_atom_name][0]['attributes'][body_arg_index].name
                    negation_str += negation_atom_alias + '.' + negation_atom_arg_name + ' = ' + \
                                    body_atom_alias + '.' + body_atom_arg_name

                    negation_str += ' AND '

        negation_str = negation_str[:len(negation_str) - 5] + ')' + ' AND '

    negation_str = negation_str[:len(negation_str) - 5]

    return negation_str


def generate_group_by_str(head_relation_attributes, aggregation_map):
    """
        Args:
            head_relation_attributes: the sorted map containing the attributes info of the rule head
            aggregation_map:{<key, value>}:
                    key - attribute index in the head involving aggregation,
                    value - corresponding aggregation operator
        Return:
            group_by string if there is any aggregation in the query
    """
    group_by_str = 'group by '
    # group_by key-list
    head_relation_attributes_num = len(head_relation_attributes)
    for attri_index in range(head_relation_attributes_num):
        if attri_index not in aggregation_map:
            group_by_str += head_relation_attributes[attri_index].name + ', '

    group_by_str = group_by_str[:len(group_by_str) - 2]

    return group_by_str


def generate_intersect_str(l_table, r_table, aggregation_map, sub_query=False):
    """ Generate SQL string to compute the common tuples (intersection) between the two given tables
    """
    intersection_str = 'select '
    l_table_name = l_table.table_name
    r_table_name = r_table.table_name
    l_table_attributes = list(l_table.attributes.items())
    r_table_attributes = list(r_table.attributes.items())
    l_table_attri_num = len(l_table_attributes)
    r_table_attri_num = len(r_table_attributes)

    if l_table_attri_num != r_table_attri_num:
        raise Exception('generate_intersect_str: Inconsistent attribute number')

    for i in range(l_table_attri_num):
        l_table_attri = l_table_attributes[i]
        r_table_attri = r_table_attributes[i]
        if l_table_attri[0] != r_table_attri[0]:
            raise Exception('generate_intersect_str: Inconsistent attribute names')
        if l_table_attri[1] != r_table_attri[1]:
            raise Exception('generate_intersect_str: Inconsistent attribute types')

        if sub_query:
            attri_name = r_table_attri[0]
            intersection_str += r_table_name + '.' + attri_name
        else:
            attri_name = l_table_attri[0]
            intersection_str += l_table_name + '.' + attri_name

        if i != r_table_attri_num - 1:
            intersection_str += ', '

    if sub_query:
        intersection_str += ' from ' + r_table_name
    else:
        intersection_str += ' from ' + l_table_name + ', ' + r_table_name
    intersection_str += ' where '

    aggregation_op = None
    aggregation_attri_index = -1
    if aggregation_map is not None:
        for attri_index in aggregation_map:
            aggregation_op = aggregation_map[attri_index]
            aggregation_attri_index = attri_index

    for i in range(l_table_attri_num):
        if i != aggregation_attri_index:
            intersection_str += l_table_name + '.' + l_table_attributes[i][0] + ' = ' + \
                                r_table_name + '.' + r_table_attributes[i][0]
        else:
            if aggregation_op == 'MIN':
                compare_op = ' >= '
            elif aggregation_op == 'MAX':
                compare_op = ' <= '
            else:
                raise Exception('Aggregation operator {} is currently not supported in recursive rule evaluation'.
                                format(aggregation_op))
            intersection_str += l_table_name + '.' + l_table_attributes[i][0] + compare_op + \
                                r_table_name + '.' + r_table_attributes[i][0]

        if i != l_table_attri_num - 1:
            intersection_str += ' and '

    return intersection_str


def generate_set_diff_str(l_table, r_table, dest_table, aggregation_map):
    """ Generate SQL string to compute the set difference between the two given tables (anti-join)

    Corresponding mathematical formulation:
        dest_table = dest_table UNION (l_table - r_table)

    Arguments:
            l_table:     as shown in the mathematical formulation
            r_table:     as shown in the mathematical formulation
            dest_table:  the table that the set-difference results between l_table and r_table will be inserted into
            aggregation_map: containing the aggregation info if aggregation is involved
    Returns:
            The SQL string computing the set difference between the two given tables
    """
    set_diff_str = 'insert into ' + dest_table.table_name + \
                   ' select * from ' + l_table.table_name + \
                   ' where not exists (' + \
                   generate_intersect_str(l_table, r_table, aggregation_map, sub_query=True) + ')'

    return set_diff_str
