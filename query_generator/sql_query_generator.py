from execution.config import STATIC_DEBUG
from parser.datalog_program import DatalogProgram
from rule_analyzer import translator

"""
Generate the string of SQL query components (e.g., select, from, where) given the query info data structures
"""


def generate_insertion_str(head):
    """
    Generate code to populate some specified fact into the corresponding relation:
        Example: T(0,1) :- (insert tuple (0,1) into the relation 'T')
    """
    relation_name = head["name"]
    arg_list = head["arg_list"]
    attri_num = len(arg_list)
    tuple_instance = list()
    for i in range(attri_num):
        arg = arg_list[i]
        arg_type = arg.type
        if arg_type != "constant":
            raise Exception("Only specific values could be populated into relation!")
        tuple_instance.append(arg.name)

    value_str = ",".join(tuple_instance)
    insertion_str = "INSERT INTO {} VALUES ({})".format(relation_name, value_str)
    return insertion_str


def generate_select(
    head_name, body_atoms, select_map, relation_def_map, body_atom_aliases
):
    head_relation_args = relation_def_map[head_name]["relation"]["attributes"]
    head_arg_to_body_atom_arg_map = select_map["head_arg_to_body_atom_arg_map"]
    head_arg_type_map = select_map["head_arg_type_map"]
    aggregation_map = select_map["head_aggregation_map"]
    head_arg_num = len(head_relation_args)
    select_item_strs = list()
    selection_index = 0
    for arg_index in range(head_arg_num):
        select_item_str = ""
        if head_arg_type_map[arg_index] == "var":
            mapped_atom_index = head_arg_to_body_atom_arg_map[arg_index]["atom_index"]
            mapped_arg_index = head_arg_to_body_atom_arg_map[arg_index]["arg_index"]
            select_item_str = "{}.{}".format(
                body_atom_aliases[mapped_atom_index],
                relation_def_map[body_atoms[mapped_atom_index]["name"]]["relation"][
                    "attributes"
                ][mapped_arg_index].name,
            )
        if head_arg_type_map[arg_index] == "agg":
            aggregate_func_str = ""
            aggregate_func_arg_str = ""
            aggregate_func_str = aggregation_map[arg_index]
            if head_arg_to_body_atom_arg_map[arg_index]["type"] == "attribute":
                mapped_arg_index = head_arg_to_body_atom_arg_map[arg_index]["map"][
                    "atom_index"
                ]
                mapped_atom_index = head_arg_to_body_atom_arg_map[arg_index]["map"][
                    "arg_index"
                ]
                aggregate_func_arg_str = "{}.{}".format(
                    body_atom_aliases[mapped_atom_index],
                    relation_def_map[body_atoms[mapped_atom_index]["name"]]["relation"][
                        "attributes"
                    ][mapped_arg_index].name,
                )
            elif head_arg_to_body_atom_arg_map[arg_index]["type"] == "math_expr":
                # currently only supports binary arithematic operations (e.g., a + b is okay but a + b + c is not)
                lhs_body_atom_index = head_arg_to_body_atom_arg_map[arg_index][
                    "lhs_map"
                ]["atom_index"]
                lhs_body_atom_arg_index = head_arg_to_body_atom_arg_map[arg_index][
                    "lhs_map"
                ]["arg_index"]
                rhs_body_atom_index = head_arg_to_body_atom_arg_map[arg_index][
                    "rhs_map"
                ]["atom_index"]
                rhs_body_atom_arg_index = head_arg_to_body_atom_arg_map[arg_index][
                    "rhs_map"
                ]["arg_index"]
                math_op = head_arg_to_body_atom_arg_map[arg_index]["math_op"]
                aggregate_func_arg_str = "{}.{} {} {}.{}".format(
                    body_atom_aliases[lhs_body_atom_index],
                    relation_def_map[body_atoms[lhs_body_atom_index]["name"]][
                        "relation"
                    ]["attributes"][lhs_body_atom_arg_index].name,
                    math_op,
                    body_atom_aliases[rhs_body_atom_index],
                    relation_def_map[body_atoms[rhs_body_atom_index]["name"]][
                        "relation"
                    ]["attributes"][rhs_body_atom_arg_index].name,
                )

            select_item_str = "{}({})".format(
                aggregate_func_str, aggregate_func_arg_str
            )
            if aggregation_map[arg_index] == "COUNT_DISTINCT":
                select_item_str = "COUNT(DISTINCT({}))".format(aggregate_func_arg_str)

        if head_arg_type_map[arg_index] == "math_expr":
            # currently only supports binary arithematic operations (e.g., a + b is okay but a + b + c is not)
            lhs_body_atom_index = head_arg_to_body_atom_arg_map[arg_index]["lhs_map"][
                "atom_index"
            ]
            lhs_body_atom_arg_index = head_arg_to_body_atom_arg_map[arg_index][
                "lhs_map"
            ]["arg_index"]
            rhs_body_atom_index = head_arg_to_body_atom_arg_map[arg_index]["rhs_map"][
                "atom_index"
            ]
            rhs_body_atom_arg_index = head_arg_to_body_atom_arg_map[arg_index][
                "rhs_map"
            ]["arg_index"]
            math_op = head_arg_to_body_atom_arg_map[arg_index]["math_op"]
            select_item_str = "{}.{} {} {}.{}".format(
                body_atom_aliases[lhs_body_atom_index],
                relation_def_map[body_atoms[lhs_body_atom_index]["name"]]["relation"][
                    "attributes"
                ][lhs_body_atom_arg_index].name,
                math_op,
                body_atom_aliases[rhs_body_atom_index],
                relation_def_map[body_atoms[rhs_body_atom_index]["name"]]["relation"][
                    "attributes"
                ][rhs_body_atom_arg_index].name,
            )

        if head_arg_type_map[arg_index] == "constant":
            select_item_str = head_arg_to_body_atom_arg_map[arg_index]

        select_item_str = "{} AS {}".format(
            select_item_str, head_relation_args[selection_index].name
        )
        select_item_strs.append(select_item_str)
        selection_index += 1

    select_items_str = ", ".join(select_item_strs)
    select_str = "SELECT {}".format(select_items_str)
    return select_str


def generate_from(body_atoms, body_atom_aliases):
    table_alias_pairs = list()
    body_atom_num = len(body_atoms)
    for atom_index in range(body_atom_num):
        table_alias_pairs.append(
            "{} {}".format(
                body_atoms[atom_index]["name"], body_atom_aliases[atom_index]
            )
        )
    table_alias_pairs_str = ", ".join(table_alias_pairs)
    from_str = "FROM {}".format(table_alias_pairs_str)
    return from_str


def generate_from_recursive(atom_eval_name_groups, body_atom_aliases):
    from_strs = list()
    body_atom_num = len(body_atom_aliases)
    for group in atom_eval_name_groups:
        table_alias_pairs = list()
        for atom_index in range(body_atom_num):
            table_alias_pairs.append(
                "{} {}".format(
                    group[atom_index]["alias"], body_atom_aliases[atom_index]
                )
            )
        table_alias_pairs_str = ", ".join(table_alias_pairs)
        from_str = "FROM {}".format(table_alias_pairs_str)
        from_strs.append(from_str)
    return from_strs


def generate_join_str(body_atoms, join_map, relation_def_map, body_atom_aliases):
    join_equality_strs = list()
    for arg in join_map:
        prev_atom_arg = None
        join_atom_arg_indices = join_map[arg]
        for atom_index in join_atom_arg_indices:
            atom_name = body_atoms[atom_index]["name"]
            atom_alias = body_atom_aliases[atom_index]
            atom_attributes = relation_def_map[atom_name]["relation"]["attributes"]
            join_args_indices = join_atom_arg_indices[atom_index]
            for join_arg_index in join_args_indices:
                attribute_name = atom_attributes[join_arg_index].name
                atom_arg = "{}.{}".format(atom_alias, attribute_name)
                if prev_atom_arg:
                    join_equality_str = "{} = {}".format(prev_atom_arg, atom_arg)
                    join_equality_strs.append(join_equality_str)
                prev_atom_arg = atom_arg

    join_str = " AND ".join(join_equality_strs)
    return join_str


def generate_comparison_str(
    body_atoms, comparison_map, relation_def_map, body_atom_aliases
):
    comparison_strs = list()
    for atom_index in comparison_map:
        atom_alias = body_atom_aliases[atom_index]
        atom_name = body_atoms[atom_index]["name"]
        atom_attributes = relation_def_map[atom_name]["relation"]["attributes"]
        attributes_to_compare = comparison_map[atom_index]
        for attribute_index in attributes_to_compare:
            atom_attribute = atom_attributes[attribute_index].name
            comparisons = attributes_to_compare[attribute_index]
            for comparison in comparisons:
                base_variable_side = comparison["base_variable_side"]
                compare_op = comparison["compare_op"]
                other_side_type = comparison["other_side_type"]
                if other_side_type == "num":
                    compare_value = comparison["numerical_value"]
                    if base_variable_side == "l":
                        comparison_str = "{}.{} {} {}".format(
                            atom_alias, atom_attribute, compare_op, compare_value
                        )
                    else:
                        comparison_str = "{} {} {}.{}".format(
                            compare_value, compare_op, atom_alias, atom_attribute
                        )
                    comparison_strs.append(comparison_str)
                elif other_side_type == "var":
                    other_side_atom_index = comparison["other_side_atom_index"]
                    other_side_arg_index = comparison["other_side_arg_index"]
                    other_side_atom_alias = body_atom_aliases[other_side_atom_index]
                    other_side_atom_name = body_atoms[other_side_atom_index]["name"]
                    compare_atom_attributes = relation_def_map[other_side_atom_name][
                        "relation"
                    ]["attributes"]
                    compare_atom_attribute = compare_atom_attributes[
                        other_side_arg_index
                    ].name
                    if base_variable_side == "l":
                        comparison_str = "{}.{} {} {}.{}".format(
                            atom_alias,
                            atom_attribute,
                            compare_op,
                            other_side_atom_alias,
                            compare_atom_attribute,
                        )
                    else:
                        comparison_str = "{}.{} {} {}.{}".format(
                            other_side_atom_alias,
                            compare_atom_attribute,
                            compare_op,
                            atom_alias,
                            atom_attribute,
                        )
                    comparison_strs.append(comparison_str)

    comparison_str = " AND ".join(comparison_strs)
    return comparison_str


def generate_constant_constraint_str(
    body_atoms, constant_constraint_map, relation_def_map, body_atom_aliases
):
    constant_constraint_strs = list()
    for atom_index in constant_constraint_map:
        atom = body_atoms[atom_index]
        atom_name = atom["name"]
        atom_alias = body_atom_aliases[atom_index]
        atom_constant_constraints = constant_constraint_map[atom_index]
        for atom_arg_index in atom_constant_constraints:
            atom_attribute_name = relation_def_map[atom_name]["relation"]["attributes"][
                atom_arg_index
            ].name
            atom_attribute_type = relation_def_map[atom_name]["relation"]["attributes"][
                atom_arg_index
            ].type
            constant_constraint = atom_constant_constraints[atom_arg_index]
            if atom_attribute_type in ["int", "long", "double", "float"]:
                constant_constraint_str = "{}.{} = {}".format(
                    atom_alias, atom_attribute_name, constant_constraint
                )
            elif atom_attribute_type == "str":
                constant_constraint_str = "{}.{} = '{}'".format(
                    atom_alias, atom_attribute_name, constant_constraint
                )
            constant_constraint_strs.append(constant_constraint_str)

    constant_constraint_str = " AND ".join(constant_constraint_strs)
    return constant_constraint_str


def generate_negation_str(
    body_atoms,
    negation_atoms,
    negation_maps,
    relation_def_map,
    body_atom_aliases,
    negation_atom_aliases,
):

    negation_map = negation_maps["negation_map"]
    anti_join_map = negation_maps["anti_join_map"]
    negation_strs = list()
    for negation_atom_index in negation_map:
        negation_atom = negation_atoms[negation_atom_index]
        negation_atom_name = negation_atom["name"]
        negation_atom_alias = negation_atom_aliases[negation_atom_index]
        negation_atom_map = negation_map[negation_atom_index]
        negation_equality_strs = list()
        find_negation_constraint = False
        for arg_index in negation_atom_map:
            if negation_atom_map[arg_index]["arg_type"] == "constant":
                negation_atom_attriubte_name = relation_def_map[negation_atom_name][
                    "relation"
                ]["attributes"][arg_index].name
                negation_atom_attribute_type = relation_def_map[negation_atom_name][
                    "relation"
                ]["attributes"][arg_index].type
                negation_atom_constant_val = negation_atom_map[arg_index]["arg_name"]
                if negation_atom_attribute_type == "int":
                    negation_equality_str = "{}.{} = {}".format(
                        negation_atom_alias,
                        negation_atom_attriubte_name,
                        negation_atom_constant_val,
                    )
                elif negation_atom_attribute_type == "str":
                    negation_equality_str = "{}.{} = '{}'".format(
                        negation_atom_alias,
                        negation_atom_attriubte_name,
                        negation_atom_constant_val,
                    )
                negation_equality_strs.append(negation_equality_str)
                find_negation_constraint = True

        if negation_atom_index in anti_join_map:
            negation_atom_anti_join_map = anti_join_map[negation_atom_index]
            for arg_index in negation_atom_anti_join_map:
                negation_atom_attriubte_name = relation_def_map[negation_atom_name][
                    "relation"
                ]["attributes"][arg_index].name
                mapped_atom_arg_indexes = negation_atom_anti_join_map[arg_index]
                mapped_atom_index = mapped_atom_arg_indexes["atom_index"]
                mapped_arg_index = mapped_atom_arg_indexes["arg_index"]
                atom_name = body_atoms[mapped_atom_index]["name"]
                body_atom_arg_name = relation_def_map[atom_name]["relation"][
                    "attributes"
                ][mapped_arg_index].name
                body_atom_alias = body_atom_aliases[mapped_atom_index]
                negation_equality_str = "{}.{} = {}.{}".format(
                    negation_atom_alias,
                    negation_atom_attriubte_name,
                    body_atom_alias,
                    body_atom_arg_name,
                )
                negation_equality_strs.append(negation_equality_str)
            find_negation_constraint = True

        if find_negation_constraint:
            negation_equalities_str = " AND ".join(negation_equality_strs)
            negation_str = "NOT EXISTS (SELECT * FROM {} {} WHERE {})".format(
                negation_atom_name, negation_atom_alias, negation_equalities_str
            )
        else:
            negation_str = "NOT EXISTS (SELECT * FROM {} {})".format(
                negation_atom_name, negation_atom_alias
            )
        negation_strs.append(negation_str)

    negation_str = " AND ".join(negation_strs)
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
    group_by_attributes_names = list()
    head_relation_attributes_num = len(head_relation_attributes)
    for attribute_index in range(head_relation_attributes_num):
        if attribute_index not in aggregation_map:
            group_by_attributes_names.append(
                head_relation_attributes[attribute_index].name
            )

    group_by_attributes_str = ", ".join(group_by_attributes_names)
    group_by_str = "GROUP BY {}".format(group_by_attributes_str)
    return group_by_str


def generate_unified_idb_evaluation_str(
    idb_table_name,
    sub_query_list,
    with_subquery=False,
    select_into=False,
    distinct=False,
    store_output=True,
):
    union_all_sub_query_str = " UNION ALL ".join(sub_query_list)
    if not with_subquery:
        if not store_output:
            if distinct:
                eval_m_delta_str = "SELECT DISTINCT * FROM ({}) t;".format(
                    union_all_sub_query_str
                )
            else:
                eval_m_delta_str = "SELECT * FROM ({}) t;".format(
                    union_all_sub_query_str
                )
        else:
            if not select_into:
                if distinct:
                    eval_m_delta_str = (
                        "INSERT INTO {} SELECT DISTINCT * FROM ({}) t;".format(
                            idb_table_name, union_all_sub_query_str
                        )
                    )
                else:
                    eval_m_delta_str = "INSERT INTO {} SELECT * FROM ({}) t;".format(
                        idb_table_name, union_all_sub_query_str
                    )
            else:
                if distinct:
                    eval_m_delta_str = "SELECT DISTINCT * INTO {} FROM ({}) t;".format(
                        idb_table_name, union_all_sub_query_str
                    )
                else:
                    eval_m_delta_str = "SELECT * INTO {} FROM ({}) t;".format(
                        idb_table_name, union_all_sub_query_str
                    )
    else:
        if distinct:
            eval_m_delta_str = " {} AS (SELECT DISTINCT * FROM ({}) t)".format(
                idb_table_name, union_all_sub_query_str
            )
        else:
            eval_m_delta_str = " {} AS (SELECT * FROM ({}) t)".format(
                idb_table_name, union_all_sub_query_str
            )

    return eval_m_delta_str


def generate_insertion_evaluation_str(idb_table_name, query):
    evaluation_str = "INSERT INTO {} {};".format(idb_table_name, query)
    return evaluation_str


def generate_intersect_str(l_table, r_table, aggregation_map, sub_query=False):
    """Generate SQL string to compute the common tuples (intersection) between the two given tables"""
    l_table_name = l_table.table_name
    r_table_name = r_table.table_name
    l_table_attributes = list(l_table.attributes.items())
    r_table_attributes = list(r_table.attributes.items())
    l_table_attri_num = len(l_table_attributes)
    r_table_attri_num = len(r_table_attributes)

    if l_table_attri_num != r_table_attri_num:
        raise Exception("generate_intersect_str: Inconsistent attribute number")

    project_attributes = list()
    for i in range(l_table_attri_num):
        l_table_attri = l_table_attributes[i]
        r_table_attri = r_table_attributes[i]
        if l_table_attri[0] != r_table_attri[0]:
            raise Exception("generate_intersect_str: Inconsistent attribute names")
        if l_table_attri[1] != r_table_attri[1]:
            raise Exception("generate_intersect_str: Inconsistent attribute types")

        project_attributes.append("{}.{}".format(r_table_name, r_table_attri[0]))
    project_str = ", ".join(project_attributes)

    aggregation_op = None
    aggregation_attribute_index = -1
    if aggregation_map is not None:
        for attribute_index in aggregation_map:
            aggregation_op = aggregation_map[attribute_index]
            aggregation_attribute_index = attribute_index

    constraint_strs = list()
    for i in range(l_table_attri_num):
        if i != aggregation_attribute_index:
            compare_op = " = "
        else:
            if aggregation_op == "MIN":
                compare_op = " >= "
            elif aggregation_op == "MAX":
                compare_op = " <= "
            else:
                raise Exception(
                    "Aggregation operator {} is currently not supported in recursive rule evaluation".format(
                        aggregation_op
                    )
                )
        constraint_strs.append(
            "{}.{} {} {}.{}".format(
                l_table_name,
                l_table_attributes[i][0],
                compare_op,
                r_table_name,
                r_table_attributes[i][0],
            )
        )
    constraint_str = " AND ".join(constraint_strs)

    if sub_query:
        intersection_str = "SELECT {} FROM {} WHERE {}".format(
            project_str, r_table_name, constraint_str
        )
    else:
        intersection_str = "SELECT {} FROM {}, {} WHERE {}".format(
            project_str, l_table_name, r_table_name, constraint_str
        )

    return intersection_str


def generate_set_diff_str(l_table, r_table, dest_table, aggregation_map):
    """Generate SQL string to compute the set difference between the two given tables (anti-join)

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
    intersect_str = generate_intersect_str(
        l_table, r_table, aggregation_map, sub_query=True
    )
    set_diff_str = "INSERT INTO {} SELECT * FROM {} WHERE NOT EXISTS ({})".format(
        dest_table.table_name, l_table.table_name, intersect_str
    )
    return set_diff_str


def gen_rule_eval_sql_str(
    datalog_rule, relation_def_map, eval_idb_rule_maps, iter_num=0, recursive=False
):
    # map attributes to be projected
    head = datalog_rule["head"]
    head_name = head["name"]
    body = datalog_rule["body"]
    body_atoms = body["atoms"]
    body_comparisons = body["compares"]
    negation_atoms = body["negations"]

    variable_arg_to_atom_map = translator.extract_variable_arg_to_atom_map(body_atoms)
    if STATIC_DEBUG:
        print(
            "\n-----body argument variable to body atom and argument indices map-----\n"
        )
        print(variable_arg_to_atom_map)

    select_maps = translator.extract_selection_map(head, variable_arg_to_atom_map)
    if STATIC_DEBUG:
        print("\n-----select maps-----\n")
        print(select_maps)

    join_map = translator.extract_join_map(variable_arg_to_atom_map)
    if STATIC_DEBUG:
        print("\n-----join map-----\n")
        print(join_map)

    comparison_map = translator.extract_comparison_map(
        body_comparisons, variable_arg_to_atom_map
    )
    if STATIC_DEBUG:
        print("\n-----comparison map-----\n")
        print(comparison_map)

    constant_constraint_map = translator.extract_constant_constraint_map(body_atoms)
    if STATIC_DEBUG:
        print("\n-----constant-constraint map-----\n")
        print(constant_constraint_map)

    negation_maps = translator.extract_negation_map(
        negation_atoms, variable_arg_to_atom_map
    )
    if STATIC_DEBUG:
        print("\n-----negation maps-----\n")
        print(negation_maps)

    body_atom_aliases = translator.build_atom_aliases(body_atoms)
    if STATIC_DEBUG:
        print("\n-----body-atom aliases-----\n")
        print(body_atom_aliases)

    negation_atom_aliases = translator.build_negation_atom_aliases(negation_atoms)
    if STATIC_DEBUG:
        print("\n-----negation-atom aliases-----\n")
        print(negation_atom_aliases)

    if recursive:
        atom_aliases_map = translator.build_recursive_atom_aliases(
            body_atoms, eval_idb_rule_maps, iter_num
        )
        if STATIC_DEBUG:
            print("\n-----atom-aliases map-----\n")
            print(atom_aliases_map)

        atom_eval_name_groups = translator.build_recursive_atom_alias_groups(
            body_atoms, atom_aliases_map
        )
        if STATIC_DEBUG:
            print("\n-----atom-eval-name groups-----\n")
            print(atom_eval_name_groups)

    select_str = generate_select(
        head_name, body_atoms, select_maps, relation_def_map, body_atom_aliases
    )
    if STATIC_DEBUG:
        print("\n-----select string-----\n")
        print(select_str)

    if recursive:
        from_strs = generate_from_recursive(atom_eval_name_groups, body_atom_aliases)
        if STATIC_DEBUG:
            print("\n-----from strings-----\n")
            print(from_strs)
    else:
        from_str = generate_from(body_atoms, body_atom_aliases)
        if STATIC_DEBUG:
            print("\n-----from string-----\n")
            print(from_str)

    join_str = generate_join_str(
        body_atoms, join_map, relation_def_map, body_atom_aliases
    )
    if STATIC_DEBUG:
        print("\n-----join string-----\n")
        print(join_str)

    comparison_str = generate_comparison_str(
        body_atoms, comparison_map, relation_def_map, body_atom_aliases
    )
    if STATIC_DEBUG:
        print("\n-----comparison string-----\n")
        print(comparison_str)

    constant_constraint_str = generate_constant_constraint_str(
        body_atoms, constant_constraint_map, relation_def_map, body_atom_aliases
    )
    if STATIC_DEBUG:
        print("\n------constant-constraint string-----\n")
        print(constant_constraint_str)

    negation_str = generate_negation_str(
        body_atoms,
        negation_atoms,
        negation_maps,
        relation_def_map,
        body_atom_aliases,
        negation_atom_aliases,
    )
    if STATIC_DEBUG:
        print("\n-----negation string-----\n")
        print(negation_str)

    predicate_strs = list()
    where_predicate_str = ""
    if len(join_str) > 0:
        predicate_strs.append(join_str)
    if len(comparison_str) > 0:
        predicate_strs.append(comparison_str)
    if len(constant_constraint_str) > 0:
        predicate_strs.append(constant_constraint_str)
    if len(negation_str) > 0:
        predicate_strs.append(negation_str)
    if len(predicate_strs) > 0:
        all_predicate_str = " AND ".join(predicate_strs)
        where_predicate_str = " WHERE {}".format(all_predicate_str)

    # aggregation (group by)
    aggregation_map = select_maps["head_aggregation_map"]
    group_by_str = ""
    if len(aggregation_map) > 0:
        group_by_str = " {}".format(
            generate_group_by_str(
                relation_def_map[head_name]["relation"]["attributes"], aggregation_map
            )
        )

    if recursive:
        #  one recursive rule could be evaluated by
        # 'multiple sub-queries' (delta & non-delta combination in nonlinear recursive rule)
        recursive_rule_eval_strs = list()
        for from_str in from_strs:
            recursive_rule_eval_strs.append(
                "{} {}{}{}".format(
                    select_str, from_str, where_predicate_str, group_by_str
                )
            )
        if STATIC_DEBUG:
            print("\n-----recursive rule-----")
            print(DatalogProgram.iterate_datalog_rule(datalog_rule))
            print("\n-----recursive-rule evaluation strings-----\n")
            for eval_str in recursive_rule_eval_strs:
                print("{}\n".format(eval_str))
        return recursive_rule_eval_strs, aggregation_map
    else:
        non_recursive_eval_str = "{} {}{}{}".format(
            select_str, from_str, where_predicate_str, group_by_str
        )
        if STATIC_DEBUG:
            print("\n-----non-recursive rule-----\n")
            print(DatalogProgram.iterate_datalog_rule(datalog_rule))
            print("\n-----non-recursive-rule evaluation string-----\n")
            print(non_recursive_eval_str)
        return non_recursive_eval_str
