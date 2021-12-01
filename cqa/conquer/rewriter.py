from cqa.conquer.join_graph import JoinGraph
from execution.config import *
from rule_analyzer import translator
from collections import OrderedDict


def rewrite(
    edb_decl,
    rule,
    visualize_join_graph=True,
    c_forest_check=True,
    c_forest_merge_evaluation=True,
):
    variable_arg_to_atom_map = translator.extract_variable_arg_to_atom_map(
        rule["body"]["atoms"]
    )
    if STATIC_DEBUG:
        print("\n-----variable arguments to atom mapping-----\n")
        print(variable_arg_to_atom_map)
    selection_map = translator.extract_selection_map(
        rule["head"], variable_arg_to_atom_map
    )
    # here we currently only handle boolean and non-boolean conjunctive queries without aggregation
    selection_type_map = selection_map["head_arg_type_map"]
    selection_map = selection_map["head_arg_to_body_atom_arg_map"]
    selection_variable_map = OrderedDict()
    index = 0
    for projection_pos in selection_map:
        if selection_type_map[projection_pos] == "var":
            selection_variable_map[index] = selection_map[projection_pos]
            index += 1

    if STATIC_DEBUG:
        print("\n-----selection map-----\n")
        print(selection_variable_map)

    constant_constraint_map = translator.extract_constant_constraint_map(
        rule["body"]["atoms"]
    )
    if STATIC_DEBUG:
        print("\n-----constant constraint map-----\n")
        print(constant_constraint_map)

    relation_attributes_map = dict()
    for relation in edb_decl:
        relation_attributes_map[relation["name"]] = relation["attributes"]

    selection_atom_arg_index_pairs = set()
    for projection_pos in selection_variable_map:
        atom_index = selection_variable_map[projection_pos]["atom_index"]
        arg_index = selection_variable_map[projection_pos]["arg_index"]
        selection_atom_arg_index_pairs.add((atom_index, arg_index))

    join_graph = JoinGraph(
        rule,
        selection_atom_arg_index_pairs,
        variable_arg_to_atom_map,
        relation_attributes_map,
        c_forest_check=c_forest_check,
    )
    if STATIC_DEBUG:
        print("\n-----join graph-----\n")
        print(join_graph)
        print("\n----roots of join graph-----\n")
        print(join_graph.get_roots())
    if visualize_join_graph:
        join_graph.visualize_join_graph()

    join_graph_roots = join_graph.get_roots()
    if STATIC_DEBUG:
        for root in join_graph_roots:
            print("\n----root of the tree: {}-----\n".format(root))
            rooted_tree_relations = get_rooted_tree_relations(
                join_graph.join_graph, root
            )
            print("\n-----relations of current rooted join tree-----\n")
            print(rooted_tree_relations)

    if c_forest_merge_evaluation or len(join_graph_roots) == 1:
        print("\n-----Unified Evaluation for C-Forest-----\n")
        consistent_query = rewrite_join(
            rule,
            join_graph.join_graph,
            join_graph.key_to_key_join_map,
            join_graph.get_roots(),
            relation_attributes_map,
            selection_variable_map,
            constant_constraint_map,
        )
    else:
        print("\n-----Decoupled Evaluation for C-Tree in C-Forest-----\n")
        for root in join_graph_roots:
            rooted_tree = join_graph.get_rooted_tree_join_graph(root)
            rooted_tree_key_to_key_join_map = (
                join_graph.get_rooted_tree_key_to_key_join_map(root)
            )
            rooted_tree_selection_map = join_graph.get_rooted_tree_selection_map(
                root, rule["body"]["atoms"], selection_map
            )
            rooted_tree_constant_constraint_map = (
                join_graph.get_rooted_tree_constant_constraint_map(
                    root, rule["body"]["atoms"], constant_constraint_map
                )
            )
            consistent_tree_query = rewrite_join(
                rule,
                rooted_tree,
                rooted_tree_key_to_key_join_map,
                [root],
                relation_attributes_map,
                rooted_tree_selection_map,
                rooted_tree_constant_constraint_map,
            )

    if STATIC_DEBUG:
        print("\n-----consistent query-----\n")
        print(consistent_query)
    return consistent_query


def get_rooted_tree_relations(join_graph, root):
    rooted_tree_relations = set()
    bfs_q = list()
    bfs_q.append(root)
    while len(bfs_q) > 0:
        relation_node = bfs_q.pop()
        rooted_tree_relations.add(relation_node)
        for child_relation in join_graph[relation_node]["children"]:
            bfs_q.append(child_relation)

    return rooted_tree_relations


def rewrite_join(
    rule,
    join_graph,
    key_to_key_join_map,
    roots,
    relation_attributes_map,
    selection_map,
    constant_constraint_map,
    tmp_table_suffix=None,
):
    if tmp_table_suffix is None:
        tmp_table_suffix = "_".join(roots)

    root_relation_keys = OrderedDict()
    for root in roots:
        for attribute in relation_attributes_map[root]:
            if attribute.key_attribute and attribute not in root_relation_keys:
                root_relation_keys[attribute.name] = root

    if STATIC_DEBUG:
        print("\n-----root relation keys-----\n")
        print(root_relation_keys)

    projection_attributes = get_projection_attributes(
        rule["body"]["atoms"],
        selection_map,
        relation_attributes_map,
    )
    if STATIC_DEBUG:
        print("\n-----projection attributes-----\n")
        print(projection_attributes)

    key_to_key_join_predicates = get_key_to_key_join_predicates(key_to_key_join_map)
    if STATIC_DEBUG:
        print("\n-----key-to-key join predicates-----\n")
        print(key_to_key_join_predicates)

    non_key_to_key_join_predicates = get_non_key_to_key_join_predicates(join_graph)
    if STATIC_DEBUG:
        print("\n-----non-key-to-key join predicates-----\n")
        print(non_key_to_key_join_predicates)

    selection_predicates = get_selection_predicates(
        rule["body"]["atoms"],
        constant_constraint_map,
        relation_attributes_map,
        negation=False,
    )
    if STATIC_DEBUG:
        print("\n-----selection predicates-----\n")
        print(selection_predicates)

    negated_selection_predicates = get_selection_predicates(
        rule["body"]["atoms"],
        constant_constraint_map,
        relation_attributes_map,
        negation=True,
    )
    if STATIC_DEBUG:
        print("\n-----negated selection predicates-----\n")
        print(negated_selection_predicates)

    candidate_query = generate_candidate_query(
        join_graph,
        root_relation_keys,
        projection_attributes,
        key_to_key_join_predicates,
        non_key_to_key_join_predicates,
        selection_predicates,
    )
    if STATIC_DEBUG:
        print("\n------candidate query----\n")
        print(candidate_query)

    filter_query = generate_filter_query(
        relation_attributes_map,
        join_graph,
        roots,
        root_relation_keys,
        key_to_key_join_predicates,
        negated_selection_predicates,
        tmp_table_suffix,
    )
    if STATIC_DEBUG:
        print("\n-----filter query-----\n")
        print(filter_query)

    consistent_answer_query = generate_consistent_answer_query(
        projection_attributes, root_relation_keys, tmp_table_suffix
    )
    if STATIC_DEBUG:
        print("\n-----consistent answer query-----\n")
        print(consistent_answer_query)

    consistent_answer_query = "\n\t{}".format(consistent_answer_query)

    conquer_query = (
        "WITH Candidates_{} AS (\n\t{}\n), \n\tFilter_{} AS (\n\t\t{}\n){}".format(
            tmp_table_suffix,
            candidate_query,
            tmp_table_suffix,
            filter_query,
            consistent_answer_query,
        )
    )

    return conquer_query


def generate_candidate_query(
    join_graph,
    root_relation_keys,
    projection_attributes,
    key_to_key_join_predicates,
    non_key_to_key_join_predicates,
    selection_predicates,
):
    # Deduplication between root keys and projected attributes
    projected_attributes = OrderedDict()
    for key_attribute in root_relation_keys:
        projected_attributes[key_attribute] = root_relation_keys[key_attribute]
    for key_attribute in projection_attributes:
        if key_attribute not in projected_attributes:
            projected_attributes[key_attribute] = projection_attributes[key_attribute]

    where_str = ""
    where_predicates = list()
    if len(key_to_key_join_predicates) > 0:
        where_predicates.extend(key_to_key_join_predicates)
    if len(non_key_to_key_join_predicates) > 0:
        where_predicates.extend(non_key_to_key_join_predicates)
    if len(selection_predicates) > 0:
        where_predicates.extend(selection_predicates)
    if len(where_predicates) > 0:
        where_str = " WHERE {}".format(" AND ".join(where_predicates))

    return "SELECT DISTINCT {} FROM {}{}".format(
        ", ".join(
            [
                "{}.{}".format(projected_attributes[attribute], attribute)
                for attribute in projected_attributes
            ]
        ),
        ", ".join([relation for relation in join_graph]),
        where_str,
    )


def generate_filter_query(
    relation_attributes_map,
    join_graph,
    roots,
    root_relation_keys,
    key_to_key_join_predicates,
    negated_selection_predicateds,
    tmp_table_suffix,
):
    left_outer_join_strs = [
        generate_left_outer_join(relation_attributes_map, join_graph, root)
        for root in roots
    ]
    left_outer_join_str = " LEFT OUTER JOIN ".join(
        [loj for loj in left_outer_join_strs if len(loj) > 0]
    )

    where_str = ""
    where_predicates = list()
    negation_predicates = list()
    if len(key_to_key_join_predicates) > 0:
        where_predicates.append(" AND ".join(key_to_key_join_predicates))
    for relation in join_graph:
        negation_predicates.extend(
            [
                "{}.{} IS NULL".format(relation, attribute.name)
                for attribute in relation_attributes_map[relation]
                if attribute.key_attribute and relation not in roots
            ]
        )
    negation_predicates.extend(negated_selection_predicateds)
    if len(negation_predicates) > 0:
        where_predicates.append("({})".format(" OR ".join(negation_predicates)))
    if len(where_predicates) > 0:
        where_str = " WHERE {}".format(" AND ".join(where_predicates))

    inconsistent_filter_strs = list()
    candidate_key_attributes = ", ".join(
        ["C.{}".format(key_attribute) for key_attribute in root_relation_keys]
    )
    inverse_root_key_relation_map = OrderedDict()
    for key_attribute in root_relation_keys:
        root = root_relation_keys[key_attribute]
        if root not in inverse_root_key_relation_map:
            inverse_root_key_relation_map[root] = list()
        inverse_root_key_relation_map[root].append(key_attribute)

    root_key_attribute_join_strs = list()
    for root in inverse_root_key_relation_map:
        root_key_attribute_join_strs.append(
            "JOIN {} ON {}".format(
                root,
                " AND ".join(
                    [
                        "C.{} = {}.{}".format(key_attribute, root, key_attribute)
                        for key_attribute in inverse_root_key_relation_map[root]
                    ]
                ),
            )
        )

    if len(left_outer_join_str) > 0:
        join_graph_filter_str = (
            "SELECT {} FROM Candidates_{} C {} LEFT OUTER JOIN {}{}".format(
                candidate_key_attributes,
                tmp_table_suffix,
                " ".join(root_key_attribute_join_strs),
                left_outer_join_str,
                where_str,
            )
        )
        inconsistent_filter_strs.append(join_graph_filter_str)

    inconsistent_block_filter_str = (
        "SELECT {} FROM Candidates_{} C GROUP BY {} HAVING COUNT(*) > 1".format(
            candidate_key_attributes, tmp_table_suffix, candidate_key_attributes
        )
    )
    inconsistent_filter_strs.append(inconsistent_block_filter_str)
    return " UNION ALL ".join(inconsistent_filter_strs)


def generate_consistent_answer_query(
    projection_attributes,
    root_relation_keys,
    tmp_table_suffix,
    set_semantics=True,
):
    key_join_str = " AND ".join(
        [
            "C.{} = F.{}".format(key_attribute, key_attribute)
            for key_attribute in root_relation_keys
        ]
    )

    projection_attributes_str = "'true'"
    if len(projection_attributes) > 0:
        projection_attributes_str = ", ".join(
            [attribute for attribute in projection_attributes]
        )

    if set_semantics:
        return "SELECT DISTINCT {} FROM Candidates_{} C WHERE NOT EXISTS (SELECT * FROM Filter_{} F WHERE {})".format(
            projection_attributes_str, tmp_table_suffix, tmp_table_suffix, key_join_str
        )
    else:
        return "SELECT DISTINCT {} FROM Candidates_{} C WHERE NOT EXISTS (SELECT * FROM Filter_{} F WHERE {})".format(
            projection_attributes_str, tmp_table_suffix, tmp_table_suffix, key_join_str
        )


def get_projection_attributes(
    body_atoms,
    selection_map,
    relation_attributes_map,
):
    projection_attributes = OrderedDict()
    for projection_pos in selection_map:
        atom_index = selection_map[projection_pos]["atom_index"]
        arg_index = selection_map[projection_pos]["arg_index"]
        relation_name = body_atoms[atom_index]["name"]
        attribute_name = relation_attributes_map[relation_name][arg_index].name
        projection_attributes[attribute_name] = relation_name

    return projection_attributes


def get_key_to_key_join_predicates(key_to_key_join_map):
    key_to_key_joins = list()
    for var in key_to_key_join_map:
        prev_key_attribute = None
        for relation in key_to_key_join_map[var]:
            for attribute in key_to_key_join_map[var][relation]:
                key_attribute = "{}.{}".format(relation, attribute)
                if prev_key_attribute == None:
                    prev_key_attribute = key_attribute
                    continue
                else:
                    key_to_key_joins.append(
                        "{} = {}".format(prev_key_attribute, key_attribute)
                    )
    return key_to_key_joins


def get_non_key_to_key_join_predicates(join_graph):
    non_key_to_key_joins = list()
    for relation in join_graph:
        for child_relation in join_graph[relation]["children"]:
            for var in join_graph[relation]["children"][child_relation]:
                parent_attributes = join_graph[relation]["children"][child_relation][
                    var
                ]["parent_attributes"]
                for parent_attribute in parent_attributes:
                    for child_attribute in join_graph[relation]["children"][
                        child_relation
                    ][var]["child_attributes"]:
                        non_key_to_key_joins.append(
                            "{}.{} = {}.{}".format(
                                relation,
                                parent_attribute,
                                child_relation,
                                child_attribute,
                            )
                        )
    return non_key_to_key_joins


def get_selection_predicates(
    body_atoms, constant_constraint_map, relation_attributes_map, negation=False
):
    predicates = list()
    predicate_operator = "="
    if negation:
        predicate_operator = "!="

    for atom_index in constant_constraint_map:
        relation_name = body_atoms[atom_index]["name"]
        atom_constant_constraints = constant_constraint_map[atom_index]
        for arg_index in atom_constant_constraints:
            attribute_name = relation_attributes_map[relation_name][arg_index].name
            attribute_type = relation_attributes_map[relation_name][arg_index].type
            constant_constraint = atom_constant_constraints[arg_index]
            if attribute_type in ["int", "long", "double", "float"]:
                constant_constraint_str = "{}.{} {} {}".format(
                    relation_name,
                    attribute_name,
                    predicate_operator,
                    constant_constraint,
                )
            elif attribute_type == "str":
                constant_constraint_str = "{}.{} {} '{}'".format(
                    relation_name,
                    attribute_name,
                    predicate_operator,
                    constant_constraint,
                )
            predicates.append(constant_constraint_str)

    return predicates


def generate_left_outer_join(relation_attributes_map, join_graph, root):
    if len(join_graph[root]["children"]) == 0:
        return ""
    parent_children_joins = list()
    for subtree_relation_root in join_graph[root]["children"]:
        cur_subtree_joins = list()
        for join_var in join_graph[root]["children"][subtree_relation_root]:
            root_attributes = join_graph[root]["children"][subtree_relation_root][
                join_var
            ]["parent_attributes"]
            for root_attribute in root_attributes:
                for child_attribute in join_graph[root]["children"][
                    subtree_relation_root
                ][join_var]["child_attributes"]:
                    cur_subtree_joins.append(
                        "{}.{} = {}.{}".format(
                            root, root_attribute, subtree_relation_root, child_attribute
                        )
                    )
        parent_children_joins.append(
            "{} ON {}".format(subtree_relation_root, " AND ".join(cur_subtree_joins))
        )

    cur_root_left_outer_join_str = " LEFT OUTER JOIN ".join(parent_children_joins)
    subtree_roots_left_outer_join_strs = [
        generate_left_outer_join(
            relation_attributes_map, join_graph, subtree_relation_root
        )
        for subtree_relation_root in join_graph[root]["children"]
    ]
    subtree_roots_left_outer_join_strs = " LEFT OUTER JOIN ".join(
        [substr for substr in subtree_roots_left_outer_join_strs if len(substr) > 0]
    )
    if len(subtree_roots_left_outer_join_strs) > 0:
        return "{} LEFT OUTER JOIN {}".format(
            cur_root_left_outer_join_str, subtree_roots_left_outer_join_strs
        )
    else:
        return cur_root_left_outer_join_str
