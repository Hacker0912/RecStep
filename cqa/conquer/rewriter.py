from cqa.conquer.join_graph import JoinGraph
from execution.config import *
from rule_analyzer import translator
from collections import OrderedDict


def rewrite(edb_decl, rule, visualize_join_graph=True, c_forest_check=True):
    variable_arg_to_atom_map = translator.extract_variable_arg_to_atom_map(
        rule["body"]["atoms"]
    )

    selection_map = translator.extract_selection_map(
        rule["head"], variable_arg_to_atom_map
    )["head_arg_to_body_atom_arg_map"]

    constant_constraint_map = translator.extract_constant_constraint_map(
        rule["body"]["atoms"]
    )

    relation_attributes_map = dict()
    for relation in edb_decl:
        relation_attributes_map[relation["name"]] = relation["attributes"]

    join_graph = JoinGraph(
        rule,
        variable_arg_to_atom_map,
        relation_attributes_map,
        c_forest_check=c_forest_check,
    )
    if STATIC_DEBUG:
        print("\n-----join graph-----")
        print(join_graph)
        print("\n----roots of join graph-----\n")
        print(join_graph.get_roots())
    if visualize_join_graph:
        join_graph.visualize_join_graph()

    multiple_roots = len(join_graph.get_roots()) > 1
    consistent_tree_queries = list()
    for root in join_graph.get_roots():
        if STATIC_DEBUG:
            print("\n----root of the tree: {}-----\n".format(root))
        rooted_tree_relations = get_rooted_tree_relations(join_graph.join_graph, root)
        if STATIC_DEBUG:
            print("\n-----relations of current rooted join tree-----\n")
            print(rooted_tree_relations)

        consistent_tree_query = rewrite_join(
            rule,
            join_graph,
            root,
            rooted_tree_relations,
            relation_attributes_map,
            selection_map,
            constant_constraint_map,
            c_forest=multiple_roots,
        )
        if STATIC_DEBUG:
            print("\n-----consistent query-----\n")
            print(consistent_tree_query)
        consistent_tree_queries.append(consistent_tree_query)

    if multiple_roots:
        return consistent_tree_queries


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


def merge_consistent_tree_quries(consistent_tree_queries):
    print("TODO")


def rewrite_join(
    rule,
    join_graph,
    root,
    rooted_tree_relations,
    relation_attributes_map,
    selection_map,
    constant_constraint_map,
    c_forest=False,
):
    root_relation_keys = [
        attribute.name
        for attribute in relation_attributes_map[root]
        if attribute.key_attribute
    ]
    if STATIC_DEBUG:
        print("\n-----root relation keys-----\n")
        print(root_relation_keys)

    projection_attributes = get_projection_attributes(
        rule["body"]["atoms"],
        rooted_tree_relations,
        selection_map,
        relation_attributes_map,
    )
    if STATIC_DEBUG:
        print("\n-----projection attributes-----\n")
        print(projection_attributes)

    key_to_key_join_predicates = get_key_to_key_join_predicates(
        join_graph.key_to_key_join_map,
        rooted_tree_relations,
    )
    if STATIC_DEBUG:
        print("\n-----key-to-key join predicates-----\n")
        print(key_to_key_join_predicates)

    non_key_to_key_join_predicates = get_non_key_to_key_join_predicates(
        join_graph.join_graph, rooted_tree_relations
    )
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
        root,
        root_relation_keys,
        projection_attributes,
        rooted_tree_relations,
        key_to_key_join_predicates,
        non_key_to_key_join_predicates,
        selection_predicates,
    )
    if STATIC_DEBUG:
        print("\n------candidate query----\n")
        print(candidate_query)

    filter_query = generate_filter_query(
        relation_attributes_map,
        join_graph.join_graph,
        root,
        root_relation_keys,
        key_to_key_join_predicates,
        rooted_tree_relations,
        negated_selection_predicates,
    )
    if STATIC_DEBUG:
        print("\n-----filter query-----\n")
        print(filter_query)

    consistent_answer_query = generate_consistent_answer_query(
        root, projection_attributes, root_relation_keys
    )
    if STATIC_DEBUG:
        print("\n-----consistent answer query-----\n")
        print(consistent_answer_query)

    if c_forest:
        consistent_answer_query = ",\n\tConsistent_{} AS (\n\t{}\n\t)".format(
            root, consistent_answer_query
        )
    else:
        consistent_answer_query = "\n\t{}".format(consistent_answer_query)

    conquer_query = (
        "WITH Candidates_{} AS (\n\t{}\n), \n\tFilter_{} AS (\n\t\t{}\n){}".format(
            root, candidate_query, root, filter_query, consistent_answer_query
        )
    )

    return conquer_query


def generate_candidate_query(
    root,
    root_relation_keys,
    projection_attributes,
    rooted_tree_relations,
    key_to_key_join_predicates,
    non_key_to_key_join_predicates,
    selection_predicates,
):
    # Deduplication between root keys and projected attributes
    projected_attributes = OrderedDict()
    for key in root_relation_keys:
        projected_attributes[key] = root
    for attribute in projection_attributes:
        if attribute not in projected_attributes:
            projected_attributes[attribute] = projection_attributes[attribute]

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
        ", ".join(rooted_tree_relations),
        where_str,
    )


def generate_filter_query(
    relation_attributes_map,
    join_graph,
    root,
    root_relation_keys,
    key_to_key_join_predicates,
    rooted_tree_relations,
    negated_selection_predicateds,
):
    left_outer_join_str = generate_left_outer_join(
        relation_attributes_map, join_graph, root
    )

    where_str = ""
    where_predicates = list()
    negation_predicates = list()
    if len(key_to_key_join_predicates) > 0:
        where_predicates.append(" AND ".join(key_to_key_join_predicates))
    for relation in rooted_tree_relations:
        negation_predicates.extend(
            [
                "{}.{} IS NULL".format(relation, attribute.name)
                for attribute in relation_attributes_map[relation]
                if attribute.key_attribute and relation != root
            ]
        )
    negation_predicates.extend(negated_selection_predicateds)
    if len(negation_predicates) > 0:
        where_predicates.append("({})".format(" OR ".join(negation_predicates)))
    if len(where_predicates) > 0:
        where_str = " WHERE {}".format(" AND ".join(where_predicates))

    inconsistent_filter_strs = list()
    candidate_key_attributes = ", ".join(root_relation_keys)
    if len(left_outer_join_str) > 0:
        join_graph_filter_str = (
            "SELECT {} FROM Candidates_{} C JOIN {} ON {} LEFT OUTER JOIN {}{}".format(
                candidate_key_attributes,
                root,
                root,
                ", ".join(
                    [
                        "C.{} = {}.{}".format(root_key, root, root_key)
                        for root_key in root_relation_keys
                    ]
                ),
                left_outer_join_str,
                where_str,
            )
        )
        inconsistent_filter_strs.append(join_graph_filter_str)

    inconsistent_block_filter_str = (
        "SELECT {} FROM Candidates_{} C GROUP BY {} HAVING COUNT(*) > 1".format(
            candidate_key_attributes, root, candidate_key_attributes
        )
    )
    inconsistent_filter_strs.append(inconsistent_block_filter_str)
    return " UNION ALL ".join(inconsistent_filter_strs)


def generate_consistent_answer_query(
    root, projection_attributes, root_relation_keys, set_semantics=True
):
    key_join_str = ", ".join(
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
            projection_attributes_str, root, root, key_join_str
        )
    else:
        return "SELECT DISTINCT {} FROM Candidates_{} C WHERE NOT EXISTS (SELECT * FROM Filter F WHERE {})".format(
            projection_attributes_str, root, key_join_str
        )


def get_projection_attributes(
    body_atoms,
    rooted_tree_relations,
    selection_map,
    relation_attributes_map,
):
    projection_attributes = OrderedDict()
    for project_index in selection_map:
        atom_index = selection_map[project_index]["atom_index"]
        arg_index = selection_map[project_index]["arg_index"]
        relation_name = body_atoms[atom_index]["name"]
        if relation_name not in rooted_tree_relations:
            continue
        attribute_name = relation_attributes_map[relation_name][arg_index].name
        projection_attributes[attribute_name] = relation_name

    return projection_attributes


def get_key_to_key_join_predicates(key_to_key_join_map, rooted_tree_relations):
    key_to_key_joins = list()
    for var in key_to_key_join_map:
        prev_key_attribute = None
        for relation in key_to_key_join_map[var]:
            if relation not in rooted_tree_relations:
                continue
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


def get_non_key_to_key_join_predicates(join_graph, rooted_tree_relations):
    non_key_to_key_joins = list()
    for relation in join_graph:
        if relation not in rooted_tree_relations:
            continue
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
    # predicate_concat_token = " AND "
    if negation:
        predicate_operator = "!="
        # predicate_concat_token = " OR "

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

    # return predicate_concat_token.join(predicates)
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
