from cqa.conquer.join_graph import JoinGraph
from rule_analyzer import translator


def rewrite(edb_decl, rule, visualize_join_graph=False):
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

    join_graph = JoinGraph(rule, variable_arg_to_atom_map, relation_attributes_map)
    print("\n-----join graph-----")
    print(join_graph)
    print("\n----roots of join graph-----\n")
    print(join_graph.get_roots())
    if visualize_join_graph:
        join_graph.visualize_join_graph()

    for root in join_graph.get_roots():
        print("\n----root of the tree: {}-----\n".format(root))
        rooted_tree_relations = get_rooted_tree_relations(join_graph.join_graph, root)
        print("\n-----relations of current rooted join tree-----\n")
        print(rooted_tree_relations)
        rewrite_join(
            rule,
            join_graph,
            root,
            rooted_tree_relations,
            relation_attributes_map,
            selection_map,
            constant_constraint_map,
        )


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
    root,
    rooted_tree_relations,
    relation_attributes_map,
    selection_map,
    constant_constraint_map,
):
    root_relation_keys = [
        "{}".format(attribute.name)
        for attribute in relation_attributes_map[root]
        if attribute.key_attribute
    ]
    root_relation_key_str = ", ".join(
        ["{}.{}".format(root, key) for key in root_relation_keys]
    )
    candidate_key_str = ", ".join(["{}".format(key) for key in root_relation_keys])
    print("\n-----root relation key attributes string-----\n")
    print(root_relation_key_str)

    projection_attributes = get_projection_attributes(
        rule["body"]["atoms"],
        rooted_tree_relations,
        selection_map,
        relation_attributes_map,
        with_relation_name=True,
    )
    projection_attributes_str = ", ".join(projection_attributes)
    print("\n-----projection attributes string-----\n")
    print(projection_attributes_str)

    candidate_relations_str = ", ".join(rooted_tree_relations)

    key_to_key_join_predicates = get_key_to_key_join_predicates(
        join_graph.key_to_key_join_map,
        rooted_tree_relations,
    )
    print("\n-----key-to-key join predicates-----\n")
    print(key_to_key_join_predicates)
    key_to_key_join_predicates_str = " AND ".join(key_to_key_join_predicates)

    non_key_to_key_join_predicates = get_non_key_to_key_join_predicates(
        join_graph.join_graph, rooted_tree_relations
    )
    print("\n-----non-key-to-key join predicates-----\n")
    print(non_key_to_key_join_predicates)

    join_predicates = list()
    join_predicates.extend(key_to_key_join_predicates)
    join_predicates.extend(non_key_to_key_join_predicates)
    join_predicates_str = " AND ".join(join_predicates)
    print("\n-----join predicates string-----\n")
    print(join_predicates_str)

    selection_predicates_str = get_selection_predicates(
        rule["body"]["atoms"], constant_constraint_map, relation_attributes_map
    )
    print("\n-----selection predicates string-----\n")
    print(selection_predicates_str)

    relation_key_attributes_null_strs = list()
    for relation in rooted_tree_relations:
        relation_key_attributes_null_strs.extend(
            [
                "{}.{} IS NULL".format(relation, attribute.name)
                for attribute in relation_attributes_map[relation]
                if attribute.key_attribute and relation != root
            ]
        )
    relation_key_attributes_null_str = " OR ".join(relation_key_attributes_null_strs)
    print("\n-----relation key attributes null str-----\n")
    print(relation_key_attributes_null_str)

    candidate_query = generate_candidate_query(
        root_relation_key_str,
        projection_attributes_str,
        candidate_relations_str,
        join_predicates_str,
        selection_predicates_str,
    )
    print("\n------candidate query----\n")
    print(candidate_query)

    filter_query = generate_filter_query(
        relation_attributes_map,
        join_graph.join_graph,
        root,
        key_to_key_join_predicates_str,
        relation_key_attributes_null_str,
        root_relation_key_str,
        candidate_key_str,
        root_relation_keys,
    )
    print("\n-----filter query-----\n")
    print(filter_query)

    projection_attributes = get_projection_attributes(
        rule["body"]["atoms"],
        rooted_tree_relations,
        selection_map,
        relation_attributes_map,
        with_relation_name=False,
    )
    projection_attributes_str = ", ".join(projection_attributes)
    consistent_answer_query = generate_consistent_answer_query(
        projection_attributes_str, root_relation_keys
    )
    print("\n-----consistent answer query-----\n")
    print(consistent_answer_query)

    conquer_query = """WITH Candidates AS (
        {}
    ),
    Filter AS (
        {}
    )
    {}
    """.format(
        candidate_query, filter_query, consistent_answer_query
    )
    print("\n-----conquer query-----\n")
    print(conquer_query)


def generate_candidate_query(
    root_relation_key_str,
    projection_attributes_str,
    candidate_relations_str,
    join_predicate_str,
    selection_predicates_str,
):
    where_str = ""
    where_predicates = list()
    if len(join_predicate_str) > 0:
        where_predicates.append(join_predicate_str)
    if len(selection_predicates_str) > 0:
        where_predicates.append(selection_predicates_str)
    return "SELECT DISTINCT {}, {} FROM {}{}".format(
        root_relation_key_str,
        projection_attributes_str,
        candidate_relations_str,
        join_predicate_str,
        selection_predicates_str,
    )


def generate_filter_query(
    relation_attributes_map,
    join_graph,
    root,
    key_to_key_join_predicates_str,
    relation_key_attributes_null_str,
    root_relation_key_str,
    candidate_key_str,
    root_relation_keys,
):
    left_outer_join_str = generate_left_outer_join(
        relation_attributes_map, join_graph, root
    )

    key_to_key_and_relation_key_attributes_null = list()
    if len(key_to_key_join_predicates_str) > 0:
        key_to_key_and_relation_key_attributes_null.append(
            key_to_key_join_predicates_str
        )

    if len(relation_key_attributes_null_str) > 0:
        key_to_key_and_relation_key_attributes_null.append(
            "({})".format(relation_key_attributes_null_str)
        )

    key_to_key_and_relation_key_attributes_null_str = " AND ".join(
        key_to_key_and_relation_key_attributes_null
    )
    join_graph_filter_str = (
        "SELECT {} FROM Candidates C JOIN {} ON {} LEFT OUTER JOIN {} WHERE {}".format(
            root_relation_key_str,
            root,
            ", ".join(
                [
                    "C.{} = {}.{}".format(root_key, root, root_key)
                    for root_key in root_relation_keys
                ]
            ),
            left_outer_join_str,
            key_to_key_and_relation_key_attributes_null_str,
        )
    )

    inconsistent_block_filter_str = (
        "SELECT {} FROM Candidates C GROUP BY {} HAVING COUNT(*) > 1".format(
            candidate_key_str, candidate_key_str
        )
    )
    return "{} UNION ALL {}".format(
        join_graph_filter_str, inconsistent_block_filter_str
    )


def generate_consistent_answer_query(
    projection_attributes_str, root_relation_keys, set_semantics=True
):
    key_join_str = ", ".join(
        [
            "C.{} = F.{}".format(key_attribute, key_attribute)
            for key_attribute in root_relation_keys
        ]
    )
    if projection_attributes_str == "'true'":
        return "SELECT DISTINCT {} FROM Candidates C WHERE NOT EXISTS (SELECT * FROM Filter F WHERE {})".format(
            projection_attributes_str, key_join_str
        )

    if set_semantics:
        return "SELECT DISTINCT {} FROM Candidates C WHERE NOT EXISTS (SELECT * FROM Filter F WHERE {})".format(
            projection_attributes_str, key_join_str
        )
    else:
        return "SELECT DISTINCT {} FROM Candidates C WHERE NOT EXISTS (SELECT * FROM Filter F WHERE {})".format(
            projection_attributes_str, key_join_str
        )


def get_projection_attributes(
    body_atoms,
    rooted_tree_relations,
    selection_map,
    relation_attributes_map,
    with_relation_name=True,
):
    projection_attributes = list()
    for project_index in selection_map:
        atom_index = selection_map[project_index]["atom_index"]
        arg_index = selection_map[project_index]["arg_index"]
        relation_name = body_atoms[atom_index]["name"]
        if relation_name not in rooted_tree_relations:
            continue
        attribute_name = relation_attributes_map[relation_name][arg_index].name
        if with_relation_name:
            projection_attributes.append("{}.{}".format(relation_name, attribute_name))
        else:
            projection_attributes.append(attribute_name)

    if len(projection_attributes) == 0:
        projection_attributes.append("'true'")

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


def generate_left_outer_join(relation_attributes_map, join_graph, root):
    if len(join_graph[root]["children"]) == 0:
        return
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
        [substr for substr in subtree_roots_left_outer_join_strs if substr is not None]
    )
    if len(subtree_roots_left_outer_join_strs) > 0:
        return "{} LEFT OUTER JOIN {}".format(
            cur_root_left_outer_join_str, subtree_roots_left_outer_join_strs
        )
    else:
        return cur_root_left_outer_join_str


def get_selection_predicates(
    body_atoms, constant_constraint_map, relation_attributes_map, negation=False
):
    predicates = list()
    predicate_operator = "="
    predicate_concat_token = " AND "
    if negation:
        predicate_operator = "!="
        predicate_concat_token = " OR "

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

    return predicate_concat_token.join(predicates)
