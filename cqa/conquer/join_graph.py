import networkx as nx
import matplotlib.pyplot as plt
from execution.config import STATIC_DEBUG
from rule_analyzer import translator
from collections import OrderedDict


class JoinGraph(object):
    """DEF 3 http://www.cs.toronto.edu/~afuxman/publications/sigmod05.pdf"""

    def __init__(
        self,
        rule,
        selection_atom_arg_index_pairs,
        variable_arg_to_atom_map,
        relation_attributes_map,
        c_forest_check=True,
    ):
        self.check_self_join(rule["body"]["atoms"])
        self.c_forest_check = c_forest_check
        self.cache = dict()
        join_map = translator.extract_join_map(variable_arg_to_atom_map)
        if STATIC_DEBUG:
            print("-----join map-----")
            print(join_map)
        self.key_to_key_join_map = OrderedDict()
        self.join_graph = self.construct_join_graph(
            rule["body"]["atoms"],
            relation_attributes_map,
            selection_atom_arg_index_pairs,
            join_map,
        )
        if STATIC_DEBUG:
            print("-----key_to_key_join_map-----:")
            print(self.key_to_key_join_map)
        if self.c_forest_check:
            self.check_full_key_join(relation_attributes_map)

    def __str__(self):
        return str(self.join_graph)

    def check_full_key_join(self, relation_attributes_map):
        for relation in self.join_graph:
            for child_relation in self.join_graph[relation]["children"]:
                child_join_attributes = set()
                for join_var in self.join_graph[relation]["children"][child_relation]:
                    child_join_attributes.update(
                        self.join_graph[relation]["children"][child_relation][join_var][
                            "child_attributes"
                        ]
                    )
                for attribute in relation_attributes_map[child_relation]:
                    if (
                        attribute.key_attribute
                        and attribute.name not in child_join_attributes
                    ):
                        raise Exception(
                            "non-key to key join is not full - not supported by conquer"
                        )

    def get_roots(self):
        roots = list()
        in_degree_map = OrderedDict()
        for node in self.join_graph:
            in_degree_map[node] = 0
        for node in self.join_graph:
            for child_node in self.join_graph[node]["children"]:
                in_degree_map[child_node] += 1
        for node in in_degree_map:
            if in_degree_map[node] == 0:
                roots.append(node)
        return roots

    def get_rooted_tree_relations(self, root):
        if "rooted_tree_relations" not in self.cache:
            self.cache["rooted_tree_relations"] = dict()
        if root in self.cache["rooted_tree_relations"]:
            return self.cache["rooted_tree_relations"][root]
        rooted_tree_relations = set()
        bfs_q = list()
        bfs_q.append(root)
        while len(bfs_q) > 0:
            relation_node = bfs_q.pop()
            rooted_tree_relations.add(relation_node)
            for child_relation in self.join_graph[relation_node]["children"]:
                bfs_q.append(child_relation)

        self.cache["rooted_tree_relations"][root] = rooted_tree_relations
        return rooted_tree_relations

    def get_rooted_tree_join_graph(self, root):
        rooted_tree_relations = self.get_rooted_tree_relations(root)
        rooted_tree_join_graph = OrderedDict()
        for relation in self.join_graph:
            if relation not in rooted_tree_relations:
                continue
            rooted_tree_join_graph[relation] = self.join_graph[relation]

    def get_rooted_tree_key_to_key_join_map(self, root):
        rooted_tree_relations = self.get_rooted_tree_relations(root)
        rooted_tree_key_to_key_join_map = OrderedDict()
        for join_var in self.key_to_key_join_map:
            rooted_tree_key_to_key_join_map[join_var] = self.key_to_key_join_map[
                join_var
            ]
            keep = False
            relation_count = 0
            for relation in self.key_to_key_join_map[join_var]:
                if relation in rooted_tree_relations:
                    rooted_tree_key_to_key_join_map[join_var][
                        relation
                    ] = self.key_to_key_join_map[join_var][relation]
                    relation_count += 1
                    if len(rooted_tree_key_to_key_join_map[join_var][relation] > 1):
                        keep = True
            if relation_count > 1 or keep:
                continue
            else:
                del rooted_tree_key_to_key_join_map[join_var]
        return rooted_tree_key_to_key_join_map

    def get_rooted_tree_selection_map(self, root, body_atoms, selection_map):
        rooted_tree_relations = self.get_rooted_tree_relations(root)
        rooted_tree_selection_map = OrderedDict()
        index = 0
        for projection_pos in selection_map:
            atom_index = selection_map[projection_pos]["atom_index"]
            relation_name = body_atoms[atom_index]["name"]
            if relation_name in rooted_tree_relations:
                rooted_tree_selection_map[index] = selection_map[projection_pos]
                index += 1
        return rooted_tree_selection_map

    def get_rooted_tree_constant_constraint_map(
        self, root, body_atoms, constant_constraint_map
    ):
        rooted_tree_relations = self.get_rooted_tree_relations(root)
        rooted_tree_constaint_constraint_map = OrderedDict()
        for atom_index in constant_constraint_map:
            relation_name = body_atoms[atom_index]["name"]
            if relation_name in rooted_tree_relations:
                rooted_tree_constaint_constraint_map[
                    atom_index
                ] = constant_constraint_map[atom_index]
        return rooted_tree_constaint_constraint_map

    def visualize_join_graph(self):
        g = nx.DiGraph(directed=True)
        for node in self.join_graph:
            g.add_node(node)
            for child_node in self.join_graph[node]["children"]:
                g.add_edge(node, child_node)
        pos = nx.spring_layout(g)
        nx.draw_networkx_nodes(g, pos)
        nx.draw_networkx_labels(g, pos)
        nx.draw_networkx_edges(g, pos, edge_color="blue", arrows=True)
        plt.show()

    @staticmethod
    def check_self_join(rule_body):
        relation_name_set = set()
        for atom in rule_body:
            if atom["name"] not in relation_name_set:
                relation_name_set.add(atom["name"])
            else:
                raise Exception(
                    "Self-join detected - rewriting supports only self-join-free queries"
                )

    def construct_join_graph(
        self,
        body_atoms,
        relation_attributes_map,
        selection_atom_arg_index_pairs,
        join_map,
    ):
        join_graph = OrderedDict()
        for var in join_map:
            for atom_index in join_map[var]:
                # name this relation R
                relation_name = body_atoms[atom_index]["name"]
                if len(join_map[var][atom_index]) > 1:
                    if var not in self.key_to_key_join_map:
                        self.key_to_key_join_map[var] = OrderedDict()
                    if relation_name not in self.key_to_key_join_map[var]:
                        self.key_to_key_join_map[var][relation_name] = list()
                    for var_index in join_map[var][atom_index]:
                        if self.c_forest_check:
                            if not relation_attributes_map[relation_name][
                                var_index
                            ].key_attribute:
                                raise Exception(
                                    """Self-loop found when constructing the join graph - 
                                    (same non-key variable appears twice in the same atom)"""
                                )
                        self.key_to_key_join_map[var][relation_name].append(
                            relation_attributes_map[relation_name][var_index].name
                        )

                join_arg_indexes = join_map[var][atom_index]
                # construct edge between relations
                for other_atom_index in join_map[var]:
                    if atom_index <= other_atom_index:
                        continue
                    # name this relation S
                    other_relation_name = body_atoms[other_atom_index]["name"]
                    if len(join_map[var][other_atom_index]) > 1:
                        for var_index in join_map[var][other_atom_index]:
                            if self.c_forest_check:
                                # TODO: free variables checking
                                if not relation_attributes_map[relation_name][
                                    var_index
                                ].key_attribute:
                                    raise Exception(
                                        """Self-loop found when constructing the join graph - 
                                        (same non-key variable appears twice in the same atom)"""
                                    )
                    other_atom_join_arg_indexes = join_map[var][other_atom_index]
                    for join_arg_index in join_arg_indexes:
                        for other_join_arg_index in other_atom_join_arg_indexes:
                            is_arg_index_key = relation_attributes_map[relation_name][
                                join_arg_index
                            ].key_attribute
                            is_other_arg_index_key = relation_attributes_map[
                                other_relation_name
                            ][other_join_arg_index].key_attribute
                            if (not is_arg_index_key) and (not is_other_arg_index_key):
                                if self.c_forest_check:
                                    # TODO: free variables checking
                                    raise Exception(
                                        "Non-key to non-key join found between {} and {}".format(
                                            relation_name, other_relation_name
                                        )
                                    )

                            if (not is_arg_index_key) or (not is_other_arg_index_key):
                                # free variables of a query do not introduce arcs to the join graph
                                #  TODO: this part needs more careful thinking
                                if (
                                    atom_index,
                                    join_arg_index,
                                ) in selection_atom_arg_index_pairs or (
                                    other_atom_index,
                                    other_join_arg_index,
                                ) in selection_atom_arg_index_pairs:
                                    print(
                                        "{} and {} join on free variable {}".format(
                                            relation_name, other_relation_name, var
                                        )
                                    )

                                if relation_name not in join_graph:
                                    join_graph[relation_name] = OrderedDict()
                                    join_graph[relation_name][
                                        "children"
                                    ] = OrderedDict()
                                if other_relation_name not in join_graph:
                                    join_graph[other_relation_name] = OrderedDict()
                                    join_graph[other_relation_name][
                                        "children"
                                    ] = OrderedDict()
                            else:
                                # there will be no edge for key-to-key join
                                if var not in self.key_to_key_join_map:
                                    self.key_to_key_join_map[var] = OrderedDict()
                                if relation_name not in self.key_to_key_join_map[var]:
                                    self.key_to_key_join_map[var][
                                        relation_name
                                    ] = list()
                                self.key_to_key_join_map[var][relation_name].append(
                                    relation_attributes_map[relation_name][
                                        join_arg_index
                                    ].name
                                )
                                if (
                                    other_relation_name
                                    not in self.key_to_key_join_map[var]
                                ):
                                    self.key_to_key_join_map[var][
                                        other_relation_name
                                    ] = list()
                                self.key_to_key_join_map[var][
                                    other_relation_name
                                ].append(
                                    relation_attributes_map[other_relation_name][
                                        other_join_arg_index
                                    ].name
                                )
                                continue

                            parent_relation = None
                            child_relation = None
                            parent_arg_indexes = None
                            child_arg_indexes = None
                            # R->S
                            if (not is_arg_index_key) and is_other_arg_index_key:
                                parent_relation = relation_name
                                child_relation = other_relation_name
                                parent_arg_indexes = join_arg_indexes
                                child_arg_indexes = other_atom_join_arg_indexes
                            # S->R
                            if (is_arg_index_key) and (not is_other_arg_index_key):
                                parent_relation = other_relation_name
                                child_relation = relation_name
                                parent_arg_indexes = other_atom_join_arg_indexes
                                child_arg_indexes = join_arg_indexes

                            if (
                                child_relation
                                not in join_graph[parent_relation]["children"]
                            ):
                                join_graph[parent_relation]["children"][
                                    child_relation
                                ] = OrderedDict()

                            if (
                                var
                                not in join_graph[parent_relation]["children"][
                                    child_relation
                                ]
                            ):
                                join_graph[parent_relation]["children"][child_relation][
                                    var
                                ] = OrderedDict()
                                join_graph[parent_relation]["children"][child_relation][
                                    var
                                ]["parent_attributes"] = [
                                    relation_attributes_map[parent_relation][
                                        attribute_index
                                    ].name
                                    for attribute_index in parent_arg_indexes
                                ]
                                join_graph[parent_relation]["children"][child_relation][
                                    var
                                ]["child_attributes"] = [
                                    relation_attributes_map[child_relation][
                                        attribute_index
                                    ].name
                                    for attribute_index in child_arg_indexes
                                ]

        # add singleton relation to the join graph
        for atom in body_atoms:
            relation_name = atom["name"]
            if relation_name not in join_graph:
                join_graph[relation_name] = OrderedDict()
                join_graph[relation_name]["children"] = OrderedDict()
        return join_graph
