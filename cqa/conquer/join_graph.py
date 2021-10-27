import networkx as nx
import matplotlib.pyplot as plt
from rule_analyzer import translator


class JoinGraph(object):
    """DEF 3 http://www.cs.toronto.edu/~afuxman/publications/sigmod05.pdf"""

    def __init__(self, rule, variable_arg_to_atom_map, relation_attributes_map):
        self.check_self_join(rule["body"]["atoms"])
        join_map = translator.extract_join_map(variable_arg_to_atom_map)
        print("-----join map-----")
        print(join_map)
        self.key_to_key_join_map = dict()
        self.join_graph = self.construct_join_graph(
            rule["body"]["atoms"], relation_attributes_map, join_map
        )
        print("-----key_to_key_join_map-----:")
        print(self.key_to_key_join_map)
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
        in_degree_map = dict()
        for node in self.join_graph:
            in_degree_map[node] = 0
        for node in self.join_graph:
            for child_node in self.join_graph[node]["children"]:
                in_degree_map[child_node] += 1
        for node in in_degree_map:
            if in_degree_map[node] == 0:
                roots.append(node)
        return roots

    def visualize_join_graph(self):
        g = nx.DiGraph(directed=True)
        for node in self.join_graph:
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

    def construct_join_graph(self, body_atoms, relation_attributes_map, join_map):
        join_graph = dict()
        for var in join_map:
            for atom_index in join_map[var]:
                # name this relation R
                relation_name = body_atoms[atom_index]["name"]
                if len(join_map[var][atom_index]) > 1:
                    for var_index in join_map[var][atom_index]:
                        if not relation_attributes_map[relation_name][
                            var_index
                        ].key_attribute:
                            raise Exception(
                                """Self-loop found when constructing the join graph - 
                                (same non-key variable appears twice in the same atom)"""
                            )
                join_arg_indexes = join_map[var][atom_index]
                # construct edge between relations
                for other_atom_index in join_map[var]:
                    if atom_index == other_atom_index:
                        continue
                    # name this relation S
                    other_relation_name = body_atoms[other_atom_index]["name"]
                    if len(join_map[var][other_atom_index]) > 1:
                        for var_index in join_map[var][other_atom_index]:
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
                                raise Exception(
                                    "Non-key to non-key join found between {} and {}".format(
                                        relation_name, other_relation_name
                                    )
                                )

                            if (not is_arg_index_key) or (not is_other_arg_index_key):
                                if relation_name not in join_graph:
                                    join_graph[relation_name] = dict()
                                    join_graph[relation_name]["children"] = dict()
                                if other_relation_name not in join_graph:
                                    join_graph[other_relation_name] = dict()
                                    join_graph[other_relation_name]["children"] = dict()
                            else:
                                # there will be no edge for key-to-key join
                                if var not in self.key_to_key_join_map:
                                    self.key_to_key_join_map[var] = dict()
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
                                ] = dict()

                            if (
                                var
                                not in join_graph[parent_relation]["children"][
                                    child_relation
                                ]
                            ):
                                join_graph[parent_relation]["children"][child_relation][
                                    var
                                ] = dict()
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
                join_graph[relation_name] = dict()
                join_graph[relation_name]["children"] = dict()
        return join_graph
