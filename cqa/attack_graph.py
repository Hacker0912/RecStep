from typing import List
from collections import defaultdict


def get_atom_arguments(
    atom,
    variable=True,
    variable_name=False,
    key_variables=False,
    non_key_variables=False,
    treat_free_variables_as_constants=False,
    relation_attributes_map=None,
    head_variables=None,
):
    variable_list = list()
    atom_arguments = atom["arg_list"]
    arg_num = len(atom_arguments)

    for i in range(arg_num):
        condition = True
        if variable:
            condition = atom["arg_list"][i].type == "variable" and condition

        if key_variables:
            condition = (
                relation_attributes_map[atom["name"]][i].key_attribute and condition
            )

        if non_key_variables:
            condition = (
                not relation_attributes_map[atom["name"]][i].key_attribute
            ) and condition

        if treat_free_variables_as_constants:
            condition = (atom["arg_list"][i].name not in head_variables) and condition

        if condition:
            if variable_name:
                variable_list.append(atom["arg_list"][i].name)
            else:
                variable_list.append(atom["arg_list"][i])

    return variable_list


class PrimaryKeyConstraint(object):
    def __init__(
        self,
        primary_key_variables,
        non_primary_key_variables,
    ):
        self.__primary_key_variables = primary_key_variables
        self.__non_primary_key_variables = non_primary_key_variables

    @property
    def primary_key_variables(self):
        return self.__primary_key_variables

    @property
    def non_primary_key_variables(self):
        return self.__non_primary_key_variables

    def __str__(self) -> str:
        primary_key_variables_str = "[{}]".format(
            ", ".join(map(str, self.__primary_key_variables))
        )
        non_primary_key_variables_str = "[{}]".format(
            ", ".join(map(str, self.__non_primary_key_variables))
        )
        return "{} -> {}".format(
            primary_key_variables_str, non_primary_key_variables_str
        )


class PrimaryKeyConstraintList(object):
    """
    A list storing primary-key constraints
    """

    def __init__(self):
        self.__constraints: List[PrimaryKeyConstraint] = list()

    def __str__(self) -> str:
        primary_key_constraint_set_str = "\n".join(map(str, self.__constraints))
        return primary_key_constraint_set_str

    def add_from_atom(self, atom, head_variables, relation_attributes_map):
        """
        Construct primary-key functional dependency
                (deduplicate primary-key variables -> deduplicate non-primary-key variables) from the given atom
        """
        primary_key_variables = get_atom_arguments(
            atom,
            key_variables=True,
            variable_name=True,
            treat_free_variables_as_constants=False,
            relation_attributes_map=relation_attributes_map,
            head_variables=head_variables,
        )

        primary_key_arguments = [
            True if arg.key_attribute else False
            for arg in relation_attributes_map[atom["name"]]
        ]

        if sum(primary_key_arguments) > 0:
            non_primary_key_variables = get_atom_arguments(
                atom,
                non_key_variables=True,
                variable_name=True,
                relation_attributes_map=relation_attributes_map,
            )
            self.__constraints.append(
                PrimaryKeyConstraint(primary_key_variables, non_primary_key_variables)
            )

    @property
    def constraints(self):
        return self.__constraints

    def get(self, index):
        return self.__constraints[index]


class AttackGraph(object):
    """
    Attach graph: the main tool for solving the complexity classification task
    of Certainty(q).

    Reference Paper Link:
    https://sigmodrecord.org/publications/sigmodRecord/1603/pdfs/06_Consistent_RH_Koutris.pdf
    """

    def __init__(self, rule, relation_attributes_map, verbose=False):
        self.__atoms = rule["body"]["atoms"]
        self.__head_variables = get_atom_arguments(rule["head"], variable_name=True)
        self.__relation_attributes_map = relation_attributes_map
        self.__primary_key_constraints: PrimaryKeyConstraintList = (
            self.__compute_primary_key_constraints()
        )
        self.__attacks = defaultdict(set)
        # compute attack graph
        self.__compute_attack_graph(verbose=verbose)
        # check if there is a cycle in attack graph
        self.__acyclic = True
        self.__check_attack_graph_cycle()
        self.__atoms_in_topological_order = None

    def __str__(self) -> str:
        s = "\n".join(
            map(
                lambda a: a[0] + " --> " + ", ".join(a[1]),
                self.__attacks.items(),
            )
        )
        return s

    def __compute_primary_key_constraints(self) -> PrimaryKeyConstraintList:
        primary_key_constraints = PrimaryKeyConstraintList()
        for atom in self.__atoms:
            primary_key_constraints.add_from_atom(
                atom, self.__head_variables, self.__relation_attributes_map
            )

        return primary_key_constraints

    def __compute_f_plus_q(self, removed_atom):
        """
        Computes the set F^{+,q} as described in the reference paper
        """
        atom_index = self.__atoms.index(removed_atom)
        atom_num = len(self.__atoms)
        # the bit-map used to keep track of the availability of primary-key constraints of the datalog_program
        primary_key_constraint_bit_map = [0] * atom_num
        primary_key_constraint_bit_map[atom_index] = 1
        atom_primary_variables = get_atom_arguments(
            removed_atom,
            variable_name=True,
            key_variables=True,
            treat_free_variables_as_constants=False,
            relation_attributes_map=self.__relation_attributes_map,
            head_variables=self.__head_variables,
        )
        if len(atom_primary_variables) == 0:
            return
        # initialize F^{+, q}
        f_plus_q = set()
        # partial-key trivially depends on primary-key
        for v in atom_primary_variables:
            f_plus_q.add(v)

        # all primary key arguments of the current atom are constants or free variables - F^{+, q} is empty

        find = True
        # iteratively compute f_plus_q
        while find:
            # if no new variable is found in f_plus_q, the iteration terminates
            find = False
            for i in range(atom_num):
                if primary_key_constraint_bit_map[i] == 1:
                    continue
                cur_primary_key_constraints = self.__primary_key_constraints.get(i)
                cur_primary_key_variables = (
                    cur_primary_key_constraints.primary_key_variables
                )
                cur_non_primary_key_variables = (
                    cur_primary_key_constraints.non_primary_key_variables
                )
                if f_plus_q.issuperset(cur_primary_key_variables):
                    f_plus_q.update(cur_non_primary_key_variables)
                    primary_key_constraint_bit_map[i] = 1
                    find = True

        return f_plus_q

    def __compute_attacks_from(self, f0, verbose=False):
        """
        Computes every attack from the given atom
        """
        f0_plus_q = self.__compute_f_plus_q(f0)
        if verbose:
            print("Compute attacks from: {}".format(f0["name"]))
            print("F+,q: [{}]".format(", ".join([v for v in f0_plus_q])))
        for f1 in self.__atoms:
            if f1["name"] != f0["name"]:
                # for $z$ in $F_0 \cap F_1$, if $z \notin {F_0}^{+, q}$, then $F_0$ attacks $F_1$
                f1_variables = set(get_atom_arguments(f1, variable_name=True))
                f0_variables = set(get_atom_arguments(f0, variable_name=True))
                z_vars = f1_variables.intersection(f0_variables).difference(f0_plus_q)
                if verbose:
                    print("----------")
                    print("Atom: {}".format(f1["name"]))
                    print("z_vars: {}".format([z for z in z_vars]))
                if len(z_vars) != 0:
                    self.__attacks[f0["name"]].add(f1["name"])
                    self.__compute_attacks_from_recursive(
                        f0, f0_plus_q, f1, {f0["name"], f1["name"]}, verbose=verbose
                    )

        if verbose:
            print("\n")

    def __compute_attacks_from_recursive(
        self,
        f0,
        f0_plus_q,
        f1,
        visited,
        verbose=False,
    ):

        # if there is a witness from $F_0$ to $F_1$, and exists $z \in F_1 \cap \F_2$, $z \notin {F_0}^{+, q}$
        # then $F_0$ attacks $F_2$
        for f2 in self.__atoms:
            f2_name = f2["name"]
            if f2_name not in visited:
                f1_variables = set(get_atom_arguments(f1, variable_name=True))
                f2_variables = set(get_atom_arguments(f2, variable_name=True))
                z_vars = f2_variables.intersection(f1_variables).difference(f0_plus_q)
                if verbose:
                    print("Atom: {}".format(f2["name"]))
                    print("z_vars: {}".format([z for z in z_vars]))
                if len(z_vars) != 0:
                    self.__attacks[f0["name"]].add(f2["name"])
                    self.__compute_attacks_from_recursive(
                        f0, f0_plus_q, f2, visited.union({f2["name"]})
                    )

    def __compute_attack_graph(self, verbose=False):
        for atom in self.__atoms:
            self.__compute_attacks_from(atom, verbose=verbose)

    def __check_attack_graph_cycle(self):
        for atom in self.__atoms:
            for attacked_atom in self.__attacks[atom["name"]]:
                if atom["name"] in self.__attacks[attacked_atom]:
                    self.__acyclic = False
                    break
            if not self.__acyclic:
                break

    @property
    def atoms_in_topological_order(self):
        if self.__atoms_in_topological_order is None:
            atom_name_to_atom_mapping = dict()
            # compute the atoms in topological order respects to attack graph
            self.__atoms_in_topological_order = list()
            in_degree_map = dict()
            for atom in self.__atoms:
                in_degree_map[atom["name"]] = 0
                atom_name_to_atom_mapping[atom["name"]] = atom
            for atom in self.__attacks:
                for attacked_atom in self.__attacks[atom]:
                    in_degree_map[attacked_atom] += 1

            queue = list()
            for atom in in_degree_map:
                if in_degree_map[atom] == 0:
                    queue.append(atom)

            while queue:
                atom = queue.pop(0)
                self.__atoms_in_topological_order.append(
                    atom_name_to_atom_mapping[atom]
                )
                for attacked_atom in self.__attacks[atom]:
                    in_degree_map[attacked_atom] -= 1
                    if in_degree_map[attacked_atom] == 0:
                        queue.append(attacked_atom)

        return self.__atoms_in_topological_order

    @property
    def atom_num(self) -> int:
        return len(self.__atoms)

    @property
    def unattacked_atoms(self):
        unattacked = list()
        for atom in self.__atoms:
            is_attacked = False
            for a in self.__atoms:
                if atom["name"] in self.__attacks[a["name"]]:
                    is_attacked = True
                    break

            if not is_attacked:
                unattacked.append(atom)
        return unattacked

    @property
    def is_acyclic(self):
        return self.__acyclic
