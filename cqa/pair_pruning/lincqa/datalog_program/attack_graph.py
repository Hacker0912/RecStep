from typing import List, Set, Dict, Tuple
from collections import defaultdict

import datalog_program.conjunctivequery as query
import datalog_program.datalog_program


class PrimaryKeyConstraint(object):

    def __init__(self, primary_key_variables: Tuple[datalog_program.datalog_program.Variable],
                 non_primary_key_variables: Tuple[datalog_program.datalog_program.Variable]):
        self.__primary_key_variables = primary_key_variables
        self.__non_primary_key_variables = non_primary_key_variables

    @property
    def primary_key_variables(self):
        return self.__primary_key_variables

    @property
    def non_primary_key_variables(self):
        return self.__non_primary_key_variables

    def __str__(self) -> str:
        primary_key_variables_str = "[{}]".format(', '.join(map(str, self.__primary_key_variables)))
        non_primary_key_variables_str = "[{}]".format(', '.join(map(str, self.__primary_key_variables)))
        return "{} -> {}".format(primary_key_variables_str, non_primary_key_variables_str)


class PrimaryKeyConstraintList(object):
    """
    A list storing primary-key constraints
    """

    def __init__(self):
        self.__constraints: List[PrimaryKeyConstraint] = list()

    def __str__(self) -> str:
        primary_key_constraint_set_str = '\n'.join(map(str, self.__constraints))
        return primary_key_constraint_set_str

    def add_from_atom(self, atom: datalog_program.datalog_program.Atom):
        """
        Construct primary-key functional dependency
                (deduplicate primary-key variables -> deduplicate non-primary-key variables) from the given atom
        """
        primary_key_arguments = atom.primary_key_arguments
        if len(primary_key_arguments) > 0:
            non_primary_key_arguments = atom.non_primary_key_arguments
            primary_key_variables = \
                tuple(set(
                    filter(lambda a: isinstance(a, datalog_program.datalog_program.Variable), primary_key_arguments)))
            non_primary_key_variables = \
                tuple(set(filter(lambda a: isinstance(a, datalog_program.datalog_program.Variable),
                                 non_primary_key_arguments)))

            self.__constraints.append(PrimaryKeyConstraint(primary_key_variables, non_primary_key_variables))

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

    def __init__(self, q: query.ConjunctiveQuery, verbose=False):
        self.__query = q
        self.__atoms: List[datalog_program.datalog_program.Atom] = q.body_atoms
        self.__primary_key_constraints: PrimaryKeyConstraintList = self.__compute_primary_key_constraints()
        self.__attacks: Dict[
            datalog_program.datalog_program.Atom, Set[datalog_program.datalog_program.Atom]] = defaultdict(set)
        # compute attack graph
        self.__compute_attack_graph(verbose=verbose)
        # check if there is a cycle in attack graph
        self.__acyclic = True
        self.__check_attack_graph_cycle()
        self.__atoms_in_topological_order = None

    def __str__(self) -> str:
        s = '\n'.join(
            map(lambda a: a[0].name + " --> " + ", ".join(map(lambda x: x.name, a[1])), self.__attacks.items()))
        return s

    def __compute_primary_key_constraints(self) -> PrimaryKeyConstraintList:
        primary_key_constraints = PrimaryKeyConstraintList()
        for atom in self.__atoms:
            primary_key_constraints.add_from_atom(atom)

        return primary_key_constraints

    def __compute_f_plus_q(self, removed_atom: datalog_program.datalog_program.Atom) -> Set[
        datalog_program.datalog_program.Argument]:
        """
        Computes the set F^{+,q} as described in the reference paper
        """
        atom_index = self.__atoms.index(removed_atom)
        atom_num = len(self.__atoms)
        # the bit-map used to keep track of the availability of primary-key constraints of the datalog_program
        primary_key_constraint_bit_map = [0] * atom_num
        primary_key_constraint_bit_map[atom_index] = 1

        atom_primary_arguments = removed_atom.primary_key_arguments
        atom_primary_variables = \
            tuple(
                set(filter(lambda a: isinstance(a, datalog_program.datalog_program.Variable), atom_primary_arguments)))

        # initialize F^{+, q}
        f_plus_q: Set[datalog_program.datalog_program.Argument] = set()
        # partial-key trivially depends on primary-key
        for v in atom_primary_variables:
            f_plus_q.add(v)

        find = True
        # iteratively compute f_plus_q
        while find:
            # if no new variable is found in f_plus_q, the iteration terminates
            find = False
            for i in range(atom_num):
                if primary_key_constraint_bit_map[i] == 1:
                    continue
                cur_primary_key_constraints = self.__primary_key_constraints.get(i)
                cur_primary_key_variables = cur_primary_key_constraints.primary_key_variables
                cur_non_primary_key_variables = cur_primary_key_constraints.non_primary_key_variables
                if f_plus_q.issuperset(cur_primary_key_variables):
                    f_plus_q.update(cur_non_primary_key_variables)
                    primary_key_constraint_bit_map[i] = 1
                    find = True

        return f_plus_q


    def __compute_f_plus_q_square(self, removed_atom: datalog_program.datalog_program.Atom) -> Set[
        datalog_program.datalog_program.Argument]:
        """
        Computes the set F^{+,q} as described in the reference paper
        """
        atom_index = self.__atoms.index(removed_atom)
        atom_num = len(self.__atoms)
        # the bit-map used to keep track of the availability of primary-key constraints of the datalog_program
        primary_key_constraint_bit_map = [0] * atom_num
        # primary_key_constraint_bit_map[atom_index] = 1

        atom_primary_arguments = removed_atom.primary_key_arguments
        atom_primary_variables = \
            tuple(
                set(filter(lambda a: isinstance(a, datalog_program.datalog_program.Variable), atom_primary_arguments)))

        # initialize F^{+, q}
        f_plus_q: Set[datalog_program.datalog_program.Argument] = set()
        # partial-key trivially depends on primary-key
        for v in atom_primary_variables:
            f_plus_q.add(v)

        find = True
        # iteratively compute f_plus_q
        while find:
            # if no new variable is found in f_plus_q, the iteration terminates
            find = False
            for i in range(atom_num):
                if primary_key_constraint_bit_map[i] == 1:
                    continue
                cur_primary_key_constraints = self.__primary_key_constraints.get(i)
                cur_primary_key_variables = cur_primary_key_constraints.primary_key_variables
                cur_non_primary_key_variables = cur_primary_key_constraints.non_primary_key_variables
                if f_plus_q.issuperset(cur_primary_key_variables):
                    f_plus_q.update(cur_non_primary_key_variables)
                    primary_key_constraint_bit_map[i] = 1
                    find = True

        return f_plus_q



    def is_strong_attack(self, atom1, atom2):
        if not self.attack(atom1, atom2):
            return False

        closure = self.__compute_f_plus_q_square(atom1)
        closure_names = [v.name for v in closure]

        for key_var in atom2.arguments:
            if key_var.data_type == datalog_program.datalog_program.Variable and isinstance(key_var, datalog_program.datalog_program.PrimaryKey):
                if key_var.name not in closure_names:
                    return True

        return False 


    def __compute_attacks_from(self, f0: datalog_program.datalog_program.Atom, verbose=False):
        """
        Computes every attack from the given atom
        """
        f0_plus_q = self.__compute_f_plus_q(f0)
        if verbose:
            print("Compute attacks from: {}".format(f0))
            print("F+,q: [{}]".format(', '.join([a.name for a in f0_plus_q])))
        for f1 in self.__atoms:
            if f1 != f0:
                # for $z$ in $F_0 \cap F_1$, if $z \notin {F_0}^{+, q}$, then $F_0$ attacks $F_1$
                z_vars = f1.variables.intersection(f0.variables).difference(f0_plus_q)
                if verbose:
                    print("----------")
                    print("Atom: {}".format(f1))
                    print("z_vars: {}".format([z.name for z in z_vars]))
                if len(z_vars) != 0:
                    self.__attacks[f0].add(f1)
                    self.__compute_attacks_from_recursive(f0, f0_plus_q, f1, {f0, f1}, verbose=verbose)

        if verbose:
            print("\n")

    def __compute_attacks_from_recursive(self, f0: datalog_program.datalog_program.Atom, f0_plus_q: Set[
        datalog_program.datalog_program.Argument], f1: datalog_program.datalog_program.Atom,
                                         visited: Set[datalog_program.datalog_program.Atom], verbose=False):

        # if there is a witness from $F_0$ to $F_1$, and exists $z \in F_1 \cap \F_2$, $z \notin {F_0}^{+, q}$
        # then $F_0$ attacks $F_2$
        for f2 in self.__atoms:
            if f2 not in visited:
                z_vars = f2.variables.intersection(f1.variables).difference(f0_plus_q)
                if verbose:
                    print("Atom: {}".format(f2))
                    print("z_vars: {}".format([z.name for z in z_vars]))
                if len(z_vars) != 0:
                    self.__attacks[f0].add(f2)
                    self.__compute_attacks_from_recursive(f0, f0_plus_q, f2, visited.union({f2}))

    def __compute_attack_graph(self, verbose=False):
        for atom in self.__atoms:
            self.__compute_attacks_from(atom, verbose=verbose)

    def __check_attack_graph_cycle(self):
        for atom in self.__atoms:
            for attacked_atom in self.__attacks[atom]:
                if atom in self.__attacks[attacked_atom]:
                    self.__acyclic = False
                    break
            if not self.__acyclic:
                break

    @property
    def atoms_in_topological_order(self):
        if self.__atoms_in_topological_order is None:
            # compute the atoms in topological order respects to attack graph
            self.__atoms_in_topological_order = list()
            in_degree_map = dict()
            for atom in self.__atoms:
                in_degree_map[atom] = 0
            for atom in self.__attacks:
                for attacked_atom in self.__attacks[atom]:
                    in_degree_map[attacked_atom] += 1

            queue = list()
            for atom in in_degree_map:
                if in_degree_map[atom] == 0:
                    queue.append(atom)

            while queue:
                atom = queue.pop(0)
                self.__atoms_in_topological_order.append(atom)
                for attacked_atom in self.__attacks[atom]:
                    in_degree_map[attacked_atom] -= 1
                    if in_degree_map[attacked_atom] == 0:
                        queue.append(attacked_atom)

        return self.__atoms_in_topological_order

    @property
    def atom_num(self) -> int:
        return len(self.__atoms)

    @property
    def unattacked_atoms(self) -> List[datalog_program.datalog_program.Atom]:
        unattacked: List[datalog_program.datalog_program.Atom] = []
        for atom in self.__atoms:
            is_attacked = False
            for a in self.__atoms:
                if atom in self.__attacks[a]:
                    is_attacked = True
                    break

            if not is_attacked:
                unattacked.append(atom)
        return unattacked

    @property
    def is_acyclic(self):
        return self.__acyclic


    def attack(self, atom1, atom2):
        if atom2 in self.__attacks[atom1]:
            return True
        return False


    def has_strong_attack(self):

        for atom1 in self.__atoms:
            for atom2 in self.__atoms:
                if self.attack(atom1, atom2) and self.is_strong_attack(atom1, atom2):
                    return True
        return False
