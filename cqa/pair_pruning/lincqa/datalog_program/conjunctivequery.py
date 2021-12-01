from __future__ import annotations

import copy
import multiprocessing
from typing import List, Set, Dict, Tuple

from datalog_program.datalog_program import Argument, Constant, Variable, PrimaryKey, Atom


class QueryCreationException(Exception):
    """
    Class for handling the exceptions that can occur when creating a datalog_program
    """

    def __init__(self, *args):
        """
        Constructor of an exception of creation of a datalog_program

        Args:
            message: In *args, it contains the message we want to deliver to the user
        """
        self.__message = args[0] if args else None

    def __str__(self) -> str:
        return "QueryCreationException: {}".format(self.__message) if self.__message is not None \
            else "QueryCreationException has been raised"


class ConjunctiveQuery(object):
    """
    ConjunctiveQuery class

    Represents a conjunctive datalog_program

    A datalog_program can not be empty.
    """

    def __init__(self,
                 arguments: List[Argument] = None,
                 free_variables: List[Variable] = None,
                 body_atom_arguments: List[List[Argument]] = None,
                 body_atom_names=None,
                 body_atoms: List[Atom] = None,
                 check_query=True):
        """

        Args:
            arguments: the list of the arguments of the datalog_program
            free_variables: the list of free variables of the datalog_program
            body_atom_arguments: the list of arguments of each body atom
            body_atom_names: the list of names of each body atom
            check_query: True if the datalog_program has to be checked (for example, if the datalog_program is handwritten
                         and the author is not sure having made no errors)
        """

        if body_atom_arguments is not None:
            self.__body_atom_number: int = len(body_atom_arguments)
        else:
            if body_atoms is not None:
                self.__body_atom_number: int = len(body_atoms)
            else:
                raise Exception("body_atom_arguments and body_atoms cannot both be None")

        if self.__body_atom_number < 0:
            raise QueryCreationException("A datalog_program must contain at least one atom")

        # If/else to avoid mistakes from the user
        if arguments is not None:
            self.__arguments: Tuple[Argument] = tuple(arguments)
        else:
            if body_atoms is None:
                raise Exception("arguments and body_atoms cannot both be None")
            self.__arguments = tuple(set([arg for atom in body_atoms for arg in atom.arguments]))

        if body_atom_arguments is not None:
            self.__body_atom_arguments: Tuple[Tuple[Argument, ...], ...] = \
                tuple(tuple(atom_arguments) for atom_arguments in body_atom_arguments)
        else:
            if body_atoms is None:
                raise Exception("body_atom_arguments and body_atoms cannot both be None")
            self.__body_atom_arguments: Tuple[Tuple[Argument, ...]] = tuple([atom.arguments for atom in body_atoms])

        # We limit the maximum different tables/relations to be 26 and the name starts with "R"
        # when body atom names are not specified
        if body_atom_names is None:
            body_atom_names = list()
        if len(body_atom_names) == 0:
            self.__body_atom_names: Tuple[str, ...] = tuple(chr(65 + (17 + i) % 26) \
                                                            for i in range(self.__body_atom_number))
        else:
            self.__body_atom_names: Tuple[str, ...] = body_atom_names

        if body_atoms is not None:
            self.__body_atoms = body_atoms
        else:
            self.__body_atoms = [Atom(self.body_atom_names[i], self.__body_atom_arguments[i]) \
                                 for i in range(self.__body_atom_number)]

        if free_variables is None:
            self.__free_variables = tuple()
        else:
            self.__free_variables = tuple(free_variables)

        check_query = check_query and self.__body_atom_number > 0
        if check_query:
            self.check_query()

    @property
    def free_variables(self):
        return self.__free_variables

    @property
    def arguments(self):
        return self.__arguments

    @property
    def body_atom_number(self):
        return self.__body_atom_number

    @property
    def body_atom_names(self):
        return self.__body_atom_names

    @property
    def body_atom_arguments(self):
        return self.__body_atom_arguments

    @property
    def body_atoms(self):
        return self.__body_atoms

    def is_boolean(self) -> bool:
        return len(self.__free_variables) == 0

    def __str__(self) -> str:
        return "The datalog_program is empty" if self.__body_atom_number == 0 \
            else self._construct_str()

    def __getstate__(self):
        return self.__body_atom_arguments, self.__body_atom_names, self.__body_atom_number, \
               self.__arguments, self.__free_variables, self.__body_atoms

    def __setstate__(self, d):
        self.__body_atom_arguments, self.__body_atom_names, self.__body_atom_number, \
        self.__arguments, self.__free_variables, self.__body_atoms = d

    # TODO: we need to check the purpose of this method later
    def __eq__(self, other) -> bool:
        # Two queries_without_goodfacts are equal if there exist an homomorphism from self to other and an homomorphism from other to self
        if not isinstance(other, ConjunctiveQuery):
            return False

        if self.__body_atom_arguments is None:
            return True if other.body_atom_arguments is None \
                else False

        if not len(self.__body_atom_arguments) == len(other.body_atom_arguments):
            return False

        return self._has_homomorphism(other) and other._has_homomorphism(self)

    # TODO: we need to check the purpose of this method later
    def _has_homomorphism(self, other: ConjunctiveQuery) -> bool:
        class RunHomomorphism(object):
            """
            Class used to parallelize the homomorphism's computations
            """

            def __init__(self, base_q: ConjunctiveQuery, other_q: ConjunctiveQuery):
                self.base = base_q
                self.other = other_q

            def __call__(self, homomorphism):
                self.base._check_homomorphism(self.other, homomorphism)

        # We generate every possible homomorphism and checks if it is valid
        gen = self._generate_homomorphism(self.arguments, other.arguments, {})
        homomorphisms = list(gen)

        # We parallelize the search of a valid homomorphism
        pool = multiprocessing.Pool(8)
        res_list = pool.imap_unordered(RunHomomorphism(self, other), homomorphisms)
        pool.close()

        result = False
        for res in res_list:
            if res:
                pool.terminate()
                result = True
                break
        pool.join()
        return result

    # TODO: we need to check the purpose of this method later
    def _generate_homomorphism(self, self_arguments: List[Argument], other_arguments: List[Argument],
                               homomorphism: Dict[str, str]):
        if len(self_arguments) == 0:
            yield homomorphism
        else:
            self_argument = self_arguments[0]
            remaining_self = self_arguments[1:] if len(self_arguments) > 0 else []
            for j, var_other in enumerate(other_arguments):
                homomorphism[self_argument.name] = var_other.name
                remaining_other = other_arguments[:j]
                if j != len(other_arguments) - 1:
                    other_arguments[j + 1:]
                yield from self._generate_homomorphism(remaining_self, remaining_other, homomorphism)

    # TODO: we need to check the purpose of this method later
    def _check_homomorphism(self, other: ConjunctiveQuery, homomorphism: Dict[str, str]) -> bool:
        tables_other = copy.deepcopy(other.body_atom_arguments)

        for table_self in self.body_atom_arguments:
            table_other: List[Argument] = []
            for var in table_self:
                if isinstance(homomorphism[var.name], Constant) != isinstance(var, Constant):
                    return False
                if isinstance(homomorphism[var.name], PrimaryKey) != isinstance(var, PrimaryKey):
                    return False

                homo_name = homomorphism[var.name]
                if isinstance(var, PrimaryKey) and isinstance(var, Constant):
                    table_other.append(var)
                elif isinstance(var, Constant):
                    table_other.append(Constant(homo_name))
                elif isinstance(var, PrimaryKey):
                    table_other.append(PrimaryKey(homo_name))
                else:
                    table_other.append(Argument(homo_name))

            if table_other not in tables_other:
                return False
            tables_other.remove(table_other)

        return True

    def _construct_str(self) -> str:
        """
        Constructs the string to print when there is at least one table
        """
        head_vars = ', '.join([str(var) for var in self.__free_variables])
        str_repr = "Q({}) :- ".format(head_vars)
        for i, atom in enumerate(self.__body_atoms):
            str_repr += str(atom)
            str_repr += ", " if i + 1 < self.__body_atom_number else ""

        return str_repr

    def check_query(self):
        """ Ensures that the parameters provided for sjf-datalog_program initialization is good to
            make a consistent and FO-rewritable datalog_program
        """
        # TODO: simplify
        if self.__body_atom_number != len(self.__body_atom_arguments) or \
                len(self.__body_atom_names) != len(self.__body_atom_arguments):
            raise QueryCreationException("The body atom number is different from the size of body atom arguments list")

        for arg in self.__arguments:
            if isinstance(arg, Constant):
                for other_arg in [a for atom_arguments in self.__body_atom_arguments for a in atom_arguments]:
                    if arg.name == other_arg.name and not isinstance(other_arg, Constant):
                        if not isinstance(other_arg, PrimaryKey) or not isinstance(other_arg.argument, Constant):
                            raise QueryCreationException("The argument \"{}\" occurs both as a constant "
                                                         "and as a variable".format(arg.name))

        # TODO: remove this trivial constraint
        # The primary keys should be put first for easy readability
        # for atom in self.__body_atom_arguments:
        #     no_more_pk: bool = False
        #     for arg in atom:
        #         if not isinstance(arg, PrimaryKey):
        #             no_more_pk = True
        #         elif no_more_pk and isinstance(arg, PrimaryKey):
        #             raise QueryCreationException(
        #                 "The primary key \"{}\" is found after a non primary key variable".format(arg.name))

        return

    @property
    def variables(self) -> Set[Argument]:
        """
        Gets all variables as instances of Variable.
        """
        s: Set[Argument] = set()
        for x in self.arguments:
            if not isinstance(x, Constant):
                s.add(Variable(x.name))
        return s
