from typing import List, Set, Tuple, Dict


class Argument(object):
    """ Argument class representing an argument of an atom
    """

    def __init__(self, name: str = "x"):
        """ Constructor of an Argument instance

        Args:
            name: The name of the argument. Each other argument that has the same name is
                  considered as equal. Note: name is case-insensitive
            data_type: The lowest level type of the object
        """
        self.__name = name
        self.__type = type(self)

    def __str__(self) -> str:
        return "{}".format(self.__name)

    def __eq__(self, other) -> bool:
        return isinstance(other, type(self)) and self.__name.upper() == other.__name.upper()

    def __hash__(self):
        return hash(self.__name)

    @property
    def name(self):
        return self.__name

    @property
    def data_type(self):
        return self.__type

    @property
    def asp_str(self) -> str:
        return 'ASP string of argument without specific type (variable/constant) is not defined'


class Any(Argument):
    """ Any class representing a unconstrained argument (_) in an atom
    """

    def __init__(self):
        super(Any, self).__init__("_")
        self.__type = type(self)

    def __str__(self) -> str:
        return "ANY"

    def __eq__(self, other) -> bool:
        return isinstance(other, type(self)) and self.name.lower() == other.name.lower()

    def __hash__(self):
        return super().__hash__()

    @property
    def name(self):
        return self._Argument__name

    @property
    def data_type(self):
        return self.__type

    @property
    def asp_str(self) -> str:
        return "_"


class Constant(Argument):
    """ Constant class representing a constant in an atom
    """

    def __init__(self, name: str = "x"):
        super(Constant, self).__init__(name)
        self.__type = type(self)

    def __str__(self) -> str:
        return "CST({})".format(self.name)

    def __eq__(self, other) -> bool:
        return isinstance(other, type(self)) and self.name.lower() == other.name.lower()

    def __hash__(self):
        return super().__hash__()

    @property
    def name(self):
        return self._Argument__name

    @property
    def data_type(self):
        return self.__type

    @property
    def asp_str(self) -> str:
        return self.name.lower()


class Variable(Argument):
    """ Variable class representing a variable in an atom
    """

    def __init__(self, name: str = "x"):
        super(Variable, self).__init__(name)
        self.__type = type(self)

    def __str__(self) -> str:
        return "VAR({})".format(self.name)

    def __eq__(self, other) -> bool:
        return isinstance(other, type(self)) and self.name.lower() == other.name.lower()

    def __hash__(self):
        return super().__hash__()

    @property
    def name(self):
        return self._Argument__name

    @property
    def data_type(self):
        return self.__type

    @property
    def asp_str(self) -> str:
        return self.name.upper()


class PrimaryKey(Argument):
    """ PrimaryKey class

    Represents an argument of an atom that is at a primary key position in the corresponding relation
    """

    def __init__(self, argument: Argument):
        super(PrimaryKey, self).__init__(argument.name)
        if argument is None:
            raise Exception("Argument passed as primary key cannot be None")
        if not isinstance(argument, Variable) and not isinstance(argument, Constant):
            raise Exception("Primary key needs to be either variable or constant")
        self.__argument = argument
        self.__type = argument.data_type

    def __str__(self) -> str:
        return "PK({})".format(self.__argument)

    def __eq__(self, other) -> bool:
        return isinstance(other, type(self.__argument)) and self.name.lower() == other.name.lower()

    def __hash__(self):
        return super().__hash__()

    @property
    def name(self):
        return self.__argument.name

    @property
    def data_type(self):
        return self.__type

    @property
    def argument(self):
        return self.__argument

    @property
    def asp_str(self) -> str:
        return self.__argument.asp_str


class Atom(object):
    """ An atom in a datalog body
    """

    def __init__(self, name: str, arguments: Tuple[Argument, ...], negated=False):
        self.__name: str = name
        self.__arguments: Tuple[Argument, ...] = arguments
        self.__negated = negated

    def __str__(self) -> str:
        if len(self.__arguments) > 0:
            argument_str = ', '.join(map(str, self.__arguments))
            return "{}({})".format(self.__name, argument_str)
        else:
            return self.__name

    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other) -> bool:
        return isinstance(other, type(self)) and str(other) == str(self)

    @property
    def negated(self):
        return self.__negated

    @property
    def asp_str(self):
        argument_str = ', '.join(map(lambda x: x.asp_str, self.__arguments))
        not_symbol = "not"
        if not self.__negated:
            if len(self.__arguments) > 0:
                return "{}({})".format(self.__name, argument_str)
            else:
                return self.__name
        else:
            if len(self.__arguments) > 0:
                return "{} {}({})".format(not_symbol, self.__name, argument_str)
            else:
                return "{} {}".format(not_symbol, self.__name)

    @property
    def name(self):
        return self.__name

    @property
    def arguments(self) -> Tuple[Argument, ...]:
        return self.__arguments

    @property
    def variables(self) -> Set[Variable]:
        s: Set[Variable] = set()
        for x in self.__arguments:
            if isinstance(x, Variable):
                s.add(x)
            if isinstance(x, PrimaryKey) and x.data_type == Variable:
                s.add(x.argument)

        return s

    @property
    def primary_key_arguments(self) -> Tuple[Argument, ...]:
        return tuple([x.argument for x in self.__arguments if isinstance(x, PrimaryKey)])

    @property
    def non_primary_key_arguments(self) -> Tuple[Argument, ...]:
        return tuple([x for x in self.__arguments if not isinstance(x, PrimaryKey)])


class Constraint(object):

    def __init__(self, constraint_variable: Variable, constraint_condition: Argument):
        self.__constraint_variable = constraint_variable
        self.__constraint_condition = constraint_condition

    @property
    def constraint_tuple(self) -> Tuple[Variable, Argument]:
        return self.__constraint_variable, self.__constraint_condition

    @property
    def asp_str(self) -> str:
        return "{} ? {}".format(self.__constraint_variable.asp_str, self.__constraint_condition.asp_str)


class EqualityConstraint(Constraint):

    def __init__(self, constraint_variable: Variable, constraint_condition: Argument):
        super(EqualityConstraint, self).__init__(constraint_variable, constraint_condition)

    @property
    def asp_str(self) -> str:
        return "{} = {}".format(self._Constraint__constraint_variable.asp_str, self._Constraint__constraint_condition.asp_str)


class InequalityConstraint(Constraint):

    def __init__(self, constraint_variable: Variable, constraint_condition: Argument):
        super(InequalityConstraint, self).__init__(constraint_variable, constraint_condition)

    @property
    def asp_str(self) -> str:
        return "{} != {}".format(self._Constraint__constraint_variable.asp_str, self._Constraint__constraint_condition.asp_str)

class GreaterThanConstraint(Constraint):

    def __init__(self, constraint_variable: Variable, constraint_condition: Argument):
        super(GreaterThanConstraint, self).__init__(constraint_variable, constraint_condition)

    @property
    def asp_str(self) -> str:
        return "{} > {}".format(self._Constraint__constraint_variable.asp_str, self._Constraint__constraint_condition.asp_str)


class LessThanConstraint(Constraint):

    def __init__(self, constraint_variable: Variable, constraint_condition: Argument):
        super(LessThanConstraint, self).__init__(constraint_variable, constraint_condition)

    @property
    def asp_str(self) -> str:
        return "{} < {}".format(self._Constraint__constraint_variable.asp_str, self._Constraint__constraint_condition.asp_str)



class Constraints(object):

    def __init__(self):
        self.__constraints: List[Constraint] = list()
        self.__size = 0

    def add_constraint(self, constraint: Constraint):
        self.__constraints.append(constraint)
        self.__size += 1

    @property
    def constraints(self):
        return self.__constraints

    @property
    def asp_str(self) -> str:
        return ', '.join([c.asp_str for c in self.__constraints])

    @property
    def size(self):
        return self.__size


class Rule(object):

    def __init__(self, head_atom: Atom, body_atoms: Tuple[Atom, ...], constraints: Constraints = None):
        self.__head_atom = head_atom
        self.__body_atoms = body_atoms
        self.__body_constraints = constraints


    def get_head_atom(self):
        return self.__head_atom 

    def get_body_atoms(self):
        return self.__body_atoms


    def __str__(self):
        # empty rule used as "rule block" breakline for better readability
        if self.__head_atom is None and self.__body_atoms is None:
            return ""

        rule_str = "{} :- {}" \
            .format(self.__head_atom.asp_str,
                    ', '.join([atom.asp_str for atom in self.__body_atoms]))

        if self.__body_constraints is not None and self.__body_constraints.size > 0:
            rule_str = "{}, {}.".format(rule_str, self.__body_constraints.asp_str)
        else:
            rule_str = "{}.".format(rule_str)

        return rule_str


class DatalogProgram(object):

    def __init__(self, rules: List[Rule] = None):
        if rules is not None:
            self.__rules = rules
        else:
            self.__rules = list()

    def __str__(self):
        return '\n'.join(map(str, self.__rules))


    def get_rules(self):
        return self.__rules


    def add_rule(self, rule: Rule):
        self.__rules.append(rule)

    def save_to_file(self, file_path='./program.fo'):
        program_file = open(file_path, 'w')
        program_file.write(str(self))
        program_file.write("\n")
        program_file.close()

    def append_to_file(self, file_path='./program.fo'):
        program_file = open(file_path, 'a')
        program_file.write(str(self))
        program_file.write("\n")
        program_file.close()
