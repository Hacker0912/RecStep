"""
Translate the given datalog rules into sql query mapping info -
which can later be used to generate the corresponding sql queries
"""
import collections
from copy import deepcopy


def extract_variable_arg_to_atom_map(body_atoms):
    """Extract and return the mapping between
    each body variable argument to the indexes of body atoms containing the argument

    Args:
        body_atoms:
            the list of atoms of the datalog rule body

    Return:
        variable_arg_to_atom_map: {<key, value>}
            key - the name of variable appearing in the argument (e.g., x for T(x); x, y for T(y-1, x))
            value - {<key, value>}
                    key - atom index
                    value - list of dictionaries:
                        for each dictionary, it contains the argument index, type
                            and the arg object if the arg is not a variable (e.g., math expression)
    """
    body_atom_num = len(body_atoms)
    variable_arg_to_atom_map = collections.OrderedDict()
    for atom_index in range(body_atom_num):
        atom = body_atoms[atom_index]
        atom_arg_list = atom["arg_list"]
        atom_arg_num = len(atom_arg_list)
        for arg_index in range(atom_arg_num):
            arg = atom_arg_list[arg_index]
            if arg.type != "variable" and arg.type != "math_expr":
                continue
            variables = list()
            if arg.type == "variable":
                variables.append(arg.object)
            if arg.type == "math_expr":
                if arg.object["lhs"]["type"] == "variable":
                    variables.append(arg.object["lhs"]["value"])
                if arg.object["rhs"]["type"] == "variable":
                    variables.append(arg.object["rhs"]["value"])
            if len(variables) == 0:
                continue
            for variable in variables:
                if variable not in variable_arg_to_atom_map:
                    variable_arg_to_atom_map[variable] = collections.OrderedDict()
                if atom_index not in variable_arg_to_atom_map[variable]:
                    variable_arg_to_atom_map[variable][atom_index] = list()
                arg_dict = {"arg_index": arg_index, "arg_type": arg.type}
                if arg.type == "math_expr":
                    arg_dict["arg_object"] = arg.object
                variable_arg_to_atom_map[variable][atom_index].append(arg_dict)

    return variable_arg_to_atom_map


def search_argument_mapping_in_body_atoms(variable_arg_to_atom_map, var_name):
    """Return the first atom-arg index pair found matching the given variable name

    Args:
        variable_arg_to_atom_map:
            mapping between variable to the args of the body atoms containing the variable
        var_name:
            the variable name
    """
    atom_index_to_arg_index_map = variable_arg_to_atom_map[var_name]
    # return the the indices of the first mapped atom and argument found
    for atom_index in atom_index_to_arg_index_map:
        for arg in atom_index_to_arg_index_map[atom_index]:
            if arg["arg_type"] == "variable":
                return {
                    "type": "attribute", 
                    "atom_index": atom_index, 
                    "arg_index": arg["arg_index"]
                }

    return None


def search_math_expr_arg_mapping_in_body_atoms(variable_arg_to_atom_map, math_expr):
    # empty dict means the arg of the math expression is a constant
    lhs_variable_arg_mapping = dict()
    rhs_variable_arg_mapping = dict()
    if math_expr["lhs"]["type"] == "variable":
        lhs_variable_arg_name = math_expr["lhs"]["value"]
        lhs_variable_arg_mapping = search_argument_mapping_in_body_atoms(
            variable_arg_to_atom_map, lhs_variable_arg_name
        )
    if math_expr["rhs"]["type"] == "variable":
        rhs_variable_arg_name = math_expr["rhs"]["value"]
        rhs_variable_arg_mapping = search_argument_mapping_in_body_atoms(
            variable_arg_to_atom_map, rhs_variable_arg_name
        )

    math_op = math_expr["op"]
    return {
        "type": "math_expr",
        "lhs_map": lhs_variable_arg_mapping,
        "rhs_map": rhs_variable_arg_mapping,
        "math_op": math_op,
    }


def extract_selection_map(variable_arg_to_atom_map, head):
    """Extract and store the information for attributes selected (computed) from the datalog rule/query

    Args:
        variable_arg_to_atom_map:
            mapping between
            each body variable argument to the indexes of body atoms containing the argument 
            and the corresponding arg_index, arg_type and object if arg is not variable (e.g., math expression)
        body_atoms:
            the list of atoms of the datalog rule body
    """
    head_arg_list = head["arg_list"]
    head_arg_num = len(head_arg_list)

    # map arguments of the head to the position of the corresponding arguments in the body
    head_arg_to_body_atom_arg_map = collections.OrderedDict()
    # keep track of the types of head arguments (e.g., variable, aggregation, constants)
    head_arg_types = list()
    # map the aggregation attributes of the head to the specific aggregation operator
    head_aggregation_map = dict()
    for arg_index in range(head_arg_num):
        head_arg = head_arg_list[arg_index]
        if head_arg.type == "variable":
            head_arg_name = head_arg.object
            head_arg_types.append("variable")
            head_arg_to_body_atom_arg_map[
                arg_index
            ] = search_argument_mapping_in_body_atoms(
                variable_arg_to_atom_map, head_arg_name
            )

        elif head_arg.type == "aggregation":
            head_agg_arg = head_arg.object["agg_arg"]
            head_arg_types.append("aggregation")
            head_aggregation_map[arg_index] = head_arg.object["agg_op"]
            if head_agg_arg["type"] == "attribute":
                head_arg_to_body_atom_arg_map[arg_index] = search_argument_mapping_in_body_atoms(
                    variable_arg_to_atom_map, head_agg_arg["content"]
                )
            elif head_agg_arg["type"] == "math_expr":
                math_expr = head_agg_arg["content"]
                head_arg_to_body_atom_arg_map[
                    arg_index
                ] = search_math_expr_arg_mapping_in_body_atoms(
                    variable_arg_to_atom_map, math_expr
                )

        elif head_arg.type == "math_expr":
            head_arg_types.append("math_expr")
            math_expr = head_arg.object
            head_arg_to_body_atom_arg_map[
                arg_index
            ] = search_math_expr_arg_mapping_in_body_atoms(
                variable_arg_to_atom_map, math_expr
            )

        elif head_arg.type == "constant":
            head_arg_types.append("constant")
            head_arg_to_body_atom_arg_map[arg_index] = head_arg.object

    return {
        "head_arg_to_body_atom_arg_map": head_arg_to_body_atom_arg_map,
        "head_arg_type_map": head_arg_types,
        "head_aggregation_map": head_aggregation_map,
    }


def extract_join_map(variable_arg_to_atom_map):
    """
    Args:
        variable_arg_to_atom_map:
            mapping between
            each body variable argument to the indexes of body atoms containing the argument 
            and the corresponding arg_index, arg_type and object if arg is not variable (e.g., math expression)
    Returns:
        join_map:
            mapping beween join variable name to the indices of join atoms and variable positions
    """
    # Conditionally support some more complex join cases - join between variable and mathematical expression
    # https://github.com/Hacker0912/RecStep/issues/5#issue-1566057619
    # e.g., arc(t, _), elem(t-1, j, w)
    join_map = collections.OrderedDict()
    # Iterate grounded variables
    for var in variable_arg_to_atom_map:
        join_on_var = False
        # same variable shown in different atoms - at least one of them has to be *grounded* 
        # (e.g., t is grounded in arc(t, _) but not in elem(t-1, j, w))
        if len(variable_arg_to_atom_map[var]) > 1:
            for atom_index in variable_arg_to_atom_map[var]:
                for arg_dict in variable_arg_to_atom_map[var][atom_index]:
                    if arg_dict["arg_type"] == "variable":
                        join_on_var = True
        if not join_on_var and len(variable_arg_to_atom_map[var]) > 1:
            raise Exception("Join variable {} is not grounded".format(var))             
            
        # same variable shown more than once in the same atom
        if not join_on_var:
            for atom in variable_arg_to_atom_map[var]:
                if len(variable_arg_to_atom_map[var][atom]) > 1:
                    for arg_dict in variable_arg_to_atom_map[var][atom_index]:
                        if arg_dict["arg_type"] == "variable":
                            join_on_var = True
                            break   
                    if not join_on_var and len(variable_arg_to_atom_map[var][atom]) > 1:   
                        raise Exception("Variable {} (inside the same atom) is not grounded".format(var))  
        
        if join_on_var:
            join_map[var] = variable_arg_to_atom_map[var]

    return join_map


def extract_comparison_map(body_comparisons, variable_arg_to_atom_map):
    """Extract and store the information of comparison in the body

    Args:
        body_comparisons:
            the comparison items of the rule body
        body_atoms:
            the list of atoms of the datalog rule body

    Return:
        comparison_map 
    """
    comparison_map = dict()
    for comparison in body_comparisons:
        lhs = comparison["lhs"]
        rhs = comparison["rhs"]
        lhs_arg_type = lhs["type"]
        rhs_arg_type = rhs["type"]
        lhs_arg = lhs["value"]
        rhs_arg = rhs["value"]
        if lhs_arg_type != "variable" and rhs_arg_type != "variable":
            raise Exception(
                "At least one side of the comparison has to be variable in the body relations"
            )

        base_side = "left"
        if lhs_arg_type == "variable":
            base_var = lhs_arg
            compare_arg = rhs_arg
            compare_arg_type = rhs_arg_type
        else:
            base_var = rhs_arg
            compare_arg = lhs_arg
            compare_arg_type = lhs_arg_type
            base_side = "right"

        arg_to_body_atom_arg_map = search_argument_mapping_in_body_atoms(
            variable_arg_to_atom_map, base_var
        )
        mapped_atom_index = arg_to_body_atom_arg_map["atom_index"]
        mapped_arg_index = arg_to_body_atom_arg_map["arg_index"]
        if mapped_atom_index not in comparison_map:
            comparison_map[mapped_atom_index] = dict()
        if mapped_arg_index not in comparison_map[mapped_atom_index]:
            comparison_map[mapped_atom_index][mapped_arg_index] = list()
        comparison_struct = dict()
        comparison_struct["base_variable_side"] = base_side
        comparison_struct["compare_op"] = comparison["op"]
        if compare_arg_type == "number":
            comparison_struct["other_side_type"] = "number"
            comparison_struct["numerical_value"] = float(compare_arg)
        else:
            other_side_atom_arg_indices = search_argument_mapping_in_body_atoms(
                variable_arg_to_atom_map, compare_arg
            )
            comparison_struct["other_side_type"] = "variable"
            comparison_struct["other_side_atom_index"] = other_side_atom_arg_indices[
                "atom_index"
            ]
            comparison_struct["other_side_arg_index"] = other_side_atom_arg_indices[
                "arg_index"
            ]

        comparison_map[mapped_atom_index][mapped_arg_index].append(comparison_struct)

    return comparison_map


def extract_constant_constraint_map(body_atoms):
    """Extract constant specification in the rule body (e.g., T(x,y) :- X(x, 1), Y(x, 2))

    Args:
        body_atoms:
            the list of atoms of the datalog rule body

    Return:
        constant_constraint_map: <key, value>
            key - body_atom_index
            value - <key, value>
                key - body_atom_arg_index
                value - body_atom_arg_constant_specification
    """
    body_atom_num = len(body_atoms)
    constant_constraint_map = dict()
    for atom_index in range(body_atom_num):
        atom = body_atoms[atom_index]
        atom_arg_list = atom["arg_list"]
        atom_arg_num = len(atom_arg_list)
        for arg_index in range(atom_arg_num):
            arg_type = atom_arg_list[arg_index].type
            if arg_type == "constant":
                arg_object = atom_arg_list[arg_index].object
                if atom_index not in constant_constraint_map:
                    constant_constraint_map[atom_index] = dict()
                constant_constraint_map[atom_index][arg_index] = arg_object

    return constant_constraint_map


def extract_negation_map(body_negation_atoms, variable_arg_to_atom_map):
    """Extract the negation in the rule body (e.g., T(x,y) :- A(x,w), B(w,y), !C(x,y).
    negation map can be considered as the "anti-join" map

     Args:
         body_negation_atoms:
             the list of negation atoms of the datalog rule body
         variable_arg_to_atom_map:
             mapping between
             each body variable argument to the indexes of body atoms containing the argument

     Return:
         negation_map
    """
    body_negation_num = len(body_negation_atoms)
    negation_map = dict()
    anti_join_map = dict()
    for negation_index in range(body_negation_num):
        negation = body_negation_atoms[negation_index]
        negation_arg_list = negation["arg_list"]
        negation_arg_num = len(negation_arg_list)
        negation_map[negation_index] = dict()
        negation_atom_map = negation_map[negation_index]
        for arg_index in range(negation_arg_num):
            negation_arg = negation_arg_list[arg_index]
            negation_arg_object = negation_arg.object
            negation_arg_type = negation_arg.type
            negation_atom_map[arg_index] = dict()
            negation_atom_map[arg_index]["arg_object"] = negation_arg_object
            negation_atom_map[arg_index]["arg_type"] = negation_arg_type

    for negation_atom_index in negation_map:
        negation_atom_map = negation_map[negation_atom_index]
        for arg_index in negation_atom_map:
            arg_to_body_atom_arg_map = None
            if negation_atom_map[arg_index]["arg_type"] == "variable":
                arg_name = negation_atom_map[arg_index]["arg_object"]
                arg_to_body_atom_arg_map = search_argument_mapping_in_body_atoms(
                    variable_arg_to_atom_map, arg_name
                )
            if negation_atom_map[arg_index]["arg_type"] == "math_expr":
                math_expr = negation_atom_map[arg_index]["arg_object"]
                arg_to_body_atom_arg_map = search_math_expr_arg_mapping_in_body_atoms(
                    variable_arg_to_atom_map, math_expr
                )
            if arg_to_body_atom_arg_map is None:
                continue
            if negation_atom_index not in anti_join_map:
                anti_join_map[negation_atom_index] = dict()
            anti_join_map[negation_atom_index][arg_index] = arg_to_body_atom_arg_map
            
    return {
        "negation_map": negation_map, 
        "anti_join_map": anti_join_map
    }


def build_atom_aliases(body_atoms):
    """Name each atom in the body with aliases and return the alias list

    Args:
        body_atoms:
            the list of atoms of the rule body
    Return:
        body_atom_alias_list:
            the list of aliases of the body atoms
    """
    body_atom_alias_list = list()
    body_atom_naming_index = 0
    for atom in body_atoms:
        alias = "{}_{}".format(atom["name"].lower(), body_atom_naming_index)
        body_atom_alias_list.append(alias)
        body_atom_naming_index += 1

    return body_atom_alias_list


def build_negation_atom_aliases(negation_atoms):
    """Name each negation atom in the body with aliases and return the alias list

    Args:
        negation_atoms:
            the list of negation atoms of the rule body
    Return:
        negation_atom_alias_list:
            the list of aliases of negation atoms
    """
    negation_atom_aliases = list()
    negation_atom_naming_index = 0
    for negation_atom in negation_atoms:
        alias = "neg_{}_{}".format(
            negation_atom["name"].lower(), negation_atom_naming_index
        )
        negation_atom_aliases.append(alias)
        negation_atom_naming_index += 1

    return negation_atom_aliases


def build_recursive_atom_aliases(body_atoms, eval_idb_to_rule_maps, iter_num):
    """Each idb atom in the body may have different aliases in terms of 'delta', 'prev' tables:
    the function builds the list storing idb aliases (i.e. atom_aliases)
    """
    body_atom_num = len(body_atoms)
    body_atom_eval_names = list()
    idb_num = 0
    for atom in body_atoms:
        if atom["name"] in eval_idb_to_rule_maps:
            idb_num += 1
        atom_alias_list = list()
        body_atom_eval_names.append(atom_alias_list)

    for i in range(body_atom_num):
        atom_name = body_atoms[i]["name"]
        atom_alias_list = body_atom_eval_names[i]
        # idb or edb not evaluated in the current rule group keeps the original name
        if atom_name not in eval_idb_to_rule_maps:
            atom_alias_list.append({"alias": atom_name, "type": "default"})
        else:
            prev_idb_name = "{}_prev".format(atom_name)
            delta_idb_name = "{}_delta_{}".format(atom_name, iter_num - 1)
            atom_alias_list.append({"alias": prev_idb_name, "type": "prev"})
            atom_alias_list.append({"alias": delta_idb_name, "type": "delta"})

    return {
        "body_atom_eval_names": body_atom_eval_names,
        "idb_num": idb_num
    }


def build_recursive_atom_alias_groups(atom_aliases_map):
    """Datalog rules having multiple recursive idbs are evaluated by "multiple" subqueries, each of which
    has different combination of recursive atom aliases (e.g., 'delta', 'prev'):
        The function builds a list of combinations of idb aliases considering a single recursive datalog rule

    Args:
        atom_aliases_map: {
            "body_atom_eval_names": body_atom_eval_names,
            "idb_num": idb_num
        }   - storing list of aliases of atoms and the number of idb atoms of a given Datalog rule
            - body_atom_eval_names: 
            - idb_num: number of 
    Return:
        atom_eval_name_groups:
            list containing groups of idb aliases each of which corresponds to a single recursive datalog rule
    """
    body_atom_eval_names = atom_aliases_map["body_atom_eval_names"]
    idb_num = atom_aliases_map["idb_num"]
    atom_eval_name_groups = list()
    for atom_eval_names in body_atom_eval_names:
        # just start to build the groups (i.e., just start BFS)
        if len(atom_eval_name_groups) == 0:
            for atom_eval_name in atom_eval_names:
                atom_eval_name_groups.append([atom_eval_name])
        else:
            # check to see if the node needs to split (check if the children number is more than 1)
            if len(atom_eval_names) > 1:
                group_increasing_factor = len(atom_eval_names) - 1
                groups_to_be_added = list()
                for i in range(group_increasing_factor):
                    groups_to_be_added.append(deepcopy(atom_eval_name_groups))
                # extend the existing groups with the first eval name of the current IDB to be evaluated
                for group in atom_eval_name_groups:
                    group.append(atom_eval_names[0])
                for k in range(group_increasing_factor):
                    for group in groups_to_be_added[k]:
                        group.append(atom_eval_names[k + 1])
                        atom_eval_name_groups.append(group)
            else:
                for group in atom_eval_name_groups:
                    group.append(atom_eval_names[0])

    # remove the group in which aliases of all recursive atoms are 'prev' (e.g., R_prev) - for semi-naive
    for group in atom_eval_name_groups:
        prev_count = 0
        for alias in group:
            if alias["type"] == "prev":
                prev_count += 1
        if prev_count == idb_num:
            atom_eval_name_groups.remove(group)

    return atom_eval_name_groups
