from typing import OrderedDict
from cqa.attack_graph import *
from parser.datalog_program import DatalogProgram
from parser.datalog_parser import DatalogParser
from query_generator.sql_query_generator import *
from rule_analyzer import translator
from execution.config import *


def rewrite(edb_decl, rule, verbose=False):
    relation_attributes_map = dict()
    for relation in edb_decl:
        relation_attributes_map[relation["name"]] = relation["attributes"]
    attack_graph = AttackGraph(rule, relation_attributes_map, verbose=False)
    verbose = False
    if verbose:
        print("\n-----attach graph-----\n")
        print(attack_graph)

    if not attack_graph.is_acyclic:
        raise Exception("Cycle in attack graph detected - not fo-rewritable")

    if verbose:
        print("\n-----Generating FO-Rewriting in Datalog Program-----")
        datalog_rules, idbs = generate_fastfo_rewriting(
            rule,
            attack_graph,
            relation_attributes_map,
            output_datalog=True,
            verbose=verbose,
        )
        print("\n-----Output Program-----\n")
        print("EDB_DECL: ")
        DatalogProgram.iterate_datalog_edb_idb_decl(edb_decl)
        print("\n")
        print("IDB_DECL: ")
        for idb in idbs:
            attributes_str = ", ".join(
                ["a{} int".format(i) for i in range(len(idb["arg_list"]))]
            )
            print("{}({})".format(idb["name"], attributes_str))
        print("\n")
        print("RULE_DECL:")
        for r in datalog_rules:
            print(DatalogProgram.iterate_datalog_rule(r))

    if verbose:
        print("\n-----Generating FO-Rewriting as SQL Statements-----")

    sql_query = generate_fastfo_rewriting(
        rule,
        attack_graph,
        relation_attributes_map,
        edb_decl=edb_decl,
        output_datalog=False,
        verbose=verbose,
    )
    print(sql_query)


def construct_idb_decl(relation_name, attribute_num):
    attributes = [
        DatalogParser.AtomArg("a{}".format(i), "int") for i in range(attribute_num)
    ]
    return {"name": relation_name, "attributes": attributes}


def deduplicate_arguments(argument_list):
    argument_set = set()
    argument_names_set = set()
    for arg in argument_list:
        if arg.object not in argument_names_set:
            argument_set.add(arg)
            argument_names_set.add(arg.object)

    return argument_set


def generate_argument_set(r_i, q_i, relation_attributes_map):
    """$q_{i}(\vec{x}) :- r_{i}(\vec{u}, \vec{w}), ... r_{n}(\vec{u_n}, \vec{w_n})$
        $r_{i}(\vec{u}, \vec{w})$
        $\vec{u} = u_1, ... ,u_k$
        $\vec{w} = w_1, ... ,w_l$
        $\vec{v} = v_{1}, ... v_{l}$
        $\forall i \in \{1, ..., l\}, \text{ if } w_i \notin \vec{x, u}w_{1}w_{2}...w_{i-1}, then $v_i = w_i$,
         otherwise, $v_i$ is a fresh variable itself

    Args:
        r_i: the current atom being considered in the attack graph
             (top-down following topological order of attack graph)
        q_i: the subquery as described above

    Returns:
        the dictionary containing the argument set ($\vec{x}, \vec{w}, \vec{v}$) as described above
    """
    vec_x = get_atom_arguments(q_i["head"])
    vec_u = get_atom_arguments(
        r_i, key_variables=True, relation_attributes_map=relation_attributes_map
    )
    vec_w = get_atom_arguments(
        r_i,
        variable=False,
        non_key_variables=True,
        relation_attributes_map=relation_attributes_map,
    )
    vec_x_u = vec_x + vec_u
    vec_x_u_w = vec_x + vec_u + vec_w
    vec_x_u_w_names = [v.name for v in vec_x_u_w]
    vec_w_len = len(vec_w)
    vec_x_u_len = len(vec_x_u)
    vec_v = list()
    # C=(\vec{x}, \vec{u}, \vec{v})
    vec_v_constraints = list()
    for i in range(vec_w_len):
        if (
            vec_w[i].name not in vec_x_u_w_names[: vec_x_u_len + i]
            and vec_w[i].type == "variable"
        ):
            vec_v.append(vec_w[i])
        else:
            v_i = DatalogParser.AtomArg("v_{}".format(i + 1), "variable")
            vec_v.append(v_i)
            vec_v_constraints.append((v_i, vec_w[i]))

    return {
        "vec_x": vec_x,
        "vec_u": vec_u,
        "vec_w": vec_w,
        "vec_v": vec_v,
        "vec_v_map": vec_v_constraints,
    }


def get_global_query_key_free_join_variables(
    rule, relation_attributes_map, topological_sequence
):
    v_free = get_atom_arguments(rule["head"])
    key_variables = list()
    sub_query_free_variables = list()
    body_atom_num = len(rule["body"]["atoms"])
    for atom_index in range(body_atom_num):
        relation_name = rule["body"]["atoms"][atom_index]["name"]
        atom_arguments = rule["body"]["atoms"][atom_index]["arg_list"]
        arg_index = 0
        for arg in atom_arguments:
            if (
                relation_attributes_map[relation_name][arg_index].key_attribute
                and arg.type == "variable"
            ):
                key_variables.append(arg)
            arg_index += 1

    # add free variables in subquery
    cur_query = rule
    atom_num = len(topological_sequence)
    for i in range(atom_num):
        cur_atom = topological_sequence[i]
        argument_set = generate_argument_set(
            cur_atom, cur_query, relation_attributes_map
        )
        if i < atom_num - 1:
            subquery = generate_new_query_with_atom_being_removed(
                cur_query, cur_atom, argument_set
            )
            sub_query_free_variables.extend(subquery["head"]["arg_list"])
            cur_query = subquery

    return sorted(
        list(deduplicate_arguments(key_variables + v_free + sub_query_free_variables)),
        key=lambda arg: arg.object,
    )


def generate_global_safe_range_rule(
    rule, relation_attributes_map, topological_sequence
):
    free_variables = get_global_query_key_free_join_variables(
        rule, relation_attributes_map, topological_sequence
    )
    head_atom = {"name": "sfr", "arg_list": free_variables}
    return {"head": head_atom, "body": rule["body"]}


def get_corrected_safe_range_atom(
    full_safe_range_atom, cur_query, cur_atom, relation_attributes_map
):
    cur_query_free_variable_names = [
        arg.object for arg in get_atom_arguments(cur_query["head"])
    ]
    cur_atom_key_variable_names = [
        arg.object
        for arg in get_atom_arguments(
            cur_atom,
            key_variables=True,
            relation_attributes_map=relation_attributes_map,
        )
    ]
    new_arg_list = list()
    for arg in full_safe_range_atom["arg_list"]:
        if (
            arg.object in cur_query_free_variable_names
            or arg.object in cur_atom_key_variable_names
        ):
            if arg not in new_arg_list:
                new_arg_list.append(arg)
        else:
            new_arg_list.append(DatalogParser.AtomArg("_", "any"))

    return {"name": full_safe_range_atom["name"], "arg_list": new_arg_list}


def generate_new_query_with_atom_being_removed(rule, atom, argument_set):
    atoms_to_keep = list(
        filter(lambda a: a["name"] != atom["name"], rule["body"]["atoms"])
    )
    free_variables = list()
    vec_xuw = argument_set["vec_x"] + argument_set["vec_u"] + argument_set["vec_w"]
    vec_xuw_names = set([arg.object for arg in vec_xuw])
    for a in atoms_to_keep:
        for arg in a["arg_list"]:
            if arg.type == "variable" and arg.object in vec_xuw_names:
                free_variables.append(arg)

    # Ensure the outputs from different runs are the same
    free_variables.sort(key=lambda x: x.name)

    return {
        "head": {"name": "q", "arg_list": free_variables},
        "body": {
            "atoms": atoms_to_keep,
            "compares": [],
            "assigns": [],
            "negations": [],
        },
    }


def generate_bad_block_rule(r_i, argument_set, safe_range_atom):
    variable_list = sorted(
        list(deduplicate_arguments(argument_set["vec_x"] + argument_set["vec_u"])),
        key=lambda v: v.name,
    )
    head_atom = {"name": "bb_{}".format(r_i["name"]), "arg_list": variable_list}

    body_atoms = list()
    body_atoms.append(safe_range_atom)

    # current atom
    arguments = list()
    key_argument_names = [arg.object for arg in argument_set["vec_u"]]
    v_index = 0
    for arg in r_i["arg_list"]:
        # key arguments
        if arg.object in key_argument_names:
            arguments.append(arg)
        else:
            # in argument_set["vec_v"]
            arguments.append(argument_set["vec_v"][v_index])
            v_index += 1
    body_atoms.append({"name": r_i["name"], "arg_list": arguments})

    # negated good facts
    variable_list = sorted(
        list(
            deduplicate_arguments(
                argument_set["vec_x"] + argument_set["vec_u"] + argument_set["vec_v"]
            )
        ),
        key=lambda v: v.name,
    )
    return {
        "head": head_atom,
        "body": {
            "atoms": body_atoms,
            "compares": [],
            "assigns": [],
            "negations": [
                {"name": "gf_{}".format(r_i["name"]), "arg_list": variable_list}
            ],
        },
    }


def generate_good_fact_rule(
    r_i,
    argument_set,
    safe_range_atom,
    yes_atom=None,
    equality_constraint=False,
):
    if equality_constraint:
        head_atom_arguments = sorted(
            list(
                deduplicate_arguments(
                    argument_set["vec_x"]
                    + argument_set["vec_u"]
                    + argument_set["vec_v"]
                )
            ),
            key=lambda v: v.name,
        )
    else:
        head_atom_arguments = sorted(
            list(
                deduplicate_arguments(
                    argument_set["vec_x"]
                    + argument_set["vec_u"]
                    + argument_set["vec_w"]
                )
            ),
            key=lambda v: v.name,
        )
    head_atom = {"name": "gf_{}".format(r_i["name"]), "arg_list": head_atom_arguments}

    body_atoms = list()
    body_atoms.append(safe_range_atom)
    compares = list()
    if equality_constraint:
        arguments = list()
        key_argument_names = [arg.object for arg in argument_set["vec_u"]]
        v_index = 0
        for arg in r_i["arg_list"]:
            # key arguments
            if arg.object in key_argument_names:
                arguments.append(arg)
            else:
                # in argument_set["vec_v"]
                arguments.append(argument_set["vec_v"][v_index])
                v_index += 1
        r_i_atom_arguments = arguments

        for vw in argument_set["vec_v_map"]:
            if vw[1].type == "variable":
                rhs_type = "var"
            else:
                rhs_type = "num"
            compares.append(
                {"lhs": [vw[0].name, "var"], "op": "=", "rhs": [vw[1].name, rhs_type]}
            )
    else:
        r_i_atom_arguments = r_i["arg_list"]
    body_atoms.append({"name": r_i["name"], "arg_list": r_i_atom_arguments})
    if yes_atom is not None:
        body_atoms.append(yes_atom)

    return {
        "head": head_atom,
        "body": {
            "atoms": body_atoms,
            "compares": compares,
            "assigns": [],
            "negations": [],
        },
    }


def generate_yes_rule(
    r_i,
    q_i,
    argument_set,
    safe_range_atom,
    badblock=True,
):
    """$yes_{i}(\vec{X}) :- saferange_{i}(\vec{X}), r_i{\vec{u}, \vec{w}}, not badblock_{i}(\vec{X}, \vec{u}).

    Args:
        r_i: the atom currently being considered in topological order of attack graph
        q_i: subquery containing all the atoms with relation names $r_i, r_{i+1},..., $r_n$
        argument_set: returned by "generate_argument_set"

    Returns:
        yes rule
    """
    head_atom = {
        "name": "yes_{}".format(r_i["name"]),
        "arg_list": get_atom_arguments(q_i["head"], variable=True),
    }
    body_atoms = list()
    if safe_range_atom is not None:
        body_atoms.append(safe_range_atom)
    body_atoms.append(r_i)
    negations = list()
    if badblock:
        negations.append(
            {
                "name": "bb_{}".format(r_i["name"]),
                "arg_list": sorted(
                    list(
                        deduplicate_arguments(
                            argument_set["vec_x"] + argument_set["vec_u"]
                        )
                    ),
                    key=lambda var: var.name,
                ),
            }
        )

    return {
        "head": head_atom,
        "body": {
            "atoms": body_atoms,
            "compares": [],
            "assigns": [],
            "negations": negations,
        },
    }


def generate_bad_block_query_from_good_fact_rule(
    good_fact_rule, bad_block_rule, relation_def_map
):
    # inner join part
    variable_arg_to_atom_map = translator.extract_variable_arg_to_atom_map(
        bad_block_rule["body"]["atoms"]
    )
    select_maps = translator.extract_selection_map(
        bad_block_rule["head"], variable_arg_to_atom_map
    )
    body_atom_aliases = translator.build_atom_aliases(bad_block_rule["body"]["atoms"])
    select_str = generate_select(
        bad_block_rule["head"]["name"],
        bad_block_rule["body"]["atoms"],
        select_maps,
        relation_def_map,
        body_atom_aliases,
    )
    from_str = "FROM sfr {}".format(body_atom_aliases[0])
    join_map = translator.extract_join_map(variable_arg_to_atom_map)
    join_str = "INNER JOIN {} {} ON {}".format(
        bad_block_rule["body"]["atoms"][1]["name"],
        body_atom_aliases[1],
        generate_join_str(
            bad_block_rule["body"]["atoms"],
            join_map,
            relation_def_map,
            body_atom_aliases,
        ),
    )

    # left outer join part
    variable_arg_to_atom_map = translator.extract_variable_arg_to_atom_map(
        good_fact_rule["body"]["atoms"]
    )
    join_map = translator.extract_join_map(variable_arg_to_atom_map)
    body_atom_aliases = translator.build_atom_aliases(good_fact_rule["body"]["atoms"])
    # filter out the join information between saferange and r_i atom
    filtered_join_map = OrderedDict()
    for join_attribute in join_map:
        if 2 not in join_map[join_attribute]:
            continue
        filtered_join_map[join_attribute] = join_map[join_attribute]
    anti_join_str = generate_join_str(
        good_fact_rule["body"]["atoms"],
        filtered_join_map,
        relation_def_map,
        body_atom_aliases,
    )
    if len(good_fact_rule["body"]["atoms"]) == 3 and len(anti_join_str) > 0:
        lfj_str = " LEFT OUTER JOIN {} {} ON {}".format(
            good_fact_rule["body"]["atoms"][2]["name"],
            body_atom_aliases[2],
            anti_join_str,
        )
    else:
        lfj_str = ""

    if len(lfj_str) > 0:
        null_join_str = " OR ".join(
            [
                "{} IS NULL".format(i.split(" = ")[1])
                for i in anti_join_str.split(" AND ")
            ]
        )
    else:
        null_join_str = ""

    comparison_map = translator.extract_comparison_map(
        good_fact_rule["body"]["compares"], variable_arg_to_atom_map
    )
    comparison_str = generate_comparison_str(
        good_fact_rule["body"]["atoms"],
        comparison_map,
        relation_def_map,
        body_atom_aliases,
    )
    negated_comparison_str = " OR ".join(
        [" != ".join(i.split(" = ")) for i in comparison_str.split(" AND ")]
    )
    null_or_strs = list()
    if len(null_join_str) > 0:
        null_or_strs.append(null_join_str)
    if len(negated_comparison_str) > 0:
        null_or_strs.append(negated_comparison_str)
    null_or_str = ""
    if len(null_or_strs) != 0:
        null_or_str = " WHERE {}".format(" OR ".join(null_or_strs))

    return "{} {} {}{}{}".format(select_str, from_str, join_str, lfj_str, null_or_str)


def generate_fastfo_rewriting(
    rule,
    attack_graph,
    relation_attributes_map,
    edb_decl=None,
    output_datalog=True,
    verbose=False,
):
    relation_def_map = dict()
    if not output_datalog:
        if edb_decl is None:
            raise Exception("edb_decl cannot be None if output SQL statements")
        for relation in edb_decl:
            relation_def_map[relation["name"]] = {"relation": relation, "type": "edb"}

    tmp_table_queries = list()
    final_result_eval_str = None
    idbs = list()
    topological_sequence = attack_graph.atoms_in_topological_order
    if verbose:
        print("\nTopological Order: \n")
        print(", ".join([atom["name"] for atom in topological_sequence]))

    atom_num = len(topological_sequence)
    datalog_rules = list()
    cur_query = rule
    if verbose:
        print("\nQuery: ")
        print(DatalogProgram.iterate_datalog_rule(cur_query))

    global_safe_range_rule = generate_global_safe_range_rule(
        rule, relation_attributes_map, topological_sequence
    )
    datalog_rules.append(global_safe_range_rule)
    safe_range_atom = global_safe_range_rule["head"]
    idbs.append(safe_range_atom)
    if not output_datalog:
        relation_def_map[safe_range_atom["name"]] = {
            "relation": construct_idb_decl(
                safe_range_atom["name"], len(safe_range_atom["arg_list"])
            ),
            "type": "idb",
        }
        global_safe_range_sub_query = gen_rule_eval_sql_str(
            global_safe_range_rule, relation_def_map, None, recursive=False
        )

    if verbose:
        print("\nGlobal Safe Range Rule: ")
        print(DatalogProgram.iterate_datalog_rule(global_safe_range_rule))
        if not output_datalog:
            print(global_safe_range_sub_query)

    for i in range(atom_num):
        cur_atom = topological_sequence[i]
        if verbose:
            print("\nCurrent Atom: {}".format(cur_atom["name"]))

        argument_set = generate_argument_set(
            cur_atom, cur_query, relation_attributes_map
        )
        if verbose:
            print("vec_x: ")
            print("[{}]".format(", ".join([x.name for x in argument_set["vec_x"]])))
            print("vec_u: ")
            print("[{}]".format(", ".join([u.name for u in argument_set["vec_u"]])))
            print("vec_v: ")
            print("[{}]".format(", ".join([v.name for v in argument_set["vec_v"]])))
            print("vec_w: ")
            print("[{}]".format(", ".join([w.name for w in argument_set["vec_w"]])))
            print("constraints: ")
            for vw in argument_set["vec_v_map"]:
                print("{} = {}".format(vw[0].name, vw[1].name))

        subquery = None
        next_head_atom = None
        if i < atom_num - 1:
            next_head_atom = topological_sequence[i + 1]
            subquery = generate_new_query_with_atom_being_removed(
                cur_query, cur_atom, argument_set
            )

        if verbose:
            print("Next Subquery: ")
            print(DatalogProgram.iterate_datalog_rule(subquery))

        bad_block_rule = None
        good_fact_rule = None
        safe_range_atom_to_use = safe_range_atom
        if subquery is not None:
            corrected_safe_range_atom = get_corrected_safe_range_atom(
                safe_range_atom, cur_query, cur_atom, relation_attributes_map
            )
            safe_range_atom_to_use = corrected_safe_range_atom

        if len(argument_set["vec_v_map"]) > 0 or subquery is not None:
            bad_block_rule = generate_bad_block_rule(
                cur_atom,
                argument_set,
                safe_range_atom_to_use,
            )
            yes_atom = None
            if subquery is not None and next_head_atom is not None:
                yes_atom = {
                    "name": "yes_{}".format(next_head_atom["name"]),
                    "arg_list": subquery["head"]["arg_list"],
                }
                relation_def_map[yes_atom["name"]] = {
                    "relation": construct_idb_decl(
                        yes_atom["name"], len(yes_atom["arg_list"])
                    ),
                    "type": "idb",
                }

            good_fact_rule = generate_good_fact_rule(
                cur_atom,
                argument_set,
                yes_atom=yes_atom,
                safe_range_atom=safe_range_atom_to_use,
                equality_constraint=len(argument_set["vec_v_map"]) > 0,
            )
            datalog_rules.append(good_fact_rule)
            idbs.append(bad_block_rule["head"])
            idbs.append(good_fact_rule["head"])
            if not output_datalog:
                relation_def_map[bad_block_rule["head"]["name"]] = {
                    "relation": construct_idb_decl(
                        bad_block_rule["head"]["name"],
                        len(bad_block_rule["head"]["arg_list"]),
                    ),
                    "type": "idb",
                }
                bad_block_sub_query = generate_bad_block_query_from_good_fact_rule(
                    good_fact_rule, bad_block_rule, relation_def_map
                )

        if bad_block_rule is not None:
            datalog_rules.append(bad_block_rule)

        if bad_block_rule is not None:
            yes_rule = generate_yes_rule(
                cur_atom, cur_query, argument_set, safe_range_atom
            )
        else:
            yes_rule = generate_yes_rule(
                cur_atom,
                cur_query,
                argument_set,
                safe_range_atom,
                badblock=False,
            )
        datalog_rules.append(yes_rule)
        idbs.append(yes_rule["head"])
        if not output_datalog:
            if yes_rule["head"]["name"] not in relation_def_map:
                relation_def_map[yes_rule["head"]["name"]] = {
                    "relation": construct_idb_decl(
                        yes_rule["head"]["name"], len(yes_rule["head"]["arg_list"])
                    ),
                    "type": "idb",
                }
            yes_rule_sub_query = gen_rule_eval_sql_str(
                yes_rule, relation_def_map, None, recursive=False
            )
            if i == 0:
                final_result_eval_str = generate_unified_idb_evaluation_str(
                    yes_rule["head"]["name"],
                    [yes_rule_sub_query],
                    with_subquery=False,
                    store_output=FINAL_OUTPUT_STORE,
                    distinct=True,
                )
            else:
                tmp_table_queries.append(
                    generate_unified_idb_evaluation_str(
                        yes_rule["head"]["name"],
                        [yes_rule_sub_query],
                        with_subquery=True,
                        distinct=INTERMEDIATE_DISTINCT,
                    )
                )

            if bad_block_rule is not None:
                tmp_table_queries.append(
                    generate_unified_idb_evaluation_str(
                        bad_block_rule["head"]["name"],
                        [bad_block_sub_query],
                        with_subquery=True,
                        distinct=INTERMEDIATE_DISTINCT,
                    )
                )

        cur_query = subquery
        if verbose:
            print("-----Yes Rule-----")
            print(DatalogProgram.iterate_datalog_rule(yes_rule))
            if not output_datalog:
                print("\n")
                print(yes_rule_sub_query)
                print("\n")
            print("-----BadBlock Rule-----")
            print(DatalogProgram.iterate_datalog_rule(bad_block_rule))
            if not output_datalog and bad_block_rule is not None:
                print("\n")
                print(bad_block_sub_query)
                print("\n")
            print("-----GoodFact Rule-----")
            print(DatalogProgram.iterate_datalog_rule(good_fact_rule))

    if output_datalog:
        return datalog_rules, idbs
    else:
        tmp_table_queries.reverse()
        tmp_table_queries.insert(
            0,
            generate_unified_idb_evaluation_str(
                safe_range_atom["name"],
                [global_safe_range_sub_query],
                with_subquery=True,
                distinct=True,
            ),
        )
        return "WITH {} \n     {}".format(
            ",\n     ".join(tmp_table_queries), final_result_eval_str
        )
