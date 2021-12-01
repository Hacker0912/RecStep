import sys
sys.path.append("../../")

from datalog_program.attack_graph import AttackGraph
from datalog_program.datalog_program import Rule, Constraints, DatalogProgram, InequalityConstraint, GreaterThanConstraint, Constraint, \
    EqualityConstraint, Any, PrimaryKey, Variable, Constant, Atom
from datalog_program.conjunctivequery import ConjunctiveQuery

from rule_generators.pair_pruning.jointree import *
from rule_generators.conquesto.rule_generators import *


def get_datalog_program(rules):

    if not rules:
        return None

    program = DatalogProgram()

    for rule in rules:
        program.add_rule(rule)

    return program



def get_parameterized_atom(atom, q):

    free_var_names = [u.name for u in q.free_variables]

    new_args = []

    for var in atom.arguments:

        if isinstance(var, PrimaryKey):
            if var.data_type == Variable:
                if var.name in free_var_names:
                    new_args.append(PrimaryKey(Constant(var.name)))    
                else:
                    new_args.append(var)
            else:
                new_args.append(var)
        else:
            if var.name in free_var_names:
                new_args.append(Constant(var.name))
            else:
                new_args.append(var)

    new_atom = Atom(atom.name, tuple(new_args))

    return new_atom


def boolean_parameterize(q):

    new_body = []

    for atom in q.body_atoms:
        new_atom = get_parameterized_atom(atom, q)
        new_body.append(new_atom)

    return ConjunctiveQuery(free_variables = [], body_atoms = new_body)


def get_ground(q, query_index):

    
    all_atoms = q.body_atoms

    ground_atom_args_str = []
    ground_atom_args = []

    free_variable_names = [var.name for var in q.free_variables]

    if len(free_variable_names) > 0:
        for atom in all_atoms:
            for var in atom.arguments:
                if var.name not in ground_atom_args_str:
                    if (isinstance(var, PrimaryKey) and var.data_type == Variable) or var.name in free_variable_names:
                        ground_atom_args_str.append(var.name)
                        ground_atom_args.append(Variable(var.name))

    if query_index < 0:
        ground_name = "ground"
    else:
        ground_name = "ground_{}".format(query_index)

    ground_rule_head = Atom(ground_name, tuple(ground_atom_args))

    ground_rule = Rule(
                    ground_rule_head,
                    all_atoms
                )   

    return ground_rule_head, ground_rule


def get_appropriate_ground(join_tree, ground_rule_head):

    if not ground_rule_head:
        return None

    center = join_tree.root

    center_key_names = [var.name for var in center.arguments if isinstance(var, PrimaryKey)]

    free_variable_names = [var.name for var in join_tree.free_variables]

    appropriate_ground_args = []

    for var in ground_rule_head.arguments:
        if var.name in center_key_names or var.name in free_variable_names:
            appropriate_ground_args.append(var)
        else:
            appropriate_ground_args.append(Variable("_"))

    return Atom(ground_rule_head.name, tuple(appropriate_ground_args))


def get_free_version(join_tree):
    center = join_tree.root
    args = []


    free_variable_names = [var.name for var in join_tree.free_variables]


    seen_vars_dict = {}
    seen_vars_list = []

    cnt = 0
    for var in center.arguments:
        if isinstance(var, PrimaryKey):
            args.append(var)
            if var.name not in seen_vars_list:
                seen_vars_list.append(var.name)
        else:
            if isinstance(var, Constant) or var.name in free_variable_names:
                free_name = "FREE_{}".format(cnt)
                args.append(Variable(free_name))
                seen_vars_dict[var.name] = free_name
                cnt += 1
            else:
                if var.name not in seen_vars_list:
                    seen_vars_list.append(var.name)
                    args.append(var)
                else:
                    free_name = "FREE_{}".format(cnt)
                    args.append(Variable(free_name))
                    seen_vars_dict[var.name] = free_name
                    cnt += 1

    return Atom(center.name, tuple(args)), seen_vars_dict

def negated_atom(atom, joining_atoms=[]):

    args = []

    all_vars = []
    for join_atom in joining_atoms:
        for var in join_atom.arguments:
            if var.name not in all_vars:
                all_vars.append(var.name)


    for var in atom.arguments:
        if var.name in all_vars or len(joining_atoms) == 0:
            args.append(var)
        else:
            args.append(Variable("_"))



    return Atom(atom.name, tuple(args), negated = True)



def join_tree_rewriting_self_pruning(join_tree, ground_rule_head):
    rules = []

    atom_free_version, seen_vars_dict = get_free_version(join_tree)
    rule_head = join_tree.get_bad_key_rule_head()

    
    ground_rule_head_here = get_appropriate_ground(join_tree, ground_rule_head)

    for var in seen_vars_dict:
        
        inequality_constraint = InequalityConstraint(Variable(var), Variable(seen_vars_dict[var]))
        constraints = Constraints()
        constraints.add_constraint(inequality_constraint)

        body_atoms = [
                boolean_atom_to_nonboolean(atom_free_version, join_tree.free_variables),
                constraints,
                ground_rule_head_here
        ]

        rule = Rule(rule_head, body_atoms)
        rules.append(rule)

    join_tree.rules += rules

############# new new new new new ############################

def join_tree_rewriting_pair_pruning(join_tree, sub_join_tree, ground_rule_head, use_aggregation = False):
    rules = []

    center = join_tree.root 
    ground_rule_head_here = get_appropriate_ground(join_tree, ground_rule_head)

    joining_vars = join_tree.get_joining_vars(sub_join_tree)

    small_closure = sub_join_tree.get_closure(sub_join_tree.root)
    big_closure = join_tree.get_closure(sub_join_tree.root)

    
    sub_tree_rules = []


    aggregation_rule_heads = []

    for var in joining_vars:
        if var not in big_closure:
            raise Exception("Check the PODS 2015 paper for closure containment bug")

        if var not in small_closure:

            if use_aggregation:
                
                aggregation_rule_head_name = "{}_{}_count".format(sub_join_tree.root.name, var)
                args = []
                args_bad = []
                for variable in sub_join_tree.root.arguments:
                    if isinstance(variable, PrimaryKey):
                        if variable.data_type == Variable:
                            args.append(variable)
                            args_bad.append(variable)
                args.append(Variable("count({})".format(var)))
                args_bad.append(Variable("CNT"))

                aggregation_rule_head = Atom(aggregation_rule_head_name, args)
                aggregation_rule_heads.append(aggregation_rule_head)
                
                cnt_args = []
                marked = False
                for variable in sub_join_tree.root.arguments:
                    if isinstance(variable, PrimaryKey):
                        cnt_args.append(variable)
                    else:
                        if not marked:
                            if variable.name == var:
                                cnt_args.append(variable)
                                marked = True
                            else:
                                cnt_args.append(Variable("_"))
                        else:
                            cnt_args.append(Variable("_"))



                cnt_rule = Rule(
                    aggregation_rule_head,
                    [
                        Atom(sub_join_tree.root.name, cnt_args),
                        sub_join_tree.get_good_key_rule_head()
                    ])


                sub_tree_rules.append(cnt_rule)
                
                # print(rule)
                # input()

            else:

                petal_center_saturation_free, dictionary = sub_join_tree.get_saturation_version()
                var_name = var

                inequality_constraint = InequalityConstraint(Variable(var_name), Variable(dictionary[var_name]))
                constraints = Constraints()
                constraints.add_constraint(inequality_constraint)

                rule_body = [
                        boolean_atom_to_nonboolean(sub_join_tree.root, join_tree.free_variables),
                        petal_center_saturation_free,
                        constraints,
                        ground_rule_head_here
                    ]


                rule = Rule(
                        sub_join_tree.get_bad_key_rule_head(),
                        rule_body
                    )

                sub_tree_rules.append(rule)


    # filtered_rule_head = join_tree.get_good_join_rule_head()

    # if use_aggregation:
    #     filtered_rule_body = [
    #                                 sub_join_tree.get_good_join_rule_head(), 
    #                                 boolean_atom_to_nonboolean(sub_join_tree.root, join_tree.free_variables),
    #                                 ground_rule_head_here
    #                             ]

    #     for aggregation_rule_head in aggregation_rule_heads:
    #         args = []
    #         for var in aggregation_rule_head.arguments:
    #             if "count" in var.name.lower():
    #                 args.append(Variable("1"))
    #             else:
    #                 args.append(var)

    #         filtered_rule_body.append(Atom(aggregation_rule_head.name, args))

        
    #     filtered_rule = Rule(filtered_rule_head, filtered_rule_body)
    #     sub_tree_rules.append(filtered_rule)

    # else:
    #     filtered_rule = Rule(filtered_rule_head, 
    #                             [
    #                                 sub_join_tree.get_good_join_rule_head(), 
    #                                 boolean_atom_to_nonboolean(sub_join_tree.root, join_tree.free_variables),
    #                                 ground_rule_head_here
    #                             ])

    #     sub_tree_rules.append(filtered_rule)

    sub_good_join_atom = sub_join_tree.get_good_join_rule_head()

    sub_good_join_atom_negated = Atom(sub_good_join_atom.name, sub_good_join_atom.arguments, negated = True)


    bad_key_rule_body = [
                boolean_atom_to_nonboolean(center, join_tree.free_variables),
                sub_good_join_atom_negated,
                ground_rule_head_here
            ]

    bad_key_rule = Rule(
            join_tree.get_bad_key_rule_head(),
            bad_key_rule_body,
        )

    rules.append(bad_key_rule)

    join_tree.rules += rules
    sub_join_tree.rules += sub_tree_rules

############################################################################


def join_tree_rewriting_pair_pruning_old(join_tree, sub_join_tree, ground_rule_head, use_aggregation = False):
    rules = []

    center = join_tree.root 
    ground_rule_head_here = get_appropriate_ground(join_tree, ground_rule_head)

    joining_vars = join_tree.get_joining_vars(sub_join_tree)

    small_closure = sub_join_tree.get_closure(sub_join_tree.root)
    big_closure = join_tree.get_closure(sub_join_tree.root)

    
    sub_tree_rules = []


    aggregation_rule_heads = []

    for var in joining_vars:
        if var not in big_closure:
            raise Exception("Check the PODS 2015 paper for closure containment bug")

        if var not in small_closure:

            if use_aggregation:
                
                aggregation_rule_head_name = "{}_{}_count".format(sub_join_tree.root.name, var)
                args = []
                args_bad = []
                for variable in sub_join_tree.root.arguments:
                    if isinstance(variable, PrimaryKey):
                        if variable.data_type == Variable:
                            args.append(variable)
                            args_bad.append(variable)
                args.append(Variable("count({})".format(var)))
                args_bad.append(Variable("CNT"))

                aggregation_rule_head = Atom(aggregation_rule_head_name, args)
                aggregation_rule_heads.append(aggregation_rule_head)
                
                cnt_args = []
                marked = False
                for variable in sub_join_tree.root.arguments:
                    if isinstance(variable, PrimaryKey):
                        cnt_args.append(variable)
                    else:
                        if not marked:
                            if variable.name == var:
                                cnt_args.append(variable)
                                marked = True
                            else:
                                cnt_args.append(Variable("_"))
                        else:
                            cnt_args.append(Variable("_"))



                cnt_rule = Rule(
                    aggregation_rule_head,
                    [
                        Atom(sub_join_tree.root.name, cnt_args),
                        sub_join_tree.get_good_key_rule_head()
                    ])


                sub_tree_rules.append(cnt_rule)
                
                # print(rule)
                # input()

            else:

                petal_center_saturation_free, dictionary = sub_join_tree.get_saturation_version()
                var_name = var

                inequality_constraint = InequalityConstraint(Variable(var_name), Variable(dictionary[var_name]))
                constraints = Constraints()
                constraints.add_constraint(inequality_constraint)

                rule_body = [
                        boolean_atom_to_nonboolean(sub_join_tree.root, join_tree.free_variables),
                        petal_center_saturation_free,
                        constraints,
                        ground_rule_head_here
                    ]


                rule = Rule(
                        sub_join_tree.get_bad_key_rule_head(),
                        rule_body
                    )

                sub_tree_rules.append(rule)


    petal_good_key_atom = sub_join_tree.get_good_key_rule_head()
    good_key_vars = [var.name for var in petal_good_key_atom.arguments]

    if is_subset(joining_vars, good_key_vars):

        joining_args = []

        for var in petal_good_key_atom.arguments:
            if var.name in joining_vars:
                joining_args.append(var)
            else:
                joining_args.append(Variable("_"))


        petal_good_key_negated = Atom(petal_good_key_atom.name, tuple(joining_args), negated = True)

        bad_key_rule_body = [
                    boolean_atom_to_nonboolean(center, join_tree.free_variables),
                    petal_good_key_negated,
                    ground_rule_head_here
                ]

    else:
        filtered_rule_head_name = "{}_consistent_filter".format(sub_join_tree.root.name)

        filtered_rule_head_args = [var for var in sub_join_tree.root.arguments if var.name in joining_vars]

        for var in sub_join_tree.free_variables:
            if var not in filtered_rule_head_args:
                filtered_rule_head_args.append(var)

        filtered_rule_head = Atom(filtered_rule_head_name, tuple(filtered_rule_head_args))
        

        if use_aggregation:
            filtered_rule_body = [
                                        sub_join_tree.get_good_key_rule_head(), 
                                        boolean_atom_to_nonboolean(sub_join_tree.root, join_tree.free_variables),
                                        ground_rule_head_here
                                    ]

            for aggregation_rule_head in aggregation_rule_heads:
                args = []
                for var in aggregation_rule_head.arguments:
                    if "count" in var.name.lower():
                        args.append(Variable("1"))
                    else:
                        args.append(var)

                filtered_rule_body.append(Atom(aggregation_rule_head.name, args))

            
            filtered_rule = Rule(filtered_rule_head, filtered_rule_body)
            sub_tree_rules.append(filtered_rule)

        else:
            filtered_rule = Rule(filtered_rule_head, 
                                    [
                                        sub_join_tree.get_good_key_rule_head(), 
                                        boolean_atom_to_nonboolean(sub_join_tree.root, join_tree.free_variables),
                                        ground_rule_head_here
                                    ])

            sub_tree_rules.append(filtered_rule)

        filtered_negated_atom = Atom(filtered_rule_head.name, filtered_rule_head.arguments, negated = True)


        bad_key_rule_body = [
                    boolean_atom_to_nonboolean(center, join_tree.free_variables),
                    filtered_negated_atom,
                    ground_rule_head_here
                ]

    bad_key_rule = Rule(
            join_tree.get_bad_key_rule_head(),
            bad_key_rule_body,
        )

    rules.append(bad_key_rule)

    join_tree.rules += rules
    sub_join_tree.rules += sub_tree_rules




def boolean_atom_to_nonboolean(atom, free_variables):
    free_var_names = [var.name for var in free_variables]
    args = []

    for var in atom.arguments:
        if var.name in free_var_names:
            if isinstance(var, PrimaryKey):
                args.append(PrimaryKey(Variable(var.name)))
            else:
                args.append(Variable(var.name))
        else:
            args.append(var)

    return Atom(atom.name, tuple(args))


def join_tree_rewriting(join_tree, ground_rule_head, use_aggregation = False):

    join_tree_rewriting_self_pruning(join_tree, ground_rule_head)
    ground_rule_head_here = get_appropriate_ground(join_tree, ground_rule_head)

    for sub_join_tree in join_tree.children:
        join_tree_rewriting_pair_pruning(join_tree, sub_join_tree, ground_rule_head, use_aggregation)
        join_tree_rewriting(sub_join_tree, ground_rule_head, use_aggregation)


####################################################################
def add_good_join_rule(join_tree, ground_rule_head):

    good_key_rule_body = [
                            boolean_atom_to_nonboolean(join_tree.root, join_tree.free_variables),
                        ]

    ground_rule_head_here = get_appropriate_ground(join_tree, ground_rule_head)

    if len(join_tree.rules):
        for rule in join_tree.rules:
            rule_head = rule.get_head_atom()
            if "bad_key" in rule_head.name:
                good_key_rule_body.append(negated_atom(join_tree.get_bad_key_rule_head(), joining_atoms = []))
                break
        
        for child in join_tree.children:
            good_key_rule_body.append(child.get_good_join_rule_head())

        good_key_rule_body.append(ground_rule_head_here)

        
    good_key_rule = Rule(
            join_tree.get_good_join_rule_head(),
            good_key_rule_body
        )

    join_tree.rules.append(good_key_rule)


    for child in join_tree.children:
        add_good_join_rule(child, ground_rule_head)

####################################################################


def add_good_key_rule(join_tree, ground_rule_head):

    good_key_rule_body = [
                            boolean_atom_to_nonboolean(join_tree.root, join_tree.free_variables),
                        ]

    ground_rule_head_here = get_appropriate_ground(join_tree, ground_rule_head)

    if len(join_tree.rules):
        for rule in join_tree.rules:
            rule_head = rule.get_head_atom()
            if "bad_key" in rule_head.name:
                good_key_rule_body.append(negated_atom(join_tree.get_bad_key_rule_head(), joining_atoms = []))
                break
        
        for child in join_tree.children:
            good_key_rule_body.append(child.get_good_key_rule_head())

        good_key_rule_body.append(ground_rule_head_here)

        
    good_key_rule = Rule(
            join_tree.get_good_key_rule_head(),
            good_key_rule_body
        )

    join_tree.rules.append(good_key_rule)


    for child in join_tree.children:
        add_good_key_rule(child, ground_rule_head)



def get_declaration_form(atom_name, arity):

    if arity == 0:
        return atom_name

    args = []
    for i in range(arity):
        args.append("a{} int".format(i))

    args_str = ", ".join(args)

    form = "{}({})".format(atom_name, args_str)
    return form



def get_recstep_header_rules(program):

    all_atom_names = {}
    all_head_atom_names = {}

    for rule in program.get_rules():
        
        head_atom = rule.get_head_atom()
        head_atom_arity = len(head_atom.arguments)

        body_atoms = rule.get_body_atoms()

        if head_atom.name not in all_atom_names:
            all_atom_names[head_atom.name] = head_atom_arity

        if head_atom.name not in all_head_atom_names:
            all_head_atom_names[head_atom.name] = head_atom_arity

        for atom in body_atoms:
            if isinstance(atom, Constraints):
                continue
            if atom.name not in all_atom_names:
                arity = len(atom.arguments)
                all_atom_names[atom.name] = arity 

    
    edb_atoms = []
    idb_atoms = []

    for atom_name in all_atom_names:
        form = get_declaration_form(atom_name, all_atom_names[atom_name])
        if atom_name in all_head_atom_names:
            idb_atoms.append(form)
        else:
            edb_atoms.append(form)


    rules = []
    rules += ["EDB_DECL:"]
    rules += edb_atoms
    rules += ["\n"]

    rules += ["IDB_DECL:"]
    rules += idb_atoms
    rules += ["\n"]

    rules += ["RULE_DECL:"]
    rules += ["\n"]

    return rules




def generate_fo_datalog_program_pair_pruning(q: ConjunctiveQuery, 
                                             use_aggregation = False, query_index = -1) -> DatalogProgram:
    bool_q = boolean_parameterize(q)


    # join_tree = get_a_pair_pruning_tree(bool_q)
    join_tree = get_a_pair_pruning_tree(bool_q)
    
    if not join_tree:
        msg = "Pair-pruning is not applicable on query : {}.".format(q)
        # raise Exception(msg)
        print(msg)
        return None 

    join_tree.add_free_variables(q.free_variables)
    join_tree.set_parent_joining_variables()

    rules = []
    

    
    ### first produce the ground rule

    ground_rule_head, ground_rule = get_ground(q, query_index)
    rules.append(ground_rule)


    ### next produce the bad-key rules for pair pruning for each subtree recursively

    join_tree_rewriting(join_tree, ground_rule_head, use_aggregation)
    add_good_join_rule(join_tree, ground_rule_head)
    rules += join_tree.get_all_rules()


    ### finally generate the return rule

    if query_index < 0:
        return_name = "return"
    else:
        return_name = "return_{}".format(query_index)


    return_head = Atom(return_name, q.free_variables)
    return_rule = Rule(
            return_head,
            [join_tree.get_good_join_rule_head()]
        )
    rules.append(return_rule)


    ### generate the datalog program containing the generated rules

    return rules, return_head



# def is_joining_non_free(atom, atom_prime, free_variables):
#     atom_vars = [var.name for var in atom.arguments if var.data_type == Variable]
#     atom_prime_vars = [var.name for var in atom_prime.arguments if var.data_type == Variable]

#     free_names = [var.name for var in free_variables]

#     for var_name in atom_prime_vars:
#         if var_name not in free_names:
#             if var_name in atom_vars:
#                 return True

#     return False 


# def decompose_query(q):

#     components = []

#     for atom in q.body_atoms:
#         joining_component = None
#         for component in components:
#             for atom_prime in component:
#                 if is_joining_non_free(atom, atom_prime, q.free_variables):
#                     joining_component = component
#                     break
#             if joining_component:
#                 break

#         if joining_component:
#             joining_component.append(atom)
#         else:
#             components.append([atom])

#     qs = []
#     for component in components:
#         all_vars = []

#         for atom in component:
#             for var in atom.arguments:
#                 if var.data_type == Variable:
#                     all_vars.append(var.name)

#         free_var = []
#         for var in q.free_variables:
#             if var.name in all_vars:
#                 free_var.append(var)


#         query = ConjunctiveQuery(free_variables = free_var, body_atoms = component.copy())
#         qs.append(query)


#     return qs


def append_constant(atom):
    if not isinstance(atom, Atom):
        return atom

    if len(atom.arguments) > 0:
        return atom 
    else:
        return Atom(atom.name, [Constant("1")])

def append_constant_to_boolean_atoms(rules):

    ret = []
    for rule in rules:
        rule_head = rule.get_head_atom()
        rule_body = rule.get_body_atoms()

        new_rule = Rule(
            append_constant(rule_head),
            tuple([append_constant(atom) for atom in rule_body])
        )

        ret.append(new_rule)

    return ret




def generate_fo_rewriting(q, 
                          output_dir,
                          use_aggregation = False,
                          generate_recstep_rules = False):
    
    qs = decompose_query(q)

    rules = []  
    return_heads = []
    if len(qs) == 1:
        curr_rules, return_head = generate_fo_datalog_program_pair_pruning(q, use_aggregation)
        rules += curr_rules
        return_heads.append(return_head)

    else:
        cnt = 1
        for subq in qs:
            curr_rules, return_head = generate_fo_datalog_program_pair_pruning(subq, use_aggregation, cnt)
            rules += curr_rules
            return_heads.append(return_head)
            cnt += 1

        return_rule = Rule(
                Atom("return", q.free_variables),
                return_heads
            )
        rules.append(return_rule)

    rules = append_constant_to_boolean_atoms(rules)
    program = get_datalog_program(rules)

    if not program:
        return
    print(program)
    if generate_recstep_rules:
        recstep_header_rules = get_recstep_header_rules(program)
        file = open(output_dir, "w")    
        for rule in recstep_header_rules:
            print(rule, file=file)
        file.close()

        program.append_to_file(output_dir)

    else:
        program.save_to_file(output_dir)





