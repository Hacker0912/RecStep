import sys
import os

sys.path.append("../../")

from rule_generators.conquesto.rule_generators import *
from rule_generators.pair_pruning.rewriter import *
from datalog_program.datalog_program import *
import json

#######################################
# Variants of Queries to be generated #
#######################################

boolean = False
good_fact = True


def parse(sql):

    select_line = sql.split("select")[1]
    from_line = select_line.split("from")[1]
    select_line = select_line.split("from")[0]


    debris = from_line.split("where")
    from_line = debris[0]
    join_line = ""

    if len(debris) > 1:
        join_line = debris[1]

    select_line = select_line.replace(" ", "")
    from_line = from_line.replace(" ", "")
    # join_line = join_line.replace(" ", "")

    
    atom_names = from_line.split(",")
    free_vars_temp = select_line.split(",")

    free_vars = []
    for free_var in free_vars_temp:
        if "." in free_var:
            free_vars.append(free_var.split(".")[1])
        else:
            free_vars.append(free_var)


    joining_vars_temp = join_line.split("and")
    joining_vars = []
    for jv in joining_vars_temp:
        joining_vars.append(jv.replace(" ", ""))

    return atom_names, free_vars, joining_vars
    

def construct_cq_from_sql(sqlfilename, schemafile):
    print("****************************")
    try:
        f = open(schemafile)
    except FileNotFoundError:
        print("File {} not found.".format(schemafile))
        return None

    try:
        file = open(sqlfilename, "r")
    except FileNotFoundError:
        print("File {} not found.".format(sqlfilename)) 
        return None

    schema = json.load(f)
    
    sql = file.readline()[:-1]
    sql = sql.lower()
    file.close()
    
    print(sql)
    atom_names, free_vars, joining_vars = parse(sql)

    # print(atom_names)
    # print(free_vars)
    # print(joining_vars)


    attr_mapping = {}
    for table in atom_names:
        for attr in schema[table]["attributes"]:
            attr_mapping[attr] = attr

    cnt = 0 
    for joining_var in joining_vars:
        if "=" not in joining_var:
            continue



        arg1, arg2 = joining_var.split("=")
        var1 = arg1
        var2 = arg2
        if "." in var1:
            var1 = arg1.split(".")[1]
        if "." in var2:
            var2 = arg2.split(".")[1]

        if var1.isnumeric():
            attr_mapping[var2] = var1
        elif var2.isnumeric():
            attr_mapping[var1] = var2 
        else:
            var1_root = var1 
            var2_root = var2 

            while var1_root != attr_mapping[var1_root]:
                var1_root = attr_mapping[var1_root]
            while var2_root != attr_mapping[var2_root]:
                var2_root = attr_mapping[var2_root]

            attr_mapping[var1_root] = var2

    for attr in attr_mapping:
        if attr != attr_mapping[attr]:
            print(attr, "=", attr_mapping[attr])

    # input()


    q_free = [Variable(attr_mapping[free_var].upper()) for free_var in free_vars if free_var in attr_mapping]
    q_body = []

    for table in atom_names:
        body = []
        index = 0
        

        for attr in schema[table]["attributes"]:
            var_root = attr 
            is_constant = False
            while var_root != attr_mapping[var_root]:
                var_root = attr_mapping[var_root]
                if var_root not in attr_mapping:
                    is_constant = True
                    break

                
            var = Variable(var_root.upper())
            if is_constant:
                var = Constant(var_root)

            if index in schema[table]["key"]:
                body.append(PrimaryKey(var))
            else:
                body.append(var)
            
            index += 1 

        atom = Atom(table, body)

        q_body.append(atom)

    q = ConjunctiveQuery(free_variables = q_free, body_atoms = q_body)
    
    return q



def produce_recstep_input(sqlfilename, schemafile):

    q = construct_cq_from_sql(sqlfilename, schemafile)

    try:
        f = open(schemafile)
    except FileNotFoundError:
        print("File {} not found.".format(schemafile))
        return None

    schema = json.load(f)

    print("EDB_DECL:")
    for table_name in schema:
        attr_list = []
        for i in range(len(schema[table_name]["attributes"])):
            attr = schema[table_name]["attributes"][i]
            if i in schema[table_name]["key"]:
                attr_list.append("[{}] int".format(attr))
            else:
                attr_list.append("{} int".format(attr))
        print("{}({})".format(table_name, ", ".join(attr_list)))
    

    print("\nIDB_DECL:")
    free_var_list = [var.name + " int" for var in q.free_variables]

    print("q({})".format(", ".join(free_var_list)))

    print("\nRULE_DECL:")

    rule_head = "q({})".format(", ".join([var.name for var in q.free_variables]))

    body_list = []
    for atom in q.body_atoms:
        atom_str = "{}({})".format(atom.name, ", ".join([var.name for var in atom.arguments]))
        body_list.append(atom_str)

    rule_body = ", ".join(body_list)

    rule = "{} :- {}.".format(rule_head, rule_body)
    print(rule)





def generate_rewriting(sqlfilename, schemafile, outputfile):

    q = construct_cq_from_sql(sqlfilename, schemafile)
    generate_fo_rewriting(q, outputfile, use_aggregation = True, generate_recstep_rules = True)
         


def main():

    #################################
    # Generate and save the queries #
    #################################
    indices = [1, 2, 3, 4, 6, 10, 11, 12, 14, 16, 17, 18, 20, 21]
    
    schemafile = "schemas.json"

    for index in indices:

        
        sqlfilename = "./ground/tpch-sqls/{}.sql".format(str(index).zfill(2))
        outputfile = "./pair/{}.fo".format(index)
        generate_rewriting(sqlfilename, schemafile, outputfile)
        


def test():

    
    outputfile = "output.fo"


    if len(sys.argv) < 3:
        print("Usage: python3 sql_rewriter.py <sql file dir> <schema file dir> [<output dir>]")
        return
    
    sqlfilename = sys.argv[1]
    schemafile = sys.argv[2]
    if len(sys.argv) >= 4:
        outputfile = sys.argv[3]

    generate_rewriting(sqlfilename, schemafile, outputfile)

if __name__ == "__main__":
    # main()
    test()
