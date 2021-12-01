import sys
sys.path.append("../")

from util.sql_rewriter import *
from datalog_program.attack_graph import AttackGraph
from datalog_program.datalog_program import Rule, Constraints, DatalogProgram, InequalityConstraint, GreaterThanConstraint, Constraint, \
    EqualityConstraint, Any, PrimaryKey, Variable, Constant, Atom
from datalog_program.conjunctivequery import ConjunctiveQuery

from ppjt_rewriter.jointree import *

def get_good_join_atom(ppjt, schema):

	head_name = "{}_good_join".format(ppjt.root.name)

	var_names = ppjt.parent_joining_variables
	for var in ppjt.free_variables:
		if var.name not in var_names:
			var_names.append(var.name)


	joining_attributes = []
	root_var_names = [var.name for var in ppjt.root.arguments]

	for var_name in ppjt.parent_joining_variables:
		if var_name in root_var_names:
			i = root_var_names.index(var_name)
			attr = schema[ppjt.root.name]["attributes"][i].lower()
			joining_attributes.append(attr)
		
	for var in ppjt.free_variables:
		if var.name.lower() not in joining_attributes:
			joining_attributes.append(var.name.lower())

	head_body = [Variable(var) for var in var_names]
	if not head_body:
		head_body.append(Constant("1"))
	head = Atom(head_name, head_body)


	joining_attributes_format = ["[{}] int".format(v) for v in joining_attributes]
	return head, "{}({})".format(head_name, ", ".join(joining_attributes_format))


def get_edb_form(ppjt, schema):
	table_name = ppjt.root.name 
	arg_list = []

	for i in range(len(schema[table_name]["attributes"])):
		item = "{} int".format(schema[table_name]["attributes"][i])
		if i in schema[table_name]["key"]:
			item = "[{}] int".format(schema[table_name]["attributes"][i])
		
		arg_list.append(item)

	arg_string = ", ".join(arg_list)

	edb_form = "{}({})".format(ppjt.root.name, arg_string)

	return edb_form


def generate_intermediate_rules_from_ppjt(ppjt, schema):
	rules = []
	original_edb_forms = []
	goodjoin_edb_forms = []


	def dfs(ppjt, schema):

		root_edb_form = get_edb_form(ppjt, schema)
		original_edb_forms.append(root_edb_form)

		for child in ppjt.children:
			dfs(child, schema)

		rule_head, root_gj_edb_form = get_good_join_atom(ppjt, schema)
		goodjoin_edb_forms.append(root_gj_edb_form)

		rule_body = [ppjt.root]
		for child in ppjt.children:
			child_head, _ = get_good_join_atom(child, schema)
			rule_body.append(child_head)

		rule = Rule(rule_head, rule_body)
		rules.append(rule)

	dfs(ppjt, schema)

	return rules, original_edb_forms, goodjoin_edb_forms


def produce_output(edbs, rules, output_dir = None):
	if output_dir:
		output_file = open(output_dir, "w")
		print("\nEDB_DECL", file = output_file)
		for edb in edbs:
			print(edb, file = output_file)

		print("\nIDB_DECL:", file = output_file)

		print("\nRULE_DECL:\n", file = output_file)
		for rule in rules:
			print(rule, file = output_file)

		output_file.close()

	else:
		print("\nEDB_DECL:")
		for edb in edbs:
			print(edb)

		print("\nIDB_DECL:")

		print("\nRULE_DECL:\n")
		for rule in rules:
			print(rule)


def generate_sql_rewriting(sql_dir, schema_dir, output_dir):
	q = construct_cq_from_sql(sql_dir, schema_dir)
	if not q:
		return None

	try:
		f = open(schema_dir)
	except FileNotFoundError:
		print("File {} not found.".format(schema_dir))
		return None

	schema = json.load(f)

	q_components = decompose_query(q)

	rules = []
	all_roots = []


	edbs = []

	print("\n--- PPJT ---\n")
	for q_c in q_components:
		
		ppjt = get_ppjt(q_c)
		all_roots.append(ppjt.root)
		print(ppjt)
		
		delta_rules, delta_original_edb_forms, delta_goodjoin_edb_forms = generate_intermediate_rules_from_ppjt(ppjt, schema)

		rules += delta_rules
		edbs += delta_original_edb_forms
		edbs += delta_goodjoin_edb_forms


	produce_output(edbs, rules)
	# produce_output(edbs, rules, output_dir)
