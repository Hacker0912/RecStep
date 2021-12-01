

from datalog_program.attack_graph import *
from datalog_program.conjunctivequery import *
from datalog_program.datalog_program import *

from ppjt_rewriter.ppjt_sql_rewriter import *
from ppjt_rewriter.jointree import *


def generate_rewritings(benchmark):
	
	benchmark_dir = "benchmarks/{}".format(benchmark)
	schema_dir = "{}/schemas.json".format(benchmark_dir)

	for query_number in range(1, 22):
		query_dir = "{}/sqls/{}.sql".format(benchmark_dir, str(query_number).zfill(2))
		ppjt_output_dir = "ppjt-intermediate-rules/{}/{}.datalog".format(benchmark, query_number)
		# # conquesto_output_dir = "{}/conquesto/{}.fo".format(benchmark_dir, query_number)

		generate_sql_rewriting(query_dir, schema_dir, ppjt_output_dir)
		
		# if q:
		# 	# ppjt = get_ppjt(q)
		# 	generate_fo_rewriting(q, output_dir, generate_recstep_rules = True)

			# program = generate_fo_datalog_program(q,
   #                              eliminate_cartesian_product=False,
   #                              eliminate_good_facts=True, subquery_boundary=True, verbose=False)
			# conquestofile = open(conquesto_output_dir, "w")

			# print(program, file = conquestofile)
			# conquestofile.close()

if __name__ == "__main__":
	generate_rewritings("tpch")
	generate_rewritings("synthetic")


