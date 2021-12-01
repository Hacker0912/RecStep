import cqa.conquer.rewriter as conquer_rewriter


def rewrite(edb_decl, rules, visualize_join_graph=True):
    for rule in rules:
        conquer_rewriter.rewrite(
            edb_decl,
            rule,
            visualize_join_graph=visualize_join_graph,
            c_forest_check=False,
        )
