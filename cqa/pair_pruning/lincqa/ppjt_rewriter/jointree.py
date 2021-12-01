import sys
sys.path.append("../")

from datalog_program.attack_graph import AttackGraph
from datalog_program.datalog_program import Rule, Constraints, DatalogProgram, InequalityConstraint, Constraint, \
    EqualityConstraint, Any, PrimaryKey, Variable, Constant, Atom
from datalog_program.conjunctivequery import ConjunctiveQuery

from rule_generators.pair_pruning.util import *
from rule_generators.pair_pruning.prufer import *  

class JoinTree:


    def __init__(self, root, children):
        self.root = root
        self.children = children
        self.free_variables = []
        self.rules = []
        self.parent_joining_variables = []

    def add_child(self, tree):
        self.children.append(tree)


    def __repr__(self):

        string = "{}".format(self.root.name)
        subtrees = []

        for v in self.children:
            subtrees.append(v.__repr__())

        if len(subtrees) > 0:
            return "{} [ {} ]".format(string, " , ".join(subtrees))
        else:
            return string

    def get_all_atoms(self):

        ret = [self.root]

        for child in self.children:
            ret += child.get_all_atoms()

        return ret


    def get_attack_graph(self):
        all_atoms = self.get_all_atoms()
        q = ConjunctiveQuery(free_variables = [], body_atoms = all_atoms)

        ag = AttackGraph(q)
        return ag



    def is_pair_pruning_join_tree_check_attack(self):
        ag = self.get_attack_graph()

        ret = self.root in ag.unattacked_atoms and ag.is_acyclic

        for child in self.children:
            ret = ret and child.is_pair_pruning_join_tree_check_attack()

        return ret


    def is_pair_pruning_join_tree(self):

        tree, index_to_atom = self.get_undirected_join_tree()
        return is_join_tree(tree, index_to_atom) and self.is_pair_pruning_join_tree_check_attack()


    
    def set_parent_joining_variables(self):

        for child in self.children:
            joining_vars = self.get_joining_vars(child)
            child.parent_joining_variables = joining_vars
            child.set_parent_joining_variables()

    

    def add_free_variables(self, total_free_variables):

        atoms = self.get_all_atoms()
        total_free_variable_names = [var.name for var in total_free_variables]

        added_free_variable_names = []

        for atom in atoms:
            for var in atom.arguments:
                if var.name in total_free_variable_names:
                    if var.name not in added_free_variable_names:
                        self.free_variables.append(Variable(var.name))
                        added_free_variable_names.append(var.name)

        for child in self.children:
            child.add_free_variables(total_free_variables)


    def get_joining_vars(self, sub_join_tree):

        center_name = [var.name for var in self.root.arguments if var.data_type == Variable]
        petal_center_name = [var.name for var in sub_join_tree.root.arguments if var.data_type == Variable]

        joining_vars_name = intersect(center_name, petal_center_name)

        return joining_vars_name



    def get_all_rules(self):
    
        rules = []
        rules += self.rules 
        for child in self.children:
            rules += child.get_all_rules()
        return rules


    def get_undirected_join_tree(self):

        tree = {}
        index_to_atom = {}

        queue = [(self, -1)]
        cnt = 1
        while len(queue) > 0:

            jt, parent_index = queue.pop(0) 
            
            index_to_atom[cnt] = jt.root 
            if cnt not in tree:
                tree[cnt] = []

            if parent_index > 0:
                if parent_index not in tree:
                    tree[parent_index] = []
                tree[parent_index].append(cnt)

                if cnt not in tree:
                    tree[cnt] = []
                tree[cnt].append(parent_index)

            
            for child in jt.children:
                queue.append((child, cnt))
            cnt += 1


        return tree, index_to_atom

    
def is_connected(tree, vertex_set):

    v = vertex_set[0]

    visited = {}
    for u in vertex_set:
        visited[u] = False

    visited[v] = True 


    def dfs_util(v, visited):

        for u in tree[v]:
            if u in vertex_set and not visited[u]:
                visited[u] = True 
                dfs_util(u, visited) 



    dfs_util(v, visited)

    for u in vertex_set:
        if not visited[u]:
            return False
    return True



def is_acyclic(q):

    n = len(q.body_atoms)

    index_to_atom = {}

    for i in range(1, n+1):
        index_to_atom[i] = q.body_atoms[i-1]
    
    all_trees = enumerate_all_sequences(n, convert_sequence_to_tree)

    jointrees = []
    for tree in all_trees:
        if is_join_tree(tree, index_to_atom):
            return True
    
    return False



def is_joining(atom1, atom2):

    var1 = [var.name for var in atom1.arguments if var.data_type == Variable]
    var2 = [var.name for var in atom2.arguments if var.data_type == Variable]

    join = intersect(var1, var2)
    return len(join) > 0

def is_join_tree(tree, index_to_atom):

    occurring_atoms = {}

    for index in index_to_atom:
        atom = index_to_atom[index]
        for var in atom.arguments:
            if (isinstance(var, Variable) or isinstance(var, PrimaryKey)):
                if var.name not in occurring_atoms:
                    occurring_atoms[var.name] = []
                occurring_atoms[var.name].append(index)

    for u in tree:
        for v in tree[u]:
            atom1 = index_to_atom[u]
            atom2 = index_to_atom[v]
            if not is_joining(atom1, atom2):
                return False


    for var_name in occurring_atoms:
        vertex_set = occurring_atoms[var_name]

        if not is_connected(tree, vertex_set):
            
            return False
    return True




def graft(tree, graft_vertices, base):
    new_tree = {}
    for v in tree:
        new_tree[v] = []

    for v in tree:
        if v not in graft_vertices:
            for u in tree[v]:
                if u in graft_vertices:
                    new_tree[v].append(base)
                    new_tree[base].append(v)
                else:
                    new_tree[v].append(u)
        else:
            new_tree[v] = tree[v]

    return new_tree



def is_joining_non_free(atom, atom_prime, free_variables):
    atom_vars = [var.name for var in atom.arguments if var.data_type == Variable]
    atom_prime_vars = [var.name for var in atom_prime.arguments if var.data_type == Variable]

    free_names = [var.name for var in free_variables]

    for var_name in atom_prime_vars:
        if var_name not in free_names:
            if var_name in atom_vars:
                return True

    return False 


def decompose_query(q):
    index_to_atom = {}

    graph = {}
    parent = {}
    
    for i in range(len(q.body_atoms)):
        index_to_atom[i] = q.body_atoms[i]
        graph[i] = []

    for i in range(len(q.body_atoms)):
        parent[i] = -1
        index_to_atom[i] = q.body_atoms[i]
        
        for j in range(i+1, len(q.body_atoms)):
            if is_joining_non_free(index_to_atom[i], index_to_atom[j], q.free_variables):
                graph[i].append(j)
                graph[j].append(i)

    
    def dfs(u, index):
        parent[u] = index 
        for v in graph[u]:
            if parent[v] < 0:
                dfs(v, index)

    cnt = 0
    for i in range(len(q.body_atoms)):
        if parent[i] < 0:
            dfs(i, cnt)
        cnt += 1


    components = {}

    for i in parent:
        component_index = parent[i]
        if component_index not in components:
            components[component_index] = []

        components[component_index].append(index_to_atom[i])

        

    qs = []
    for component_index in components:
        component = components[component_index]

        all_vars = []

        for atom in component:
            for var in atom.arguments:
                if var.data_type == Variable:
                    all_vars.append(var.name)

        free_var = []
        for var in q.free_variables:
            if var.name in all_vars:
                free_var.append(var)


        query = ConjunctiveQuery(free_variables = free_var, body_atoms = component.copy())
        qs.append(query)

    return qs



def get_rooted_tree_util(tree, root, index_to_atom, seen, actural_root):

    if len(tree[root]) == 1 and root != actural_root:
        jointree = JoinTree(index_to_atom[root], [])
        return jointree

    children = []

    for u in tree[root]:
        if u not in seen:
            subtree = get_rooted_tree_util(tree, u, index_to_atom, seen + [u], actural_root)
            children.append(subtree)

    jointree = JoinTree(index_to_atom[root], children)
    return jointree



def get_rooted_tree(tree, root, index_to_atom):
    jointree =  get_rooted_tree_util(tree, root, index_to_atom, [root], root)
    return jointree



def get_all_ppjts_from_join_tree(tree, index_to_atom):

    jointrees = []

    for root in tree:
        
        jointree = get_rooted_tree(tree, root, index_to_atom)

        if jointree.is_pair_pruning_join_tree():
            jointrees.append(jointree)

    return jointrees


def get_all_ppjts(q):
    
    n = len(q.body_atoms)

    index_to_atom = {}

    for i in range(1, n+1):
        index_to_atom[i] = q.body_atoms[i-1]
    
    all_trees = enumerate_all_sequences(n, convert_sequence_to_tree)

    jointrees = []
    for tree in all_trees:
        if is_join_tree(tree, index_to_atom):
            jointrees += get_all_ppjts_from_join_tree(tree, index_to_atom)
    
    return jointrees



def get_ppjt(q, all_trees = False):

    jointrees = get_all_ppjts(q)
    if all_trees:
        return jointrees
    if len(jointrees) > 0:
        ppjt = jointrees[0]
        ppjt.set_parent_joining_variables()
        ppjt.add_free_variables(q.free_variables)
        return ppjt
    else:
        return None




def get_ppjt_forest(q):

    qs = decompose_query(q)

    ret = []
    for qc in qs:
        ppjt = get_ppjt(qc)
        ret.append(ppjt)

    return ret




def test():



    x = Variable("x")
    y = Variable("y")
    z = Variable("z")

    q = ConjunctiveQuery([x, y, z],
                         [y],
                         [[PrimaryKey(x), x], [PrimaryKey(z), y], [PrimaryKey(y), x]],
                         ['r', 's', 't'])

    
    ppjf = get_ppjt_forest(q)

    for ppjt in ppjf:
        print(ppjt)
    
    # print(jt)
    # print(jt.is_pair_pruning_join_tree())
    
    
if __name__ == "__main__":
    test()


