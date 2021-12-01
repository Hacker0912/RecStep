
def enumerate_all_sequences_util(n, curr, check_function):
	if n == 1:
		ret = check_function(None)
		return [ret]
		
	if len(curr) == n-2:
		ret = check_function(curr)
		return [ret]

	ret = []
	for i in range(1, n+1):
		curr.append(i)
		ret += enumerate_all_sequences_util(n, curr, check_function)
		curr.pop()
	return ret



def enumerate_all_sequences(n, check_function):

	return enumerate_all_sequences_util(n, [], check_function)



def convert_sequence_to_tree(a):
	if a == None:
		T = {}
		T[1] = []
		return T

	n = len(a)
	T = {}

	for i in range(1, n+3):
		T[i] = []

	degree = {}

	for i in T:
		degree[i] = 1
	for i in a:
		degree[i] += 1

	for i in a:
		for j in T:
			if degree[j] == 1:
				T[i].append(j)
				T[j].append(i)

				degree[i] -= 1
				degree[j] -= 1
				break

	u = v = 0
	for i in T:
		if degree[i] == 1:
			if u == 0:
				u = i 
			else:
				v = i 
				break

	T[u].append(v)
	T[v].append(u)

	degree[u] -= 1
	degree[v] -= 1

	return T

# ret = enumerate_all_sequences(5, convert_sequence_to_tree)

# for t in ret:
# 	print(t)


