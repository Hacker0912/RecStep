def is_subset(s1, s2):
    for x in s1:
        if x not in s2:
            return False
    return True

def intersect(s1, s2):

    ret = []

    for x in s1:
        if x in s2:
            ret.append(x)

    return ret

def union(s1, s2):
    ret = []
    for x in s1:
        ret.append(x)

    for x in s2:
        if x not in ret:
            ret.append(x)

    return ret



