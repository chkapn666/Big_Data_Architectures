def square(x):
   return x * x

def squarel(l):
    return [square(x) for x in l]

def cube(x):
    return x * x * x

def cubel(l):
    return [cube(x) for x in l]

def map(f, l):
    return [f(x) for x in l]


map(square, [1, 2, 3, 4, 5])
map(lambda x: x * x, [1, 2, 3, 4, 5])

map(cube, [1, 2, 3, 4, 5])
map(lambda x: x * x * x, [1, 2, 3, 4, 5])


def sum(l):
    s = 0;
    for i in range(len(l)):
        s += l[i]
    return s;


def sumr(l):
    def sumr1(v, l):
        if not l:
            return v
        return sumr1(v + l[0], l[1:])
    return sumr1(0, l)


def prod(l):
    p = 1;
    for i in range(len(l)):
        p *= l[i]
    return p


def prodr(l):
    def prodr1(v, l):
        if not l:
            return v
        return prodr1(v * l[0], l[1:])
    return prodr1(1, l)


def reduce(f, l):
    if len(l) < 1:
        raise Exception('List too short')
    def reduce1(f, v, l):
        if not l:
            return v
        return reduce1(f, f(v, l[0]), l[1:])
    return reduce1(f, l[0], l[1:])


reduce(lambda x, y: x + y, [1, 2, 3, 4])
reduce(lambda x, y: x * y, [1, 2, 3, 4])
