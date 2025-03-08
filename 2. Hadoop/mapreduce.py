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
            return 0
        return prodr1(v + l[0], l[1:])
    return prodr1(0, l)

def reduce(f, l):
    if len(l) < 1:
        raise Exception('List too short')
    def reduce1(f, v, l):
        if not l:
            return v
        return reduce1(f, f(v, l[0]), l[1:])
    return reduce1(f, l[0], l[1:])

reduce(lambda x, y: x + y, [1, 2, 3, 4])


s = 'abracadabra'
c = 'd'
found = False
i = 0
while i < len(s):
    if s[i] == c:
        found = True
        break
    i += 1
if found:
    print(f'Found it at {i}')

s = 'abracadabra'
c = 'z'
found = False
i = 0
while i < len(s):
    if s[i] == c:
        found = True
        break
    i += 1
else:
    print('Not found it')

s = 'abracadabra'
found = False
i = -1
while True:
    i += 1
    if s[i] in {'a', 'b'}:
        continue
    found = True
    break
if found:
    print(f'Found it at {i}')

def div(x, y):
    if y == 0:
        raise Exception('Divisor is zero.')
    return x / y

x = 10
y = 0

try:
    z = div(x, y)
    print(f'Result is {z}')
except Exception as ex:
    print(ex)
else:
    print('Successful completion')
finally:
    print('Clean up')


def f(x, y, z=5):
    return x+y+z

def g(x, y, z=3):
    print(x, y, z)

def f(*args):
    print(args) # args is tuple of values

def g(**args):
    print(args) # args is a dict of values

def f(a, *pargs, **kargs):
    print(a, pargs, kargs)


def g(a, b, c, d):
    print(a, b, c, d)


def makeActions():
    acts = []
    for i in range(5):
        acts.append(lambda x, i=i: i ** x)
    return acts

acts = makeActions()
acts[0](2)
acts[1](2)
acts[2](2)
acts[3](2)
acts[4](2)
