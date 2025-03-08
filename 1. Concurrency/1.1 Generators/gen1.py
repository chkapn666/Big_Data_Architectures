def gen_squares(n):
    for i in range(n):
        yield i * i

g = gen_squares(5)

print(next(g))
print(next(g))
print(next(g))
print(next(g))
print(next(g))
print(next(g))
