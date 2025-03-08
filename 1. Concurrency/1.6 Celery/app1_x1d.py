import app1

res = app1.ts.delay('ACG ITC6107')

while not res.ready():
    pass

print(f'Result: {res.result}')
