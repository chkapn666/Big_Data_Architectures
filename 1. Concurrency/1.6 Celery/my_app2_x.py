from my_app2 import store 

res = store.delay([('John', 42), ('Helen', 37), ('Maria', 25),('George', 47)])

while not res.ready():
    pass

print(f'Result: {res.result}')